package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/balancer"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

// Distinguishable gateway error codes, surfaced as JSON bodies and headers.
const (
	reasonNoRoute      = "no_route"
	reasonRateLimited  = "rate_limited"
	reasonNoUpstream   = "no_upstream_available"
	reasonUpstream     = "upstream_error"
	reasonTimeout      = "upstream_timeout"
	reasonCanceled     = "client_canceled"
	reasonBodyTooLarge = "body_too_large"
)

// clientCanceled is the internal sentinel for "client gave up; stop silently".
const clientCanceled = -2

// maxBufferedBody caps request bodies we buffer to replay across retries.
const maxBufferedBody = 1 << 20 // 1 MiB

// ProxyHandler is the public reverse-proxy entry point.
type ProxyHandler struct {
	mgr *Manager
}

func newProxyHandler(m *Manager) *ProxyHandler { return &ProxyHandler{mgr: m} }

func (h *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Pin the engine generation for the whole request: even if a reload
	// happens mid-request, this request completes against one consistent
	// snapshot and can never see a half-old/half-new config.
	h.serveWithEngine(h.mgr.Active(), w, r)
}

func (h *ProxyHandler) serveWithEngine(e *Engine, w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	match := e.table.Lookup(r.Host, r.URL.Path)
	if match == nil {
		log.Printf("[gateway gen=%d] route host=%q path=%q => REJECT no_route", e.gen, r.Host, r.URL.Path)
		writeGatewayError(w, http.StatusNotFound, reasonNoRoute, "no route matched host/path")
		return
	}

	// ---- rate limit (per client) ----
	if match.RateLimit != nil {
		client := clientKey(*match.RateLimit, r)
		d := e.limiter.Allow(client, r.Host, r.URL.Path, now.UnixMilli())
		if !d.Allowed {
			log.Printf("[gateway gen=%d] ratelimit host=%q path=%q client=%q => REJECT rate_limited retry_after_ms=%d (rate=%.2f burst=%d)",
				e.gen, r.Host, r.URL.Path, client, d.RetryAfterMS, d.Rate, d.Burst)
			w.Header().Set("Retry-After-Ms", fmt.Sprintf("%d", d.RetryAfterMS))
			writeGatewayError(w, http.StatusTooManyRequests, reasonRateLimited,
				fmt.Sprintf("client %q exceeded %.2f req/s burst %d", client, d.Rate, d.Burst))
			return
		}
	}

	pool, ok := e.pools[match.Route.Pool]
	if !ok {
		writeGatewayError(w, http.StatusBadGateway, reasonNoUpstream, "pool missing")
		return
	}

	bodyBytes, err := bufferBody(r)
	if err != nil {
		log.Printf("[gateway gen=%d] body read host=%q => REJECT body_too_large", e.gen, r.Host)
		writeGatewayError(w, http.StatusRequestEntityTooLarge, reasonBodyTooLarge, err.Error())
		return
	}

	// ---- sticky session resolution ----
	stickyName := match.Route.StickyCookie
	hasCookie := false
	boundPeer := "" // peer the client is currently pinned to ("" = pick freely)
	if stickyName != "" {
		if _, cerr := r.Cookie(stickyName); cerr == nil {
			hasCookie = true
		}
		boundPeer = e.sessions.CookiePeer(stickyName, r, now.UnixMilli())
		if boundPeer != "" {
			log.Printf("[gateway gen=%d] sticky route=%q bound_peer=%q", e.gen, match.Route.Name, boundPeer)
		}
	}

	// ---- attempts; only idempotent methods are retried ----
	poolDef := pool.Def()
	canRetry := poolDef.RetryMethods[strings.ToUpper(r.Method)]
	maxAttempts := 1
	if canRetry {
		maxAttempts = poolDef.MaxAttempts
	}

	var lastCode int
	var lastReason string
	triedPeers := []string{}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if r.Context().Err() != nil {
			log.Printf("[gateway gen=%d] client canceled before attempt=%d route=%q", e.gen, attempt, match.Route.Name)
			return
		}

		lease, pickErr := pool.Pick(balancer.PickRequest{StickyPeer: boundPeer, Now: time.Now()})
		migrated := false
		if errors.Is(pickErr, balancer.ErrStickyPeerDown) {
			// Bound peer ejected/removed: deterministic one-time migration.
			log.Printf("[gateway gen=%d] sticky peer=%q DOWN (route=%q, had_cookie=%v) => migrating",
				e.gen, boundPeer, match.Route.Name, hasCookie)
			lease, pickErr = pool.Pick(balancer.PickRequest{Now: time.Now()})
			if pickErr == nil {
				migrated = true
			}
		}
		if pickErr != nil {
			lastCode, lastReason = http.StatusServiceUnavailable, reasonNoUpstream
			log.Printf("[gateway gen=%d] pick route=%q => REJECT no_upstream_available attempt=%d: %v",
				e.gen, match.Route.Name, attempt, pickErr)
			break
		}

		peer := lease.Peer
		triedPeers = append(triedPeers, peer.Def.Name)

		// Sticky cookie bookkeeping. Nothing has been flushed yet (failed
		// attempts never call WriteHeader), so we clear any Set-Cookie a
		// failed prior attempt staged and (re)issue the binding that matches
		// THIS attempt. The client therefore only ever sees the cookie for
		// the peer whose response it actually receives, and an all-attempts-
		// failed request issues no misleading cookie.
		stickyAction := ""
		if stickyName != "" {
			w.Header().Del("Set-Cookie")
			switch {
			case migrated:
				newPeer := e.sessions.MigrateTo(stickyName, peer.Def.Name,
					match.Route.StickyTTLSeconds, w, r, now.UnixMilli())
				stickyAction = "migrated-to-" + newPeer
			case boundPeer == "" && attempt == 1 && !hasCookie:
				// Brand-new session: pin to the first serving peer.
				got, issued := e.sessions.Bind(stickyName, peer.Def.Name,
					match.Route.StickyTTLSeconds, w, r, now.UnixMilli())
				if issued {
					stickyAction = "bound-to-" + got
				}
			case boundPeer == "" && attempt > 1:
				// Retry after the pinned peer failed: explicitly re-bind to
				// whichever peer succeeds below.
				newPeer := e.sessions.MigrateTo(stickyName, peer.Def.Name,
					match.Route.StickyTTLSeconds, w, r, now.UnixMilli())
				stickyAction = "rebound-after-failure-to-" + newPeer
			}
		}

		status, isRetryable, reason := e.doAttempt(poolDef, peer.Def.BaseURL, r, bodyBytes, w, attempt)
		lastCode, lastReason = status, reason

		if status == clientCanceled {
			lease.Done(errors.New(reasonCanceled))
			log.Printf("[gateway gen=%d] client canceled route=%q peer=%q; upstream request aborted",
				e.gen, match.Route.Name, peer.Def.Name)
			return // no response: client is gone
		}

		if status == 0 {
			lease.Done(nil)
			log.Printf("[gateway gen=%d] success route=%q peer=%q attempt=%d/%d sticky=%q",
				e.gen, match.Route.Name, peer.Def.Name, attempt, maxAttempts, stickyAction)
			return
		}

		lease.Done(errors.New(reason))
		log.Printf("[gateway gen=%d] failure route=%q peer=%q attempt=%d/%d status=%d reason=%s retryable=%v",
			e.gen, match.Route.Name, peer.Def.Name, attempt, maxAttempts, status, reason, isRetryable)

		if stickyName != "" && boundPeer == peer.Def.Name {
			// The pinned peer itself failed; a retry must go elsewhere and
			// re-bind explicitly rather than silently splitting the session.
			boundPeer = ""
		}
		if migrated {
			boundPeer = peer.Def.Name
		}

		if !canRetry || !isRetryable || attempt == maxAttempts {
			break
		}
	}

	if lastCode == 0 || lastCode == clientCanceled {
		lastCode, lastReason = http.StatusServiceUnavailable, reasonNoUpstream
	}
	log.Printf("[gateway gen=%d] giving up route=%q peers=%v code=%d reason=%s",
		e.gen, match.Route.Name, triedPeers, lastCode, lastReason)
	writeGatewayError(w, lastCode, lastReason,
		fmt.Sprintf("upstream attempts %v exhausted", triedPeers))
}

// doAttempt performs one upstream call. On success it streams the response to
// w and returns status 0. Retryable failures return a status > 0 without
// having written anything. clientCanceled means the client aborted.
func (e *Engine) doAttempt(poolDef *config.Pool, baseURL string, orig *http.Request, body []byte, w http.ResponseWriter, attempt int) (status int, retryable bool, reason string) {
	attemptTimeout := time.Duration(poolDef.RequestTimeoutMS) * time.Millisecond
	ctx, cancel := context.WithTimeout(orig.Context(), attemptTimeout)
	defer cancel()

	targetURL := strings.TrimRight(baseURL, "/") + orig.URL.Path
	if orig.URL.RawQuery != "" {
		targetURL += "?" + orig.URL.RawQuery
	}
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, orig.Method, targetURL, bodyReader)
	if err != nil {
		return http.StatusBadGateway, false, reasonUpstream
	}
	copyHeaders(orig.Header, req.Header)
	req.Header.Set("X-Gateway-Attempt", fmt.Sprintf("%d", attempt))

	resp, err := e.client.Do(req)
	if err != nil {
		switch {
		case orig.Context().Err() != nil:
			// Client gone (or parent canceled): the upstream request shares
			// that context, so it is aborted simultaneously. No duplication,
			// no response to send.
			return clientCanceled, false, reasonCanceled
		case errors.Is(err, context.DeadlineExceeded) || isTimeout(err):
			return http.StatusGatewayTimeout, true, reasonTimeout
		default:
			return http.StatusBadGateway, true, reasonUpstream
		}
	}
	defer resp.Body.Close()

	// Bare 5xx with no bytes produced: safe to retry (the request either
	// never ran or the upstream reports failure without committing output).
	if resp.StatusCode >= 500 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, true, reasonUpstream
	}

	// Commit: once headers flush no further retry is possible.
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("X-Gateway-Attempt", fmt.Sprintf("%d", attempt))
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("[gateway gen=%d] streaming interrupted: %v", e.gen, err)
	}
	return 0, false, ""
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func bufferBody(r *http.Request) ([]byte, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, nil
	}
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, err := r.Body.Read(tmp)
		if n > 0 {
			if len(buf)+n > maxBufferedBody {
				return nil, fmt.Errorf("request body exceeds %d bytes and cannot be buffered for retry", maxBufferedBody)
			}
			buf = append(buf, tmp[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func clientKey(rule config.RateLimit, r *http.Request) string {
	if rule.ClientHeader != "" {
		if v := r.Header.Get(rule.ClientHeader); v != "" {
			return v
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func copyHeaders(src, dst http.Header) {
	for k, vs := range src {
		if _, hop := hopByHop[k]; hop {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

var hopByHop = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true,
	"Proxy-Authorization": true, "Te": true, "Trailer": true,
	"Transfer-Encoding": true, "Upgrade": true,
}

func writeGatewayError(w http.ResponseWriter, status int, reason, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Gateway-Reason", reason)
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error":%q,"reason":%q,"status":%d}`+"\n", msg, reason, status)
}
