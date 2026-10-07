// Package proxy 实现反向代理转发、限流、超时与重试。
package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/gateway"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/upstream"
)

// 可区分的失败原因，写于响应头 X-Gateway-Error，便于客户端与测试区分。
const (
	ErrRateLimited   = "rate_limited"     // 被限流拒绝
	ErrNoRoute       = "no_route"         // 无匹配路由
	ErrNoHealthy     = "no_healthy"       // 集群无健康后端
	ErrUpstreamFail  = "upstream_fail"    // 上游连接/传输失败
	ErrUpstreamTimed = "upstream_timeout" // 上游超时
	ErrUpstream5xx   = "upstream_5xx"     // 上游返回 5xx（计入健康判断；最后一次尝试会透传响应）
)

const stickyCookie = "GWSESSION"

// Handler 是网关的 HTTP 入口处理器。
type Handler struct {
	gw     *gateway.Gateway
	client *http.Client
}

// NewHandler 创建处理器。
func NewHandler(gw *gateway.Gateway) *Handler {
	return &Handler{
		gw: gw,
		client: &http.Client{
			// 不在 Transport 层设全局超时，超时由每次请求的 context 控制，
			// 这样客户端取消与单次上游超时都能正确中断上游请求。
			Transport: &http.Transport{
				MaxIdleConns:        256,
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	snap := h.gw.Snapshot() // 整个请求只使用这一份快照，热更新不会造成半新半旧

	// 1. 限流：按客户端 IP。
	if snap.Limiter != nil {
		key := clientIP(r)
		if !snap.Limiter.Allow(key) {
			log.Printf("[proxy] 限流拒绝 client=%s path=%s", key, r.URL.Path)
			writeErr(w, http.StatusTooManyRequests, ErrRateLimited)
			return
		}
	}

	// 2. 路由匹配。
	route, ok := snap.Router.Match(r)
	if !ok {
		writeErr(w, http.StatusNotFound, ErrNoRoute)
		return
	}
	pool := snap.Pools[route.Upstream]
	if pool == nil {
		writeErr(w, http.StatusNotFound, ErrNoRoute)
		return
	}

	// 3. 选择后端（粘性会话优先）。
	var sessionKey string
	if route.Sticky {
		sessionKey = sessionID(w, r)
	}

	attempts := 1
	if isIdempotent(r.Method) {
		attempts += snap.Cfg.Proxy.MaxRetries
	}

	// 可能重试时，先把请求体完整读入内存：重试的每一次都必须发送
	// 与第一次完全一致的内容（原始 Body 读完即耗尽，无法重发）。
	var body []byte
	if attempts > 1 && r.Body != nil {
		b, err := io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			log.Printf("[proxy] 读取请求体失败，无法重放 client=%s path=%s: %v", clientIP(r), r.URL.Path, err)
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}
		if len(b) > 0 {
			body = b
		}
	}

	var lastErr string
	for i := 0; i < attempts; i++ {
		srv, done, ok := pickBackend(pool, route, sessionKey)
		if !ok {
			writeErr(w, http.StatusServiceUnavailable, ErrNoHealthy)
			return
		}
		failReason, wrote := h.forward(w, r, snap.Cfg.Proxy, srv.Addr, body, i == attempts-1)
		if done != nil {
			done(failReason == "")
		}
		// 真实请求结果参与健康判断：连接/传输失败、超时、5xx 都计为失败。
		pool.ReportResult(srv.Addr, failReason == "")
		if wrote {
			return // 成功，或最后一次尝试的上游 5xx 已透传给客户端
		}
		lastErr = failReason
		// 客户端已取消：不要再重试，也不必写响应。
		if r.Context().Err() != nil {
			log.Printf("[proxy] 客户端取消，终止重试 client=%s path=%s", clientIP(r), r.URL.Path)
			return
		}
		log.Printf("[proxy] 上游失败 addr=%s reason=%s 第%d次尝试，准备重试", srv.Addr, failReason, i+1)
	}

	status := http.StatusBadGateway
	if lastErr == ErrUpstreamTimed {
		status = http.StatusGatewayTimeout
	}
	writeErr(w, status, lastErr)
}

// pickBackend 选择后端：粘性会话走 rendezvous 哈希，否则走负载均衡器。
func pickBackend(pool *upstream.Pool, route config.Route, sessionKey string) (config.Server, func(bool), bool) {
	if route.Sticky && sessionKey != "" {
		s, ok := pool.PickSticky(sessionKey)
		return s, nil, ok
	}
	picker := pool.Picker()
	if picker == nil {
		return config.Server{}, nil, false
	}
	return picker.Pick()
}

// forward 执行一次上游转发。
// body 非空时用它作为请求体（每次尝试都是全新 reader，保证内容一致）；
// pass5xx 为 true（最后一次尝试）时上游 5xx 响应会被透传给客户端。
// 返回 failReason（"" 表示成功；5xx 即使透传也返回 ErrUpstream5xx 以计入健康判断）
// 与 wrote（响应是否已写给客户端，写后调用方不得再重试或改写响应）。
func (h *Handler) forward(w http.ResponseWriter, r *http.Request, pc config.ProxyConf, addr string, body []byte, pass5xx bool) (failReason string, wrote bool) {
	ctx, cancel := context.WithTimeout(r.Context(), pc.UpstreamTimeout)
	defer cancel()

	req := r.Clone(ctx)
	req.RequestURI = ""
	req.URL.Scheme = "http"
	req.URL.Host = addr
	req.Host = addr
	if body != nil {
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
	}

	resp, err := h.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			return ErrUpstreamTimed, false
		}
		return ErrUpstreamFail, false
	}
	defer resp.Body.Close()

	// 上游 5xx 视为失败：还可重试时丢弃响应并重试（幂等请求）；
	// 最后一次尝试则原样透传，让客户端看到真实上游结果。
	if resp.StatusCode >= 500 && !pass5xx {
		_, _ = io.Copy(io.Discard, resp.Body) // 读空以便连接复用
		return ErrUpstream5xx, false
	}

	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
	if resp.StatusCode >= 500 {
		return ErrUpstream5xx, true
	}
	return "", true
}

// isIdempotent 判断方法是否幂等；只有幂等请求才允许重试，
// 避免重试造成上游被重复调用产生重复副作用。
func isIdempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete:
		return true
	}
	return false
}

// sessionID 读取或签发粘性会话 ID（通过 Cookie）。
func sessionID(w http.ResponseWriter, r *http.Request) string {
	if c, err := r.Cookie(stickyCookie); err == nil && c.Value != "" {
		return c.Value
	}
	var b [16]byte
	_, _ = rand.Read(b[:])
	id := hex.EncodeToString(b[:])
	http.SetCookie(w, &http.Cookie{Name: stickyCookie, Value: id, Path: "/"})
	return id
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func writeErr(w http.ResponseWriter, status int, reason string) {
	w.Header().Set("X-Gateway-Error", reason)
	http.Error(w, reason, status)
}

func copyHeader(dst, src http.Header) {
	for k, vs := range src {
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}
