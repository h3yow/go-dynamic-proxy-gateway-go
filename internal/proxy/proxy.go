// Package proxy 实现反向代理转发、限流、超时与重试。
package proxy

import (
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

	var lastErr string
	for i := 0; i < attempts; i++ {
		srv, done, ok := pickBackend(pool, route, sessionKey)
		if !ok {
			writeErr(w, http.StatusServiceUnavailable, ErrNoHealthy)
			return
		}
		errReason := h.forward(w, r, snap.Cfg.Proxy, srv.Addr)
		if done != nil {
			done(errReason == "")
		}
		pool.ReportResult(srv.Addr, errReason == "")
		if errReason == "" {
			return // 成功
		}
		lastErr = errReason
		// 客户端已取消：不要再重试，也不必写响应。
		if r.Context().Err() != nil {
			log.Printf("[proxy] 客户端取消，终止重试 client=%s path=%s", clientIP(r), r.URL.Path)
			return
		}
		log.Printf("[proxy] 上游失败 addr=%s reason=%s 第%d次尝试，准备重试", srv.Addr, errReason, i+1)
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

// forward 执行一次上游转发；返回 "" 表示成功，否则返回失败原因常量。
func (h *Handler) forward(w http.ResponseWriter, r *http.Request, pc config.ProxyConf, addr string) string {
	ctx, cancel := context.WithTimeout(r.Context(), pc.UpstreamTimeout)
	defer cancel()

	req := r.Clone(ctx)
	req.RequestURI = ""
	req.URL.Scheme = "http"
	req.URL.Host = addr
	req.Host = addr

	resp, err := h.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			return ErrUpstreamTimed
		}
		return ErrUpstreamFail
	}
	defer resp.Body.Close()

	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
	return ""
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
