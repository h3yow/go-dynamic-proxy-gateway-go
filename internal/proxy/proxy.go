// Package proxy 实现反向代理转发、限流、超时与重试。
package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
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

// maxBufferedBody 限制重试时在内存中缓冲的请求/响应体大小，避免异常大体耗尽内存。
const maxBufferedBody = 16 << 20

// attemptResult 是一次上游尝试的结果。
type attemptResult struct {
	errReason string // 传输类失败原因常量；为空表示拿到了上游响应
	status    int
	header    http.Header
	body      []byte
}

// failed 表示这次尝试是否应按失败计入健康判断/重试。
// 只有传输错误、超时与上游 5xx 算失败；4xx 等业务应答算成功（上游是通的）。
func (a attemptResult) failed() bool {
	if a.errReason != "" {
		return true
	}
	return a.status >= 500
}

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

	idempotent := isIdempotent(r.Method)

	// 4. 幂等请求可能重试：先把请求体完整读入内存并在每次尝试时重建，
	// 保证上游每次收到的字节与第一次完全一致。非幂等请求绝不重试，直接透传 body。
	var bodyBytes []byte
	var passThroughBody io.Reader = r.Body
	retryable := idempotent
	if idempotent && r.Body != nil {
		data, err := io.ReadAll(io.LimitReader(r.Body, maxBufferedBody+1))
		if err != nil {
			writeErr(w, http.StatusBadRequest, ErrUpstreamFail)
			return
		}
		if len(data) > maxBufferedBody {
			// 超过缓冲上限的请求体不支持重试：降级为只尝试一次，
			// 用 MultiReader 把已读前缀和未读完的流拼起来，保证不丢字节。
			retryable = false
			passThroughBody = io.MultiReader(bytes.NewReader(data), r.Body)
		} else {
			bodyBytes = data
		}
	}
	attempts := 1
	if retryable {
		attempts += snap.Cfg.Proxy.MaxRetries
	}

	var last attemptResult
	tried := 0
	for i := 0; i < attempts; i++ {
		srv, done, ok := pickBackend(pool, route, sessionKey)
		if !ok {
			writeErr(w, http.StatusServiceUnavailable, ErrNoHealthy)
			return
		}

		var bodyReader io.Reader
		if retryable {
			bodyReader = bytes.NewReader(bodyBytes)
		} else {
			bodyReader = passThroughBody
		}
		res := h.forward(r, snap.Cfg.Proxy, srv.Addr, bodyReader)
		tried++
		if done != nil {
			done(!res.failed())
		}
		pool.ReportResult(srv.Addr, !res.failed())

		if !res.failed() {
			flushResult(w, res)
			if i > 0 {
				log.Printf("[proxy] addr=%s 第%d次尝试成功（共转发 %d 次）", srv.Addr, i+1, tried)
			}
			return
		}
		last = res
		// 客户端已取消：不要再重试，也不必写响应。
		if r.Context().Err() != nil {
			log.Printf("[proxy] 客户端取消，终止重试 client=%s path=%s", clientIP(r), r.URL.Path)
			return
		}
		if i+1 < attempts {
			log.Printf("[proxy] 上游失败 addr=%s reason=%s 第%d次尝试，准备重试", srv.Addr, failureReason(res), i+1)
		} else {
			log.Printf("[proxy] 上游失败 addr=%s reason=%s 第%d次尝试，重试次数用尽", srv.Addr, failureReason(res), i+1)
		}
	}

	// 所有尝试均失败：拿到过上游 5xx 响应则透传该响应，否则按网关错误返回。
	if last.errReason == "" {
		log.Printf("[proxy] 上游连续返回 %d，共转发 %d 次仍失败，透传错误响应", last.status, tried)
		flushResult(w, last)
		return
	}
	status := http.StatusBadGateway
	if last.errReason == ErrUpstreamTimed {
		status = http.StatusGatewayTimeout
	}
	writeErr(w, status, last.errReason)
}

// failureReason 返回用于日志的失败原因（传输错误或上游状态码）。
func failureReason(a attemptResult) string {
	if a.errReason != "" {
		return a.errReason
	}
	return fmt.Sprintf("status_%d", a.status)
}

// flushResult 把缓冲的上游响应写回客户端。
func flushResult(w http.ResponseWriter, a attemptResult) {
	copyHeader(w.Header(), a.header)
	w.WriteHeader(a.status)
	if len(a.body) > 0 {
		_, _ = w.Write(a.body)
	}
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

// forward 执行一次上游转发。返回值中 errReason 为空表示拿到了上游响应
// （调用方再根据状态码决定成败与是否重试）。响应体先缓冲在内存中，
// 这样 5xx 等失败响应不会在重试前已经写回客户端。
func (h *Handler) forward(r *http.Request, pc config.ProxyConf, addr string, body io.Reader) attemptResult {
	ctx, cancel := context.WithTimeout(r.Context(), pc.UpstreamTimeout)
	defer cancel()

	req := r.Clone(ctx)
	req.RequestURI = ""
	req.URL.Scheme = "http"
	req.URL.Host = addr
	req.Host = addr
	if body != nil {
		req.Body = io.NopCloser(body)
		// 已缓冲的 body 长度是已知的，设置 ContentLength 让每次尝试一致。
		if rc, ok := body.(*bytes.Reader); ok {
			req.ContentLength = rc.Size()
		}
	}

	resp, err := h.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			return attemptResult{errReason: ErrUpstreamTimed}
		}
		return attemptResult{errReason: ErrUpstreamFail}
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBufferedBody+1))
	if err != nil {
		return attemptResult{errReason: ErrUpstreamFail}
	}
	if len(data) > maxBufferedBody {
		// 响应体过大：按上游失败处理，避免缓冲无界内存。
		return attemptResult{errReason: ErrUpstreamFail}
	}

	header := make(http.Header, len(resp.Header))
	copyHeader(header, resp.Header)
	return attemptResult{status: resp.StatusCode, header: header, body: data}
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
