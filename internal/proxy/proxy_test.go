package proxy_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/gateway"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/proxy"
)

func newGateway(t *testing.T, mutate func(*config.Config)) (*gateway.Gateway, string) {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "OK")
	}))
	t.Cleanup(backend.Close)
	cfg := &config.Config{
		ListenAddr: "127.0.0.1:0",
		Routes:     []config.Route{{PathPrefix: "/", Upstream: "svc"}},
		Upstreams: []config.Upstream{{
			Name:     "svc",
			Policy:   "wrr",
			Backends: []config.Server{{Addr: strings.TrimPrefix(backend.URL, "http://"), Weight: 1}},
			HealthCheck: config.HealthCheckConf{
				Interval: time.Hour, Timeout: time.Second,
				FailThreshold: 1, RecoverThreshold: 1,
			},
		}},
		Proxy: config.ProxyConf{UpstreamTimeout: 100 * time.Millisecond, MaxRetries: 2},
	}
	if mutate != nil {
		mutate(cfg)
	}
	gw, err := gateway.New(cfg)
	if err != nil {
		t.Fatalf("构建网关失败: %v", err)
	}
	return gw, strings.TrimPrefix(backend.URL, "http://")
}

// TestUpstreamTimeout 上游超时返回 504 且原因可区分。
func TestUpstreamTimeout(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
	}))
	defer slow.Close()
	gw, _ := newGateway(t, func(c *config.Config) {
		c.Upstreams[0].Backends[0].Addr = strings.TrimPrefix(slow.URL, "http://")
		c.Upstreams[0].HealthCheck.FailThreshold = 100 // 避免超时上报触发摘除，干扰重试链路
		c.Proxy.UpstreamTimeout = 50 * time.Millisecond
	})
	srv := httptest.NewServer(proxy.NewHandler(gw))
	defer srv.Close()

	start := time.Now()
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	resp.Body.Close()
	elapsed := time.Since(start)
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("超时应返回 504，实际 %d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Gateway-Error"); got != proxy.ErrUpstreamTimed {
		t.Fatalf("X-Gateway-Error 应为 upstream_timeout，实际 %q", got)
	}
	t.Logf("上游超时：状态=504，X-Gateway-Error=upstream_timeout，耗时=%v（远小于上游 500ms）", elapsed)
}

// TestClientCancelPropagates 客户端取消后，上游请求被中断。
func TestClientCancelPropagates(t *testing.T) {
	cancelSeen := make(chan struct{}, 1)
	blocking := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			cancelSeen <- struct{}{}
		case <-time.After(5 * time.Second):
		}
	}))
	defer blocking.Close()
	gw, _ := newGateway(t, func(c *config.Config) {
		c.Upstreams[0].Backends[0].Addr = strings.TrimPrefix(blocking.URL, "http://")
		c.Proxy.UpstreamTimeout = 5 * time.Second
		c.Proxy.MaxRetries = 3 // 即使允许重试，取消后也不应重试
	})
	srv := httptest.NewServer(proxy.NewHandler(gw))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/", nil)
	done := make(chan struct{})
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("客户端取消后处理器未及时返回")
	}
	select {
	case <-cancelSeen:
		t.Log("判定依据：上游处理器的 request context 已 Done，取消链路贯通")
	case <-time.After(2 * time.Second):
		t.Fatal("客户端取消未传播到上游请求")
	}
}

// countingCloseListener 统计建连次数并立即断开，模拟上游传输失败。
type countingCloseListener struct {
	count atomic.Int64
	ln    net.Listener
}

func newCountingCloseListener(t *testing.T) *countingCloseListener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c := &countingCloseListener{ln: ln}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			c.count.Add(1)
			conn.Close()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return c
}

// TestRetryOnlyIdempotent GET 失败会重试，POST 失败不重试（避免重复副作用）。
func TestRetryOnlyIdempotent(t *testing.T) {
	flaky := newCountingCloseListener(t)
	gw, _ := newGateway(t, func(c *config.Config) {
		c.Upstreams[0].Backends[0].Addr = flaky.ln.Addr().String()
		c.Upstreams[0].HealthCheck.FailThreshold = 100 // 避免失败上报触发摘除，干扰重试计数
		c.Proxy.MaxRetries = 2
	})
	srv := httptest.NewServer(proxy.NewHandler(gw))
	defer srv.Close()

	flaky.count.Store(0)
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET 请求失败: %v", err)
	}
	resp.Body.Close()
	getCalls := flaky.count.Load()
	if getCalls != 3 { // 1 + 2 次重试
		t.Fatalf("幂等 GET 应尝试 3 次，实际 %d 次", getCalls)
	}
	t.Logf("幂等 GET：上游被调用 %d 次 = 1 + max_retries(2)", getCalls)

	flaky.count.Store(0)
	resp, err = http.Post(srv.URL+"/", "text/plain", strings.NewReader("data"))
	if err != nil {
		t.Fatalf("POST 请求失败: %v", err)
	}
	resp.Body.Close()
	postCalls := flaky.count.Load()
	if postCalls != 1 {
		t.Fatalf("非幂等 POST 不应重试，上游应只被调用 1 次，实际 %d 次", postCalls)
	}
	t.Logf("非幂等 POST：上游只被调用 %d 次，重试不会造成重复副作用", postCalls)
}

// TestRateLimitRejected 超限请求返回 429 且原因可区分。
func TestRateLimitRejected(t *testing.T) {
	gw, _ := newGateway(t, func(c *config.Config) {
		c.RateLimit = config.RateLimitConf{Enabled: true, Rate: 0.0001, Burst: 1}
	})
	srv := httptest.NewServer(proxy.NewHandler(gw))
	defer srv.Close()

	resp1, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("首个请求失败: %v", err)
	}
	body1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	if string(body1) != "OK" {
		t.Fatalf("首个请求应放行，实际 %q", body1)
	}

	resp2, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("第二个请求失败: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("超限应返回 429，实际 %d", resp2.StatusCode)
	}
	if got := resp2.Header.Get("X-Gateway-Error"); got != proxy.ErrRateLimited {
		t.Fatalf("X-Gateway-Error 应为 rate_limited，实际 %q", got)
	}
	t.Log("限流边界：burst=1，第 2 个请求返回 429 + X-Gateway-Error=rate_limited")
}

// TestNoHealthyBackend 后端全部摘除后返回 503 且原因可区分。
func TestNoHealthyBackend(t *testing.T) {
	gw, addr := newGateway(t, nil)
	srv := httptest.NewServer(proxy.NewHandler(gw))
	defer srv.Close()

	gw.Snapshot().Pools["svc"].ReportResult(addr, false) // failThreshold=1，立即摘除

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("无健康后端应返回 503，实际 %d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Gateway-Error"); got != proxy.ErrNoHealthy {
		t.Fatalf("X-Gateway-Error 应为 no_healthy，实际 %q", got)
	}
	t.Log("后端摘除后返回 503 + X-Gateway-Error=no_healthy")
}

// TestNoRoute 无匹配路由返回 404。
func TestNoRoute(t *testing.T) {
	gw, _ := newGateway(t, func(c *config.Config) {
		c.Routes = []config.Route{{Host: "api.example.com", PathPrefix: "/v1/", Upstream: "svc"}}
	})
	srv := httptest.NewServer(proxy.NewHandler(gw))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/other")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("无路由应返回 404，实际 %d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Gateway-Error"); got != proxy.ErrNoRoute {
		t.Fatalf("X-Gateway-Error 应为 no_route，实际 %q", got)
	}
	t.Log("Host/前缀不匹配时返回 404 + X-Gateway-Error=no_route")
}
