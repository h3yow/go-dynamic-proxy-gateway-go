package proxy_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strings"
	"sync"
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

// TestPassive5xxEjectAndRecoverViaRealTraffic 真实请求链路观察摘除与恢复：
// 上游业务返回 500 -> 真实请求计入失败 -> 达到阈值后摘除（客户端看到 503）；
// 业务恢复 200 -> 主动 HTTP 探测连续成功 -> 自动恢复（客户端重新看到 200）。
func TestPassive5xxEjectAndRecoverViaRealTraffic(t *testing.T) {
	var bizOK atomic.Bool
	bizOK.Store(false) // 初始即业务故障：端口在监听，但业务请求与探测都返回 500
	var bizHits atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" { // 主动探测路径
			if bizOK.Load() {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
		bizHits.Add(1)
		if bizOK.Load() {
			fmt.Fprint(w, "OK")
		} else {
			w.WriteHeader(http.StatusInternalServerError) // 业务故障
		}
	}))
	defer up.Close()

	gw, _ := newGateway(t, func(c *config.Config) {
		c.Upstreams[0].Backends[0].Addr = strings.TrimPrefix(up.URL, "http://")
		c.Upstreams[0].HealthCheck = config.HealthCheckConf{
			Interval: 10 * time.Millisecond, Timeout: time.Second,
			FailThreshold: 3, RecoverThreshold: 2,
			Path: "/healthz",
		}
		c.Proxy.MaxRetries = 2
	})
	srv := httptest.NewServer(proxy.NewHandler(gw))
	defer srv.Close()

	// 1. 业务 500：一个幂等 GET 触发 1+2 次尝试，全部 500；最后一次透传给客户端。
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
		resp.Body.Close()
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("最后一次尝试应透传上游 500，实际 %d", resp.StatusCode)
	}
	hits := bizHits.Load()
	if hits != 3 {
		t.Fatalf("幂等 GET 应尝试 3 次（1+2 重试），实际 %d 次", hits)
	}
	t.Logf("业务 500：客户端收到透传的 500（body=%q），上游被真实请求打了 %d 次 = 1 + max_retries(2)，3 次失败均已计入健康判断", body, hits)

	// 2. 连续失败达到 fail_threshold=3，后端已被摘除：客户端直接看到 503，不再打上游。
	before := bizHits.Load()
	resp2, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("摘除后应返回 503，实际 %d", resp2.StatusCode)
	}
	if got := resp2.Header.Get("X-Gateway-Error"); got != proxy.ErrNoHealthy {
		t.Fatalf("X-Gateway-Error 应为 no_healthy，实际 %q", got)
	}
	if got := bizHits.Load(); got != before {
		t.Fatalf("摘除后不应再有流量打到故障上游，实际新增 %d 次", got-before)
	}
	t.Logf("摘除生效：客户端看到 503 + no_healthy，故障上游请求数保持 %d 不再增长（端口仍在监听）", before)

	// 3. 业务恢复 200：主动探测连续成功 recover_threshold=2 次后自动加回。
	bizOK.Store(true)
	deadline := time.Now().Add(3 * time.Second)
	var finalStatus int
	for time.Now().Before(deadline) {
		resp3, err := http.Get(srv.URL + "/")
		if err != nil {
			t.Fatalf("请求失败: %v", err)
		}
		finalStatus = resp3.StatusCode
		resp3.Body.Close()
		if finalStatus == http.StatusOK {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if finalStatus != http.StatusOK {
		t.Fatalf("业务恢复后应重新看到 200，实际 %d", finalStatus)
	}
	t.Logf("恢复生效：/healthz 连续成功达到 recover_threshold=2，后端自动加回，客户端重新看到 200（上游累计被真实请求调用 %d 次）", bizHits.Load())
}

// TestRetryBodyReplay 带请求体的幂等请求（PUT）重试时，
// 上游每次收到的请求体必须与第一次完全一致。
func TestRetryBodyReplay(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	var calls atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		mu.Unlock()
		n := calls.Add(1)
		if n < 3 { // 前两次返回 500，第三次才成功
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, "OK")
	}))
	defer up.Close()

	gw, _ := newGateway(t, func(c *config.Config) {
		c.Upstreams[0].Backends[0].Addr = strings.TrimPrefix(up.URL, "http://")
		c.Upstreams[0].HealthCheck.FailThreshold = 100 // 避免 5xx 上报触发摘除，干扰重试计数
		c.Proxy.MaxRetries = 2
	})
	srv := httptest.NewServer(proxy.NewHandler(gw))
	defer srv.Close()

	payload := "payload-" + strings.Repeat("x", 1024) // 非平凡请求体
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/resource/1", strings.NewReader(payload))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT 请求失败: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "OK" {
		t.Fatalf("第 3 次尝试应成功，实际 status=%d body=%q", resp.StatusCode, body)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("应尝试 3 次（1+2 重试），实际 %d 次", got)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, b := range bodies {
		if b != payload {
			t.Fatalf("第 %d 次尝试的请求体与第一次不一致：len=%d（期望 %d）", i+1, len(b), len(payload))
		}
	}
	t.Logf("PUT 重试 %d 次，上游每次收到的请求体均为 %d 字节且逐字节一致（前 2 次 500，第 3 次 200）", len(bodies), len(payload))
}

// TestStickySessionAcrossHealthChange 粘性会话在健康集合变化时保持唯一且可复现：
// 同一会话始终落同一后端；该后端被摘除后确定性地迁移到另一后端；
// 恢复后回到初始后端。全程通过真实请求链路（Cookie）观察。
func TestStickySessionAcrossHealthChange(t *testing.T) {
	mk := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, name)
		}))
	}
	alpha, beta := mk("alpha"), mk("beta")
	defer alpha.Close()
	defer beta.Close()

	gw, _ := newGateway(t, func(c *config.Config) {
		c.Routes = []config.Route{{PathPrefix: "/", Upstream: "svc", Sticky: true}}
		c.Upstreams[0].Backends = []config.Server{
			{Addr: strings.TrimPrefix(alpha.URL, "http://"), Weight: 1},
			{Addr: strings.TrimPrefix(beta.URL, "http://"), Weight: 1},
		}
		c.Upstreams[0].HealthCheck.FailThreshold = 1
		c.Upstreams[0].HealthCheck.RecoverThreshold = 1
	})
	srv := httptest.NewServer(proxy.NewHandler(gw))
	defer srv.Close()

	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	get := func() string {
		resp, err := client.Get(srv.URL + "/")
		if err != nil {
			t.Fatalf("请求失败: %v", err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("期望 200，实际 %d", resp.StatusCode)
		}
		return string(b)
	}

	// 1. 健康集合不变：同一会话（同一 Cookie）永远落同一后端。
	first := get()
	for i := 0; i < 5; i++ {
		if got := get(); got != first {
			t.Fatalf("健康集合不变时会话乱跳：%s -> %s", first, got)
		}
	}
	t.Logf("健康集合不变：同一会话连续 6 次请求均落在 %s", first)

	// 2. 摘除该后端：会话确定性迁移到另一后端，且多次请求结果一致。
	pool := gw.Snapshot().Pools["svc"]
	victimAddr := strings.TrimPrefix(alpha.URL, "http://")
	want := "beta"
	if first == "beta" {
		victimAddr = strings.TrimPrefix(beta.URL, "http://")
		want = "alpha"
	}
	pool.ReportResult(victimAddr, false) // failThreshold=1，立即摘除
	for i := 0; i < 3; i++ {
		if got := get(); got != want {
			t.Fatalf("摘除后会话应确定性迁移到 %s，实际 %s", want, got)
		}
	}
	t.Logf("摘除 %s 所在后端后：同一会话 3 次请求均确定性落在 %s（rendezvous 哈希最小迁移）", first, want)

	// 3. 恢复后会话回到初始后端。
	pool.ReportResult(victimAddr, true) // recoverThreshold=1，立即恢复
	if got := get(); got != first {
		t.Fatalf("恢复后会话应回到 %s，实际 %s", first, got)
	}
	t.Logf("后端恢复后：同一会话回到初始后端 %s，全程无错配", first)
}
