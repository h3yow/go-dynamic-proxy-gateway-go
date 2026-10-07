package proxy_test

import (
	"fmt"
	"io"
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

// labeledBackend 是可切换业务状态的模拟上游：
// healthy=false 时所有业务请求返回 500（端口仍在监听），true 时返回 200+名称。
func labeledBackend(t *testing.T, name string, healthy *atomic.Bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "biz-error")
			return
		}
		fmt.Fprintf(w, "backend=%s", name)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func addrOf(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }

func healthGWConfig(addrs []string, sticky bool, failTh, recoverTh int, probePath string) *config.Config {
	backends := make([]config.Server, len(addrs))
	for i, a := range addrs {
		backends[i] = config.Server{Addr: a, Weight: 1}
	}
	return &config.Config{
		ListenAddr: "127.0.0.1:0",
		Routes:     []config.Route{{PathPrefix: "/", Upstream: "svc", Sticky: sticky}},
		Upstreams: []config.Upstream{{
			Name:     "svc",
			Policy:   "wrr",
			Backends: backends,
			HealthCheck: config.HealthCheckConf{
				Interval:         time.Hour, // 测试里手动触发探测，避免定时器干扰
				Timeout:          time.Second,
				FailThreshold:    failTh,
				RecoverThreshold: recoverTh,
				ProbePath:        probePath,
			},
		}},
		Proxy: config.ProxyConf{UpstreamTimeout: 2 * time.Second, MaxRetries: 0},
	}
}

func doGet(t *testing.T, url string, cookie *http.Cookie) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if cookie != nil {
		req.AddCookie(cookie)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var c *http.Cookie
	for _, ck := range resp.Cookies() {
		if ck.Name == "GWSESSION" {
			c = ck
		}
	}
	if c == nil {
		c = cookie
	}
	return resp, string(data)
}

// TestEjectByBusinessFailureRealChain 业务 500 经真实请求链路触发摘除：
// 连续 failThreshold 次 500 -> 后端被摘除 -> 该会话流量确定性迁移到健康后端 ->
// 业务恢复并被主动探测加回 -> 同一会话确定性回到原后端。全程通过真实 HTTP 观察。
func TestEjectByBusinessFailureRealChain(t *testing.T) {
	var hA, hB, hC atomic.Bool
	hA.Store(false) // A 业务异常（端口活着）
	hB.Store(true)
	hC.Store(true)
	bA := labeledBackend(t, "A", &hA)
	bB := labeledBackend(t, "B", &hB)
	bC := labeledBackend(t, "C", &hC)

	cfg := healthGWConfig([]string{addrOf(bA), addrOf(bB), addrOf(bC)}, true, 3, 2, "/healthz")
	gw, err := gateway.New(cfg)
	if err != nil {
		t.Fatalf("构建网关失败: %v", err)
	}
	srv := httptest.NewServer(proxy.NewHandler(gw))
	t.Cleanup(srv.Close)
	pool := gw.Snapshot().Pools["svc"]

	// 找到一个粘性映射到 A 的会话 key（用 Cookie 指定，保证可复现）。
	var victimAddr = addrOf(bA)
	var sessionCookie *http.Cookie
	for i := 0; ; i++ {
		id := fmt.Sprintf("fixed-session-%d", i)
		c := &http.Cookie{Name: "GWSESSION", Value: id}
		picked, ok := pool.PickSticky(id)
		if !ok {
			t.Fatal("无健康后端")
		}
		if picked.Addr == victimAddr {
			sessionCookie = c
			break
		}
		if i > 10000 {
			t.Fatal("未能找到映射到 A 的会话")
		}
	}
	t.Logf("选定会话 %q 初始粘性后端为故障节点 %s", sessionCookie.Value, victimAddr)

	// 连续 3 次真实请求都拿到业务 500（每次都只打 A，因为粘性固定）。
	for i := 1; i <= 3; i++ {
		resp, body := doGet(t, srv.URL+"/orders", sessionCookie)
		if resp.StatusCode != http.StatusInternalServerError || body != "biz-error" {
			t.Fatalf("第 %d 次应透传 A 的业务 500，实际 status=%d body=%q", i, resp.StatusCode, body)
		}
		healthy := pool.HealthyBackends()
		if i < 3 {
			if len(healthy) != 3 {
				t.Fatalf("第 %d 次失败未达摘除阈值，健康集合不应变化，实际 %v", i, healthy)
			}
			t.Logf("真实请求第 %d 次拿到业务 500（连续失败 %d/3），%s 仍在健康集合", i, i, victimAddr)
		} else {
			if len(healthy) != 2 || contains(healthy, victimAddr) {
				t.Fatalf("第 3 次业务失败后 %s 应被摘除，健康集合=%v", victimAddr, healthy)
			}
			t.Logf("真实请求第 3 次业务 500，连续失败达到阈值 3，%s 被摘除，健康集合=%v", victimAddr, healthy)
		}
	}

	// 摘除后：同一会话确定性迁移到健康后端，收到 200 + 其它后端名称，不再见到 A。
	resp, body := doGet(t, srv.URL+"/orders", sessionCookie)
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(body, "backend=") || body == "backend=A" {
		t.Fatalf("摘除后流量应迁移到健康后端，实际 status=%d body=%q", resp.StatusCode, body)
	}
	movedTo := strings.TrimPrefix(body, "backend=")
	t.Logf("摘除后同一会话迁移到健康后端 %s（200 %q），流量切走可直接观察", movedTo, body)

	// 再请求多次，结果必须稳定可复现（健康集合不变，粘性不允许乱跳）。
	for i := 0; i < 5; i++ {
		_, b := doGet(t, srv.URL+"/orders", sessionCookie)
		if b != body {
			t.Fatalf("摘除后粘性选择不稳定：%q != %q", b, body)
		}
	}
	t.Logf("摘除后连续 6 次请求全部落在 %s，会话未乱跳", movedTo)

	// 业务恢复：主动探测连续成功 2 次把 A 加回（真实流量无法打到已摘除节点，恢复由探测完成）。
	hA.Store(true)
	pool.CheckOnce()
	if contains(pool.HealthyBackends(), victimAddr) {
		t.Fatal("探测成功 1/2 不应立即恢复")
	}
	t.Log("业务恢复后探测成功 1/2，A 仍摘除，不接新流量")
	pool.CheckOnce()
	if !contains(pool.HealthyBackends(), victimAddr) {
		t.Fatalf("探测连续成功 2 次后 A 应被加回，健康集合=%v", pool.HealthyBackends())
	}
	t.Logf("探测连续成功 2/2，%s 自动加回，健康集合=%v", victimAddr, pool.HealthyBackends())

	// 加回后：同一会话确定性回到 A（rendezvous 哈希只依赖 key+健康集合）。
	resp, body = doGet(t, srv.URL+"/orders", sessionCookie)
	if resp.StatusCode != http.StatusOK || body != "backend=A" {
		t.Fatalf("恢复后同一会话应回到 A，实际 status=%d body=%q", resp.StatusCode, body)
	}
	t.Log("恢复后同一会话确定性回到原后端 A，映射与摘除前一致、可复现")
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
