package gateway_test

import (
	"errors"
	"fmt"
	"io"
	"net/http"
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

func testConfig(addr, listen string) *config.Config {
	return &config.Config{
		ListenAddr: listen,
		Routes:     []config.Route{{Host: "", PathPrefix: "/", Upstream: "svc"}},
		Upstreams: []config.Upstream{{
			Name:   "svc",
			Policy: "wrr",
			Backends: []config.Server{
				{Addr: addr, Weight: 1},
			},
			HealthCheck: config.HealthCheckConf{
				Interval: time.Hour, Timeout: time.Second,
				FailThreshold: 3, RecoverThreshold: 2,
			},
		}},
		Proxy: config.ProxyConf{UpstreamTimeout: 2 * time.Second, MaxRetries: 1},
	}
}

// TestReloadAtomicUnderConcurrency 并发请求下热更新：
// 每个响应必须完整来自旧后端或新后端，不允许半新半旧。
func TestReloadAtomicUnderConcurrency(t *testing.T) {
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Millisecond) // 让请求在飞，增加与 reload 交叠的概率
		fmt.Fprint(w, "A")
	}))
	defer backendA.Close()
	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "B")
	}))
	defer backendB.Close()

	addrA := strings.TrimPrefix(backendA.URL, "http://")
	addrB := strings.TrimPrefix(backendB.URL, "http://")

	gw, err := gateway.New(testConfig(addrA, "127.0.0.1:0"))
	if err != nil {
		t.Fatalf("构建网关失败: %v", err)
	}
	srv := httptest.NewServer(proxy.NewHandler(gw))
	defer srv.Close()

	var stop atomic.Bool
	var wg sync.WaitGroup
	var bad atomic.Int64
	var countA, countB atomic.Int64

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				resp, err := http.Get(srv.URL + "/x")
				if err != nil {
					t.Errorf("请求失败: %v", err)
					return
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				switch string(body) {
				case "A":
					countA.Add(1)
				case "B":
					countB.Add(1)
				default:
					bad.Add(1)
					t.Errorf("出现半新半旧或异常响应: %q", body)
					return
				}
			}
		}()
	}

	// 请求进行中执行多次热更新。
	for i := 0; i < 5; i++ {
		time.Sleep(5 * time.Millisecond)
		if err := gw.Reload(testConfig(addrB, "127.0.0.1:0")); err != nil {
			t.Fatalf("热更新失败: %v", err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	if bad.Load() > 0 {
		t.Fatalf("出现 %d 个异常响应", bad.Load())
	}
	if countA.Load() == 0 || countB.Load() == 0 {
		t.Fatalf("应同时观察到旧配置(A)与新配置(B)的响应，A=%d B=%d", countA.Load(), countB.Load())
	}
	t.Logf("并发热更新完成：旧配置响应 %d 个，新配置响应 %d 个，无半新半旧响应", countA.Load(), countB.Load())
}

// TestReloadInvalidRejected 非法新配置被拒绝，原因可区分，旧配置继续生效。
func TestReloadInvalidRejected(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "OK")
	}))
	defer backend.Close()
	addr := strings.TrimPrefix(backend.URL, "http://")

	gw, err := gateway.New(testConfig(addr, "127.0.0.1:0"))
	if err != nil {
		t.Fatalf("构建网关失败: %v", err)
	}
	srv := httptest.NewServer(proxy.NewHandler(gw))
	defer srv.Close()

	invalid := testConfig(addr, "127.0.0.1:0")
	invalid.Routes[0].Upstream = "不存在的集群"
	err = gw.Reload(invalid)
	if err == nil {
		t.Fatal("非法配置应被拒绝")
	}
	if !errors.Is(err, gateway.ErrInvalidConfig) {
		t.Fatalf("拒绝原因应可用 errors.Is(err, ErrInvalidConfig) 区分，实际: %v", err)
	}
	t.Logf("非法配置被拒绝，原因可区分: %v", err)

	// 旧配置继续生效。
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "OK" {
		t.Fatalf("拒绝后旧配置应继续生效，响应应为 OK，实际 %q", body)
	}
	t.Log("拒绝后旧配置继续生效，响应正常")
}
