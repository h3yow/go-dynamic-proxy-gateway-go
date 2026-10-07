package upstream

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

// newBusinessBackend 模拟“端口仍在监听、但业务健康接口返回错误”的上游：
// /healthz 按 healthy 标志返回 200 / codeWhenUnhealthy，其它路径始终 200（进程活着）。
func newBusinessBackend(t *testing.T, healthy *atomic.Bool, codeWhenUnhealthy int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" && !healthy.Load() {
			w.WriteHeader(codeWhenUnhealthy)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestActiveHTTPProbeEjectsByBusinessResult 端口活着但 /healthz 连续 500 也要被摘除，
// 业务恢复后连续成功达阈值自动加回。
func TestActiveHTTPProbeEjectsByBusinessResult(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(false) // 启动即业务异常
	bad := newBusinessBackend(t, &healthy, http.StatusInternalServerError)

	var alwaysOK atomic.Bool
	alwaysOK.Store(true)
	good := newBusinessBackend(t, &alwaysOK, http.StatusInternalServerError)

	p := NewPool(config.Upstream{
		Name:   "svc",
		Policy: "wrr",
		Backends: []config.Server{
			{Addr: strings.TrimPrefix(bad.URL, "http://"), Weight: 1},
			{Addr: strings.TrimPrefix(good.URL, "http://"), Weight: 1},
		},
		HealthCheck: config.HealthCheckConf{
			Interval:         time.Hour,
			Timeout:          time.Second,
			FailThreshold:    3,
			RecoverThreshold: 2,
			ProbePath:        "/healthz",
		},
	})
	badAddr := strings.TrimPrefix(bad.URL, "http://")
	goodAddr := strings.TrimPrefix(good.URL, "http://")

	// 连续 3 轮业务失败 -> 摘除（good 始终健康，不会被误伤）。
	for i := 1; i <= 3; i++ {
		p.CheckOnce()
		got := p.HealthyBackends()
		if i < 3 {
			if len(got) != 2 {
				t.Fatalf("第 %d 轮失败未达阈值，不应摘除，健康=%v", i, got)
			}
			t.Logf("主动探测第 %d 轮：%s 业务 500，连续失败 %d/3，仍在健康集合", i, badAddr, i)
		} else {
			if len(got) != 1 || got[0] != goodAddr {
				t.Fatalf("第 3 轮业务失败后应只摘除 %s，健康=%v", badAddr, got)
			}
			t.Logf("主动探测第 3 轮：%s 业务 500 连续失败达到阈值 3，被摘除，健康集合=%v", badAddr, got)
		}
	}

	// 业务恢复：第 1 次成功不恢复，第 2 次成功才加回。
	healthy.Store(true)
	p.CheckOnce()
	if got := p.HealthyBackends(); len(got) != 1 {
		t.Fatalf("恢复探测成功 1/2 不应立即加回，健康=%v", got)
	}
	t.Logf("业务恢复后主动探测成功 1/2，%s 仍处摘除态，不接新流量", badAddr)
	p.CheckOnce()
	got := p.HealthyBackends()
	if len(got) != 2 {
		t.Fatalf("连续成功 2 次后应自动加回，健康=%v", got)
	}
	t.Logf("业务恢复后主动探测连续成功 2/2，%s 自动加回，健康集合=%v", badAddr, got)
}

// TestActiveHTTPProbeCustomStatus 自定义成功状态码白名单生效。
func TestActiveHTTPProbeCustomStatus(t *testing.T) {
	var teapot atomic.Bool // true -> 418（在白名单中），false -> 200（不在白名单）
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if teapot.Load() {
			w.WriteHeader(http.StatusTeapot)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	p := NewPool(config.Upstream{
		Name:   "svc",
		Policy: "wrr",
		Backends: []config.Server{
			{Addr: addr, Weight: 1},
		},
		HealthCheck: config.HealthCheckConf{
			Interval:         time.Hour,
			Timeout:          time.Second,
			FailThreshold:    1,
			RecoverThreshold: 1,
			ProbePath:        "/healthz",
			SuccessStatuses:  []int{http.StatusTeapot},
		},
	})

	teapot.Store(false) // 返回 200，但白名单只认 418 -> 判失败摘除
	p.CheckOnce()
	if got := p.HealthyBackends(); len(got) != 0 {
		t.Fatalf("200 不在白名单应摘除，健康=%v", got)
	}
	t.Log("success_statuses=[418] 时，业务返回 200 也判失败并摘除")

	teapot.Store(true)
	p.CheckOnce()
	if got := p.HealthyBackends(); len(got) != 1 {
		t.Fatalf("返回白名单状态码 418 应恢复，健康=%v", got)
	}
	t.Log("业务返回 418（白名单内）判成功并恢复")
}

// TestTCPProbeLegacyFallback 不配置 probe_path 时仍是 TCP 探测：
// 服务进程在监听就算健康，不关心业务状态码。
func TestTCPProbeLegacyFallback(t *testing.T) {
	var healthy atomic.Bool // 业务一直 500
	srv := newBusinessBackend(t, &healthy, http.StatusInternalServerError)
	addr := strings.TrimPrefix(srv.URL, "http://")

	p := NewPool(config.Upstream{
		Name:   "svc",
		Policy: "wrr",
		Backends: []config.Server{
			{Addr: addr, Weight: 1},
		},
		HealthCheck: config.HealthCheckConf{
			Interval: time.Hour, Timeout: time.Second,
			FailThreshold: 1, RecoverThreshold: 1,
			// ProbePath 留空：旧版 TCP 语义
		},
	})
	p.CheckOnce()
	if got := p.HealthyBackends(); len(got) != 1 {
		t.Fatalf("TCP 探测只看端口存活，业务 500 不应摘除，健康=%v", got)
	}
	t.Log("未配置 probe_path：端口可建连即健康，行为与旧版本一致（向后兼容）")
}
