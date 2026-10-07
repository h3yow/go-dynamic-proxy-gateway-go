package proxy_test

import (
	"bytes"
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

// recordingBackend 记录每次收到的请求体与方法，前 failNTimes 次返回 500（读完 body 后），之后返回 200。
func recordingBackend(t *testing.T, failNTimes int64) (*httptest.Server, *atomic.Int64, func() [][]byte) {
	t.Helper()
	var calls atomic.Int64
	var mu sync.Mutex
	bodies := make([][]byte, 0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("上游读取请求体失败: %v", err)
		}
		mu.Lock()
		bodies = append(bodies, data)
		mu.Unlock()
		n := calls.Add(1)
		if n <= failNTimes {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "biz-error")
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	}))
	t.Cleanup(srv.Close)
	return srv, &calls, func() [][]byte {
		mu.Lock()
		defer mu.Unlock()
		out := make([][]byte, len(bodies))
		copy(out, bodies)
		return out
	}
}

func singleBackendGW(addr string, maxRetries, failThreshold int) (*gateway.Gateway, error) {
	cfg := &config.Config{
		ListenAddr: "127.0.0.1:0",
		Routes:     []config.Route{{PathPrefix: "/", Upstream: "svc"}},
		Upstreams: []config.Upstream{{
			Name:   "svc",
			Policy: "wrr",
			Backends: []config.Server{
				{Addr: addr, Weight: 1},
			},
			HealthCheck: config.HealthCheckConf{
				Interval: time.Hour, Timeout: time.Second,
				FailThreshold: failThreshold, RecoverThreshold: 1,
			},
		}},
		Proxy: config.ProxyConf{UpstreamTimeout: 2 * time.Second, MaxRetries: maxRetries},
	}
	return gateway.New(cfg)
}

// TestIdempotentRetryBodyByteIdentical 带请求体的幂等 PUT 重试时，
// 上游每次收到的内容必须与第一次逐字节一致（含二进制与中文），重试次数有上限。
func TestIdempotentRetryBodyByteIdentical(t *testing.T) {
	backend, calls, getBodies := recordingBackend(t, 2) // 前 2 次 500，第 3 次成功
	gw, err := singleBackendGW(addrOf(backend), 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(proxy.NewHandler(gw))
	t.Cleanup(srv.Close)

	// 构造含中文、特殊字符与随机二进制字节的请求体。
	payload := bytes.Join([][]byte{
		[]byte("订单提交 payload\n换行\t制表符\r\n"),
		{0x00, 0x01, 0x02, 0xFE, 0xFF, 0x7F, 0x80},
		[]byte("\n尾部内容-end")}, nil)

	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/resource/1", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if got := calls.Load(); got != 3 {
		t.Fatalf("PUT 应转发 1+2=3 次，实际 %d 次", got)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("第 3 次成功后应返回 200 ok，实际 %d %q", resp.StatusCode, body)
	}
	t.Logf("幂等 PUT：前 2 次业务 500，第 3 次 200，上游实际被调用 %d 次（重试发生 2 次）", calls.Load())

	got := getBodies()
	if len(got) != 3 {
		t.Fatalf("上游应记录 3 份请求体，实际 %d", len(got))
	}
	for i, b := range got {
		if !bytes.Equal(b, payload) {
			t.Fatalf("第 %d 次尝试的请求体与首次不一致：len=%d，want len=%d，首字节=%x", i+1, len(b), len(payload), firstByte(b))
		}
	}
	t.Logf("判定依据：3 次尝试上游收到的请求体长度均为 %d 字节且 bytes.Equal 全为 true（含中文/二进制 0x00 0xFE 0xFF）", len(payload))
}

func firstByte(b []byte) byte {
	if len(b) == 0 {
		return 0
	}
	return b[0]
}

// TestNonIdempotentNoRetryWithBody POST 带请求体失败也绝不重试：上游只收到一次完整 body。
func TestNonIdempotentNoRetryWithBody(t *testing.T) {
	backend, calls, getBodies := recordingBackend(t, 1) // 唯一一次也返回 500
	gw, err := singleBackendGW(addrOf(backend), 3, 100)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(proxy.NewHandler(gw))
	t.Cleanup(srv.Close)

	payload := "post-once-only"
	resp, err := http.Post(srv.URL+"/resource", "text/plain", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if got := calls.Load(); got != 1 {
		t.Fatalf("非幂等 POST 不应重试，上游应只调用 1 次，实际 %d 次", got)
	}
	if resp.StatusCode != http.StatusInternalServerError || string(body) != "biz-error" {
		t.Fatalf("POST 失败应透传 500 biz-error，实际 %d %q", resp.StatusCode, body)
	}
	if b := getBodies()[0]; string(b) != payload {
		t.Fatalf("POST 请求体应完整到达上游，实际 %q", b)
	}
	t.Logf("非幂等 POST：即使配置 max_retries=3 且上游 500，上游只被调用 1 次，body=%q 完整", payload)
}

// TestServerErrorRetriedThenPassedThrough GET 连续 5xx 达重试上限后透传最后一次错误响应。
func TestServerErrorRetriedThenPassedThrough(t *testing.T) {
	backend, calls, _ := recordingBackend(t, 10) // 始终 500
	gw, err := singleBackendGW(addrOf(backend), 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(proxy.NewHandler(gw))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/x")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if got := calls.Load(); got != 2 {
		t.Fatalf("GET 应转发 1+1=2 次，实际 %d 次", got)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("重试耗尽后应透传上游 500，实际 %d", resp.StatusCode)
	}
	if string(body) != "biz-error" {
		t.Fatalf("应透传上游错误响应体，实际 %q", body)
	}
	t.Logf("GET 连续业务 500：重试 1 次（共 %d 次转发）后透传 500 biz-error，而非网关 502", calls.Load())
}

// TestClientErrorNotFailure 4xx 是有效业务应答：不重试、不计入健康失败。
func TestClientErrorNotFailure(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "missing")
	}))
	t.Cleanup(backend.Close)
	gw, err := singleBackendGW(addrOf(backend), 3, 1) // failThreshold=1：误判就会被摘除
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(proxy.NewHandler(gw))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/missing")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if calls.Load() != 1 {
		t.Fatalf("4xx 不应重试，上游应只调用 1 次，实际 %d", calls.Load())
	}
	if resp.StatusCode != http.StatusNotFound || string(body) != "missing" {
		t.Fatalf("应透传 404 missing，实际 %d %q", resp.StatusCode, body)
	}
	if got := gw.Snapshot().Pools["svc"].HealthyBackends(); len(got) != 1 {
		t.Fatalf("4xx 不应计入健康失败，健康后端=%v", got)
	}
	t.Log("上游 404 透传且不重试，也未被摘除（4xx 视为上游可达的有效应答）")
}
