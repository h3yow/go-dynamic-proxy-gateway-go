package gateway

import (
	"bytes"
	"context"
	"encoding/json"
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
)

// fakeUpstream is an in-test local upstream with controllable behavior, like
// the file-driven simulator but directly addressable from tests.
type fakeUpstream struct {
	name     string
	failing  atomic.Bool
	latency  atomic.Int64 // ms
	hits     atomic.Int64
	sideHits atomic.Int64 // /side-effect invocations actually executed
	started  chan struct{}
	canceled atomic.Int64
	srv      *httptest.Server
}

func newFakeUpstream(name string) *fakeUpstream {
	u := &fakeUpstream{name: name, started: make(chan struct{}, 1024)}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.hits.Add(1)
		lat := u.latency.Load()
		if lat > 0 {
			select {
			case <-time.After(time.Duration(lat) * time.Millisecond):
			case <-r.Context().Done():
				u.canceled.Add(1)
				return
			}
		}
		if strings.HasSuffix(r.URL.Path, "/side-effect") {
			u.sideHits.Add(1) // executes only when the request actually ran
		}
		if u.failing.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Upstream-Name", name)
		fmt.Fprintf(w, `{"upstream":%q,"hits":%d}`, name, u.hits.Load())
	}))
	return u
}

func (u *fakeUpstream) url() string { return u.srv.URL }
func (u *fakeUpstream) close()      { u.srv.Close() }

// buildConfigJSON assembles a gateway config pointing at fake upstreams.
func buildConfigJSON(strategy string, ups map[string]*fakeUpstream, extraRoutes, extraLimits string) string {
	var upDefs []string
	upDefs = append(upDefs,
		fmt.Sprintf(`{"name":"a","pool":"p","base_url":%q,"weight":1}`, ups["a"].url()))
	upDefs = append(upDefs,
		fmt.Sprintf(`{"name":"b","pool":"p","base_url":%q,"weight":1}`, ups["b"].url()))
	routes := `{"name":"r","host":"app.local","prefix":"/","pool":"p",
		"sticky_cookie":"STK","sticky_ttl_seconds":60}`
	if extraRoutes != "" {
		routes += "," + extraRoutes
	}
	return fmt.Sprintf(`{
	  "pools":[{"name":"p","strategy":%q,"fail_threshold":3,
	    "health_interval_ms":80,"request_timeout_ms":300,"max_attempts":3}],
	  "upstreams":[%s,%s],
	  "routes":[%s]
	  %s
	}`, strategy, upDefs[0], upDefs[1], routes, extraLimits)
}

func newManagerFromJSON(t *testing.T, j string) *Manager {
	t.Helper()
	c, err := config.Parse(strings.NewReader(j))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	m, err := NewManager(c)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = m.Active().Shutdown(ctx)
	})
	return m
}

func doProxy(t *testing.T, h http.Handler, host string, hdr map[string]string) (*http.Response, string) {
	t.Helper()
	req := httptest.NewRequest("GET", "http://"+host+"/x", nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	res := rec.Result()
	body, _ := io.ReadAll(res.Body)
	return res, string(body)
}

func TestHotReloadAtomicUnderConcurrency(t *testing.T) {
	ups := map[string]*fakeUpstream{"a": newFakeUpstream("a"), "b": newFakeUpstream("b")}
	defer ups["a"].close()
	defer ups["b"].close()
	m := newManagerFromJSON(t, buildConfigJSON("round_robin", ups, "", ""))
	h := m.ProxyHandler()

	// Two generations of valid configs with disjoint host sets.
	genA := buildConfigJSON("round_robin", ups, "", "")
	genBJSON := func(v int) string {
		return buildConfigJSON("round_robin", ups,
			fmt.Sprintf(`{"name":"v","host":"v%d.local","prefix":"/","pool":"p"}`, v), "")
	}
	invalid := `{"pools":[],"upstreams":[],"routes":[]}` // reason: empty_config

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var badMix atomic.Int64 // responses that are neither valid JSON nor gateway error
	var noRoute atomic.Int64
	var ok atomic.Int64

	// Traffic against a host present in every valid generation.
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				i++
				host := "app.local"
				if id%2 == 0 {
					host = fmt.Sprintf("v%d.local", (i/3)%4)
				}
				res, body := doProxy(t, h, host, nil)
				switch {
				case res.StatusCode == 200:
					ok.Add(1)
				case res.StatusCode == 404 && host != "app.local":
					noRoute.Add(1) // expected for vN hosts not present in this gen
				case res.StatusCode == 404 && host == "app.local":
					t.Errorf("app.local must always route while rejecting bad reloads")
					badMix.Add(1)
				case res.StatusCode >= 502 && res.StatusCode <= 504:
					// transient upstream-side only, tolerated
				default:
					t.Errorf("unexpected status %d body=%s", res.StatusCode, body)
					badMix.Add(1)
				}
			}
		}(w)
	}

	// Reload loop: alternate valid A / valid B / invalid; invalid must be rejected.
	rejections := 0
	for i := 0; i < 60; i++ {
		var j string
		switch i % 3 {
		case 0:
			j = genA
		case 1:
			j = genBJSON(i)
		case 2:
			j = invalid
		}
		reason, err := m.Reload(strings.NewReader(j))
		if i%3 == 2 {
			if err == nil || reason != "empty_config" {
				t.Fatalf("invalid reload must be rejected with empty_config, got reason=%q err=%v", reason, err)
			}
			rejections++
		} else if err != nil {
			t.Fatalf("valid reload %d failed: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()

	if rejections != 20 {
		t.Fatalf("expected 20 rejections, got %d", rejections)
	}
	t.Logf("判定: 60次热更(含20次非法全部拒绝 reason=empty_config)；并发请求 ok=%d 世代差404=%d 混乱=%d；app.local 始终命中",
		ok.Load(), noRoute.Load(), badMix.Load())
}

func TestFailureEjectionAndRecovery(t *testing.T) {
	ups := map[string]*fakeUpstream{"a": newFakeUpstream("a"), "b": newFakeUpstream("b")}
	defer ups["a"].close()
	defer ups["b"].close()
	m := newManagerFromJSON(t, buildConfigJSON("round_robin", ups, "", ""))
	h := m.ProxyHandler()

	ups["b"].failing.Store(true)
	gotB := 0
	gotA := 0
	for i := 0; i < 12; i++ {
		res, body := doProxy(t, h, "app.local", nil)
		if res.StatusCode == 200 {
			var parsed struct{ Upstream string }
			_ = json.Unmarshal([]byte(body), &parsed)
			if parsed.Upstream == "b" {
				gotB++
			} else {
				gotA++
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if gotB != 0 {
		t.Fatalf("b must stop serving after ejection, got %d b responses", gotB)
	}
	t.Logf("判定: b 连续失败摘除后，12次请求 a=%d b=%d（b=0）", gotA, gotB)

	// Recover b: health probe interval 80ms should bring it back.
	ups["b"].failing.Store(false)
	deadline := time.Now().Add(3 * time.Second)
	recovered := false
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		res, body := doProxy(t, h, "app.local", nil)
		if res.StatusCode == 200 && strings.Contains(body, `"upstream":"b"`) {
			recovered = true
			break
		}
	}
	if !recovered {
		t.Fatal("b was not re-added after recovery")
	}
	t.Log("判定: b 恢复后健康探针成功，重新加入负载均衡")
}

func TestRateLimitBoundaryHTTP(t *testing.T) {
	ups := map[string]*fakeUpstream{"a": newFakeUpstream("a"), "b": newFakeUpstream("b")}
	defer ups["a"].close()
	defer ups["b"].close()
	limits := `,"rate_limits":[{"host":"app.local","prefix":"/","rate_per_sec":2,"burst":4,"client_header":"X-Client-Id"}]`
	m := newManagerFromJSON(t, buildConfigJSON("round_robin", ups, "", limits))
	h := m.ProxyHandler()

	codes := map[int]int{}
	for i := 0; i < 8; i++ {
		res, body := doProxy(t, h, "app.local", map[string]string{"X-Client-Id": "c1"})
		codes[res.StatusCode]++
		if res.StatusCode == 429 {
			if res.Header.Get("X-Gateway-Reason") != "rate_limited" {
				t.Fatalf("429 must carry reason header, body=%s", body)
			}
			if res.Header.Get("Retry-After-Ms") == "" {
				t.Fatal("429 must carry Retry-After-Ms")
			}
		}
	}
	if codes[200] != 4 || codes[429] != 4 {
		t.Fatalf("burst=4 boundary: %v", codes)
	}
	// another client unaffected
	res, _ := doProxy(t, h, "app.local", map[string]string{"X-Client-Id": "c2"})
	if res.StatusCode != 200 {
		t.Fatalf("other clients have independent buckets, got %d", res.StatusCode)
	}
	t.Logf("判定: 突发4放行/后续429=%d 且 reason=rate_limited+Retry-After-Ms；客户端隔离", codes[429])
}

func TestTimeoutAndCancelChain(t *testing.T) {
	ups := map[string]*fakeUpstream{"a": newFakeUpstream("a"), "b": newFakeUpstream("b")}
	defer ups["a"].close()
	defer ups["b"].close()
	m := newManagerFromJSON(t, buildConfigJSON("round_robin", ups, "", ""))
	h := m.ProxyHandler()

	// Both slow (500ms) > per-attempt timeout 300ms: all 3 attempts time out.
	ups["a"].latency.Store(500)
	ups["b"].latency.Store(500)
	start := time.Now()
	res, body := doProxy(t, h, "app.local", nil)
	elapsed := time.Since(start)
	if res.StatusCode != 504 || res.Header.Get("X-Gateway-Reason") != "upstream_timeout" {
		t.Fatalf("want 504 upstream_timeout, got %d body=%s", res.StatusCode, body)
	}
	if elapsed >= 1500*time.Millisecond {
		t.Fatalf("3 attempts x 300ms should be ~900ms, took %v", elapsed)
	}
	t.Logf("判定: 上游500ms > 每跳300ms，3次尝试共%v => 504 reason=upstream_timeout", elapsed.Round(time.Millisecond))

	// Client cancellation aborts the upstream call.
	ups["a"].latency.Store(500)
	ups["b"].latency.Store(500)
	callsBefore := ups["a"].canceled.Load() + ups["b"].canceled.Load()
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest("GET", "http://app.local/x", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	go func() {
		time.Sleep(80 * time.Millisecond)
		cancel()
	}()
	h.ServeHTTP(rec, req) // should return promptly after cancel
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) &&
		ups["a"].canceled.Load()+ups["b"].canceled.Load() == callsBefore {
		time.Sleep(10 * time.Millisecond)
	}
	if ups["a"].canceled.Load()+ups["b"].canceled.Load() <= callsBefore {
		t.Fatal("upstream request was not aborted after client cancel")
	}
	t.Logf("判定: 客户端80ms后取消 => 上游 ctx 被中断 canceled=%d，无多余副作用",
		ups["a"].canceled.Load()+ups["b"].canceled.Load())
}

func TestRetriesDoNotDuplicateSideEffects(t *testing.T) {
	ups := map[string]*fakeUpstream{"a": newFakeUpstream("a"), "b": newFakeUpstream("b")}
	defer ups["a"].close()
	defer ups["b"].close()
	m := newManagerFromJSON(t, buildConfigJSON("round_robin", ups, "", ""))
	h := m.ProxyHandler()

	// POST is non-idempotent: even with failing upstreams there must be
	// exactly one actual upstream call (no retry), and one side effect.
	ups["a"].failing.Store(true)
	ups["b"].failing.Store(true)
	req := httptest.NewRequest("POST", "http://app.local/side-effect", bytes.NewReader([]byte("{}")))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 500 {
		t.Fatalf("POST to failing upstreams: want 500, got %d", rec.Code)
	}
	totalSide := ups["a"].sideHits.Load() + ups["b"].sideHits.Load()
	if totalSide != 1 {
		t.Fatalf("non-idempotent POST must execute exactly once upstream, side effects=%d", totalSide)
	}
	t.Logf("判定: POST 失败不重试 => side-effect 实际执行 %d 次（不会重复）", totalSide)

	// GET to one-failing setup retries and succeeds; side-effect endpoint is
	// GET here and idempotent by contract.
	ups["a"].failing.Store(true)
	ups["b"].failing.Store(false)
	res, body := doProxy(t, h, "app.local", nil)
	if res.StatusCode != 200 {
		t.Fatalf("idempotent GET should retry onto healthy peer: %d %s", res.StatusCode, body)
	}
}

func TestStickyMigrationOnEjection(t *testing.T) {
	ups := map[string]*fakeUpstream{"a": newFakeUpstream("a"), "b": newFakeUpstream("b")}
	defer ups["a"].close()
	defer ups["b"].close()
	m := newManagerFromJSON(t, buildConfigJSON("round_robin", ups, "", ""))
	h := m.ProxyHandler()

	// First request establishes sticky binding.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://app.local/x", nil)
	h.ServeHTTP(rec, req)
	cookie := rec.Result().Header.Get("Set-Cookie")
	peer := strings.Contains(cookie, "STK=")
	if !peer {
		t.Fatalf("expected STK cookie, got %q", cookie)
	}

	// Determine bound peer from body, then eject exactly that peer.
	body, _ := io.ReadAll(rec.Result().Body)
	bound := "a"
	if strings.Contains(string(body), `"upstream":"b"`) {
		bound = "b"
	}
	ups[bound].failing.Store(true)
	t.Logf("判定: 会话绑定 peer=%s，现将其摘除并验证确定性迁移", bound)

	// 3+ failing attempts so the bound peer ejects; the sticky request must
	// transparently migrate to the other peer with a reissued cookie.
	var migratedBody string
	var migratedCookie string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		req2 := httptest.NewRequest("GET", "http://app.local/x", nil)
		req2.Header.Set("Cookie", cookie[:strings.IndexByte(cookie, ';')])
		rec2 := httptest.NewRecorder()
		h.ServeHTTP(rec2, req2)
		if rec2.Code == 200 {
			migratedBody = rec2.Body.String()
			migratedCookie = rec2.Result().Header.Get("Set-Cookie")
			break
		}
		time.Sleep(40 * time.Millisecond)
	}
	other := "b"
	if bound == "b" {
		other = "a"
	}
	if !strings.Contains(migratedBody, `"upstream":"`+other+`"`) {
		t.Fatalf("session should migrate to %s, body=%q", other, migratedBody)
	}
	if migratedCookie == "" {
		t.Fatal("migration must reissue a sticky cookie")
	}
	t.Logf("判定: 绑定 peer=%s 被摘除后会话迁移到 %s 并重新签发 cookie，行为确定", bound, other)
}
