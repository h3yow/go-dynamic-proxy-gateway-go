package upstream

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBehaviorFileHotReloadAndFailureModes(t *testing.T) {
	dir := t.TempDir()
	bp := filepath.Join(dir, "a-behavior.json")
	sim := NewSim("a", bp)
	srv := httptest.NewServer(sim.Handler())
	defer srv.Close()

	// default healthy
	if resp, _ := http.Get(srv.URL + "/ping"); resp.StatusCode != 200 {
		t.Fatalf("default healthy, got %d", resp.StatusCode)
	}

	// fresh simulator: every 3rd request fails
	bp2 := filepath.Join(dir, "b-behavior.json")
	sim2 := NewSim("b", bp2)
	srv2 := httptest.NewServer(sim2.Handler())
	defer srv2.Close()
	sim2.SetBehavior(Behavior{FailEvery: 3})
	codes := []int{}
	for i := 0; i < 3; i++ {
		r, _ := http.Get(srv2.URL + "/ping")
		codes = append(codes, r.StatusCode)
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != 500 {
		t.Fatalf("fail_every=3 => %v", codes)
	}
	t.Logf("判定: fail_every=3 => %v（第3个500）", codes)

	// externally rewrite the file atomically (like an operator/control plane):
	// all-failing mode takes effect on the next request
	if err := atomicWriteFile(bp, []byte(`{"failing":true}`)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if r, _ := http.Get(srv.URL + "/ping"); r.StatusCode != 500 {
		t.Fatalf("file hot-reload to failing => 500, got %d", r.StatusCode)
	}
	// corrupt file: previous behavior retained (fail-closed to known-good)
	_ = os.WriteFile(bp, []byte("{garbage"), 0o644)
	time.Sleep(20 * time.Millisecond)
	if r, _ := http.Get(srv.URL + "/ping"); r.StatusCode != 500 {
		t.Fatalf("corrupt behavior file must retain failing mode, got %d", r.StatusCode)
	}
	t.Log("判定: 行为文件原子写入即生效；损坏文件保留上一已知行为")

	// latency + client cancel: request aborts, no success response
	sim.SetBehavior(Behavior{LatencyMS: 400})
	time.Sleep(20 * time.Millisecond)
	client := &http.Client{Timeout: 100 * time.Millisecond}
	start := time.Now()
	_, err := client.Get(srv.URL + "/ping")
	if err == nil || time.Since(start) >= 350*time.Millisecond {
		t.Fatalf("client timeout must abort the delayed response, err=%v", err)
	}
	t.Log("判定: 客户端100ms超时中断 => 上游延迟响应被丢弃")
}
