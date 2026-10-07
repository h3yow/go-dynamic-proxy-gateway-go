package upstream

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

func testPool(failTh, recoverTh int) *Pool {
	return NewPool(config.Upstream{
		Name:   "svc",
		Policy: "wrr",
		Backends: []config.Server{
			{Addr: "127.0.0.1:9001", Weight: 1},
			{Addr: "127.0.0.1:9002", Weight: 1},
			{Addr: "127.0.0.1:9003", Weight: 1},
		},
		HealthCheck: config.HealthCheckConf{
			Interval: 1e9, Timeout: 1e9,
			FailThreshold: failTh, RecoverThreshold: recoverTh,
		},
	})
}

// TestEjectAndRecover 验证连续失败摘除、连续成功恢复的状态机。
func TestEjectAndRecover(t *testing.T) {
	p := testPool(3, 2)
	addr := "127.0.0.1:9001"

	p.ReportResult(addr, false)
	p.ReportResult(addr, false)
	if got := p.HealthyBackends(); len(got) != 3 {
		t.Fatalf("失败 2 次未达阈值，不应摘除，健康数=%d", len(got))
	}
	p.ReportResult(addr, false)
	if got := p.HealthyBackends(); len(got) != 2 {
		t.Fatalf("连续失败 3 次应摘除，健康数=%d", len(got))
	}
	t.Logf("连续失败达到阈值 3，%s 被摘除，健康后端=%v", addr, p.HealthyBackends())

	p.ReportResult(addr, true)
	if got := p.HealthyBackends(); len(got) != 2 {
		t.Fatalf("成功 1 次未达恢复阈值，健康数应为 2，实际 %d", len(got))
	}
	p.ReportResult(addr, true)
	if got := p.HealthyBackends(); len(got) != 3 {
		t.Fatalf("连续成功 2 次应恢复，健康数=%d", len(got))
	}
	t.Logf("连续成功达到阈值 2，%s 恢复，健康后端=%v", addr, p.HealthyBackends())
}

// TestStickyDeterminism 验证粘性会话：健康集合不变时选择稳定；
// 后端摘除后迁移是确定且最小的；恢复后映射可解释。
func TestStickyDeterminism(t *testing.T) {
	p := testPool(1, 1)

	pick := func(key string) string {
		s, ok := p.PickSticky(key)
		if !ok {
			t.Fatal("应能选中后端")
		}
		return s.Addr
	}

	// 1. 同一 key 多次选择结果一致。
	keys := make([]string, 50)
	before := make(map[string]string)
	for i := range keys {
		keys[i] = fmt.Sprintf("session-%d", i)
		before[keys[i]] = pick(keys[i])
	}
	for _, k := range keys {
		if got := pick(k); got != before[k] {
			t.Fatalf("健康集合不变时 key=%s 选择不稳定：%s -> %s", k, before[k], got)
		}
	}
	t.Log("健康集合不变时，50 个会话的选择全部稳定")

	// 2. 摘除一个后端：只有映射到它的会话迁移，且迁移结果确定。
	victim := "127.0.0.1:9002"
	p.ReportResult(victim, false) // failThreshold=1，立即摘除
	moved, stayed := 0, 0
	after := make(map[string]string)
	for _, k := range keys {
		after[k] = pick(k)
		if before[k] == victim {
			if after[k] == victim {
				t.Fatalf("key=%s 仍映射到已摘除后端 %s", k, victim)
			}
			moved++
		} else {
			if after[k] != before[k] {
				t.Fatalf("key=%s 未映射到被摘除后端，不应迁移：%s -> %s", k, before[k], after[k])
			}
			stayed++
		}
	}
	// 迁移结果可复现：再选一次仍相同。
	for _, k := range keys {
		if got := pick(k); got != after[k] {
			t.Fatalf("摘除后 key=%s 的迁移结果不确定：%s -> %s", k, after[k], got)
		}
	}
	t.Logf("摘除 %s 后：%d 个会话确定性迁移，%d 个会话保持不动（rendezvous 哈希最小迁移）", victim, moved, stayed)

	// 3. 恢复后，映射回到与初始一致（哈希只取决于 key+健康集合）。
	p.ReportResult(victim, true) // recoverThreshold=1，立即恢复
	for _, k := range keys {
		if got := pick(k); got != before[k] {
			t.Fatalf("恢复后 key=%s 应回到初始映射 %s，实际 %s", k, before[k], got)
		}
	}
	t.Log("后端恢复后，所有会话映射回到初始状态，行为完全可解释")
}

// TestAllDown 全部摘除时 Pick 失败而非panic。
func TestAllDown(t *testing.T) {
	p := testPool(1, 1)
	for _, addr := range []string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"} {
		p.ReportResult(addr, false)
	}
	if _, ok := p.PickSticky("s"); ok {
		t.Fatal("无健康后端时 PickSticky 应返回 ok=false")
	}
	if picker := p.Picker(); picker != nil {
		if _, _, ok := picker.Pick(); ok {
			t.Fatal("无健康后端时 Picker.Pick 应返回 ok=false")
		}
	}
	t.Log("全部后端摘除后，选择返回 ok=false，由上层返回 503")
}

// waitFor 轮询直到 cond 成立或超时，用于观察异步健康检查循环的效果。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

// TestHTTPProbeEjectAndRecover 配置 path 后按业务结果探测：
// /healthz 返回 500 时连续失败 fail_threshold 次被摘除，
// 恢复 200 后连续成功 recover_threshold 次重新加入。
func TestHTTPProbeEjectAndRecover(t *testing.T) {
	var bizOK atomic.Bool
	bizOK.Store(true)
	var probeCount atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Errorf("探测路径应为 /healthz，实际 %s", r.URL.Path)
		}
		probeCount.Add(1)
		if !bizOK.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()

	p := NewPool(config.Upstream{
		Name:   "svc",
		Policy: "wrr",
		Backends: []config.Server{
			{Addr: strings.TrimPrefix(up.URL, "http://"), Weight: 1},
		},
		HealthCheck: config.HealthCheckConf{
			Interval: 10 * time.Millisecond, Timeout: time.Second,
			FailThreshold: 2, RecoverThreshold: 2,
			Path: "/healthz",
		},
	})
	stop := make(chan struct{})
	defer close(stop)
	go p.Start(stop)

	waitFor(t, "初始应探测为健康", func() bool { return len(p.HealthyBackends()) == 1 })
	t.Logf("初始：/healthz 返回 200，后端健康（已探测 %d 次）", probeCount.Load())

	// 端口仍在监听，但业务返回 500：必须被判定为不可用。
	bizOK.Store(false)
	waitFor(t, "业务 500 连续 2 次后应摘除", func() bool { return len(p.HealthyBackends()) == 0 })
	t.Logf("业务失败：/healthz 返回 500，连续失败达到 fail_threshold=2，后端被摘除（此时端口仍在监听）")

	// 业务恢复后自动加回。
	bizOK.Store(true)
	waitFor(t, "业务恢复连续 2 次后应重新加入", func() bool { return len(p.HealthyBackends()) == 1 })
	t.Logf("业务恢复：/healthz 返回 200，连续成功达到 recover_threshold=2，后端自动恢复")
}

// TestTCPProbeBackwardCompat 不配置 path 时保持原有 TCP 建连探测行为：
// 旧配置不写新字段也能照常工作。
func TestTCPProbeBackwardCompat(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	p := NewPool(config.Upstream{
		Name:   "svc",
		Policy: "wrr",
		Backends: []config.Server{
			{Addr: addr, Weight: 1},
		},
		HealthCheck: config.HealthCheckConf{ // 无 Path 字段，等价于旧配置
			Interval: 10 * time.Millisecond, Timeout: 200 * time.Millisecond,
			FailThreshold: 1, RecoverThreshold: 1,
		},
	})
	stop := make(chan struct{})
	defer close(stop)
	go p.Start(stop)

	waitFor(t, "TCP 可建连时应健康", func() bool { return len(p.HealthyBackends()) == 1 })
	t.Log("未配置 path：TCP 建连成功即视为健康（旧行为）")

	ln.Close()
	waitFor(t, "端口关闭后应摘除", func() bool { return len(p.HealthyBackends()) == 0 })
	t.Log("端口关闭后 TCP 建连失败，后端被摘除（旧行为保持不变）")
}
