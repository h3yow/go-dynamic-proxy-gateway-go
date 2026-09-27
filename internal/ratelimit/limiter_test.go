package ratelimit

import (
	"sync"
	"testing"
	"time"
)

// TestBurstBoundary 验证桶容量边界：burst 个请求放行，第 burst+1 个被拒绝。
func TestBurstBoundary(t *testing.T) {
	l := New(1, 3) // 每秒补 1 个，容量 3
	now := time.Now()
	l.now = func() time.Time { return now }

	for i := 1; i <= 3; i++ {
		if !l.Allow("c1") {
			t.Fatalf("第 %d 个请求应放行（burst=3）", i)
		}
		t.Logf("第 %d 个请求放行，符合 burst 边界", i)
	}
	if l.Allow("c1") {
		t.Fatal("第 4 个请求应被拒绝（桶已空）")
	}
	t.Log("第 4 个请求被拒绝，判定依据：桶内令牌 < 1")

	// 时间前进 2 秒，补充 2 个令牌。
	now = now.Add(2 * time.Second)
	if !l.Allow("c1") || !l.Allow("c1") {
		t.Fatal("补充 2 个令牌后应放行 2 个请求")
	}
	if l.Allow("c1") {
		t.Fatal("第 3 个请求应再次被拒绝")
	}
	t.Log("时间前进后按速率补充令牌，边界行为正确")
}

// TestPerClientIsolation 验证不同客户端互不影响。
func TestPerClientIsolation(t *testing.T) {
	l := New(0.0001, 1)
	if !l.Allow("a") {
		t.Fatal("客户端 a 首个请求应放行")
	}
	if l.Allow("a") {
		t.Fatal("客户端 a 第二个请求应被拒绝")
	}
	if !l.Allow("b") {
		t.Fatal("客户端 b 应有自己的桶，首个请求应放行")
	}
	t.Log("判定依据：限流按 client key 隔离")
}

// TestConcurrentAllow 并发下限流总数不能超过 容量+补充量。
func TestConcurrentAllow(t *testing.T) {
	l := New(0.0001, 10) // 几乎不补充
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Allow("c") {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowed != 10 {
		t.Fatalf("并发下放行数应为 10（burst），实际 %d", allowed)
	}
	t.Logf("并发 100 个请求，放行 %d 个，等于 burst=10，令牌扣减无竞态", allowed)
}
