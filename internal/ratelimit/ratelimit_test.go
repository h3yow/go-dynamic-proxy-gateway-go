package ratelimit

import (
	"sync"
	"testing"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

func TestBurstBoundaryUnderConcurrency(t *testing.T) {
	l := New([]config.RateLimit{{
		Host: "h.local", Prefix: "/", RatePerSec: 1, Burst: 5,
	}})
	const n = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed, rejected := 0, 0
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			d := l.Allow("client-1", "h.local", "/x", 1000)
			mu.Lock()
			if d.Allowed {
				allowed++
			} else {
				rejected++
				if d.RetryAfterMS <= 0 {
					t.Errorf("rejected decision must carry RetryAfterMS, got %+v", d)
				}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if allowed != 5 || rejected != 45 {
		t.Fatalf("burst=5 at same instant: allowed=%d rejected=%d", allowed, rejected)
	}
	t.Logf("判定: 50并发同刻到达，恰好%d放行/%d拒绝（桶容量边界精确）", allowed, rejected)
}

func TestRefillOverTimeAndPerClientIsolation(t *testing.T) {
	l := New([]config.RateLimit{{
		Host: "h.local", Prefix: "/", RatePerSec: 10, Burst: 2,
	}})
	// Two clients each independently get burst.
	for _, c := range []string{"a", "b"} {
		if !l.Allow(c, "h.local", "/", 0).Allowed || !l.Allow(c, "h.local", "/", 0).Allowed {
			t.Fatalf("client %s first two must pass", c)
		}
		if l.Allow(c, "h.local", "/", 0).Allowed {
			t.Fatalf("client %s third must be rejected", c)
		}
	}
	// After 100ms at 10 tokens/s exactly 1 token refilled.
	d := l.Allow("a", "h.local", "/", 100)
	if !d.Allowed {
		t.Fatal("refilled token should be available after 100ms")
	}
	if l.Allow("a", "h.local", "/", 100).Allowed {
		t.Fatal("only one token should have refilled")
	}
	t.Log("判定: 按客户端独立桶；100ms 按速率精确补充 1 个令牌")
}
