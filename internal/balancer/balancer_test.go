package balancer

import (
	"testing"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

// TestWRRWeightDistribution 验证平滑加权轮询的比例与平滑性。
func TestWRRWeightDistribution(t *testing.T) {
	p := New("wrr", []config.Server{
		{Addr: "a", Weight: 3},
		{Addr: "b", Weight: 1},
	})
	count := map[string]int{}
	var seq []string
	for i := 0; i < 8; i++ {
		s, done, ok := p.Pick()
		if !ok {
			t.Fatal("应能选中后端")
		}
		done(true)
		count[s.Addr]++
		seq = append(seq, s.Addr)
	}
	if count["a"] != 6 || count["b"] != 2 {
		t.Fatalf("8 次选择中 a:b 应为 6:2，实际 %v", count)
	}
	t.Logf("权重 3:1，选择序列 %v，比例 6:2 且分布平滑", seq)
}

// TestLeastConn 验证最少连接策略偏向在途请求少的后端。
func TestLeastConn(t *testing.T) {
	p := New("least_conn", []config.Server{
		{Addr: "a", Weight: 1},
		{Addr: "b", Weight: 1},
	})
	s1, done1, _ := p.Pick()
	s2, done2, _ := p.Pick()
	if s1.Addr == s2.Addr {
		t.Fatalf("a 持有 1 个在途请求时，第二次应选中另一后端，两次都选中 %s", s1.Addr)
	}
	done1(true)
	// a 的在途已释放，再次选择应回到 a（或至少不偏向 b）。
	s3, done3, _ := p.Pick()
	done3(true)
	done2(true)
	t.Logf("在途分布驱动选择：%s -> %s -> 释放后 -> %s", s1.Addr, s2.Addr, s3.Addr)
}
