package balancer

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

func poolWithPeers(strategy string, weights ...int) *Pool {
	pc := &config.Pool{
		Name: "p", Strategy: strategy, FailThreshold: 3,
		HealthIntervalMS: 1000, RequestTimeoutMS: 1000,
		MaxAttempts: 1,
	}
	var ups []*config.Upstream
	for i, w := range weights {
		ups = append(ups, &config.Upstream{
			Name: string(rune('a' + i)), Pool: "p",
			BaseURL: "http://127.0.0.1", Weight: w,
		})
	}
	return NewPool(pc, ups)
}

func TestWeightedRoundRobinSmoothAndProportional(t *testing.T) {
	p := poolWithPeers("round_robin", 5, 1)
	counts := map[string]int{}
	var seq string
	for i := 0; i < 12; i++ {
		l, err := p.Pick(PickRequest{Now: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
		counts[l.Peer.Def.Name]++
		seq += l.Peer.Def.Name
		l.Done(nil)
	}
	if counts["a"] != 10 || counts["b"] != 2 {
		t.Fatalf("weights 5:1 => expected 10:2, got %v", counts)
	}
	// Smoothness: with weight 5 the algorithm interleaves b within the
	// window, so the longest a run stays at the weight (5) rather than
	// dumping all 10 a selections back to back. Boundary cycles may show 5.
	bestRun, run := 0, 0
	for i := 0; i < len(seq); i++ {
		if seq[i] == 'a' {
			run++
			if run > bestRun {
				bestRun = run
			}
		} else {
			run = 0
		}
	}
	if bestRun > 5 {
		t.Fatalf("WRR not smooth: seq=%s (max a-run=%d > 5)", seq, bestRun)
	}
	if run := contiguous(seq, "aa"); run > 0 && len(seq) > 10 && bestRun == 10 {
		t.Fatalf("all a selections were contiguous")
	}
	t.Logf("判定: 权重5:1 序列=%s => %v，最大连击=%d（交错，不平滑则为10连）", seq, counts, bestRun)
}

func TestLeastConnPrefersLessLoaded(t *testing.T) {
	p := poolWithPeers("least_conn", 1, 1)
	// Hold 3 leases on 'a', expect picks go to 'b'.
	var held []*Lease
	for i := 0; i < 3; i++ {
		l, err := p.Pick(PickRequest{StickyPeer: "a", Now: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, l)
	}
	for i := 0; i < 3; i++ {
		l, err := p.Pick(PickRequest{Now: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
		if l.Peer.Def.Name != "b" {
			t.Fatalf("least_conn must pick b (a holds 3), got %s", l.Peer.Def.Name)
		}
		l.Done(nil)
	}
	for _, l := range held {
		l.Done(nil)
	}
	t.Log("判定: a在途3个时，新请求全部选择 b（最少连接）")
}

func TestStickyDeterministicAndDownDistinction(t *testing.T) {
	p := poolWithPeers("round_robin", 1, 1)
	// Sticky always returns the bound peer while healthy, even 10 times.
	for i := 0; i < 10; i++ {
		l, err := p.Pick(PickRequest{StickyPeer: "b", Now: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
		if l.Peer.Def.Name != "b" {
			t.Fatalf("sticky must pin b, got %s", l.Peer.Def.Name)
		}
		l.Done(nil)
	}
	// Eject b with threshold failures (marked directly via leases).
	for i := 0; i < 3; i++ {
		l, _ := p.Pick(PickRequest{StickyPeer: "b", Now: time.Now()})
		l.Done(errors.New("boom"))
	}
	_, err := p.Pick(PickRequest{StickyPeer: "b", Now: time.Now()})
	if !errors.Is(err, ErrStickyPeerDown) {
		t.Fatalf("ejected sticky peer => ErrStickyPeerDown, got %v", err)
	}
	// Unknown sticky name is also a down-distinction (binding stale).
	_, err = p.Pick(PickRequest{StickyPeer: "ghost", Now: time.Now()})
	if !errors.Is(err, ErrStickyPeerDown) {
		t.Fatalf("unknown sticky peer => ErrStickyPeerDown, got %v", err)
	}
	// Total outage only when everyone is ejected: eject a too.
	for i := 0; i < 3; i++ {
		l, e := p.Pick(PickRequest{StickyPeer: "a", Now: time.Now()})
		if e != nil {
			t.Fatal(e)
		}
		l.Done(errors.New("boom"))
	}
	if _, err = p.Pick(PickRequest{Now: time.Now()}); !errors.Is(err, ErrNoPeer) {
		t.Fatalf("all ejected => ErrNoPeer, got %v", err)
	}
	t.Log("判定: 粘性健康时恒定；摘除返回 sticky_peer_down；全摘除返回 no_peer")
}

func TestConcurrentPicksNoRace(t *testing.T) {
	p := poolWithPeers("least_conn", 2, 3, 1)
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := p.Pick(PickRequest{Now: time.Now()})
			if err == nil {
				l.Done(nil)
			}
		}()
	}
	wg.Wait()
	snap := p.Snapshot()
	total := int64(0)
	for _, ps := range snap.Peers {
		if ps.InFlight != 0 {
			t.Fatalf("in-flight leaked: %s=%d", ps.Name, ps.InFlight)
		}
		total += ps.InFlight
	}
	t.Logf("判定: 200并发选主无竞态，在途全部归零: %+v", snap.Peers)
}

func contiguous(s, sub string) int {
	count := 0
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			count++
		}
	}
	return count
}
