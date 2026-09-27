package health

import (
	"testing"
	"time"
)

func TestEjectHalfOpenRecover(t *testing.T) {
	base := time.Now()
	tr := NewTracker(3, 100*time.Millisecond)

	for i := 0; i < 2; i++ {
		tr.RecordFailure()
		if tr.CurrentState() != StateHealthy {
			t.Fatalf("failure %d must not eject yet", i+1)
		}
	}
	tr.RecordFailure()
	if tr.CurrentState() != StateUnhealthy {
		t.Fatalf("3rd consecutive failure must eject, got %s", tr.CurrentState())
	}
	t.Logf("判定: 连续3次失败 => unhealthy, failures=%d", tr.Snapshot().Failures)

	if tr.Allow(base) {
		t.Fatal("freshly ejected peer must not be selected")
	}
	// before cooldown: still denied
	if tr.Allow(base.Add(90 * time.Millisecond)) {
		t.Fatal("must stay ejected during cooldown")
	}
	// exactly one probe admitted
	if !tr.Allow(base.Add(101 * time.Millisecond)) {
		t.Fatal("cooldown elapsed: one half-open probe must be admitted")
	}
	if tr.CurrentState() != StateHalfOpen {
		t.Fatal("expected half_open")
	}
	if tr.Allow(base.Add(200 * time.Millisecond)) {
		t.Fatal("half-open admits exactly one probe")
	}
	// failed probe re-ejects
	tr.RecordFailure()
	if tr.CurrentState() != StateUnhealthy {
		t.Fatal("failed probe must re-eject")
	}
	// later probe succeeds => recovered
	if !tr.Allow(base.Add(301 * time.Millisecond)) {
		t.Fatal("second probe should be admitted")
	}
	tr.RecordSuccess()
	if tr.CurrentState() != StateHealthy || tr.Snapshot().Failures != 0 {
		t.Fatalf("successful probe must restore healthy, got %s/%d",
			tr.CurrentState(), tr.Snapshot().Failures)
	}
	t.Log("判定: 探活成功 => healthy, 失败计数清零")
}

func TestOnlyOneProbeConcurrent(t *testing.T) {
	tr := NewTracker(1, time.Millisecond)
	tr.RecordFailure()
	time.Sleep(5 * time.Millisecond)
	const n = 64
	admits := make(chan bool, n)
	for i := 0; i < n; i++ {
		go func() { admits <- tr.Allow(time.Now()) }()
	}
	allowed := 0
	for i := 0; i < n; i++ {
		if <-admits {
			allowed++
		}
	}
	if allowed != 1 {
		t.Fatalf("exactly 1 half-open probe expected, got %d", allowed)
	}
	t.Logf("判定: 并发%d 个请求下仅 %d 个探活被放行", n, allowed)
}
