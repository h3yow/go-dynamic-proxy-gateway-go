// Package health tracks per-peer health with failure ejection, half-open
// probing and recovery.
package health

import (
	"sync"
	"time"
)

// State of a peer.
type State int

const (
	StateHealthy State = iota
	StateUnhealthy
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateHealthy:
		return "healthy"
	case StateUnhealthy:
		return "unhealthy"
	case StateHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// Tracker tracks the health of one peer. Safe for concurrent use.
//
// State machine:
//
//	healthy --(failThreshold consecutive failures)--------> unhealthy
//	unhealthy --(probeInterval elapsed, one probe)---> half_open
//	half_open --(probe success)--> healthy
//	half_open --(probe failure)--> unhealthy (cool down again)
type Tracker struct {
	mu            sync.Mutex
	failThreshold int
	state         State
	failures      int
	openedAt      time.Time
	probeInterval time.Duration
}

// NewTracker creates a tracker.
func NewTracker(failThreshold int, probeInterval time.Duration) *Tracker {
	if failThreshold < 1 {
		failThreshold = 1
	}
	if probeInterval <= 0 {
		probeInterval = time.Second
	}
	return &Tracker{failThreshold: failThreshold, probeInterval: probeInterval}
}

// RecordSuccess marks a result as successful and fully reopens the peer.
func (t *Tracker) RecordSuccess() {
	t.mu.Lock()
	t.state = StateHealthy
	t.failures = 0
	t.mu.Unlock()
}

// RecordFailure marks a result as failed. In healthy state the peer is
// ejected once failThreshold consecutive failures are reached; in half-open
// state a single probe failure ejects it again immediately.
func (t *Tracker) RecordFailure() {
	t.mu.Lock()
	t.failures++
	switch t.state {
	case StateHealthy:
		if t.failures >= t.failThreshold {
			t.state = StateUnhealthy
			t.openedAt = time.Now()
		}
	case StateHalfOpen:
		t.state = StateUnhealthy
		t.failures = t.failThreshold
		t.openedAt = time.Now()
	}
	t.mu.Unlock()
}

// Allow returns whether the peer may receive a request now. When the cool
// down has elapsed, exactly one caller transitions Unhealthy -> HalfOpen and
// gets permission for a probe; all other callers are rejected until the
// probe resolves.
func (t *Tracker) Allow(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch t.state {
	case StateHealthy:
		return true
	case StateHalfOpen:
		return false
	default: // Unhealthy
		if now.Sub(t.openedAt) >= t.probeInterval {
			t.state = StateHalfOpen
			return true
		}
		return false
	}
}

// CurrentState returns the state.
func (t *Tracker) CurrentState() State {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

// Snapshot is a point-in-time health view.
type Snapshot struct {
	State    State
	Failures int
}

// Snapshot returns the current view.
func (t *Tracker) Snapshot() Snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return Snapshot{State: t.state, Failures: t.failures}
}
