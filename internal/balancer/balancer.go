// Package balancer provides weighted round-robin and least-connection
// peer selection integrated with health ejection/recovery.
package balancer

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/health"
)

// ErrNoPeer is returned when every peer is ejected.
var ErrNoPeer = errNoPeer{}

type errNoPeer struct{}

func (errNoPeer) Error() string { return "balancer: no healthy peer available" }

// ErrStickyPeerDown is returned when the sticky session's bound peer exists
// but is currently ejected or mid-probe. Callers can distinguish this from a
// total outage and deterministically migrate the session.
var ErrStickyPeerDown = errStickyDown{}

type errStickyDown struct{}

func (errStickyDown) Error() string { return "balancer: sticky peer is ejected or probing" }

// Peer is one selectable backend.
type Peer struct {
	Def     *config.Upstream
	tracker *health.Tracker

	inFlight atomic.Int64
	// currentWeight is touched only under Pool.mu (smooth WRR state).
	currentWeight int64
}

// Tracker exposes the peer health tracker.
func (p *Peer) Tracker() *health.Tracker { return p.tracker }

// InFlight returns the number of requests currently using the peer.
func (p *Peer) InFlight() int64 { return p.inFlight.Load() }

// Pool is an immutable collection of peers with one balancing strategy.
type Pool struct {
	def    *config.Pool
	peers  []*Peer
	byName map[string]*Peer

	mu          sync.Mutex
	totalWeight int64
	rrCursor    int
}

// NewPool compiles a pool from configuration.
func NewPool(pool *config.Pool, ups []*config.Upstream) *Pool {
	p := &Pool{def: pool, byName: map[string]*Peer{}}
	for _, u := range ups {
		peer := &Peer{
			Def: u,
			tracker: health.NewTracker(
				pool.FailThreshold,
				time.Duration(pool.HealthIntervalMS)*time.Millisecond,
			),
		}
		peer.currentWeight = 0
		p.peers = append(p.peers, peer)
		p.byName[u.Name] = peer
		p.totalWeight += int64(u.Weight)
	}
	return p
}

// Def returns the pool settings.
func (p *Pool) Def() *config.Pool { return p.def }

// PeerByName looks up a peer (nil if unknown).
func (p *Pool) PeerByName(name string) *Peer { return p.byName[name] }

// PickRequest carries selection parameters.
type PickRequest struct {
	StickyPeer string
	Now        time.Time
}

// Lease is a checked-out peer that must be Released exactly once.
type Lease struct {
	Peer *Peer
	pool *Pool
	done bool
}

// Done reports the final outcome of the attempt (nil = success). It records
// health outcome, decrements in-flight and is idempotent-safe under misuse.
func (l *Lease) Done(err error) {
	if l.done {
		return
	}
	l.done = true
	l.Peer.inFlight.Add(-1)
	if err != nil {
		l.Peer.tracker.RecordFailure()
	} else {
		l.Peer.tracker.RecordSuccess()
	}
}

// Pick selects a peer. When StickyPeer names a usable peer it is always
// selected (deterministic stickiness). When the sticky peer exists but is
// ejected/probing, ErrStickyPeerDown is returned. With no sticky peer the
// pool strategy applies. ErrNoPeer means every peer is unavailable.
func (p *Pool) Pick(req PickRequest) (*Lease, error) {
	if req.Now.IsZero() {
		req.Now = time.Now()
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	// Sticky: deterministic. Bound healthy peer wins even if in-flight;
	// ejected or half-open peer yields a distinguishable error so callers
	// never silently split one session across peers.
	if req.StickyPeer != "" {
		peer := p.byName[req.StickyPeer]
		if peer == nil {
			return nil, ErrStickyPeerDown // binding points at a removed peer
		}
		switch peer.tracker.CurrentState() {
		case health.StateHealthy:
			if !peer.tracker.Allow(req.Now) {
				return nil, ErrStickyPeerDown
			}
			peer.inFlight.Add(1)
			return &Lease{Peer: peer, pool: p}, nil
		default:
			return nil, ErrStickyPeerDown
		}
	}

	var chosen *Peer
	if p.def.Strategy == "least_conn" {
		chosen = p.pickLeastConn(req.Now)
	} else {
		chosen = p.pickSmoothRR(req.Now)
	}
	if chosen == nil {
		return nil, ErrNoPeer
	}
	chosen.inFlight.Add(1)
	return &Lease{Peer: chosen, pool: p}, nil
}

// pickSmoothRR implements interleaved weighted round-robin (nginx style):
// selection order is smooth (weight 5 peer is not hit 5 times in a row).
// Caller holds p.mu.
func (p *Pool) pickSmoothRR(now time.Time) *Peer {
	var best *Peer
	var bestCurrent int64
	for _, peer := range p.peers {
		if !peer.tracker.Allow(now) {
			continue
		}
		peer.currentWeight += int64(peer.Def.Weight)
		if best == nil || peer.currentWeight > bestCurrent {
			best, bestCurrent = peer, peer.currentWeight
		}
	}
	if best != nil {
		best.currentWeight -= p.totalWeight
	}
	return best
}

// pickLeastConn minimizes in-flight normalized by weight; ties are broken by
// smooth WRR so behavior stays deterministic. Caller holds p.mu.
func (p *Pool) pickLeastConn(now time.Time) *Peer {
	var eligible []*Peer
	for _, peer := range p.peers {
		if peer.tracker.Allow(now) {
			eligible = append(eligible, peer)
		}
	}
	if len(eligible) == 0 {
		return nil
	}
	// Advance WRR weights for eligible peers (tiebreaker only).
	for _, peer := range eligible {
		peer.currentWeight += int64(peer.Def.Weight)
	}
	best := eligible[0]
	for _, peer := range eligible[1:] {
		// compare inFlight/weight via cross multiplication
		if peer.inFlight.Load()*int64(best.Def.Weight) <
			best.inFlight.Load()*int64(peer.Def.Weight) {
			best = peer
		}
	}
	// Among peers with the same normalized load, pick the WRR winner.
	var tieBest *Peer
	var tieCurrent int64
	for _, peer := range eligible {
		if peer.inFlight.Load()*int64(best.Def.Weight) !=
			best.inFlight.Load()*int64(peer.Def.Weight) {
			continue
		}
		if tieBest == nil || peer.currentWeight > tieCurrent {
			tieBest, tieCurrent = peer, peer.currentWeight
		}
	}
	if tieBest != nil {
		best = tieBest
	}
	best.currentWeight -= p.totalWeight
	return best
}

// Snapshot is a point-in-time pool view for diagnostics.
type Snapshot struct {
	Pool  string
	Peers []PeerSnapshot
}

// PeerSnapshot is a point-in-time peer view.
type PeerSnapshot struct {
	Name     string
	Weight   int
	InFlight int64
	Health   string
	Failures int
}

// Snapshot returns the diagnostic view.
func (p *Pool) Snapshot() Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	snap := Snapshot{Pool: p.def.Name}
	for _, peer := range p.peers {
		hs := peer.tracker.Snapshot()
		snap.Peers = append(snap.Peers, PeerSnapshot{
			Name:     peer.Def.Name,
			Weight:   peer.Def.Weight,
			InFlight: peer.inFlight.Load(),
			Health:   hs.State.String(),
			Failures: hs.Failures,
		})
	}
	return snap
}
