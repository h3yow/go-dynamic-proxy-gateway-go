// Package gateway assembles router, pools, sessions, rate limiting and the
// reverse proxy into an immutable Engine. Config reload builds a brand new
// Engine and atomically swaps the active pointer, so in-flight requests keep
// using the old engine to completion and never see a half-built config.
package gateway

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/balancer"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/health"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/ratelimit"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/router"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/session"
)

// Engine is one immutable generation of gateway state. All fields are fixed
// after construction; mutable per-peer state lives inside balancer/health.
type Engine struct {
	gen      int64
	cfg      *config.Compiled
	table    *router.Table
	pools    map[string]*balancer.Pool
	limiter  *ratelimit.Limiter
	sessions *session.Store
	client   *http.Client

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// newEngine constructs an engine from compiled config and starts its health
// prober. It never mutates after publication; an active engine is safe for
// concurrent requests.
func newEngine(gen int64, cfg *config.Compiled) *Engine {
	e := &Engine{
		gen:      gen,
		cfg:      cfg,
		table:    router.New(cfg),
		pools:    map[string]*balancer.Pool{},
		limiter:  ratelimit.New(cfg.RateLimits),
		sessions: session.NewStore(),
		client:   &http.Client{},
	}
	for name, pool := range cfg.Pools {
		e.pools[name] = balancer.NewPool(pool, cfg.Upstreams[name])
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.wg.Add(1)
	go e.healthProber(ctx)
	return e
}

// healthProber actively probes ejected peers: when a peer has cooled down the
// tracker admits exactly one probe (Unhealthy -> HalfOpen); the prober sends
// GET /healthz and records the result. Success re-adds the peer, failure
// re-ejects it for another cool-down interval.
func (e *Engine) healthProber(ctx context.Context) {
	defer e.wg.Done()
	// Tick fast enough to honor the smallest configured interval.
	interval := 50 * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			e.probeOnce(ctx, now)
		}
	}
}

func (e *Engine) probeOnce(ctx context.Context, now time.Time) {
	for _, pool := range e.pools {
		snap := pool.Snapshot()
		for _, ps := range snap.Peers {
			if ps.Health == health.StateHealthy.String() {
				continue
			}
			peer := pool.PeerByName(ps.Name)
			if peer == nil {
				continue
			}
			// Allow transitions Unhealthy -> HalfOpen for exactly one probe.
			if !peer.Tracker().Allow(now) {
				continue
			}
			e.runProbe(ctx, peer)
		}
	}
}

func (e *Engine) runProbe(parent context.Context, peer *balancer.Peer) {
	base := peer.Def.BaseURL
	reqCtx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, base+"/healthz", nil)
	if err != nil {
		peer.Tracker().RecordFailure()
		return
	}
	resp, err := e.client.Do(req)
	if err != nil {
		peer.Tracker().RecordFailure()
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		peer.Tracker().RecordSuccess()
	} else {
		peer.Tracker().RecordFailure()
	}
}

// Shutdown stops the health prober owned by this engine generation. Older
// generations are shut down only after the swap, but their in-flight proxied
// requests are tracked separately by the Manager and never interrupted.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.cancel()
	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
