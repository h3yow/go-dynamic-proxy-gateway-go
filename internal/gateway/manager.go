package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

// Manager owns the active engine generation and performs atomic reloads.
type Manager struct {
	current atomic.Pointer[Engine]
	gen     atomic.Int64

	// oldMu protects `old`: superseded generations that may still serve
	// in-flight requests captured at the handler entry.
	oldMu sync.Mutex
	old   []*Engine

	httpClient *http.Client
}

// NewManager creates a manager with the initial configuration.
func NewManager(cfg *config.Compiled) (*Manager, error) {
	m := &Manager{httpClient: &http.Client{}}
	e := newEngine(m.gen.Add(1), cfg)
	m.current.Store(e)
	return m, nil
}

// Active returns the current engine generation. In-flight requests hold their
// own pointer obtained here, so a swap never interrupts or half-applies to
// them.
func (m *Manager) Active() *Engine { return m.current.Load() }

// ProxyHandler returns the reverse proxy HTTP handler.
func (m *Manager) ProxyHandler() http.Handler { return newProxyHandler(m) }

// Reload parses and validates r. On failure the active engine is untouched and
// the rejection reason (a config.Reason string) is returned. On success a new
// engine is fully constructed first and only then atomically published, so a
// request can observe either the old or the new generation but never a mix;
// the previous generation keeps serving its already-accepted requests and is
// shut down (prober stopped) afterward.
func (m *Manager) Reload(r io.Reader) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "read_error", err
	}
	compiled, perr := config.Parse(bytes.NewReader(data))
	if perr != nil {
		// Validation failure: do NOT touch the active engine. Distinguishable
		// reason lets operators tell invalid_json from route conflicts, etc.
		reason := "unknown"
		var re *config.RejectError
		if errors.As(perr, &re) {
			reason = string(re.Reason)
		}
		log.Printf("[gateway] reload REJECTED reason=%s: %v (active gen=%d unchanged)",
			reason, perr, m.gen.Load())
		return reason, perr
	}

	// Build the complete new generation before publishing: construction
	// either succeeds wholesale or nothing changes.
	newE := newEngine(m.gen.Add(1), compiled)

	old := m.current.Swap(newE)
	log.Printf("[gateway] reload OK gen=%d published; previous gen=%d retained for in-flight requests",
		newE.gen, old.gen)
	m.retainAndShutdown(old)
	return "", nil
}

// retainAndShutdown keeps the old engine reachable until its in-flight work
// drains, then stops its background prober. Draining here is best-effort and
// bounded: requests captured their engine pointer independently, so they are
// not interrupted regardless of when Shutdown runs.
func (m *Manager) retainAndShutdown(old *Engine) {
	m.oldMu.Lock()
	m.old = append(m.old, old)
	m.oldMu.Unlock()

	go func() {
		// Give in-flight requests a bounded drain window before stopping the
		// old generation's prober. Proxied requests themselves are unaffected
		// because they hold pools by pointer directly.
		time.Sleep(200 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := old.Shutdown(ctx); err != nil {
			log.Printf("[gateway] old gen=%d shutdown: %v", old.gen, err)
		}
		m.oldMu.Lock()
		for i, e := range m.old {
			if e == old {
				m.old = append(m.old[:i], m.old[i+1:]...)
				break
			}
		}
		m.oldMu.Unlock()
		log.Printf("[gateway] old gen=%d fully retired", old.gen)
	}()
}

// ReloadResult is the JSON body of an admin reload response.
type ReloadResult struct {
	OK        bool   `json:"ok"`
	Reason    string `json:"reason,omitempty"`
	Message   string `json:"message,omitempty"`
	ActiveGen int64  `json:"active_gen"`
}

// AdminHandler exposes:
//
//	POST /admin/reload     body: new config JSON. 200 on success, 422 + reason on rejection.
//	GET  /admin/snapshot   active generation, routes, pools and peer health.
//	GET  /admin/healthz    liveness
func (m *Manager) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		reason, err := m.Reload(r.Body)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.Header().Set("X-Gateway-Reject-Reason", reason)
			w.WriteHeader(http.StatusUnprocessableEntity)
			_ = json.NewEncoder(w).Encode(ReloadResult{
				OK: false, Reason: reason, Message: err.Error(),
				ActiveGen: m.gen.Load(),
			})
			return
		}
		_ = json.NewEncoder(w).Encode(ReloadResult{OK: true, ActiveGen: m.gen.Load()})
	})
	mux.HandleFunc("/admin/snapshot", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(m.Snapshot())
	})
	mux.HandleFunc("/admin/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// Snapshot returns the active generation's diagnostic view.
func (m *Manager) Snapshot() map[string]any {
	e := m.Active()
	type routeView struct {
		Name, Host, Prefix, Pool string
		Sticky                   string `json:"sticky_cookie,omitempty"`
	}
	routes := []routeView{}
	for _, rt := range e.cfg.Routes {
		routes = append(routes, routeView{
			Name: rt.Name, Host: rt.Host, Prefix: rt.Prefix, Pool: rt.Pool,
			Sticky: rt.StickyCookie,
		})
	}
	pools := map[string]any{}
	for name, p := range e.pools {
		pools[name] = p.Snapshot()
	}
	return map[string]any{
		"generation": e.gen,
		"routes":     routes,
		"pools":      pools,
	}
}
