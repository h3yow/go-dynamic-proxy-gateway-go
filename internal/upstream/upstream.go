// Package upstream simulates real upstream services with local file servers
// whose behaviour (latency, failure injection) is driven by local files so
// no external service is required.
package upstream

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Behavior controls a simulated upstream's responses. Hot-reloaded from a
// local file; writers should publish it with atomic rename.
type Behavior struct {
	// LatencyMS delays every response (client cancellation still aborts it).
	LatencyMS int `json:"latency_ms"`
	// FailEvery > 0 makes every FailEvery-th request return 500.
	FailEvery int `json:"fail_every"`
	// Failing true makes every request return 500.
	Failing bool `json:"failing"`
	// Slow causes responses to wait LatencyMS even on the success path,
	// regardless of status; useful for timeout testing.
	Slow bool `json:"slow"`
}

// Sim is one simulated upstream.
type Sim struct {
	name         string
	behaviorPath string
	counter      atomic.Int64

	mu       sync.Mutex
	behavior Behavior
	handler  http.Handler

	statePath  string
	stateMu    sync.Mutex
	sideEffect int64
}

// NewSim creates a simulated upstream. If behaviorPath exists it is loaded;
// the file is re-read on every request so behavior changes take effect with
// no restart. Missing file means default healthy behavior.
func NewSim(name string, behaviorPath string) *Sim {
	s := &Sim{name: name, behaviorPath: behaviorPath}
	s.statePath = filepath.Join(filepath.Dir(behaviorPath), name+"-state.json")
	if b, err := LoadBehaviorFromFile(behaviorPath); err == nil {
		s.behavior = b
	}
	s.handler = http.HandlerFunc(s.serve)
	if data, err := os.ReadFile(s.statePath); err == nil {
		var st struct {
			SideEffect int64 `json:"side_effect"`
		}
		if json.Unmarshal(data, &st) == nil {
			s.sideEffect = st.SideEffect
		}
	}
	return s
}

// Handler returns the HTTP handler.
func (s *Sim) Handler() http.Handler { return s.handler }

// loadBehavior re-reads the behavior file; on any error the previous behavior
// is kept (fail-closed to known-good state).
func (s *Sim) loadBehavior() Behavior {
	b, err := LoadBehaviorFromFile(s.behaviorPath)
	if err != nil {
		s.mu.Lock()
		cur := s.behavior
		s.mu.Unlock()
		return cur
	}
	s.mu.Lock()
	s.behavior = b
	s.mu.Unlock()
	return b
}

// SetBehavior replaces the in-memory behavior and persists it to the file
// (used by tests and demos; written atomically).
func (s *Sim) SetBehavior(b Behavior) {
	s.mu.Lock()
	s.behavior = b
	s.mu.Unlock()
	data, _ := json.MarshalIndent(b, "", "  ")
	if s.behaviorPath != "" {
		_ = atomicWriteFile(s.behaviorPath, data)
	}
}

func (s *Sim) serve(w http.ResponseWriter, r *http.Request) {
	b := s.loadBehavior()
	if b.LatencyMS > 0 {
		select {
		case <-time.After(time.Duration(b.LatencyMS) * time.Millisecond):
		case <-r.Context().Done():
			// client/gateway gave up: abort, do not produce side effects
			w.WriteHeader(499)
			return
		}
	}
	n := s.counter.Add(1)
	fail := b.Failing || (b.FailEvery > 0 && int(n)%b.FailEvery == 0)

	switch {
	case strings.HasSuffix(r.URL.Path, "/side-effect"):
		// POST-only mutating endpoint: increments a durable counter. Used to
		// prove retries do not duplicate side effects.
		s.stateMu.Lock()
		s.sideEffect++
		v := s.sideEffect
		_ = atomicWriteFile(s.statePath, []byte(
			fmt.Sprintf(`{"side_effect":%d}`, v)))
		s.stateMu.Unlock()
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `{"upstream":%q,"side_effect":%d,"status":500}`, s.name, v)
			return
		}
		w.Header().Set("X-Upstream-Name", s.name)
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"upstream":%q,"side_effect":%d}`, s.name, v)
		return
	case r.URL.Path == "/healthz":
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Upstream-Name", s.name)
		w.Write([]byte("ok"))
		return
	}

	if fail {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"upstream":%q,"seq":%d,"status":500}`, s.name, n)
		return
	}
	w.Header().Set("X-Upstream-Name", s.name)
	w.Header().Set("X-Request-Seq", strconv.FormatInt(n, 10))
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"upstream":%q,"seq":%d,"method":%q,"path":%q}`,
		s.name, n, r.Method, r.URL.Path)
}

// LoadBehaviorFromFile reads and parses a behavior file.
func LoadBehaviorFromFile(path string) (Behavior, error) {
	var b Behavior
	data, err := os.ReadFile(path)
	if err != nil {
		return b, err
	}
	if err := json.Unmarshal(data, &b); err != nil {
		return b, err
	}
	return b, nil
}

// atomicWriteFile writes data to path via temp file + rename so readers never
// observe a half-written file.
func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}

// Start launches a simulated upstream listening on 127.0.0.1 at the given
// port and returns its base URL and a stop function.
func Start(port int, name, behaviorPath string) (baseURL string, stop func(), err error) {
	sim := NewSim(name, behaviorPath)
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return "", nil, err
	}
	srv := &http.Server{Handler: sim.Handler()}
	go srv.Serve(ln)
	return "http://" + ln.Addr().String(), func() {
		ctx, cancel := contextWithTimeout(500 * time.Millisecond)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}
