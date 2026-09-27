// Command gateway runs the dynamic reverse-proxy gateway with file-simulated
// upstreams, entirely on localhost:
//
//	:8080        proxy traffic (set Host header / use --resolve)
//	:18080       admin API (/admin/reload, /admin/snapshot, /admin/healthz)
//	:19001-19003 simulated upstreams a/b/c driven by runtime/*-behavior.json
//
// Everything is local: no external services are contacted.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/gateway"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/upstream"
)

type simDef struct {
	name string
	port int
}

func main() {
	addr := flag.String("addr", ":8080", "proxy listen address")
	adminAddr := flag.String("admin", ":18080", "admin listen address")
	configPath := flag.String("config", "configs/gateway.json", "gateway config file")
	runtimeDir := flag.String("runtime", "runtime", "dir for simulated upstream behavior files")
	flag.Parse()

	if err := os.MkdirAll(*runtimeDir, 0o755); err != nil {
		log.Fatalf("mkdir runtime: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(*configPath), 0o755); err != nil {
		log.Fatalf("mkdir configs: %v", err)
	}

	// Start simulated upstreams (local file-driven).
	sims := []simDef{
		{name: "a", port: 19001},
		{name: "b", port: 19002},
		{name: "c", port: 19003},
	}
	var stops []func()
	for _, s := range sims {
		bp := filepath.Join(*runtimeDir, s.name+"-behavior.json")
		ensureDefaultBehavior(bp)
		base, stop, err := upstream.Start(s.port, s.name, bp)
		if err != nil {
			log.Fatalf("start sim %s: %v", s.name, err)
		}
		stops = append(stops, stop)
		log.Printf("simulated upstream %s on %s (behavior %s)", s.name, base, bp)
	}
	defer func() {
		for _, stop := range stops {
			stop()
		}
	}()

	// First-run sample config pointing at the simulated upstreams.
	if err := ensureDefaultConfig(*configPath); err != nil {
		log.Fatalf("write default config: %v", err)
	}

	compiled, err := loadConfigFile(*configPath)
	if err != nil {
		log.Fatalf("initial config invalid: %v", err)
	}
	mgr, err := gateway.NewManager(compiled)
	if err != nil {
		log.Fatalf("new manager: %v", err)
	}

	proxySrv := &http.Server{
		Addr:              *addr,
		Handler:           mgr.ProxyHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	adminSrv := &http.Server{
		Addr:              *adminAddr,
		Handler:           mgr.AdminHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("gateway proxy listening on %s (admin %s)", *addr, *adminAddr)
		if err := proxySrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("proxy serve: %v", err)
		}
	}()
	go func() {
		if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("admin serve: %v", err)
		}
	}()

	// SIGHUP reloads the config file (invalid => old config stays active).
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			if err := reloadFromFile(mgr, *configPath); err != nil {
				log.Printf("SIGHUP reload failed: %v", err)
			}
		}
	}()

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)
	<-stopCh
	log.Printf("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = proxySrv.Shutdown(ctx)
	_ = adminSrv.Shutdown(ctx)
}

func reloadFromFile(mgr *gateway.Manager, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	reason, err := mgr.Reload(f)
	if err != nil {
		return fmt.Errorf("rejected (%s): %w", reason, err)
	}
	return nil
}

func loadConfigFile(path string) (*config.Compiled, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return config.Parse(f)
}

func ensureDefaultBehavior(path string) {
	if _, err := os.Stat(path); err == nil {
		return
	}
	b, _ := json.MarshalIndent(upstream.Behavior{}, "", "  ")
	_ = os.WriteFile(path, b, 0o644)
}

func ensureDefaultConfig(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	sample := []byte(defaultConfigJSON)
	return os.WriteFile(path, sample, 0o644)
}

const defaultConfigJSON = `{
  "pools": [
    {
      "name": "web-pool",
      "strategy": "round_robin",
      "fail_threshold": 3,
      "health_interval_ms": 1000,
      "request_timeout_ms": 2000,
      "max_attempts": 2
    },
    {
      "name": "api-pool",
      "strategy": "least_conn",
      "fail_threshold": 2,
      "health_interval_ms": 1000,
      "request_timeout_ms": 2000,
      "max_attempts": 3,
      "retry_methods": ["GET", "HEAD", "OPTIONS"]
    }
  ],
  "upstreams": [
    {"name": "web-a", "pool": "web-pool", "base_url": "http://127.0.0.1:19001", "weight": 3},
    {"name": "web-b", "pool": "web-pool", "base_url": "http://127.0.0.1:19002", "weight": 1},
    {"name": "api-a", "pool": "api-pool", "base_url": "http://127.0.0.1:19001", "weight": 1},
    {"name": "api-c", "pool": "api-pool", "base_url": "http://127.0.0.1:19003", "weight": 2}
  ],
  "routes": [
    {"name": "web", "host": "web.local", "prefix": "/", "pool": "web-pool", "sticky_cookie": "WEBSTICKY", "sticky_ttl_seconds": 60},
    {"name": "api-v2", "host": "api.local", "prefix": "/v2/", "pool": "api-pool"},
    {"name": "api", "host": "api.local", "prefix": "/", "pool": "web-pool"}
  ],
  "rate_limits": [
    {"host": "api.local", "prefix": "/", "rate_per_sec": 5, "burst": 5, "client_header": "X-Client-Id"}
  ]
}
`
