package config

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
)

// Defaults applied when fields are omitted.
const (
	DefaultStrategy         = "round_robin"
	DefaultWeight           = 1
	DefaultFailThreshold    = 3
	DefaultHealthIntervalMS = 1000
	DefaultRequestTimeoutMS = 3000
	DefaultMaxAttempts      = 1
)

// PoolConfig is the JSON representation of backend pool settings.
type PoolConfig struct {
	Name             string   `json:"name"`
	Strategy         string   `json:"strategy"`
	FailThreshold    int      `json:"fail_threshold"`
	HealthIntervalMS int      `json:"health_interval_ms"`
	RequestTimeoutMS int      `json:"request_timeout_ms"`
	MaxAttempts      int      `json:"max_attempts"`
	RetryMethods     []string `json:"retry_methods"`
}

// File is the on-disk JSON layout.
type File struct {
	Pools      []PoolConfig `json:"pools"`
	Routes     []Route      `json:"routes"`
	Upstreams  []Upstream   `json:"upstreams"`
	RateLimits []RateLimit  `json:"rate_limits"`
}

// Compiled is a validated configuration ready to build an engine from.
type Compiled struct {
	Pools      map[string]*Pool
	Routes     []Route
	Upstreams  map[string][]*Upstream
	RateLimits []RateLimit
}

var validStrategies = map[string]bool{
	"round_robin": true,
	"least_conn":  true,
}

// defaultRetryMethods are HTTP methods considered safe to retry because they
// have no server-side side effects by convention. POST is intentionally
// excluded: a failed POST may already have been processed by the upstream.
var defaultRetryMethods = []string{"GET", "HEAD", "OPTIONS"}

func reject(r Reason, format string, args ...any) error {
	return &RejectError{Reason: r, Message: fmt.Sprintf(format, args...)}
}

// Parse reads, parses and validates configuration. On failure *RejectError
// carries a distinguishable Reason and the returned *Compiled is nil.
func Parse(r io.Reader) (*Compiled, error) {
	var f File
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, reject(ReasonInvalidJSON, "decode: %v", err)
	}

	out := &Compiled{
		Pools:     map[string]*Pool{},
		Upstreams: map[string][]*Upstream{},
	}

	// ---- pools ----
	for i := range f.Pools {
		pc := f.Pools[i]
		name := strings.TrimSpace(pc.Name)
		if name == "" {
			return nil, reject(ReasonPoolBadStrategy, "pools[%d]: name is required", i)
		}
		if _, dup := out.Pools[name]; dup {
			return nil, reject(ReasonPoolBadStrategy, "pool %q: duplicate pool name", name)
		}
		strategy := pc.Strategy
		if strategy == "" {
			strategy = DefaultStrategy
		}
		if !validStrategies[strategy] {
			return nil, reject(ReasonPoolBadStrategy,
				"pool %q: strategy %q must be round_robin or least_conn", name, pc.Strategy)
		}
		weight := pc.FailThreshold
		if weight == 0 {
			weight = DefaultFailThreshold
		}
		if weight < 0 {
			return nil, reject(ReasonPoolBadStrategy, "pool %q: fail_threshold must be positive", name)
		}
		interval := pc.HealthIntervalMS
		if interval == 0 {
			interval = DefaultHealthIntervalMS
		}
		if interval < 0 {
			return nil, reject(ReasonPoolBadStrategy, "pool %q: health_interval_ms must be positive", name)
		}
		timeout := pc.RequestTimeoutMS
		if timeout == 0 {
			timeout = DefaultRequestTimeoutMS
		}
		if timeout <= 0 {
			return nil, reject(ReasonPoolBadStrategy, "pool %q: request_timeout_ms must be positive", name)
		}
		attempts := pc.MaxAttempts
		if attempts == 0 {
			attempts = DefaultMaxAttempts
		}
		if attempts < 1 {
			return nil, reject(ReasonPoolBadStrategy, "pool %q: max_attempts must be >= 1", name)
		}
		methods := pc.RetryMethods
		if methods == nil {
			methods = defaultRetryMethods
		}
		mm := map[string]bool{}
		for _, mth := range methods {
			mm[strings.ToUpper(strings.TrimSpace(mth))] = true
		}
		out.Pools[name] = &Pool{
			Name:             name,
			Strategy:         strategy,
			FailThreshold:    weight,
			HealthIntervalMS: interval,
			RequestTimeoutMS: timeout,
			MaxAttempts:      attempts,
			RetryMethods:     mm,
		}
	}

	// ---- upstreams ----
	seenUpstream := map[string]bool{}
	for i := range f.Upstreams {
		u := f.Upstreams[i]
		name := strings.TrimSpace(u.Name)
		if name == "" {
			return nil, reject(ReasonUpstreamBadURL, "upstreams[%d]: name is required", i)
		}
		if seenUpstream[name] {
			return nil, reject(ReasonUpstreamDuplicateName, "upstream %q: duplicate upstream name", name)
		}
		seenUpstream[name] = true
		if _, ok := out.Pools[u.Pool]; !ok {
			return nil, reject(ReasonUpstreamUnknownPool,
				"upstream %q: pool %q is not defined", name, u.Pool)
		}
		pu, err := url.Parse(u.BaseURL)
		if err != nil || (pu.Scheme != "http" && pu.Scheme != "https") || pu.Host == "" {
			return nil, reject(ReasonUpstreamBadURL,
				"upstream %q: base_url %q must be an absolute http(s) URL", name, u.BaseURL)
		}
		w := u.Weight
		if w == 0 {
			w = DefaultWeight
		}
		if w < 1 {
			return nil, reject(ReasonUpstreamBadWeight,
				"upstream %q: weight %d must be >= 1", name, u.Weight)
		}
		u.Name, u.Weight = name, w
		out.Upstreams[u.Pool] = append(out.Upstreams[u.Pool], &u)
	}
	for pname := range out.Pools {
		if len(out.Upstreams[pname]) == 0 {
			return nil, reject(ReasonPoolNoUpstream, "pool %q has no upstreams", pname)
		}
	}

	// ---- routes ----
	seenRouteName := map[string]bool{}
	seenHostPrefix := map[string]bool{}
	for i := range f.Routes {
		rt := f.Routes[i]
		name := strings.TrimSpace(rt.Name)
		if name == "" {
			return nil, reject(ReasonRouteBadHost, "routes[%d]: name is required", i)
		}
		if seenRouteName[name] {
			return nil, reject(ReasonDuplicateRouteName, "route %q: duplicate route name", name)
		}
		seenRouteName[name] = true
		host := strings.ToLower(strings.TrimSpace(rt.Host))
		if host == "" || strings.ContainsAny(host, " /") {
			return nil, reject(ReasonRouteBadHost, "route %q: host %q invalid", name, rt.Host)
		}
		rt.Host = host
		prefix := rt.Prefix
		if prefix == "" {
			prefix = "/"
		}
		if !strings.HasPrefix(prefix, "/") {
			return nil, reject(ReasonRouteBadPrefix, "route %q: prefix %q must start with /", name, rt.Prefix)
		}
		rt.Prefix = prefix
		if _, ok := out.Pools[rt.Pool]; !ok {
			return nil, reject(ReasonRouteMissingPool, "route %q: pool %q is not defined", name, rt.Pool)
		}
		key := rt.Host + " " + rt.Prefix
		if seenHostPrefix[key] {
			return nil, reject(ReasonRouteConflict,
				"route %q: duplicate route for host %q prefix %q", name, rt.Host, rt.Prefix)
		}
		seenHostPrefix[key] = true
		if rt.StickyCookie != "" {
			if !validCookieName(rt.StickyCookie) {
				return nil, reject(ReasonStickyNoTTL,
					"route %q: sticky_cookie %q is not a valid cookie name", name, rt.StickyCookie)
			}
			if rt.StickyTTLSeconds < 0 {
				return nil, reject(ReasonStickyNoTTL,
					"route %q: sticky_ttl_seconds must be >= 0", name)
			}
		}
		out.Routes = append(out.Routes, rt)
	}

	// ---- rate limits ----
	if len(f.Routes) == 0 {
		return nil, reject(ReasonEmptyConfig, "at least one route is required")
	}
	if len(f.Upstreams) == 0 {
		return nil, reject(ReasonEmptyConfig, "at least one upstream is required")
	}

	for i := range f.RateLimits {
		rl := f.RateLimits[i]
		rl.Host = strings.ToLower(strings.TrimSpace(rl.Host))
		if rl.Prefix == "" {
			rl.Prefix = "/"
		}
		if !strings.HasPrefix(rl.Prefix, "/") {
			return nil, reject(ReasonRateLimitInvalid,
				"rate_limits[%d]: prefix must start with /", i)
		}
		if rl.RatePerSec <= 0 {
			return nil, reject(ReasonRateLimitInvalid,
				"rate_limits[%d]: rate_per_sec must be > 0", i)
		}
		if rl.Burst < 1 {
			return nil, reject(ReasonRateLimitInvalid,
				"rate_limits[%d]: burst must be >= 1", i)
		}
		out.RateLimits = append(out.RateLimits, rl)
	}

	return out, nil
}

// validCookieName is a conservative RFC 6265 cookie-name check: one or more
// token characters, no separators or whitespace.
func validCookieName(n string) bool {
	if n == "" {
		return false
	}
	for _, c := range n {
		if c <= 0x20 || c >= 0x7f {
			return false
		}
		switch c {
		case '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"',
			'/', '[', ']', '?', '=', '{', '}':
			return false
		}
	}
	return true
}
