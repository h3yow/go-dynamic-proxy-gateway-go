// Package config defines the gateway configuration model, JSON parsing and
// validation. Validation failures are returned with a distinguished reason
// code so that reload rejections are distinguishable.
package config

// Config is the root gateway configuration.
type Config struct {
	Routes     []Route     `json:"routes"`
	Upstreams  []Upstream  `json:"upstreams"`
	RateLimits []RateLimit `json:"rate_limits"`
}

// Route maps a host and path prefix to an upstream pool.
type Route struct {
	Name   string `json:"name"`
	Host   string `json:"host"`
	Prefix string `json:"prefix"`
	Pool   string `json:"pool"`
	// StickyCookie, when non-empty, enables sticky sessions for this route.
	StickyCookie string `json:"sticky_cookie,omitempty"`
	// StickyTTL is the sticky binding lifetime; 0 means session-only.
	StickyTTLSeconds int `json:"sticky_ttl_seconds,omitempty"`
}

// Upstream defines one backend peer in a pool.
type Upstream struct {
	Name    string `json:"name"`
	Pool    string `json:"pool"`
	BaseURL string `json:"base_url"`
	Weight  int    `json:"weight"`
}

// Pool is an inferred grouping of upstreams with balancing policy.
type Pool struct {
	Name             string
	Strategy         string // "round_robin" | "least_conn"
	FailThreshold    int    // consecutive failures before ejection
	HealthIntervalMS int    // probe interval while unhealthy
	RequestTimeoutMS int    // per-attempt upstream timeout
	MaxAttempts      int    // total attempts including the first (1 = no retry)
	RetryMethods     map[string]bool
}

// RateLimit is a per-client token bucket rule scoped to a host/prefix.
type RateLimit struct {
	Host         string  `json:"host"`
	Prefix       string  `json:"prefix"`
	RatePerSec   float64 `json:"rate_per_sec"`
	Burst        int     `json:"burst"`
	ClientHeader string  `json:"client_header,omitempty"`
}

// Reason is a machine-readable rejection reason.
type Reason string

const (
	ReasonOK                    Reason = ""
	ReasonInvalidJSON           Reason = "invalid_json"
	ReasonDuplicateRouteName    Reason = "duplicate_route_name"
	ReasonRouteMissingPool      Reason = "route_missing_pool"
	ReasonRouteBadHost          Reason = "route_bad_host"
	ReasonRouteBadPrefix        Reason = "route_bad_prefix"
	ReasonRouteConflict         Reason = "route_conflict"
	ReasonUpstreamUnknownPool   Reason = "upstream_unknown_pool"
	ReasonUpstreamBadURL        Reason = "upstream_bad_url"
	ReasonUpstreamBadWeight     Reason = "upstream_bad_weight"
	ReasonUpstreamDuplicateName Reason = "upstream_duplicate_name"
	ReasonPoolNoUpstream        Reason = "pool_no_upstream"
	ReasonPoolBadStrategy       Reason = "pool_bad_strategy"
	ReasonRateLimitInvalid      Reason = "ratelimit_invalid"
	ReasonStickyNoTTL           Reason = "sticky_invalid"
	ReasonEmptyConfig           Reason = "empty_config"
)

// RejectError describes why a configuration was rejected.
type RejectError struct {
	Reason  Reason
	Message string
}

func (e *RejectError) Error() string { return string(e.Reason) + ": " + e.Message }
