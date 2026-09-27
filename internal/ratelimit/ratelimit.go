// Package ratelimit implements per-client token bucket limiting.
package ratelimit

import (
	"sync"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

type bucket struct {
	mu         sync.Mutex
	tokens     float64
	lastMillis int64
}

type ruleBuckets struct {
	rule    config.RateLimit
	mu      sync.Mutex
	buckets map[string]*bucket
}

// Limiter is the set of rate limit rules with per-client buckets.
type Limiter struct {
	rules []*ruleBuckets
}

// New builds a limiter from rules.
func New(rules []config.RateLimit) *Limiter {
	l := &Limiter{}
	for i := range rules {
		l.rules = append(l.rules, &ruleBuckets{
			rule:    rules[i],
			buckets: map[string]*bucket{},
		})
	}
	return l
}

// Decision is the result of an Allow check.
type Decision struct {
	Allowed      bool
	RetryAfterMS int64 // > 0 when rejected: wait before retrying
	Rate         float64
	Burst        int
	MatchedRule  string
}

// Allow consumes one token for the client matching host/path. When no rule
// matches the request is allowed. Concurrent calls for the same client are
// serialized per bucket so the boundary (burst-th request rejected) is exact
// under high concurrency.
func (l *Limiter) Allow(client, host, path string, nowMillis int64) Decision {
	for _, rb := range l.rules {
		if !ruleMatches(rb.rule, host, path) {
			continue
		}
		rb.mu.Lock()
		b := rb.buckets[client]
		if b == nil {
			b = &bucket{tokens: float64(rb.rule.Burst), lastMillis: nowMillis}
			rb.buckets[client] = b
		}
		rb.mu.Unlock()

		b.mu.Lock()
		defer b.mu.Unlock()
		elapsedSec := float64(nowMillis-b.lastMillis) / 1000.0
		if elapsedSec > 0 {
			b.tokens += elapsedSec * rb.rule.RatePerSec
			if b.tokens > float64(rb.rule.Burst) {
				b.tokens = float64(rb.rule.Burst)
			}
			b.lastMillis = nowMillis
		}
		d := Decision{
			Rate:        rb.rule.RatePerSec,
			Burst:       rb.rule.Burst,
			MatchedRule: rb.rule.Host + rb.rule.Prefix,
		}
		if b.tokens >= 1 {
			b.tokens--
			d.Allowed = true
			return d
		}
		need := 1 - b.tokens
		d.RetryAfterMS = int64(need/rb.rule.RatePerSec*1000) + 1
		return d
	}
	return Decision{Allowed: true}
}

func ruleMatches(rule config.RateLimit, host, path string) bool {
	hostOK := rule.Host == "" || rule.Host == "*"
	if !hostOK {
		if len(rule.Host) > 1 && rule.Host[0:2] == "*." {
			hostOK = hasSuffixFold(host, rule.Host[1:])
		} else {
			hostOK = host == rule.Host
		}
	}
	if !hostOK {
		return false
	}
	return prefixMatch(rule.Prefix, path)
}

func prefixMatch(prefix, path string) bool {
	if len(path) < len(prefix) {
		return false
	}
	if path[:len(prefix)] != prefix {
		return false
	}
	if prefix == "/" || prefix[len(prefix)-1] == '/' {
		return true
	}
	return len(path) == len(prefix) || path[len(prefix)] == '/'
}

func hasSuffixFold(s, suffix string) bool {
	if len(s) < len(suffix) {
		return false
	}
	return equalFold(s[len(s)-len(suffix):], suffix)
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
