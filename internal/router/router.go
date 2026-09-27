// Package router performs host + path-prefix route matching.
package router

import (
	"sort"
	"strings"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

// Match is the result of a route lookup.
type Match struct {
	Route     config.Route
	RateLimit *config.RateLimit
}

type routeEntry struct {
	route    config.Route
	wildcard bool // host starts with "*.": matches suffix
	host     string
	prefix   string
}

type limitEntry struct {
	rule     config.RateLimit
	wildcard bool
	anyHost  bool
	host     string
	prefix   string
}

// Table is an immutable route table.
type Table struct {
	routes []routeEntry
	limits []limitEntry
}

// New builds a table from compiled configuration. Entries are pre-sorted so
// Lookup only scans in specificity order.
func New(c *config.Compiled) *Table {
	t := &Table{}
	for _, rt := range c.Routes {
		host := rt.Host
		wild := strings.HasPrefix(host, "*.")
		if wild {
			host = host[1:] // ".example.com" suffix match
		}
		t.routes = append(t.routes, routeEntry{
			route: rt, wildcard: wild, host: host, prefix: rt.Prefix,
		})
	}
	sort.Slice(t.routes, func(i, j int) bool {
		a, b := t.routes[i], t.routes[j]
		if a.wildcard != b.wildcard {
			return !a.wildcard // exact host first
		}
		if len(a.prefix) != len(b.prefix) {
			return len(a.prefix) > len(b.prefix) // longer prefix first
		}
		return a.host < b.host
	})
	for _, rl := range c.RateLimits {
		e := limitEntry{rule: rl, prefix: rl.Prefix}
		switch {
		case rl.Host == "" || rl.Host == "*":
			e.anyHost = true
		case strings.HasPrefix(rl.Host, "*."):
			e.wildcard = true
			e.host = rl.Host[1:]
		default:
			e.host = rl.Host
		}
		t.limits = append(t.limits, e)
	}
	sort.Slice(t.limits, func(i, j int) bool {
		a, b := t.limits[i], t.limits[j]
		ai, bi := 2, 2 // specificity rank: 0 exact, 1 wildcard, 2 any
		if a.anyHost {
			ai = 2
		} else if a.wildcard {
			ai = 1
		} else {
			ai = 0
		}
		if b.anyHost {
			bi = 2
		} else if b.wildcard {
			bi = 1
		} else {
			bi = 0
		}
		if ai != bi {
			return ai < bi
		}
		return len(a.prefix) > len(b.prefix)
	})
	return t
}

func stripPort(hostport string) string {
	if i := strings.IndexByte(hostport, ':'); i >= 0 {
		return hostport[:i]
	}
	return hostport
}

func hostMatches(e routeEntry, host string) bool {
	if e.wildcard {
		return strings.HasSuffix(host, e.host)
	}
	return host == e.host
}

// Lookup returns the most specific matching route for host and path. Prefix
// matching is segment-aware for "/"-terminated prefixes; an exact prefix
// without trailing "/" matches exactly (besides the "/" default).
func (t *Table) Lookup(host, path string) *Match {
	host = strings.ToLower(stripPort(host))
	if path == "" {
		path = "/"
	}
	var found *routeEntry
	for i := range t.routes {
		e := &t.routes[i]
		if !hostMatches(*e, host) {
			continue
		}
		if prefixMatch(e.prefix, path) {
			found = e
			break // sorted by specificity
		}
	}
	if found == nil {
		return nil
	}
	m := &Match{Route: found.route}
	for i := range t.limits {
		le := &t.limits[i]
		hostOK := le.anyHost
		if !hostOK {
			if le.wildcard {
				hostOK = strings.HasSuffix(host, le.host)
			} else {
				hostOK = host == le.host
			}
		}
		if hostOK && prefixMatch(le.prefix, path) {
			rl := le.rule
			m.RateLimit = &rl
			break
		}
	}
	return m
}

// prefixMatch reports whether path matches prefix. A prefix ending in "/"
// matches everything under it; otherwise the match must be exact or at a
// segment boundary.
func prefixMatch(prefix, path string) bool {
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	if prefix == "/" || strings.HasSuffix(prefix, "/") {
		return true
	}
	return len(path) == len(prefix) || path[len(prefix)] == '/'
}
