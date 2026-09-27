package router

import (
	"testing"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

func mustParse(t *testing.T, j string) *config.Compiled {
	t.Helper()
	c, err := config.Parse(stringReader(j))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return c
}

const cfg = `{
  "pools":[{"name":"p"},{"name":"q"}],
  "upstreams":[
    {"name":"u","pool":"p","base_url":"http://1"},
    {"name":"v","pool":"q","base_url":"http://2"}],
  "routes":[
    {"name":"exact-deep","host":"api.local","prefix":"/v2/","pool":"q"},
    {"name":"exact-root","host":"api.local","prefix":"/","pool":"p"},
    {"name":"wild","host":"*.example.com","prefix":"/","pool":"p"}],
  "rate_limits":[
    {"host":"api.local","prefix":"/v2/","rate_per_sec":10,"burst":10},
    {"host":"","prefix":"/","rate_per_sec":1,"burst":1}]
}`

func TestLookupSpecificity(t *testing.T) {
	tb := New(mustParse(t, cfg))
	cases := []struct {
		host, path, pool, limited string
	}{
		{"api.local", "/v2/users", "q", "/v2/"},
		{"api.local:8080", "/v2/users", "q", "/v2/"}, // port stripped
		{"api.local", "/other", "p", "/"},
		{"a.example.com", "/x", "p", "/"},
		{"b.example.com", "/x", "p", "/"},
		{"notexample.com", "/x", "", ""}, // suffix must respect dot
	}
	for _, tc := range cases {
		m := tb.Lookup(tc.host, tc.path)
		gotPool := ""
		gotLimitPrefix := ""
		if m != nil {
			gotPool = m.Route.Pool
			if m.RateLimit != nil {
				gotLimitPrefix = m.RateLimit.Prefix
			}
		}
		if gotPool != tc.pool {
			t.Errorf("Lookup(%q,%q) pool=%q want=%q", tc.host, tc.path, gotPool, tc.pool)
		}
		if gotLimitPrefix != tc.limited {
			t.Errorf("Lookup(%q,%q) rate rule prefix=%q want=%q",
				tc.host, tc.path, gotLimitPrefix, tc.limited)
		}
		t.Logf("Lookup host=%q path=%q => pool=%q 判定: exact>wildcard, longest-prefix",
			tc.host, tc.path, gotPool)
	}
}

func TestPrefixBoundary(t *testing.T) {
	tb := New(mustParse(t, cfg))
	if m := tb.Lookup("api.local", "/v2abc"); m == nil || m.Route.Pool != "p" {
		t.Fatalf("/v2abc must not match /v2/ prefix, got %+v", m)
	}
}
