package config

import (
	"errors"
	"strings"
	"testing"
)

const validCfg = `{
  "pools": [{"name":"p","strategy":"round_robin","request_timeout_ms":500,"max_attempts":2}],
  "upstreams": [
    {"name":"u1","pool":"p","base_url":"http://127.0.0.1:1","weight":2},
    {"name":"u2","pool":"p","base_url":"http://127.0.0.1:2"}
  ],
  "routes": [{"name":"r","host":"h.local","prefix":"/","pool":"p"}]
}`

func TestParseValidAndDefaults(t *testing.T) {
	c, err := Parse(strings.NewReader(validCfg))
	if err != nil {
		t.Fatalf("expected valid: %v", err)
	}
	if c.Pools["p"].Strategy != "round_robin" {
		t.Fatalf("strategy: %s", c.Pools["p"].Strategy)
	}
	if c.Pools["p"].MaxAttempts != 2 {
		t.Fatalf("attempts: %d", c.Pools["p"].MaxAttempts)
	}
	if got := c.Upstreams["p"][1].Weight; got != DefaultWeight {
		t.Fatalf("default weight = %d", got)
	}
	// only idempotent methods retried by default
	if c.Pools["p"].RetryMethods["POST"] {
		t.Fatal("POST must not be retried by default (duplicate side effects)")
	}
	if !c.Pools["p"].RetryMethods["GET"] {
		t.Fatal("GET should retry by default")
	}
}

func TestRejectReasons(t *testing.T) {
	cases := []struct {
		name string
		json string
		want Reason
	}{
		{"invalid json", `{`, ReasonInvalidJSON},
		{"unknown pool in route", `{"pools":[],"upstreams":[],
			"routes":[{"name":"r","host":"h.local","pool":"x"}]}`, ReasonRouteMissingPool},
		{"empty routes", `{"pools":[],"upstreams":[],"routes":[]}`, ReasonEmptyConfig},
		{"route missing pool", `{
			"pools":[{"name":"p"}],"upstreams":[{"name":"u","pool":"p","base_url":"http://x"}],
			"routes":[{"name":"r","host":"h.local","pool":"nope"}]}`, ReasonRouteMissingPool},
		{"duplicate route name", `{
			"pools":[{"name":"p"}],"upstreams":[{"name":"u","pool":"p","base_url":"http://x"}],
			"routes":[{"name":"r","host":"a.local","pool":"p"},
			          {"name":"r","host":"b.local","pool":"p"}]}`, ReasonDuplicateRouteName},
		{"bad strategy", `{
			"pools":[{"name":"p","strategy":"random"}],
			"upstreams":[{"name":"u","pool":"p","base_url":"http://x"}],
			"routes":[{"name":"r","host":"h.local","pool":"p"}]}`, ReasonPoolBadStrategy},
		{"pool no upstream", `{
			"pools":[{"name":"p"},{"name":"q"}],
			"upstreams":[{"name":"u","pool":"p","base_url":"http://x"}],
			"routes":[{"name":"r","host":"h.local","pool":"p"}]}`, ReasonPoolNoUpstream},
		{"bad url", `{
			"pools":[{"name":"p"}],
			"upstreams":[{"name":"u","pool":"p","base_url":"notaurl"}],
			"routes":[{"name":"r","host":"h.local","pool":"p"}]}`, ReasonUpstreamBadURL},
		{"bad weight", `{
			"pools":[{"name":"p"}],
			"upstreams":[{"name":"u","pool":"p","base_url":"http://x","weight":0},{"name":"v","pool":"p","base_url":"http://y","weight":-2}],
			"routes":[{"name":"r","host":"h.local","pool":"p"}]}`, ReasonUpstreamBadWeight},
		{"ratelimit invalid", `{
			"pools":[{"name":"p"}],"upstreams":[{"name":"u","pool":"p","base_url":"http://x"}],
			"routes":[{"name":"r","host":"h.local","pool":"p"}],
			"rate_limits":[{"host":"h.local","rate_per_sec":-1,"burst":2}]}`, ReasonRateLimitInvalid},
		{"duplicate host+prefix", `{
			"pools":[{"name":"p"}],"upstreams":[{"name":"u","pool":"p","base_url":"http://x"}],
			"routes":[{"name":"r1","host":"h.local","prefix":"/","pool":"p"},
			          {"name":"r2","host":"h.local","prefix":"/","pool":"p"}]}`, ReasonRouteConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(strings.NewReader(tc.json))
			if err == nil {
				t.Fatalf("expected rejection %q, got success", tc.want)
			}
			var re *RejectError
			if !errors.As(err, &re) {
				t.Fatalf("want *RejectError, got %T %v", err, err)
			}
			if re.Reason != tc.want {
				t.Fatalf("reason=%q want=%q msg=%q", re.Reason, tc.want, re.Message)
			}
			t.Logf("判定依据: reason=%s message=%s", re.Reason, re.Message)
		})
	}
}
