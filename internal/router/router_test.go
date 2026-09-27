package router

import (
	"net/http/httptest"
	"testing"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

// TestMatchPriority 验证 Host 精确优先于通配、长前缀优先于短前缀。
func TestMatchPriority(t *testing.T) {
	r := New([]config.Route{
		{Host: "", PathPrefix: "/", Upstream: "catch-all"},
		{Host: "", PathPrefix: "/api/", Upstream: "api"},
		{Host: "api.example.com", PathPrefix: "/", Upstream: "host-specific"},
	})

	cases := []struct {
		host, path, want string
	}{
		{"api.example.com", "/anything", "host-specific"}, // Host 精确优先
		{"other.com", "/api/users", "api"},                // 长前缀优先
		{"other.com", "/home", "catch-all"},
	}
	for _, c := range cases {
		req := httptest.NewRequest("GET", "http://"+c.host+c.path, nil)
		got, ok := r.Match(req)
		if !ok || got.Upstream != c.want {
			t.Fatalf("host=%s path=%s 应路由到 %s，实际 %+v", c.host, c.path, c.want, got)
		}
		t.Logf("host=%s path=%s -> %s", c.host, c.path, c.want)
	}

	req := httptest.NewRequest("GET", "http://other.com/api", nil) // 不带尾斜杠，不匹配 /api/
	if got, _ := r.Match(req); got.Upstream != "catch-all" {
		t.Fatalf("/api 不应匹配前缀 /api/，实际 %s", got.Upstream)
	}
	t.Log("/api 不匹配 /api/，回退到 catch-all，前缀语义正确")
}
