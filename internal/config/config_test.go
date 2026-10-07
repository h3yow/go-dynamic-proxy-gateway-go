package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func validConfig() *Config {
	return &Config{
		ListenAddr: "127.0.0.1:8080",
		Routes:     []Route{{PathPrefix: "/", Upstream: "svc"}},
		Upstreams: []Upstream{{
			Name:   "svc",
			Policy: "wrr",
			Backends: []Server{
				{Addr: "127.0.0.1:9001", Weight: 1},
			},
			HealthCheck: HealthCheckConf{
				Interval: 1e9, Timeout: 5e8,
				FailThreshold: 2, RecoverThreshold: 2,
			},
		}},
		Proxy: ProxyConf{UpstreamTimeout: 2e9, MaxRetries: 2},
	}
}

// TestHealthCheckPathOptional 旧配置不写 health_check.path 时照常加载，
// Path 为零值（空串），探测行为保持 TCP 建连。
func TestHealthCheckPathOptional(t *testing.T) {
	c := validConfig()
	if err := Validate(c); err != nil {
		t.Fatalf("不含 path 的旧配置应通过校验: %v", err)
	}
	if c.Upstreams[0].HealthCheck.Path != "" {
		t.Fatalf("未配置时 Path 应为空，实际 %q", c.Upstreams[0].HealthCheck.Path)
	}

	// 走真实 JSON 加载路径：旧格式配置（无 path 字段）能正常解析。
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("旧格式配置应能加载: %v", err)
	}
	if loaded.Upstreams[0].HealthCheck.Path != "" {
		t.Fatalf("旧格式配置加载后 Path 应为空，实际 %q", loaded.Upstreams[0].HealthCheck.Path)
	}
	t.Log("旧配置（无 health_check.path）校验与加载均通过，Path 为空 = TCP 探测，兼容性保持")
}

// TestHealthCheckPathValidation path 非空时必须是合法请求路径。
func TestHealthCheckPathValidation(t *testing.T) {
	c := validConfig()
	c.Upstreams[0].HealthCheck.Path = "/healthz"
	if err := Validate(c); err != nil {
		t.Fatalf("合法 path 应通过校验: %v", err)
	}
	t.Log("path=/healthz 校验通过")

	c.Upstreams[0].HealthCheck.Path = "healthz" // 缺少前导斜杠
	if err := Validate(c); err == nil {
		t.Fatal("path 不以 / 开头应被拒绝")
	} else {
		t.Logf("非法 path 被拒绝：%v", err)
	}
}
