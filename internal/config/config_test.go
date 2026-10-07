package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func validCfg() *Config {
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
				Interval: time.Second, Timeout: time.Second,
				FailThreshold: 2, RecoverThreshold: 2,
			},
		}},
		Proxy: ProxyConf{UpstreamTimeout: time.Second, MaxRetries: 2},
	}
}

// TestLegacyConfigStillValid 不含新探测字段的旧配置必须照常加载通过。
func TestLegacyConfigStillValid(t *testing.T) {
	if err := Validate(validCfg()); err != nil {
		t.Fatalf("旧配置（无 probe_path/success_statuses）应校验通过，实际: %v", err)
	}
	t.Log("旧配置不写 probe_path / success_statuses 时校验通过，保持 TCP 探测语义")
}

// TestNewProbeFields 新字段合法/非法的校验口径。
func TestNewProbeFields(t *testing.T) {
	c := validCfg()
	c.Upstreams[0].HealthCheck.ProbePath = "/healthz"
	c.Upstreams[0].HealthCheck.SuccessStatuses = []int{200, 204}
	if err := Validate(c); err != nil {
		t.Fatalf("合法业务探测配置应通过，实际: %v", err)
	}

	bad := validCfg()
	bad.Upstreams[0].HealthCheck.ProbePath = "healthz"
	err := Validate(bad)
	if err == nil || !strings.Contains(err.Error(), "probe_path") {
		t.Fatalf("probe_path 不以 / 开头应被拒绝，实际: %v", err)
	}

	bad2 := validCfg()
	bad2.Upstreams[0].HealthCheck.SuccessStatuses = []int{99}
	if err := Validate(bad2); err == nil || !strings.Contains(err.Error(), "success_statuses") {
		t.Fatalf("非法状态码应被拒绝，实际: %v", err)
	}
	t.Log("probe_path 必须以 / 开头，success_statuses 必须是 100-599")
}

// TestLoadLegacyJSON 旧版 JSON（缺新字段）能正常解析。
func TestLoadLegacyJSON(t *testing.T) {
	legacy := `{
		"listen_addr": "127.0.0.1:8080",
		"routes": [{"path_prefix": "/", "upstream": "svc"}],
		"upstreams": [{"name": "svc", "policy": "wrr",
			"backends": [{"addr": "127.0.0.1:9001", "weight": 1}],
			"health_check": {"interval": 1000000000, "timeout": 500000000,
				"fail_threshold": 2, "recover_threshold": 2}}],
		"rate_limit": {"enabled": false, "rate": 0, "burst": 0},
		"proxy": {"upstream_timeout": 2000000000, "max_retries": 2}
	}`
	f := t.TempDir() + "/config.json"
	if err := os.WriteFile(f, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(f)
	if err != nil {
		t.Fatalf("旧 JSON 应能加载，实际: %v", err)
	}
	if c.Upstreams[0].HealthCheck.ProbePath != "" {
		t.Fatalf("旧配置 ProbePath 应为空，实际 %q", c.Upstreams[0].HealthCheck.ProbePath)
	}
	t.Log("旧版 JSON 缺省 probe_path 解析成功，沿用 TCP 探测")
}
