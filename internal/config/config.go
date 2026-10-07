// Package config 定义网关配置结构、加载与校验。
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Config 是网关的完整配置。
type Config struct {
	ListenAddr string        `json:"listen_addr"`
	Routes     []Route       `json:"routes"`
	Upstreams  []Upstream    `json:"upstreams"`
	RateLimit  RateLimitConf `json:"rate_limit"`
	Proxy      ProxyConf     `json:"proxy"`
}

// Route 描述一条 host + 路径前缀到上游集群的路由。
type Route struct {
	Host       string `json:"host"`        // 为空表示匹配任意 Host
	PathPrefix string `json:"path_prefix"` // 必须以 / 开头
	Upstream   string `json:"upstream"`    // 引用 Upstreams 中的 Name
	Sticky     bool   `json:"sticky"`      // 是否启用粘性会话
}

// Upstream 是一个上游集群。
type Upstream struct {
	Name        string          `json:"name"`
	Policy      string          `json:"policy"` // "wrr" 加权轮询 | "least_conn" 最少连接
	Backends    []Server        `json:"backends"`
	HealthCheck HealthCheckConf `json:"health_check"`
}

// Server 是集群中的一个后端实例。
type Server struct {
	Addr   string `json:"addr"` // host:port
	Weight int    `json:"weight"`
}

// HealthCheckConf 健康检查参数。
type HealthCheckConf struct {
	Interval         time.Duration `json:"interval"`
	Timeout          time.Duration `json:"timeout"`
	FailThreshold    int           `json:"fail_threshold"`    // 连续失败多少次摘除
	RecoverThreshold int           `json:"recover_threshold"` // 连续成功多少次恢复
	// Path 非空时按业务结果探测：GET http://addr+Path，2xx/3xx 视为健康；
	// 为空时保持原有 TCP 建连探测（旧配置不写该字段行为不变）。
	Path string `json:"path,omitempty"`
}

// RateLimitConf 按客户端限流参数（令牌桶）。
type RateLimitConf struct {
	Enabled bool    `json:"enabled"`
	Rate    float64 `json:"rate"`  // 每秒补充令牌数
	Burst   int     `json:"burst"` // 桶容量
}

// ProxyConf 代理行为参数。
type ProxyConf struct {
	UpstreamTimeout time.Duration `json:"upstream_timeout"` // 单次上游请求超时
	MaxRetries      int           `json:"max_retries"`      // 幂等请求的最大重试次数
}

// Load 从 JSON 文件加载配置并做基本校验。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("解析配置失败: %w", err)
	}
	if err := Validate(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate 校验配置合法性，返回可读的错误原因。
func Validate(c *Config) error {
	if c.ListenAddr == "" {
		return fmt.Errorf("listen_addr 不能为空")
	}
	if len(c.Upstreams) == 0 {
		return fmt.Errorf("至少需要一个 upstream")
	}
	if len(c.Routes) == 0 {
		return fmt.Errorf("至少需要一条 route")
	}

	upstreams := make(map[string]bool, len(c.Upstreams))
	for _, u := range c.Upstreams {
		if u.Name == "" {
			return fmt.Errorf("upstream 名称不能为空")
		}
		if upstreams[u.Name] {
			return fmt.Errorf("upstream %q 重复定义", u.Name)
		}
		upstreams[u.Name] = true
		switch u.Policy {
		case "wrr", "least_conn":
		default:
			return fmt.Errorf("upstream %q 的 policy %q 非法，仅支持 wrr / least_conn", u.Name, u.Policy)
		}
		if len(u.Backends) == 0 {
			return fmt.Errorf("upstream %q 至少需要一个 backend", u.Name)
		}
		for _, b := range u.Backends {
			if b.Addr == "" {
				return fmt.Errorf("upstream %q 存在空 addr 的 backend", u.Name)
			}
			if b.Weight <= 0 {
				return fmt.Errorf("upstream %q 的 backend %q 权重必须为正整数", u.Name, b.Addr)
			}
		}
		hc := u.HealthCheck
		if hc.Interval <= 0 || hc.Timeout <= 0 {
			return fmt.Errorf("upstream %q 的 health_check interval/timeout 必须为正", u.Name)
		}
		if hc.FailThreshold <= 0 || hc.RecoverThreshold <= 0 {
			return fmt.Errorf("upstream %q 的 health_check 阈值必须为正整数", u.Name)
		}
		if hc.Path != "" && !strings.HasPrefix(hc.Path, "/") {
			return fmt.Errorf("upstream %q 的 health_check path %q 必须以 / 开头", u.Name, hc.Path)
		}
	}

	for _, r := range c.Routes {
		if !strings.HasPrefix(r.PathPrefix, "/") {
			return fmt.Errorf("route 的 path_prefix %q 必须以 / 开头", r.PathPrefix)
		}
		if !upstreams[r.Upstream] {
			return fmt.Errorf("route (host=%q prefix=%q) 引用了不存在的 upstream %q", r.Host, r.PathPrefix, r.Upstream)
		}
	}

	if c.RateLimit.Enabled {
		if c.RateLimit.Rate <= 0 || c.RateLimit.Burst <= 0 {
			return fmt.Errorf("rate_limit 启用时 rate/burst 必须为正")
		}
	}
	if c.Proxy.UpstreamTimeout <= 0 {
		return fmt.Errorf("proxy.upstream_timeout 必须为正")
	}
	if c.Proxy.MaxRetries < 0 {
		return fmt.Errorf("proxy.max_retries 不能为负")
	}
	return nil
}
