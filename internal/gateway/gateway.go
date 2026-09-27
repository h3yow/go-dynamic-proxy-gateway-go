// Package gateway 持有原子配置快照并负责热更新。
package gateway

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/ratelimit"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/router"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/upstream"
)

// ErrInvalidConfig 表示新配置非法，热更新被拒绝；旧配置继续生效。
var ErrInvalidConfig = errors.New("配置非法，热更新被拒绝")

// Snapshot 是一份不可变的运行时视图：一次请求全程使用同一份快照，
// 因此热更新不会造成“半新半旧”的配置被同一次请求看到。
type Snapshot struct {
	Cfg     *config.Config
	Router  *router.Router
	Pools   map[string]*upstream.Pool
	Limiter *ratelimit.Limiter // nil 表示限流未启用

	stop chan struct{} // 关闭以停止本快照所有健康检查循环
}

// Gateway 持有当前生效的快照，Reload 时原子替换。
type Gateway struct {
	snap atomic.Value // *Snapshot

	mu sync.Mutex // 串行化 Reload
}

// New 用初始配置构建网关；配置非法时返回错误。
func New(cfg *config.Config) (*Gateway, error) {
	if err := config.Validate(cfg); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	g := &Gateway{}
	g.snap.Store(buildSnapshot(cfg))
	return g, nil
}

func buildSnapshot(cfg *config.Config) *Snapshot {
	s := &Snapshot{
		Cfg:    cfg,
		Router: router.New(cfg.Routes),
		Pools:  make(map[string]*upstream.Pool, len(cfg.Upstreams)),
		stop:   make(chan struct{}),
	}
	for _, u := range cfg.Upstreams {
		p := upstream.NewPool(u)
		s.Pools[u.Name] = p
		go p.Start(s.stop)
	}
	if cfg.RateLimit.Enabled {
		s.Limiter = ratelimit.New(cfg.RateLimit.Rate, cfg.RateLimit.Burst)
	}
	return s
}

// Snapshot 返回当前生效的快照，调用方可安全并发使用。
func (g *Gateway) Snapshot() *Snapshot { return g.snap.Load().(*Snapshot) }

// Reload 校验并原子切换到新配置。
// 新配置非法：返回包装了 ErrInvalidConfig 的错误，旧配置完全不受影响。
// 切换成功：正在使用旧快照的请求继续走完旧快照，新请求使用新快照。
func (g *Gateway) Reload(cfg *config.Config) error {
	if err := config.Validate(cfg); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	old := g.snap.Load().(*Snapshot)
	g.snap.Store(buildSnapshot(cfg))
	close(old.stop) // 旧快照的健康检查循环退出；已在途的请求不受影响
	return nil
}
