// Package upstream 管理上游集群：健康检查、摘除与恢复、粘性会话选择。
package upstream

import (
	"fmt"
	"hash/fnv"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/balancer"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

type backendState struct {
	server     config.Server
	healthy    bool
	consecFail int
	consecOK   int
}

// Pool 是一个上游集群的运行时视图，并发安全。
type Pool struct {
	name   string
	policy string
	hc     config.HealthCheckConf

	mu       sync.Mutex
	backends map[string]*backendState

	picker atomic.Value // balancer.Picker，仅覆盖当前健康后端
}

// NewPool 创建集群，初始时所有后端视为健康。
func NewPool(u config.Upstream) *Pool {
	p := &Pool{
		name:     u.Name,
		policy:   u.Policy,
		hc:       u.HealthCheck,
		backends: make(map[string]*backendState, len(u.Backends)),
	}
	for _, s := range u.Backends {
		p.backends[s.Addr] = &backendState{server: s, healthy: true}
	}
	p.rebuildPickerLocked()
	return p
}

// Name 返回集群名。
func (p *Pool) Name() string { return p.name }

// Picker 返回当前健康后端上的负载均衡器快照。
func (p *Pool) Picker() balancer.Picker {
	if v := p.picker.Load(); v != nil {
		return v.(balancer.Picker)
	}
	return nil
}

// rebuildPickerLocked 根据健康后端重建 Picker，调用方须持有锁。
func (p *Pool) rebuildPickerLocked() {
	var healthy []config.Server
	for _, b := range p.backends {
		if b.healthy {
			healthy = append(healthy, b.server)
		}
	}
	sort.Slice(healthy, func(i, j int) bool { return healthy[i].Addr < healthy[j].Addr })
	if len(healthy) == 0 {
		p.picker.Store(balancer.New(p.policy, nil))
		return
	}
	p.picker.Store(balancer.New(p.policy, healthy))
}

// report 统一处理主动健康检查与被动请求结果上报，驱动摘除/恢复状态机。
// 返回状态是否发生变化（用于日志与测试判定）。
func (p *Pool) report(addr string, success bool) (changed bool, nowHealthy bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, ok := p.backends[addr]
	if !ok {
		return false, false
	}
	if success {
		b.consecFail = 0
		if !b.healthy {
			b.consecOK++
			if b.consecOK >= p.hc.RecoverThreshold {
				b.healthy = true
				b.consecOK = 0
				p.rebuildPickerLocked()
				return true, true
			}
		}
		return false, b.healthy
	}
	b.consecOK = 0
	b.consecFail++
	if b.healthy && b.consecFail >= p.hc.FailThreshold {
		b.healthy = false
		b.consecFail = 0
		p.rebuildPickerLocked()
		return true, false
	}
	return false, b.healthy
}

// ReportResult 上报一次真实请求的结果（被动健康检查）。
func (p *Pool) ReportResult(addr string, success bool) {
	if changed, healthy := p.report(addr, success); changed {
		logf("集群 %s 后端 %s 被动状态变更 -> healthy=%v", p.name, addr, healthy)
	}
}

// HealthyBackends 返回当前健康后端地址列表（有序，供测试与日志使用）。
func (p *Pool) HealthyBackends() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for addr, b := range p.backends {
		if b.healthy {
			out = append(out, addr)
		}
	}
	sort.Strings(out)
	return out
}

// PickSticky 用 rendezvous 哈希为会话 key 选择一个健康后端。
// 确定性保证：只要健康集合不变，同一 key 永远选中同一后端；
// 某后端被摘除时，只有映射到它的会话迁移，且迁移目标由哈希唯一确定。
func (p *Pool) PickSticky(key string) (config.Server, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var best *backendState
	var bestHash uint64
	for _, b := range p.backends {
		if !b.healthy {
			continue
		}
		h := hashKey(key, b.server.Addr)
		if best == nil || h > bestHash {
			best, bestHash = b, h
		}
	}
	if best == nil {
		return config.Server{}, false
	}
	return best.server, true
}

func hashKey(key, addr string) uint64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "%s|%s", key, addr)
	return h.Sum64()
}

// Start 启动主动健康检查循环，stop 关闭后退出。
func (p *Pool) Start(stop <-chan struct{}) {
	ticker := time.NewTicker(p.hc.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			p.checkOnce()
		}
	}
}

func (p *Pool) checkOnce() {
	p.mu.Lock()
	addrs := make([]string, 0, len(p.backends))
	for addr := range p.backends {
		addrs = append(addrs, addr)
	}
	p.mu.Unlock()

	for _, addr := range addrs {
		ok := probe(addr, p.hc.Timeout)
		if changed, healthy := p.report(addr, ok); changed {
			logf("集群 %s 后端 %s 健康检查状态变更 -> healthy=%v", p.name, addr, healthy)
		}
	}
}

// probe 通过 TCP 建连探测后端是否存活。
func probe(addr string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
