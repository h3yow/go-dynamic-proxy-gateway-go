// Package balancer 实现加权轮询与最少连接两种负载均衡策略。
package balancer

import (
	"sync"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

// Picker 在一次请求中挑选一个健康后端。
type Picker interface {
	// Pick 返回选中的后端；done 在请求结束时调用（用于连接数统计），success 表示请求是否成功。
	Pick() (s config.Server, done func(success bool), ok bool)
}

// New 按策略名构造 Picker。未知策略回退到加权轮询。
func New(policy string, servers []config.Server) Picker {
	if policy == "least_conn" {
		return newLeastConn(servers)
	}
	return newWRR(servers)
}

// ---- 平滑加权轮询（smooth weighted round robin）----

type wrrNode struct {
	server  config.Server
	current int // 当前权重（运行时动态调整）
}

type wrr struct {
	mu    sync.Mutex
	nodes []wrrNode
	total int
}

func newWRR(servers []config.Server) *wrr {
	w := &wrr{nodes: make([]wrrNode, len(servers))}
	for i, s := range servers {
		w.nodes[i].server = s
		w.total += s.Weight
	}
	return w
}

func (w *wrr) Pick() (config.Server, func(bool), bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.nodes) == 0 {
		return config.Server{}, nil, false
	}
	best := -1
	for i := range w.nodes {
		w.nodes[i].current += w.nodes[i].server.Weight
		if best == -1 || w.nodes[i].current > w.nodes[best].current {
			best = i
		}
	}
	w.nodes[best].current -= w.total
	return w.nodes[best].server, func(bool) {}, true
}

// ---- 最少连接 ----

type lcNode struct {
	server config.Server
	active int64 // 当前在途请求数
}

type leastConn struct {
	mu    sync.Mutex
	nodes []*lcNode
}

func newLeastConn(servers []config.Server) *leastConn {
	lc := &leastConn{nodes: make([]*lcNode, len(servers))}
	for i, s := range servers {
		lc.nodes[i] = &lcNode{server: s}
	}
	return lc
}

func (lc *leastConn) Pick() (config.Server, func(bool), bool) {
	lc.mu.Lock()
	if len(lc.nodes) == 0 {
		lc.mu.Unlock()
		return config.Server{}, nil, false
	}
	best := lc.nodes[0]
	for _, n := range lc.nodes[1:] {
		if n.active < best.active {
			best = n
		}
	}
	best.active++
	lc.mu.Unlock()

	var once sync.Once
	done := func(bool) {
		once.Do(func() {
			lc.mu.Lock()
			best.active--
			lc.mu.Unlock()
		})
	}
	return best.server, done, true
}
