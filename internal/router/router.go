// Package router 按 Host + 路径前缀匹配路由。
package router

import (
	"net/http"
	"sort"
	"strings"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
)

// Router 是不可变路由表，热更新时整体替换，因此可安全并发读。
type Router struct {
	routes []config.Route // 已按匹配优先级排序
}

// New 由路由配置构建路由表。
// 匹配优先级：先按 Host 精确匹配优先于通配（Host 为空），再按 PathPrefix 更长者优先。
func New(routes []config.Route) *Router {
	rs := make([]config.Route, len(routes))
	copy(rs, routes)
	sort.SliceStable(rs, func(i, j int) bool {
		hi, hj := rs[i].Host != "", rs[j].Host != ""
		if hi != hj {
			return hi // 指定 Host 的优先于通配
		}
		return len(rs[i].PathPrefix) > len(rs[j].PathPrefix) // 更长前缀优先
	})
	return &Router{routes: rs}
}

// Match 返回匹配请求的路由；未匹配返回 ok=false。
func (r *Router) Match(req *http.Request) (route config.Route, ok bool) {
	host := req.URL.Hostname()
	if host == "" {
		host = req.Host
	}
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	path := req.URL.Path
	for _, rt := range r.routes {
		if rt.Host != "" && !strings.EqualFold(rt.Host, host) {
			continue
		}
		if strings.HasPrefix(path, rt.PathPrefix) {
			return rt, true
		}
	}
	return config.Route{}, false
}
