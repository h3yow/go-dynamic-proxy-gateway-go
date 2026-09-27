// Package ratelimit 实现按客户端的令牌桶限流。
package ratelimit

import (
	"sync"
	"time"
)

type bucket struct {
	tokens float64
	last   time.Time
}

// Limiter 按客户端 key 限流，并发安全。
type Limiter struct {
	rate  float64
	burst int

	mu      sync.Mutex
	buckets map[string]*bucket
	now     func() time.Time // 可注入，便于测试
}

// New 创建限流器：rate 为每秒补充令牌数，burst 为桶容量。
func New(rate float64, burst int) *Limiter {
	return &Limiter{
		rate:    rate,
		burst:   burst,
		buckets: make(map[string]*bucket),
		now:     time.Now,
	}
}

// Allow 判断该客户端本次请求是否放行：有令牌则扣一个放行，否则拒绝。
func (l *Limiter) Allow(clientKey string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[clientKey]
	if !ok {
		b = &bucket{tokens: float64(l.burst), last: l.now()}
		l.buckets[clientKey] = b
	}
	now := l.now()
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * l.rate
	if max := float64(l.burst); b.tokens > max {
		b.tokens = max
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
