// 网关入口：加载本地配置，启动 HTTP 服务并监听配置文件变化热更新。
package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/config"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/gateway"
	"github.com/h3yow/go-dynamic-proxy-gateway-go/internal/proxy"
)

func main() {
	confPath := flag.String("config", "config.json", "配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*confPath)
	if err != nil {
		log.Fatalf("加载初始配置失败: %v", err)
	}
	gw, err := gateway.New(cfg)
	if err != nil {
		log.Fatalf("构建网关失败: %v", err)
	}

	// 配置文件轮询热更新：mtime 变化即尝试 Reload。
	go watchConfig(*confPath, gw, time.Second)

	srv := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: proxy.NewHandler(gw),
	}

	go func() {
		log.Printf("网关启动，监听 %s", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP 服务异常退出: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("收到退出信号，网关关闭")
	_ = srv.Close()
}

// watchConfig 轮询配置文件 mtime，变化时尝试热更新。
// 非法配置会被拒绝并打印原因，旧配置继续生效。
func watchConfig(path string, gw *gateway.Gateway, interval time.Duration) {
	var lastMod time.Time
	if st, err := os.Stat(path); err == nil {
		lastMod = st.ModTime()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		st, err := os.Stat(path)
		if err != nil {
			continue
		}
		if !st.ModTime().After(lastMod) {
			continue
		}
		lastMod = st.ModTime()
		cfg, err := config.Load(path)
		if err != nil {
			log.Printf("[reload] 配置加载/校验失败，保持旧配置: %v", err)
			continue
		}
		if err := gw.Reload(cfg); err != nil {
			log.Printf("[reload] 热更新被拒绝，保持旧配置: %v", err)
			continue
		}
		log.Printf("[reload] 热更新成功")
	}
}
