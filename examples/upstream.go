// 本地模拟上游：go run ./examples :9001 alpha
// 提供 /healthz 业务健康端点（供网关主动探测），
// 以及 /admin/health?ok=0|1 开关，用于本地复现“端口活着但业务故障”的场景。
package main

import (
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
)

func main() {
	addr, name := os.Args[1], os.Args[2]
	var bizOK atomic.Bool
	bizOK.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if !bizOK.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "backend=%s 业务故障中\n", name)
			return
		}
		fmt.Fprintf(w, "backend=%s path=%s\n", name, r.URL.Path)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if !bizOK.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/admin/health", func(w http.ResponseWriter, r *http.Request) {
		bizOK.Store(r.URL.Query().Get("ok") != "0")
		fmt.Fprintf(w, "backend=%s healthy=%v\n", name, bizOK.Load())
	})
	fmt.Println("upstream", name, "listening on", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Println(err)
	}
}
