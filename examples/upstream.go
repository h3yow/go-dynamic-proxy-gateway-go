// 本地模拟上游：go run examples/upstream.go :9001 name-a [unhealthy]
// 第三个参数传 unhealthy 时 /healthz 返回 500（进程仍监听），用于演示业务级摘除。
package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	addr, name := os.Args[1], os.Args[2]
	unhealthy := len(os.Args) > 3 && os.Args[3] == "unhealthy"

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if unhealthy {
			http.Error(w, "business degraded", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "backend=%s path=%s\n", name, r.URL.Path)
	})
	fmt.Printf("upstream %s listening on %s (healthz unhealthy=%v)\n", name, addr, unhealthy)
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Println(err)
	}
}
