// 本地模拟上游：go run examples/upstream.go :9001 name-a
package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	addr, name := os.Args[1], os.Args[2]
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "backend=%s path=%s\n", name, r.URL.Path)
	})
	fmt.Println("upstream", name, "listening on", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Println(err)
	}
}
