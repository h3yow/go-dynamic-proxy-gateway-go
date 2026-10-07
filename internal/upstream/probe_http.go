package upstream

import (
	"context"
	"io"
	"net/http"
	"time"
)

// probeClient 不设客户端级超时：每次探测用带 timeout 的 context 控制，
// 并复用连接池，避免高频探测积累 TIME_WAIT。
var probeClient = &http.Client{}

// httpProbe 对后端发起 GET http://<addr><path> 业务探测：
//   - 建连失败、超时、读完响应体前出错：失败；
//   - 状态码命中 successStatuses（非空时）：成功；
//   - successStatuses 为空时：所有 2xx 成功，其余（含 5xx/4xx）失败。
//
// 必须读完并关闭响应体，连接才能复用。
func httpProbe(addr, path string, successStatuses []int, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		return false
	}
	resp, err := probeClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))

	if len(successStatuses) > 0 {
		for _, code := range successStatuses {
			if resp.StatusCode == code {
				return true
			}
		}
		return false
	}
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}
