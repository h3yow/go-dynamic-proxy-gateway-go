# 动态反向代理网关（Go，全本地模拟）

一个零外部依赖的动态反向代理网关：路由、上游、健康检查、负载均衡、粘性会话、
限流、超时重试与热更新全部在本地用文件 / 进程模拟实现，可直接在本机运行和验证。

- Go 标准库实现，无第三方依赖
- 代理端口默认 `:8080`，管理端口默认 `:18080`
- 模拟上游 `127.0.0.1:19001~19003`，行为由 `runtime/<name>-behavior.json` 文件热控制

## 快速开始

```bash
go run .                      # 首次启动会自动生成 configs/gateway.json 和 runtime/* 行为文件
# 另开终端：
curl -s -H 'Host: web.local' http://127.0.0.1:8080/hello
curl -s http://127.0.0.1:18080/admin/snapshot | jq .
```

模拟上游行为文件（原子写入即生效，网关每次请求都会重新读取）：

```jsonc
// runtime/b-behavior.json
{
  "latency_ms": 0,   // 响应延迟毫秒（用于制造超时；客户端取消时响应被丢弃）
  "fail_every": 0,   // >0 时每 N 个请求返回 500
  "failing": false,  // true 时全部请求返回 500（用于摘除/恢复演练）
  "slow": false
}
```

## 配置与热更新

配置文件 `configs/gateway.json`，结构见 `configs/gateway.json`（首次自动生成）。
热更新有两条入口：

```bash
# 1) 管理 API
curl -s -X POST --data-binary @configs/gateway.json http://127.0.0.1:18080/admin/reload

# 2) SIGHUP（重新读取 -config 指定的文件）
kill -HUP <pid>
```

**原子性保证**：

1. 新配置先完整解析 + 校验 + 构建出全新的 `Engine`，构建成功前不动当前引擎；
2. 发布是一次 `atomic.Pointer` 指针交换——任何请求要么全程用旧引擎，要么全程用新引擎，
   不可能看到“半新半旧”；
3. 每个请求在入口处钉住当时的引擎世代（generation），因此 reload 时在途请求继续由
   旧引擎处理完毕，不会被中断；
4. 非法配置返回 `422`，body 与响应头 `X-Gateway-Reject-Reason` 给出**可区分的拒绝原因**，
   旧配置继续生效。

拒绝原因（`config.Reason`）：

| reason | 含义 |
|---|---|
| `invalid_json` | JSON 语法错误 / 未知字段 |
| `empty_config` | 路由或上游为空 |
| `route_missing_pool` / `route_bad_host` / `route_bad_prefix` / `duplicate_route_name` / `route_conflict` | 路由问题 |
| `upstream_unknown_pool` / `upstream_bad_url` / `upstream_duplicate_name` / `upstream_bad_weight` | 上游问题 |
| `pool_no_upstream` / `pool_bad_strategy` | 上游池问题 |
| `ratelimit_invalid` / `sticky_invalid` | 限流或粘性配置问题 |

## 路由匹配

- 先按 **Host 域名**匹配：精确域名优先于 `*.example.com` 通配域名（后缀匹配，点边界）；
  Host 上的端口会被剥离。
- 再按 **路径前缀**匹配：同域名下最长前缀优先（`/v2/` 比 `/` 优先）。
  前缀以 `/` 结尾时匹配其下全部路径；不以 `/` 结尾时只在段边界匹配（`/v2/` 不会命中 `/v2abc`）。
- 限流规则独立按同样的“精确域名 > 通配域名 > 任意域名，再长前缀”优先级选择。
- 未命中任何路由返回 `404`，`X-Gateway-Reason: no_route`。

## 负载均衡与粘性会话

每个池 `strategy` 可选：

- `round_robin`：**平滑加权轮询**（nginx 风格 interleaved WRR），按 `weight` 比例分配，
  高权重节点不会被连续打满；
- `least_conn`：**最少连接**，比较 `在途请求数 / 权重`（交叉相乘避免浮点），
  平局时用 WRR 打破，行为确定。

**粘性会话**（路由配置 `sticky_cookie`，可选 `sticky_ttl_seconds`）：

- 客户端首次访问：网关签发只含**不透明随机 token** 的 Cookie，token→peer 的映射只存在服务端；
- 之后同 token 的请求**恒定**发往绑定 peer，即使该 peer 在途连接更多也不会漂移；
- 绑定 peer 被摘除 / 配置中被删除 / 探测中：返回可区分的 `sticky_peer_down`，
  网关做**一次性显式迁移**到健康 peer、删除旧 token 并重新签发 Cookie，日志打印
  `sticky ... DOWN => migrated newpeer=...`；绝不会让一个会话同时落到两个 peer；
- TTL 过期或网关重启后旧 token 解析为空，按新会话重新绑定（行为可解释）；
- 已在途的绑定 peer 请求自身失败时，后续重试才重新绑定，不会静默拆分会话。

## 健康检查

每个 peer 一个状态机（`internal/health`）：

```
healthy ──连续 fail_threshold 次失败──▶ unhealthy
unhealthy ──冷却 health_interval_ms 后，仅放行1个探活──▶ half_open
half_open ──探活成功──▶ healthy（失败计数清零）
half_open ──探活失败──▶ unhealthy（重新冷却）
```

- 失败来源包括代理请求失败、5xx 和主动探活失败；半开状态下**恰好一个**请求作为探针，
  并发下也只有一个能通过（`-race` 测试验证）。
- 后台主动探活请求 `GET /healthz`；冷却结束自动探测，恢复后自动重新加入。
- 全部 peer 被摘除时返回 `503`，`X-Gateway-Reason: no_upstream_available`。

## 限流

- 每个规则按客户端维护独立的**令牌桶**：客户端标识取 `client_header` 指定的请求头
  （如 `X-Client-Id`），缺省取对端 IP。
- `rate_per_sec` 为补充速率，`burst` 为桶容量；惰性补充，同客户端并发调用串行化，
  因此边界精确——高并发同刻到达时恰好 `burst` 个放行，其余拒绝。
- 超限返回 `429`，带 `X-Gateway-Reason: rate_limited` 与 `Retry-After-Ms`（毫秒，可区分）。

## 超时、重试与取消

- 每个池 `request_timeout_ms` 是**单次尝试**超时；`max_attempts` 是总尝试次数（含首次）。
- 只有幂等方法（默认 `GET/HEAD/OPTIONS`，可用 `retry_methods` 配置）才会重试；
  **POST 默认永不重试**——请求体在 1MiB 内会被缓冲以便重试回放，非幂等请求即使上游失败
  也只调用一次，杜绝重复副作用（测试用 `/side-effect` 持久计数器验证：5 个 POST 恰好 5 次）。
- 仅对“未写出任何字节”的失败重试：连接错误、单跳超时、上游裸 5xx；一旦响应头已提交
  （2xx/4xx/流式输出开始）就不再重试，避免重复。
- 网关每次尝试都共享客户端请求的 `context`：客户端取消或超时时，正在进行的上游请求
  会被立即中断（模拟上游收到 ctx done 返回 499，不产生副作用），网关静默结束，不返回
  半条响应。
- 可区分状态：`504 upstream_timeout`、`502 upstream_error`、`503 no_upstream_available`、
  `429 rate_limited`、`404 no_route`，统一有 `X-Gateway-Reason` 头和 JSON body。

## 验证方式

```bash
go test -race -count=1 ./...
```

测试覆盖（关键判定依据会打印在 `go test -v` 日志里）：

| 测试 | 验证点 |
|---|---|
| `config.TestRejectReasons` | 各类非法配置返回可区分 reason |
| `router.TestLookupSpecificity` | 精确 > 通配域名、最长前缀、段边界、端口剥离 |
| `health.TestEjectHalfOpenRecover` / `TestOnlyOneProbeConcurrent` | 摘除/半开/恢复状态机；64 并发仅 1 个探针 |
| `balancer.TestWeightedRoundRobinSmoothAndProportional` | WRR 权重比例与平滑交错 |
| `balancer.TestLeastConnPrefersLessLoaded` | 最少连接优先选择 |
| `balancer.TestStickyDeterministicAndDownDistinction` | 粘性恒定、摘除错误可区分、全摘返回 ErrNoPeer |
| `balancer.TestConcurrentPicksNoRace` | 200 并发无竞态、在途计数归零 |
| `ratelimit.TestBurstBoundaryUnderConcurrency` | 50 并发同刻恰好 burst 个放行，其余带 Retry-After |
| `session.TestBindPersistMigrate` | 绑定/TTL/迁移后旧 token 失效 |
| `upstream.TestBehaviorFileHotReloadAndFailureModes` | 文件热控、周期失败、取消中断 |
| `gateway.TestHotReloadAtomicUnderConcurrency` | 16 worker × 60 次交替合法/非法热更，在途请求不中断、不串配置 |
| `gateway.TestFailureEjectionAndRecovery` | 连续失败摘除后流量为 0，恢复后自动回归 |
| `gateway.TestRateLimitBoundaryHTTP` | HTTP 层 4 个放行 / 其余 429、客户端隔离 |
| `gateway.TestTimeoutAndCancelChain` | 每跳超时→504；客户端取消→上游 ctx 中断 |
| `gateway.TestRetriesDoNotDuplicateSideEffects` | POST 不重试，副作用恰好 1 次；GET 重试到健康节点 |
| `gateway.TestStickyMigrationOnEjection` | 绑定 peer 摘除后确定性迁移并重签 Cookie |

手工演练：

```bash
# 摘除：让 web-b 全失败，观察 3 次后 snapshot 里 web-b 变 unhealthy
echo '{"failing":true}' > runtime/b-behavior.json
curl -s http://127.0.0.1:18080/admin/snapshot | jq '.pools."web-pool"'
# 恢复：约 1 秒后探针成功，web-b 重新 healthy
echo '{}' > runtime/b-behavior.json

# 限流（burst=5，第 6 个起 429 + Retry-After-Ms）
for i in $(seq 1 8); do
  curl -s -o /dev/null -w '%{http_code}\n' -H 'Host: api.local' -H 'X-Client-Id: c1' \
    http://127.0.0.1:8080/v2/x
done
```

## 代码结构

```
internal/config     配置模型 / 解析 / 校验（可区分拒绝原因）
internal/router     不可变路由表（域名 + 前缀，按特异性排序）
internal/health     健康状态机（healthy/unhealthy/half_open）
internal/balancer   WRR / 最少连接 / 粘性选择 / 租约与在途计数
internal/session    粘性 token 存储（TTL、显式迁移）
internal/ratelimit  每客户端令牌桶
internal/upstream   文件行为驱动的本地模拟上游
internal/gateway    不可变 Engine 世代、原子热更新、代理（超时/重试/取消）、管理 API
main.go             装配启动、SIGHUP、模拟上游进程
```
