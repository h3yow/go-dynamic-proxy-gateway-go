# go-dynamic-proxy-gateway-go

一个纯本地可运行的动态反向代理网关：上游与配置均用本地文件/本地进程模拟，不依赖任何外部服务。

## 快速开始

```bash
# 1. 启动两个本地模拟上游
go run ./examples :9001 alpha &
go run ./examples :9002 beta &

# 2. 启动网关（使用示例配置）
go run ./cmd/gateway -config examples/config.json

# 3. 验证路由与负载均衡
curl http://127.0.0.1:8080/hello                       # 默认路由，wrr 2:1 轮询
curl -H 'Host: api.local' http://127.0.0.1:8080/users/1  # Host+前缀路由，粘性会话

# 4. 热更新：直接编辑 examples/config.json，网关每秒轮询，mtime 变化即尝试加载
```

## 配置说明（examples/config.json）

- `listen_addr`：网关监听地址。
- `routes[]`：`host`（空 = 通配任意 Host）、`path_prefix`（必须以 `/` 开头）、`upstream`（引用集群名）、`sticky`（是否启用粘性会话）。
- `upstreams[]`：`name`、`policy`（`wrr` 加权轮询 / `least_conn` 最少连接）、`backends`（`addr` + 正整数 `weight`）、`health_check`（见下）。
- `rate_limit`：`enabled`、`rate`（每秒补令牌数）、`burst`（桶容量）。
- `proxy`：`upstream_timeout`（单次上游请求超时）、`max_retries`（幂等请求最大重试次数）。

时长字段为 JSON 数字，单位纳秒（Go `time.Duration`）。

## 路由匹配规则

1. 指定 `host` 的路由优先于通配（`host` 为空）路由；
2. 同类中 `path_prefix` 更长者优先；
3. 前缀按字符串前缀匹配：`/api` 不匹配 `/api/`；
4. 无匹配返回 `404` + 响应头 `X-Gateway-Error: no_route`。

验证：`go test ./internal/router/ -v`。

## 负载均衡与粘性会话

- **wrr**：平滑加权轮询，权重决定长期比例且分布平滑（权重 3:1 时序列为 a a b a 而非 a a a b）。
- **least_conn**：选择当前在途请求数最少的后端，请求结束（无论成败）归还计数。
- **粘性会话**：路由开启 `sticky` 后，网关通过 Cookie `GWSESSION` 识别会话，用 **rendezvous 哈希** 在健康后端集合上选择：
  - 健康集合不变 → 同一会话永远落在同一后端；
  - 某后端被摘除 → 只有映射到它的会话迁移，迁移目标由哈希唯一确定（可复现），其余会话不受影响；
  - 后端恢复 → 映射回到初始状态。
  因此摘除/切换后的行为是确定且可解释的，不会会话错乱。

验证：`go test ./internal/balancer/ ./internal/upstream/ -v`。

## 健康检查与故障摘除

健康判断按**业务结果**进行，主动探测与真实请求结果共用同一台摘除/恢复状态机。

### 主动探测（`health_check`）

| 字段 | 说明 |
|---|---|
| `interval` | 探测间隔（必填，正时长） |
| `timeout` | 单次探测超时（必填，正时长） |
| `fail_threshold` | 连续失败多少次摘除（必填，正整数） |
| `recover_threshold` | 连续成功多少次恢复（必填，正整数） |
| `probe_path` | 可选。配置后对 `http://<addr><probe_path>` 发 **GET** 做业务级探测；**留空则沿用旧版 TCP 建连探测** |
| `success_statuses` | 可选。HTTP 探测视为成功的状态码白名单；留空时默认 **2xx 成功，其它（含 4xx/5xx）失败** |

- **TCP 探测（旧语义，默认）**：只看端口能否建连。旧配置不写 `probe_path` 即为此行为，完全向后兼容。
- **HTTP 业务探测**：按上游对 `probe_path` 的实际返回判定。典型场景：进程还在监听端口，但依赖的数据库挂了、
  `/healthz` 持续返回 500，网关联调 `fail_threshold` 次后将其摘除、不再接新流量；业务恢复后
  连续 `recover_threshold` 次 2xx 自动加回。建连失败、超时同样算失败。
- 状态码必须是 100–599；`probe_path` 必须以 `/` 开头，否则配置校验直接拒绝。

### 被动检查（真实请求参与判断）

- 每次真实代理请求的结果也会上报到同一状态机：
  - **失败**：连接失败、上游超时、上游返回 **5xx**；
  - **成功**：拿到上游应答（**4xx 也算成功**——上游进程与业务路由是通的，客户端错误不应摘除后端）。
- 连续失败达到 `fail_threshold` → 被动摘除；摘除后的节点收不到真实流量，因此**恢复由主动探测完成**
  （这也是生产环境的常规做法），连续探测成功达到 `recover_threshold` → 自动加回。
- 摘除/加回会立即重建该集群的健康后端集合（负载均衡 Picker 与粘性哈希都只在健康集合上工作），
  所以通过真实请求链路就能直接观察到流量被切走/切回，而不只是内部状态变化。
- 集群无健康后端时返回 `503` + `X-Gateway-Error: no_healthy`。

验证：

- `go test ./internal/upstream/ -v`：业务 500 摘除、业务恢复加回、自定义状态码白名单、TCP 旧语义兼容、粘性确定性；
- `go test ./internal/proxy/ -v -run TestEjectByBusinessFailureRealChain`：**走真实 HTTP 链路**观察
  “第几次失败被摘除 → 会话流量切到健康节点 → 探测连续成功恢复 → 会话确定性回到原节点”；
- 本地 demo：见文末「本地复现：业务级摘除与恢复」。

## 限流

- 按客户端 IP 的令牌桶：容量 `burst`，每秒补充 `rate` 个令牌。
- 超限请求返回 `429` + `X-Gateway-Error: rate_limited`，不会打到上游。
- 不同客户端的桶相互隔离；并发下令牌扣减线程安全，放行总数不超过容量+补充量。

验证：`go test ./internal/ratelimit/ -v`。

## 超时、重试与取消

- 每次上游请求带独立 `context` 超时（`upstream_timeout`），超时返回 `504` + `X-Gateway-Error: upstream_timeout`。
- **重试只对幂等方法**（GET/HEAD/OPTIONS/PUT/DELETE）生效，最多 `max_retries` 次；
  POST 等非幂等方法绝不重试，避免上游被重复调用产生重复副作用。
- **哪些结果会触发重试 / 摘除**：连接失败、上游超时、上游返回 **5xx** 算失败（会重试、计入被动健康失败）；
  **4xx 不重试也不计失败**（那是客户端的业务错误，上游是可达的）。
- **带请求体的幂等请求**：请求体会先完整读入内存（上限 16MiB），每次尝试用同一份字节重建，
  保证上游每次收到的内容与第一次**逐字节一致**（含二进制负载）；超过上限的请求体降级为只转发一次。
  上游响应同样先缓冲，因此重试前不会把某次失败的 5xx 响应写一半给客户端；
  所有尝试都拿到 5xx 时，把最后一次的状态码与响应体透传给客户端（而不是笼统的 502）。
- 客户端取消/断开会通过 `context` 传播到上游请求，立即中断且**不再重试**。
- 上游传输失败返回 `502` + `X-Gateway-Error: upstream_fail`。

验证：`go test ./internal/proxy/ -v`
（`TestIdempotentRetryBodyByteIdentical` 校验 3 次尝试请求体 bytes.Equal；
`TestServerErrorRetriedThenPassedThrough`、`TestClientErrorNotFailure` 校验 5xx/4xx 口径；
`TestRetryOnlyIdempotent`、`TestClientCancelPropagates` 校验原口径不回归）。

## 热更新

- 网关每秒轮询配置文件 mtime，变化即校验并原子切换：
  - **原子性**：一次请求全程使用同一份配置快照，绝不会看到半新半旧的配置；
  - **非法拒绝**：新配置校验失败时返回/记录包装了 `gateway.ErrInvalidConfig` 的错误（可用 `errors.Is` 区分），旧配置完全不受影响；
  - **不中断**：切换瞬间在途请求继续走完旧快照，新请求使用新快照；旧快照的健康检查循环随后退出。
- 校验规则见 `internal/config/config.go` 的 `Validate`（引用了不存在的 upstream、非法策略、非正权重等均会被拒绝）。

验证：`go test ./internal/gateway/ -v`（并发请求下热更新 + 非法配置拒绝）。

## 失败原因一览（响应头 `X-Gateway-Error`）

| 值 | 状态码 | 含义 |
|---|---|---|
| `rate_limited` | 429 | 被限流拒绝 |
| `no_route` | 404 | 无匹配路由 |
| `no_healthy` | 503 | 集群无健康后端 |
| `upstream_fail` | 502 | 上游连接/传输失败 |
| `upstream_timeout` | 504 | 上游超时 |

## 本地复现：业务级摘除与恢复

```bash
# 两个上游：9001 健康；9002 进程在监听，但 /healthz 返回 500（业务异常）
go run ./examples :9001 alpha &
go run ./examples :9002 beta unhealthy &

# 网关使用 examples/config.json（user-svc 配了 probe_path=/healthz，每 1s 探测一次）
go run ./cmd/gateway -config examples/config.json &

# 连续观察：约 2 秒（连续失败 fail_threshold=2）后 9002 被摘除，
# api.local 的粘性请求全部落到 9001；网关日志出现「主动探测摘除：连续失败达到阈值 2」
for i in $(seq 1 20); do
  curl -s -H 'Host: api.local' http://127.0.0.1:8080/users/1; sleep 0.3
done

# 让 9002 的业务恢复（重启为健康实例）
pkill -f ':9002'; sleep 0.5
go run ./examples :9002 beta &
# 约 2 秒（连续成功 recover_threshold=2）后自动加回，日志出现「主动探测恢复」，
# 原本映射到 9002 的会话确定性回到 9002
```

被动摘除（用真实请求触发）：把 `fail_threshold` 调小后，让某上游对普通业务请求也返回 500
（可在自己的模拟程序里实现），连续请求即可在日志与流量分布中观察到摘除；摘除后该节点不再收到真实流量，
恢复交由 `/healthz` 探测完成。

## 测试

```bash
go test -race -count=1 ./...
```

覆盖：业务级 HTTP 探测摘除与恢复、自定义成功状态码、TCP 旧探测兼容、
真实请求链路上的 5xx 被动摘除与流量可观察切换、摘除/恢复后粘性会话唯一且可复现、
幂等重试请求体逐字节一致（含二进制）、5xx 重试与透传、4xx 不重试不摘除、
非幂等不重试、并发请求下的热更新原子性、非法配置拒绝、限流边界、客户端取消传播、上游超时。
测试日志中会打印关键判定依据（连续失败/成功到第几次达到阈值、摘除后会话迁移到哪个节点、
重试实际发生几次、每次请求体长度与 bytes.Equal 结果、限流放行数）。
