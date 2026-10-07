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

# 5. 复现“端口活着但业务故障”：把 9002 的业务健康开关关掉（端口仍在监听）
curl 'http://127.0.0.1:9002/admin/health?ok=0'
sleep 3
curl http://127.0.0.1:8080/hello     # web-svc 唯一后端被摘除 -> no_healthy
curl 'http://127.0.0.1:9002/admin/health?ok=1'   # 业务恢复，探测连续成功后自动加回
```

## 配置说明（examples/config.json）

- `listen_addr`：网关监听地址。
- `routes[]`：`host`（空 = 通配任意 Host）、`path_prefix`（必须以 `/` 开头）、`upstream`（引用集群名）、`sticky`（是否启用粘性会话）。
- `upstreams[]`：`name`、`policy`（`wrr` 加权轮询 / `least_conn` 最少连接）、`backends`（`addr` + 正整数 `weight`）、`health_check`（`interval`/`timeout`/`fail_threshold`/`recover_threshold`，可选 `path` 启用 HTTP 业务探测）。
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

健康判断按**业务结果**而非“进程活着”：端口在监听但业务持续失败的上游会被判定为不可用、不再接新流量，业务恢复后自动加回。

- **主动检查**：按 `interval` 探测每个后端（单次超时 `timeout`）：
  - 配置 `health_check.path`（如 `/healthz`）时，发起 `GET http://addr+path`，
    **仅 2xx/3xx 视为健康**；4xx/5xx、连接失败、超时均视为失败；
  - 不写 `path` 时保持原有 TCP 建连探测（旧配置无需修改即可照常加载与运行）。
- **被动检查**：真实请求的结果参与同一套可用性状态机——连接/传输失败、超时、
  **上游返回 5xx** 都计为失败；2xx/3xx/4xx 计为成功（4xx 是客户端问题，不代表上游不健康）。
- 连续失败 `fail_threshold` 次 → 摘除（不再参与负载均衡与粘性选择）；
  连续成功 `recover_threshold` 次 → 重新加入。
- 摘除与恢复可通过真实请求链路直接观察：单后端集群被摘除后请求返回
  `503` + `X-Gateway-Error: no_healthy`；恢复后重新返回上游的正常响应。
- 健康集合变化不破坏粘性会话：rendezvous 哈希保证同一会话的落点唯一且可复现
  （详见“负载均衡与粘性会话”）。
- 集群无健康后端时返回 `503` + `X-Gateway-Error: no_healthy`。

验证：`go test ./internal/upstream/ ./internal/proxy/ -v`；或按“快速开始”第 5 步用
`/admin/health` 开关制造业务故障，观察日志中 `探测失败` / `健康检查状态变更`。

## 限流

- 按客户端 IP 的令牌桶：容量 `burst`，每秒补充 `rate` 个令牌。
- 超限请求返回 `429` + `X-Gateway-Error: rate_limited`，不会打到上游。
- 不同客户端的桶相互隔离；并发下令牌扣减线程安全，放行总数不超过容量+补充量。

验证：`go test ./internal/ratelimit/ -v`。

## 超时、重试与取消

- 每次上游请求带独立 `context` 超时（`upstream_timeout`），超时返回 `504` + `X-Gateway-Error: upstream_timeout`。
- **重试只对幂等方法**（GET/HEAD/OPTIONS/PUT/DELETE）生效，最多 `max_retries` 次；
  POST 等非幂等方法绝不重试，避免上游被重复调用产生重复副作用。
- 连接/传输失败、超时、**上游 5xx** 都会触发重试（仍有剩余次数时）；
  若是最后一次尝试，上游 5xx 响应会**原样透传**给客户端（但仍计入健康判断的失败）。
- **请求体重放**：带请求体的幂等请求（如 PUT）在重试时，上游每次收到的请求体与第一次
  逐字节一致——网关会在首次转发前把请求体完整读入内存，每次尝试都用全新 reader 发送。
- 客户端取消/断开会通过 `context` 传播到上游请求，立即中断且不再重试。
- 上游传输失败返回 `502` + `X-Gateway-Error: upstream_fail`。

验证：`go test ./internal/proxy/ -v`。

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

注：上游 5xx 不出现在该响应头中——最后一次尝试会把上游的 5xx 响应原样透传给客户端，
网关在日志中以 `reason=upstream_5xx` 记录，并将其计入健康判断的失败。

## 测试

```bash
go test -race -count=1 ./...
```

覆盖：并发请求下的热更新原子性、非法配置拒绝、HTTP 业务探测摘除与恢复（含 TCP 探测兼容）、
真实请求 5xx 触发摘除与自动恢复（走完整请求链路观察）、粘性会话在健康集合变化下的确定性迁移、
限流边界与并发一致性、客户端取消传播、上游超时、幂等重试/非幂等不重试、重试请求体逐字节一致。
测试日志中会打印关键判定依据（如连续失败/成功次数、迁移会话数、重试调用次数、限流放行数）。
