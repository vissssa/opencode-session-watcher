# stabilize-sink-locks-and-http-retry

## 需求分类

本次属于稳定性缺陷修复，涉及 JSONL FileSink 并发写入锁管理、open-code HTTP Source 重试退避策略，以及相关测试更新。按规范先完成设计说明，确认后再拆分任务并实施。

## 背景与现状

项目是 Go 实现的 `session_watcher`，周期性从 open-code HTTP 服务同步 Session 消息，并通过 `internal/sink/jsonl` 追加写入本地 JSONL 文件。

当前相关实现：

- `FileSink` 在 `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer.go` 中维护 `locks map[string]*sync.Mutex`，`lockFor(path)` 只新增 path 对应 mutex，不删除。
- `HTTPSource` 在 `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/source/opencode/client.go` 中执行最多 3 次 GET，当前重试 sleep 为 `attempt * 100ms`，实际两次等待为 100ms、200ms。
- 已有测试覆盖并发写入、HTTP 5xx/429/4xx/网络错误重试：
  - `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer_test.go`
  - `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/source/opencode/client_test.go`

## 子需求 1：限制 FileSink locks map 增长

### 场景与处理逻辑

长期运行时，Session 持续增长会生成越来越多输出 path。当前 `locks map[string]*sync.Mutex` 只增不减，会长期持有历史 path 的 mutex 对象，形成内存泄漏风险。

计划将锁值从裸 `*sync.Mutex` 改成带元数据的锁条目：

```go
type pathLock struct {
    mu       sync.Mutex
    lastUsed time.Time
}
```

`FileSink.lockFor(path)` 返回 `*pathLock`，每次获取或复用时更新 `lastUsed`。`WriteMessages` 在完成每个 path 写入后调用清理函数，删除超过 TTL 且当前可安全获取的锁。

### 技术方案

- 在 `FileSink` 中增加锁过期控制字段：
  - `lockTTL time.Duration`
  - `lastLockCleanup time.Time`
  - `now func() time.Time`（仅用于测试中稳定控制时间）
- 默认 TTL 建议为 10 分钟，清理扫描间隔建议为 1 分钟，避免每次写入都全量扫描。
- 清理时只删除满足以下条件的条目：
  - 当前时间减去 `lastUsed` 大于 TTL
  - 能通过 `TryLock()` 获取该 path 锁，说明当前没有 goroutine 正在持有或等待关键路径内写入
  - 删除前再次确认 map 中仍是同一个锁条目，避免误删被替换的条目
- 清理函数只由写入路径触发，不新增后台 goroutine，避免生命周期和 Close 复杂度。

### 影响文件

- 修改：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer.go`
  - `FileSink` 结构体：增加 path lock 元数据与清理配置
  - `NewFileSink`：初始化 TTL、清理间隔、时间函数
  - `WriteMessages`：锁定 path 后写入，并在写入结束后触发清理
  - `lockFor`：返回带 `lastUsed` 的锁条目
  - 新增 `cleanupIdleLocks`：删除长期不活跃锁
- 修改：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer_test.go`
  - 增加锁清理行为测试
  - 保持并发写入测试通过，确保清理不会破坏同 path 串行写入语义

### 边界与异常处理

- `records` 为空时仍直接返回，不触发锁创建。
- 正在写入的 path 锁不会被清理。
- 清理失败或锁正在使用时跳过，不影响写入主流程。
- 如果 path 后续再次出现，会重新创建锁条目，功能等价。

### 数据流

`Watcher.syncSession` → `Sink.WriteMessages` → `FileSink.PathFor` 分组 → `lockFor(path)` → `pathLock.mu.Lock()` → `writeFile` 追加 JSONL → `cleanupIdleLocks(now)`。

### 预期结果

长期运行时，`FileSink` 不再永久保留所有历史 path 的 mutex；不活跃 path 的锁会被周期性回收，降低内存泄漏风险。

## 子需求 2：HTTP 重试改为指数退避 + jitter

### 场景与处理逻辑

open-code 服务过载时，过短且固定的重试间隔会让多个 worker 更快重试，可能放大瞬时压力。需要改为指数退避并加入 jitter，降低同步请求的重试峰值。

### 技术方案

- 保持 `maxHTTPAttempts = 3` 不变，即最多 3 次请求。
- 对失败后的等待使用指数退避基准：
  - 第 1 次失败后：100ms
  - 第 2 次失败后：200ms
  - 如未来增加第 4 次请求，则第 3 次失败后为 400ms
- 加入 jitter，建议为基准退避的 0%~50% 随机增量，即实际等待范围：
  - 100ms ~ 150ms
  - 200ms ~ 300ms
  - 400ms ~ 600ms
- 使用 `math/rand/v2` 或标准库可用随机源计算 jitter。若当前 Go 版本为 `go 1.25.0`，优先使用标准库能力，不引入第三方依赖。
- 将 backoff 计算提取为小函数，方便测试：

```go
func retryBackoff(attempt int) time.Duration {
    base := 100 * time.Millisecond * time.Duration(1<<(attempt-1))
    jitter := time.Duration(rand.Int64N(int64(base / 2)))
    return base + jitter
}
```

实际实现会根据 Go 版本可用 API 调整，保持无第三方依赖。

### 影响文件

- 修改：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/source/opencode/client.go`
  - import 增加随机包
  - `get` 中 `backoff := time.Duration(attempt) * 100 * time.Millisecond` 改为指数退避 + jitter
  - 新增 `retryBackoff` 或等价函数
- 修改：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/source/opencode/client_test.go`
  - 增加退避范围测试，不强依赖具体随机值
  - 原有重试次数语义保持不变

### 边界与异常处理

- 对 4xx（除 429 外）仍不重试。
- 对 429、5xx、临时网络错误仍重试。
- context 取消或超时后不再等待，不再重试。
- jitter 不改变最大尝试次数，只改变等待时间。

### 数据流

`HTTPSource.ListSessions/GetSession/ListMessages` → `get` → `getOnce` → `shouldRetry` → `retryBackoff(attempt)` → `time.After(backoff)` 或 `ctx.Done()`。

### 预期结果

open-code 过载时，重试请求会分散到更长且带随机扰动的时间窗口中，降低同步 worker 集中重试造成的压力。

## 验证计划

- 运行单元测试：

```bash
go test ./...
```

- 如环境允许，运行竞态测试：

```bash
go test -race -timeout 30s ./...
```

## 不做事项

- 不引入第三方 expirable map 依赖，优先用小范围内建实现降低依赖复杂度。
- 不改 `domain.Sink` 接口，避免扩大影响面。
- 不调整 CLI 参数，TTL 与清理间隔先作为内部常量处理。
