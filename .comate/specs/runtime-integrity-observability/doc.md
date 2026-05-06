# 运行完整性与可观测性设计文档

## 背景与目标

当前项目已具备基础同步能力、SQLite 状态记录、JSONL 输出、HTTP 重试、日志持久化和日志大小滚动压缩。仍需解决以下问题：

1. JSONL 文件追加与 SQLite 状态不是原子事务；如果 JSONL 写成功但 DB 状态写失败，下次可能重复写。
2. 没有健康检查和运行状态输出；长期运行时只能看日志和 DB。
3. `max-message-fetch` 达到上限只打日志，不记录到状态里，可追溯性不足。
4. 当前开发阶段不做 migration，旧 DB 可能静默与当前 schema 不兼容，需要 schema sanity check 明确失败。

目标：

- 用 SQLite outbox 改造 JSONL 写入链路，降低“文件写成功但 DB 未记录”导致重复输出的风险。
- 增加 HTTP health/status 服务，暴露运行状态。
- 在 session 状态中记录 max-message-fetch 达上限的信息。
- 启动时检查 DB schema 是否符合当前开发期 schema，不符合则报错退出。

## 当前相关实现

### 当前写入流程

文件：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/watcher/watcher.go`

当前 `syncSession` 流程：

```text
fetch messages
-> UnseenMessages
-> sort
-> build MessageRecord
-> Sink.WriteMessages
-> Store.CommitSessionSync
```

问题：Sink 写入成功后，如果 `CommitSessionSync` 失败，下次运行仍认为 message 未处理，导致 JSONL 重复追加。

### 当前 Store

文件：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/store/store.go`

当前表：

- `sessions`
- `messages`

当前不做 migration，`init` 直接创建当前 schema。

### 当前健康状态

当前没有 HTTP server、health endpoint 或状态快照结构。

### 当前 max-message-fetch

文件：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/watcher/watcher.go`

`fetchUntilBoundary` 达到 max 时只打 warn：

```go
w.logger.Warn("message fetch reached max limit", ...)
```

DB 中没有记录。

## 设计方案

### 1. 用 SQLite outbox 改善文件写入与状态一致性

#### 核心思路

把“发现未处理 message”和“准备写文件”先落到 SQLite，再刷到 JSONL，最后标记完成。

流程从：

```text
Sink.WriteMessages -> CommitSessionSync
```

改为：

```text
PrepareMessageRecords(DB transaction)
-> Sink.WriteMessages(records)
-> MarkMessagesWritten(DB transaction)
```

这样至少保证：

- 进程在写文件前崩溃：DB 中有 `pending`，下次可继续写。
- 进程在写文件后、标记完成前崩溃：DB 中仍是 `pending`，下次可能再次写；但有明确 `pending` 状态可排查和恢复。
- DB 不再完全不知道写入过程。

注意：文件系统和 SQLite 无法真正原子提交，所以不能 100% 消除极端重复写，但 outbox 会让状态可观测，并为后续幂等 Sink 打基础。

#### messages 表增加状态字段

当前不做 migration，只改当前 schema。

`messages` 表新增字段：

```sql
status TEXT NOT NULL DEFAULT 'pending', -- pending/written
prepared_at INTEGER NOT NULL DEFAULT 0,
written_at INTEGER NOT NULL DEFAULT 0,
last_error TEXT NOT NULL DEFAULT ''
```

调整当前字段含义：

- `prepared_at`：记录进入 outbox 的时间。
- `written_at`：真正 Sink 写成功后标记时间。
- `status`：
  - `pending`：已准备，尚未确认写入完成；
  - `written`：Sink 写入成功并已确认。
- `last_error`：写入失败时记录错误摘要。

保留：

- `id`
- `session_id`
- `created_at`
- `user_id`
- `agent_id`
- `sink_type`
- `output_root`
- `output_path`
- `output_session_file`

新增索引：

```sql
CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status);
```

#### Store 接口调整

当前 watcher.Store：

```go
CommitSessionSync(ctx, session, records, syncedAt) error
```

调整为：

```go
PrepareMessageRecords(ctx context.Context, session domain.Session, records []domain.MessageRecord, preparedAt int64) ([]domain.MessageRecord, error)
MarkMessagesWritten(ctx context.Context, session domain.Session, records []domain.MessageRecord, writtenAt int64) error
MarkMessagesFailed(ctx context.Context, records []domain.MessageRecord, errText string) error
```

职责：

- `PrepareMessageRecords`：事务内插入 `messages(status='pending')`，只返回当前需要写入的 records。
- `MarkMessagesWritten`：Sink 成功后标记 `status='written'`、更新 `written_at`，同时更新 session 游标。
- `MarkMessagesFailed`：Sink 失败后记录 `last_error`，不推进游标。

#### Existing/Unseen 语义调整

当前 `MessageExists` 只要 messages 表有 id 就认为已处理。引入 pending 后需要区分：

- `written`：已处理，不应再写。
- `pending`：上次准备但未确认写入，应该允许重试写入。

因此 `UnseenMessages` 应视为：

- 不存在：待写；
- `pending`：待重试写；
- `written`：已处理。

`AnyMessageExists` 用于 boundary，应只把 `written` 视为“发现已处理边界”，避免 pending 阻断扩大 limit。

SQL：

```sql
SELECT id, status FROM messages WHERE id IN (...)
```

#### JSONL 重复风险说明

即便有 outbox，如果崩溃发生在：

```text
Sink.WriteMessages 成功之后，MarkMessagesWritten 之前
```

下次仍会重写 pending records，导致重复 JSONL 行。完全解决需要 JSONL 文件层去重或外部幂等 Sink。本次不做重写 JSONL 文件，也不扫描文件去重。

但 outbox 后 DB 会明确显示 pending 状态，后续可以人工或程序化修复。

### 2. 健康检查与运行状态 HTTP 服务

#### 配置新增

```go
DefaultHealthAddr = "127.0.0.1:0"
```

命令行：

```bash
-health-addr 127.0.0.1:8080
```

默认 `127.0.0.1:0` 表示监听本地随机端口，启动日志打印实际地址。

如果用户传空字符串：

```bash
-health-addr ""
```

则禁用 health server。

#### 运行状态结构

新增包：

- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/status/status.go`

结构：

```go
type Snapshot struct {
    StartedAt int64 `json:"started_at"`
    LastRoundAt int64 `json:"last_round_at"`
    LastRoundDurationMillis int64 `json:"last_round_duration_millis"`
    LastSuccessAt int64 `json:"last_success_at"`
    LastError string `json:"last_error"`
    RoundsTotal int64 `json:"rounds_total"`
    SessionsTotal int `json:"sessions_total"`
    SessionsSynced int `json:"sessions_synced"`
    SessionsFailed int `json:"sessions_failed"`
    MessagesNew int `json:"messages_new"`
    MaxFetchReachedTotal int64 `json:"max_fetch_reached_total"`
}
```

提供并发安全方法：

```go
type Reporter struct { mu sync.RWMutex; snapshot Snapshot }
func (r *Reporter) Snapshot() Snapshot
func (r *Reporter) RecordRound(result watcher.RoundResult, err error, duration time.Duration)
func (r *Reporter) IncMaxFetchReached(sessionID string)
```

为了避免 package import cycle，`status` 不直接依赖 watcher；或把状态 reporter 放在 watcher 包中。本次建议新增 `internal/status`，用简单结构传值。

#### Watcher 更新状态

`Watcher` 增加可选 reporter 接口：

```go
type Reporter interface {
    RecordMaxFetchReached(sessionID string)
}
```

或者更简单：在 `RoundResult` 中新增：

```go
MaxFetchReached int
```

`fetchUntilBoundary` 返回：

```go
messages []domain.Message, reachedMax bool, err error
```

sessionResult 增加 `maxFetchReached bool`，RoundResult 汇总 `MaxFetchReached`。

main 在每轮 SyncOnce 后记录状态。

#### HTTP endpoints

新增包：

- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/health/server.go`

接口：

```text
GET /healthz
GET /status
```

`/healthz`：

```json
{"status":"ok"}
```

如果最近一轮出现错误，但程序仍在运行，仍返回 200；该接口表示进程活着。

`/status`：返回 JSON Snapshot。

启动：

```go
server, err := health.Start(ctx, cfg.HealthAddr, reporter, logger)
```

如果 addr 是 `127.0.0.1:0`，日志打印实际监听地址。

### 3. max-message-fetch 达上限状态可观测

#### DB 字段

`sessions` 表新增：

```sql
last_fetch_reached_limit INTEGER NOT NULL DEFAULT 0,
last_fetch_count INTEGER NOT NULL DEFAULT 0,
last_fetch_limit INTEGER NOT NULL DEFAULT 0,
last_fetch_at INTEGER NOT NULL DEFAULT 0
```

含义：

- `last_fetch_reached_limit`：最近一次该 session fetch 是否触达 max-message-fetch。
- `last_fetch_count`：最近一次拉到多少条。
- `last_fetch_limit`：最近一次请求 limit。
- `last_fetch_at`：最近一次 fetch 时间。

Store 新增：

```go
UpdateSessionFetchStats(ctx context.Context, sessionID string, reachedLimit bool, count int, limit int, fetchedAt int64) error
```

调用点：

- 每个 session 的 `fetchUntilBoundary` 结束后更新。
- 即使后续 Sink 或 DB 写入失败，也保留 fetch stats，便于排查。

#### RoundResult 增加字段

```go
MaxFetchReached int
```

日志中每轮输出：

```go
"max_fetch_reached", result.MaxFetchReached
```

### 4. schema sanity check

当前开发阶段不做 migration。为避免旧 DB 静默错用，Store 初始化后执行 schema sanity check。

#### 期望 schema

`sessions` 必须包含：

- `id`
- `user_id`
- `agent_id`
- `updated_at`
- `latest_message_id`
- `latest_message_created_at`
- `raw_json`
- `synced_at`
- `last_fetch_reached_limit`
- `last_fetch_count`
- `last_fetch_limit`
- `last_fetch_at`

`messages` 必须包含：

- `id`
- `session_id`
- `created_at`
- `prepared_at`
- `written_at`
- `status`
- `last_error`
- `user_id`
- `agent_id`
- `sink_type`
- `output_root`
- `output_path`
- `output_session_file`

`messages` 不允许包含：

- `raw_json`

不允许存在：

- `schema_migrations`

#### 检查时机

`Open` 中：

```go
store.init(ctx)
store.checkSchema(ctx)
```

如果已有旧 DB，`CREATE TABLE IF NOT EXISTS` 不会改表，`checkSchema` 会发现字段不匹配并返回明确错误。

错误信息示例：

```text
incompatible database schema: messages.raw_json exists; remove old db or use a new -db path
```

## 数据流调整

### 当前

```text
fetch -> UnseenMessages -> Sink.WriteMessages -> CommitSessionSync
```

### 调整后

```text
fetch -> UnseenMessages(status aware)
-> build records
-> fill output tracking
-> PrepareMessageRecords(status=pending)
-> Sink.WriteMessages
-> MarkMessagesWritten(status=written + update session cursor)
```

失败路径：

```text
Sink.WriteMessages error -> MarkMessagesFailed(last_error) -> session failed
```

## 受影响文件

- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/store/store.go`
  - 表 schema 增加 outbox/status/fetch stats 字段。
  - 增加 schema sanity check。
  - 改造 message 状态查询。
  - 增加 prepare/mark written/mark failed/fetch stats 方法。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/watcher/watcher.go`
  - 改造写入流程为 outbox。
  - 汇总 max-fetch reached 状态。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/config/config.go`
  - 增加 `health-addr`。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/cmd/session-watcher/main.go`
  - 启动 health/status server。
  - 维护运行状态 reporter。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/health/server.go`
  - 新增 health/status HTTP server。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/status/status.go`
  - 新增运行状态 reporter。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/**/*_test.go`
  - 更新和新增测试。

## 测试计划

### Store

- 当前 schema 初始化后字段完整。
- `messages.raw_json` 存在时 sanity check 报错。
- `schema_migrations` 存在时 sanity check 报错。
- `PrepareMessageRecords` 写入 pending。
- `MarkMessagesWritten` 标记 written 并更新 session 游标。
- `UnseenMessages` 返回不存在和 pending，跳过 written。
- `MarkMessagesFailed` 写入 last_error。
- `UpdateSessionFetchStats` 正确写入 sessions。

### Watcher

- Sink 成功：pending -> written。
- Sink 失败：pending + last_error，不推进 session 游标。
- max-message-fetch 达上限时 RoundResult.MaxFetchReached 增加。
- fetch stats 写入 Store。

### Health/status

- `/healthz` 返回 200 和 `{"status":"ok"}`。
- `/status` 返回运行状态 JSON。
- SyncOnce 后状态更新。

### 端到端

- `go test ./...`。
- 使用临时 DB 和输出目录运行 `-once`。
- 查询 DB：messages 全部 written。
- 查询 sessions：fetch stats 有值。
- 请求 `/healthz`、`/status`。

## 预期结果

完成后：

- JSONL 与 SQLite 的写入链路具备 outbox 状态，异常可恢复和可排查。
- 服务具备本地 health/status endpoint。
- max-message-fetch 达上限可在 DB、日志、status 中看到。
- 旧 DB schema 会被明确拒绝，不会静默运行。
