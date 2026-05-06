# open-code session watcher 设计文档

## 背景与目标

当前工作区 `/Users/siegward/Developer/baidu/easydata/session_watcher` 为空目录，不存在 README、Go module 或已有源码。本需求按新建 Go 项目处理。

目标是实现一个本地运行的 Go 命令行程序，周期性从 open-code 服务同步 session message：

- 基础地址默认：`http://localhost:57811/`
- 会话列表接口：`GET /session`
- 会话详情接口：`GET /session/{sessionID}`
- 会话消息接口：`GET /session/{sessionID}/message?limit=N`

程序每隔 N 秒拉取一次会话列表，为每个需要同步的 session 启动独立 goroutine 并发拉取详情与最近消息，只把本地尚未记录过的新 message 追加输出，并用 SQLite 维护同步状态。

同时，本项目需要将“数据来源”和“数据输出”抽象为接口：

- 当前数据来源实现为 open-code HTTP URL。
- 当前数据输出实现为按 `user_id/agent_id/session_id.jsonl` 分文件的本地 JSONL 目录。
- 后续可扩展其他数据来源或输出到 ES、S3 等系统，不需要重写 watcher 核心编排逻辑。

## 已确认的接口结构

### `GET /session`

已在本地测试服务验证，可返回会话数组，单个元素示例字段如下：

```json
{
  "id": "ses_245d5a4d2ffeo786i9Y6njAvaU",
  "slug": "misty-cabin",
  "projectID": "global",
  "directory": "/Users/siegward/.qianfan/workspace/95a31ba4432b4289b1fc94467e2dd928",
  "is_subagent": false,
  "title": "图片内容解释请求",
  "version": "0.0.0-master-202604201139",
  "summary": {"additions": 0, "deletions": 0, "files": 0},
  "time": {"created": 1776944831277, "updated": 1776944851503}
}
```

### `GET /session/{sessionID}`

返回单个 session 详情，结构与列表元素基本一致。实现中会保存原始 JSON，避免字段增加时丢失信息。

### `GET /session/{sessionID}/message?limit=N`

该接口支持 `limit` 参数，可获取最近 N 条消息。本项目会通过配置控制每个 session 每轮最多拉取的最近消息数量，减少大 session 的网络传输和处理延迟。

已验证该接口返回消息数组，元素大致结构为：

```json
{
  "info": {
    "id": "msg_dba2a9039001712xeyhtZp1wAx",
    "sessionID": "ses_245d5a4d2ffeo786i9Y6njAvaU",
    "role": "assistant",
    "time": {"created": 1776944844857, "completed": 1776944851497}
  },
  "parts": [
    {"type": "text", "text": "..."}
  ]
}
```

`info.id` 作为 message ID。`info.time.created` 用于排序和游标比较；如果时间缺失，会退化为只按 message ID 去重。

## 功能范围

### 1. 新建 Go CLI 项目

创建标准 Go module，包含：

- `go.mod`
- `cmd/session-watcher/main.go`
- `internal/config`：命令行参数与默认配置
- `internal/domain`：核心领域结构与 Source/Sink 接口定义
- `internal/source/opencode`：open-code HTTP 数据源实现
- `internal/sink/jsonl`：JSONL 输出实现
- `internal/store`：SQLite 状态存储
- `internal/watcher`：轮询、并发、过滤、写入编排逻辑

推荐使用纯 Go SQLite 驱动 `modernc.org/sqlite`，避免本地 CGO 依赖。HTTP 使用标准库 `net/http`。日志使用 Go 标准库 `log/slog`，提供结构化日志能力，避免额外日志依赖。

### 2. 命令行参数

程序提供以下参数：

```bash
session-watcher \
  -base-url http://localhost:57811 \
  -interval 10s \
  -message-limit 100 \
  -session-workers 8 \
  -db ./data/state.db \
  -output-dir ./data/messages \
  -log-level info
```

参数说明：

- `-base-url`：open-code 服务基础地址，默认 `http://localhost:57811`
- `-interval`：轮询间隔，默认 `10s`，支持 Go duration 格式，如 `5s`、`1m`
- `-message-limit`：每个 session 每轮调用 `/message` 时请求最近 N 条消息，默认 `100`
- `-session-workers`：每轮最多同时同步的 session goroutine 数，默认 `8`，用于限制并发避免瞬时请求过多
- `-db`：SQLite 文件路径，默认 `./data/state.db`
- `-output-dir`：JSONL 输出根目录，默认 `./data/messages`，实际文件按 `user_id/agent_id/session_id.jsonl` 分层存储
- `-once`：只执行一轮同步后退出，便于调试和测试
- `-timeout`：HTTP 请求超时，默认 `10s`
- `-log-level`：日志级别，支持 `debug`、`info`、`warn`、`error`，默认 `info`

边界处理：

- `interval <= 0` 时启动失败并输出错误。
- `message-limit <= 0` 时启动失败并输出错误。
- `session-workers <= 0` 时启动失败并输出错误。
- `base-url` 会统一去除末尾 `/`，请求时再拼接路径。
- `db` 的父目录与 `output-dir` 输出根目录不存在时自动创建。
- session 缺少 `user_id` 或 `agent_id` 时，输出路径分别使用 `default_user` 和 `default_agent`。

### 3. Source/Sink 抽象设计

核心 watcher 不直接依赖 URL 或 JSONL 文件，而依赖接口。

#### 数据来源接口

```go
type Source interface {
    ListSessions(ctx context.Context) ([]Session, error)
    GetSession(ctx context.Context, sessionID string) (Session, error)
    ListMessages(ctx context.Context, sessionID string, limit int) ([]Message, error)
}
```

当前实现：

- `internal/source/opencode.HTTPSource`
- 负责拼接 open-code URL、发起 HTTP 请求、解析 JSON、保留原始 JSON。
- `ListMessages` 会请求 `/session/{sessionID}/message?limit={messageLimit}`。

后续扩展示例：

- 从另一个 HTTP 服务同步。
- 从本地文件读取历史 session/message。
- 从消息队列或数据库读取数据。

#### 数据输出接口

```go
type Sink interface {
    WriteMessages(ctx context.Context, records []MessageRecord) error
    Close() error
}
```

当前实现：

- `internal/sink/jsonl.FileSink`
- 负责把新增 message 以 JSONL envelope 格式追加到本地文件。
- 内部使用 mutex 保护文件写入，确保多个 session goroutine 并发写入时单行 JSON 不交叉。

后续扩展示例：

- `ESSink`：批量写入 Elasticsearch。
- `S3Sink`：按时间分区上传对象存储。
- `StdoutSink`：调试时输出到标准输出。

#### 核心领域结构

```go
type Session struct {
    ID        string
    UserID    string
    AgentID   string
    UpdatedAt int64
    Raw       json.RawMessage
}

type Message struct {
    ID        string
    SessionID string
    CreatedAt int64
    Raw       json.RawMessage
}

type MessageRecord struct {
    SyncedAt         int64           `json:"synced_at"`
    UserID           string          `json:"user_id"`
    AgentID          string          `json:"agent_id"`
    SessionID        string          `json:"session_id"`
    MessageID        string          `json:"message_id"`
    MessageCreatedAt int64           `json:"message_created_at"`
    Session          json.RawMessage `json:"session"`
    Message          json.RawMessage `json:"message"`
}
```

字段来源与默认值：

- `Session.UserID` 从 session 列表或详情 JSON 的 `user_id` / `userID` 字段提取；缺失时使用 `default_user`。
- `Session.AgentID` 从 session 列表或详情 JSON 的 `agent_id` / `agentID` 字段提取；缺失时使用 `default_agent`。
- `MessageRecord` 必须显式记录最终使用的 `user_id` 与 `agent_id`，便于后续 ES、S3 等 Sink 直接使用。

### 4. SQLite 状态设计

维护两个核心表：

```sql
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  updated_at INTEGER NOT NULL DEFAULT 0,
  latest_message_id TEXT NOT NULL DEFAULT '',
  latest_message_created_at INTEGER NOT NULL DEFAULT 0,
  raw_json TEXT NOT NULL DEFAULT '',
  synced_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT 0,
  written_at INTEGER NOT NULL,
  raw_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_session_created
ON messages(session_id, created_at);
```

设计原则：

- `sessions.updated_at` 记录 session 的最新更新时间，用于判断是否需要拉取详情和消息。
- `sessions.latest_message_id` 与 `sessions.latest_message_created_at` 记录每个 session 的最新游标。
- `messages.id` 设置唯一主键，保证输出前可做幂等检查。
- 即使程序重启或异常退出，再次运行时也不会重复写入已有 message。

### 5. 并发增量同步逻辑

每轮同步处理逻辑：

1. 请求 `Source.ListSessions` 获取 session 列表。
2. 对每个 session：
   - 读取 SQLite 中该 session 的状态。
   - 如果本地不存在该 session，则视为需要同步。
   - 如果远端 `UpdatedAt` 大于本地 `updated_at`，则视为需要同步。
   - 如果远端没有更新时间，保守同步。
3. 对需要同步的 session 建立任务队列。
4. 启动最多 `session-workers` 个 worker goroutine；每个 worker 从队列中领取 session，同步该 session。
5. 单个 session 同步逻辑：
   - 调用 `Source.GetSession(ctx, id)` 获取详情并保存原始 JSON。
   - 调用 `Source.ListMessages(ctx, id, messageLimit)` 获取最近 N 条消息。
   - 从消息中提取 ID、sessionID、createdAt。
   - 按 `createdAt` 升序排序；相同时间按 message ID 稳定排序。
   - 对每条消息检查 `messages` 表是否已有该 message ID。
   - 未存在的消息组装为 `MessageRecord`，批量调用 `Sink.WriteMessages` 输出。
   - 输出成功后插入 `messages` 表，并更新 `sessions.latest_message_id`、`latest_message_created_at`、`updated_at`、`synced_at`。
6. 等待所有 worker 完成，打印本轮统计日志。
7. 等待 N 秒进入下一轮。

关于 `limit` 的无遗漏增量策略：

- `message-limit` 表示每次扩大的步长 N，不表示最多只处理 N 条。
- 对每个需同步 session，先调用 `/message?limit=N` 获取最近 N 条消息。
- 如果返回的 N 条全部都未处理过，说明更早的位置可能仍有未处理 message，继续调用 `/message?limit=2N`。
- 如果返回的 2N 条仍全部未处理过，继续调用 `/message?limit=3N`，以此类推。
- 一旦返回结果中发现任意已处理过的 message，停止扩大 limit；此时已拿到从最新消息到已处理边界之间的所有未处理 message。
- 如果接口返回条数小于当前 limit，说明已经拿到该 session 当前可见的全部消息，也停止扩大 limit。
- 停止扩大后，从最后一次返回结果中过滤出所有 SQLite 中未处理过的 message，按 `createdAt` 升序、相同时间按 message ID 排序后输出和记录。
- 因为停止条件是“发现已处理 message”或“已拉到全部可见消息”，所以只要 open-code 的 `limit` 语义是“返回最近 limit 条”，就不会遗漏两轮之间新增超过 N 条的 message。
- 本地仍以 SQLite 的 `messages.id` 去重，确保重复返回的最近消息不会重复输出。

### 6. JSONL 输出格式

当前 Sink 实现把每个新 message 写一行 JSON，采用 envelope 格式，保留 session 与 message 的原始内容：

```json
{"synced_at":1777000000000,"session_id":"ses_xxx","message_id":"msg_xxx","message_created_at":1776944844857,"session":{"id":"ses_xxx","title":"..."},"message":{"info":{"id":"msg_xxx"},"parts":[]}}
```

字段说明：

- `synced_at`：本地写入时间，毫秒时间戳。
- `session_id`：会话 ID。
- `message_id`：消息 ID，即 `message.info.id`。
- `message_created_at`：消息创建时间，来自 `message.info.time.created`。
- `session`：本次获取到的 session 详情原始 JSON。
- `message`：消息原始 JSON。

使用 JSONL 的原因：

- 追加写入简单，适合持续同步。
- 单行损坏不会影响整个文件读取。
- 便于后续用 jq、Python、Go 批处理。

### 7. 日志设计

使用 `log/slog` 打印结构化日志。默认输出文本格式到 stderr，后续可扩展 JSON 日志格式。

关键日志点：

- 程序启动：配置项、base-url、interval、message-limit、session-workers、db、output。
- 每轮开始：轮次编号、开始时间。
- session 列表拉取完成：session 总数、耗时。
- session 过滤结果：需要同步数量、跳过数量。
- 单个 session 同步开始：session_id、remote_updated_at、local_updated_at。
- session 详情拉取完成：session_id、耗时。
- message 拉取完成：session_id、limit、返回条数、耗时。
- message 去重结果：session_id、新消息数、已存在消息数。
- Sink 写入完成：session_id、写入条数、耗时。
- SQLite 状态更新完成：session_id、latest_message_id、latest_message_created_at。
- 单个 session 同步失败：session_id、错误原因。
- 每轮结束：成功 session 数、失败 session 数、新增 message 总数、总耗时。
- 收到退出信号：signal、当前状态。

日志级别建议：

- `debug`：单条 message 去重细节、请求 URL。
- `info`：启动、轮次、session 同步结果、写入统计。
- `warn`：单个 session 同步失败、message-limit 可能不足。
- `error`：无法拉取 session 列表、SQLite 初始化失败、Sink 初始化失败。

### 8. HTTP 客户端与错误处理

HTTP 层处理：

- 所有请求使用统一超时。
- 非 2xx 状态码返回错误并记录状态码与响应片段。
- JSON 解析失败时跳过本轮或该 session，不更新游标，避免误标已同步。
- 单个 session 同步失败不影响其他 session，本轮结束后继续等待下一轮。
- `/message` 请求必须携带 `limit` 参数。

错误边界：

- `Source.ListSessions` 拉取失败：本轮整体失败，不修改状态。
- `Source.GetSession` 拉取失败：跳过该 session，不更新状态。
- `Source.ListMessages` 拉取失败：跳过该 session，不更新 message 游标。
- `Sink.WriteMessages` 失败：不插入 `messages` 表，不更新游标，避免状态领先于输出。
- SQLite 插入失败：如果是 message 主键冲突，视为已同步；其他错误返回并停止该 session 的处理。

### 9. 数据一致性策略

为避免“输出成功但状态没更新”或“状态更新了但输出没成功”的不一致，采用以下顺序：

1. 查询 message 是否已存在。
2. 对未存在的 message 调用 Sink 输出。
3. 输出成功后插入 `messages` 表。
4. 更新 `sessions` 表游标。

如果第 2 步成功、第 3 步失败，下次运行可能再次输出同一 message。为了进一步降低重复风险，插入 `messages` 与更新 `sessions` 放在事务中，但外部 Sink（JSONL、ES、S3）无法与 SQLite 原子提交。实际实现会优先做到：只有 SQLite 插入成功后才推进 session 游标；对偶发重复可通过 `message_id` 后处理去重。若需要更强一致性，可扩展为先写 SQLite outbox，再由单独流程刷 Sink；本次不引入该复杂度。

### 10. 并发安全策略

- 每个 session 的同步任务在一个 worker goroutine 内串行执行。
- 不同 session 可并发同步，最大并发由 `session-workers` 限制。
- SQLite 使用 `database/sql` 连接池，并把 `SetMaxOpenConns(1)` 设为 1，减少 SQLite 写锁冲突。
- JSONL Sink 内部使用 mutex 串行化文件写入，确保每条 JSONL 记录完整。
- context 取消时，worker 停止领取新任务，正在执行的 HTTP 请求尽快取消。

### 11. 退出与运行控制

程序监听 `SIGINT` / `SIGTERM`：

- 收到信号后停止下一轮轮询。
- 当前正在处理的 HTTP 请求通过 context 取消。
- worker goroutine 停止领取新任务。
- 已完成的输出保持不回滚。
- 输出退出日志。

`-once` 模式用于执行单轮：

```bash
go run ./cmd/session-watcher -once
```

### 12. 测试计划

实现后增加核心单元测试：

- 配置解析：默认值、非法 interval、非法 message-limit、非法 session-workers、base URL 规范化。
- HTTP source：用 `httptest.Server` 验证 `/session`、`/session/{id}`、`/message?limit=N` 请求路径和解析。
- SQLite store：初始化 schema、session upsert、message 去重、游标更新。
- JSONL sink：并发写入时每条记录保持单行完整 JSON。
- watcher 增量逻辑：首次同步写入全部消息，第二次相同数据不重复写入，新增消息只追加新增行。
- watcher 并发逻辑：多个 session 可并发处理，单个 session 失败不影响其他 session。

### 13. 受影响文件

当前目录为空，将新增以下文件：

- `/Users/siegward/Developer/baidu/easydata/session_watcher/go.mod`：Go module 定义与依赖。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/cmd/session-watcher/main.go`：程序入口、参数解析、日志初始化、信号处理。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/config/config.go`：配置结构与校验。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/domain/domain.go`：核心结构、Source/Sink 接口。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/source/opencode/client.go`：open-code HTTP Source 实现。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/source/opencode/types.go`：open-code 响应结构与原始 JSON 保留逻辑。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/store/store.go`：SQLite 初始化与读写接口。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer.go`：JSONL Sink 实现。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/watcher/watcher.go`：轮询、并发和增量同步编排。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/**/**/*_test.go`：必要单元测试。

### 14. 预期结果

完成后可以在项目根目录执行：

```bash
go run ./cmd/session-watcher \
  -base-url http://localhost:57811 \
  -interval 10s \
  -message-limit 100 \
  -session-workers 8
```

程序会：

- 周期性发现 open-code session。
- 每个需要同步的 session 由独立 goroutine 并发处理，并受 `session-workers` 控制。
- 对每个 session 通过 `/message?limit=N` 拉取最近 N 条消息。
- 将新 message 逐行追加到当前 JSONL Sink：`./data/messages.jsonl`。
- 将同步状态写入 `./data/state.db`。
- 打印丰富的结构化日志，便于观察每轮同步、每个 session 和每次写入的状态。
- 重启后继续从本地 SQLite 状态增量追加，不重复输出已记录 message。
- 后续可通过新增 Source/Sink 实现接入其他数据源或输出到 ES、S3 等系统。
