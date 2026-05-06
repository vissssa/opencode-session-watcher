# DB 输出追踪与 schema migrations 优化设计文档

## 背景与目标

当前项目已经使用 SQLite DB 作为长期持久化状态，记录 session 游标和已处理 message。当前实现可以避免重启后重复同步，但 DB 中只表达“这个 message 已经处理过”，没有完整表达：

- 每条 message 写到了哪个 user 目录；
- 写到了哪个 agent 目录；
- 写到了哪个 session 文件；
- 使用了哪个 Sink；
- 使用了哪个输出根目录；
- 最终输出文件路径是什么；
- 当前 DB schema 是哪个版本，后续如何平滑升级。

本次优化目标：

1. 增加 `schema_migrations`，为后续 DB schema 平滑升级打基础。
2. 扩展 `messages` 表，让 DB 完整记录每条 message 的输出目标信息。
3. 扩展 `sessions` 表，保存 session 的 `user_id`、`agent_id` 元数据。
4. 保持当前单副本同步逻辑不变，不实现多副本租约。
5. 兼容已有老 DB，不删除历史状态，不要求用户清空 DB。

## 明确不做的内容

本次不做多副本协调，包括：

- 不实现 session lease。
- 不实现 message claim/pending 状态。
- 不实现副本 instance ID。
- 不实现文件锁。
- 不改为 PostgreSQL/MySQL 等中心化 Store。

多副本问题后续单独处理。

## 当前相关实现

### SQLite Store

文件：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/store/store.go`

当前表结构：

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
```

当前 `CommitSessionSync` 在 Sink 写入成功后，把 message 插入 `messages` 表，并更新 session 游标。

### Watcher

文件：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/watcher/watcher.go`

当前流程：

1. 拉取 session 列表。
2. 根据 `sessions.updated_at` 判断是否需要同步。
3. 获取 session 详情和 messages。
4. 过滤未处理 message。
5. 按 `createdAt` 升序排序。
6. 写入 Sink。
7. 调用 `CommitSessionSync` 更新 SQLite。

### JSONL Sink

文件：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer.go`

当前路径逻辑：

```text
{output-dir}/{user_id}/{agent_id}/{session_id}.jsonl
```

但这个路径只在 Sink 内部计算，SQLite 不知道 message 最终写入了哪个文件。

## 设计方案

### 1. 增加 schema_migrations 表

新增表：

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  applied_at INTEGER NOT NULL
);
```

迁移执行规则：

- 启动时先确保 `schema_migrations` 存在。
- 定义有序 migrations 列表。
- 每个 migration 有 `version`、`name`、`statements`。
- 查询 `schema_migrations`，只执行未应用的 migration。
- 每个 migration 在事务中执行。
- migration 执行成功后插入 `schema_migrations` 记录。

### 2. 保留初始建表兼容性

为了兼容空 DB，保留创建基础表的逻辑，但把它纳入 migration 体系。

Migration 1：初始化基础表。

```sql
CREATE TABLE IF NOT EXISTS sessions (...);
CREATE TABLE IF NOT EXISTS messages (...);
CREATE INDEX IF NOT EXISTS idx_messages_session_created ON messages(session_id, created_at);
```

对于已有 DB：

- 表已存在时不会破坏数据。
- migration 只记录版本已应用。

### 3. 扩展 sessions 表记录 user/agent

Migration 2：扩展 session metadata。

新增字段：

```sql
ALTER TABLE sessions ADD COLUMN user_id TEXT NOT NULL DEFAULT 'default_user';
ALTER TABLE sessions ADD COLUMN agent_id TEXT NOT NULL DEFAULT 'default_agent';
```

新增索引：

```sql
CREATE INDEX IF NOT EXISTS idx_sessions_user_agent ON sessions(user_id, agent_id);
```

兼容逻辑：

- 老 DB 已有 `sessions` 表但没有字段时，通过 migration 添加字段。
- 新 DB 直接创建最终字段也可以，但为避免 migration 重复复杂度，建议统一先建基础表，再用 `AddColumnIfMissing` 添加字段。

### 4. 扩展 messages 表记录输出目标

Migration 3：扩展 message output tracking。

新增字段：

```sql
ALTER TABLE messages ADD COLUMN user_id TEXT NOT NULL DEFAULT 'default_user';
ALTER TABLE messages ADD COLUMN agent_id TEXT NOT NULL DEFAULT 'default_agent';
ALTER TABLE messages ADD COLUMN sink_type TEXT NOT NULL DEFAULT 'jsonl';
ALTER TABLE messages ADD COLUMN output_root TEXT NOT NULL DEFAULT '';
ALTER TABLE messages ADD COLUMN output_path TEXT NOT NULL DEFAULT '';
ALTER TABLE messages ADD COLUMN output_session_file TEXT NOT NULL DEFAULT '';
```

字段含义：

- `user_id`：message 输出所属 user。
- `agent_id`：message 输出所属 agent。
- `sink_type`：当前为 `jsonl`，后续可扩展 `es`、`s3`。
- `output_root`：当前运行配置的输出根目录，例如 `./data/messages`。
- `output_path`：实际写入文件路径，例如 `data/messages/default_user/default_agent/ses_xxx.jsonl`。
- `output_session_file`：session 文件名，例如 `ses_xxx.jsonl`。

新增索引：

```sql
CREATE INDEX IF NOT EXISTS idx_messages_sink_output ON messages(sink_type, output_root, output_path);
CREATE INDEX IF NOT EXISTS idx_messages_user_agent_session ON messages(user_id, agent_id, session_id);
```

兼容逻辑：

- 旧 message 行迁移后默认 `sink_type='jsonl'`。
- 旧 message 行的 `output_root/output_path/output_session_file` 为空，表示历史同步状态存在，但无法追溯旧输出目标。
- 旧行仍视为已处理，不重新输出。

### 5. 抽出输出路径计算能力

当前 `FileSink.pathFor` 是私有方法，Store 无法知道输出路径。需要让 watcher 能在提交 DB 前获得输出路径。

方案：新增接口：

```go
type PathResolver interface {
    PathFor(record domain.MessageRecord) string
    SinkType() string
    OutputRoot() string
}
```

JSONL Sink 实现：

```go
func (s *FileSink) PathFor(record domain.MessageRecord) string
func (s *FileSink) SinkType() string { return "jsonl" }
func (s *FileSink) OutputRoot() string { return s.rootDir }
```

同时保留 `Sink` 接口不变：

```go
type Sink interface {
    WriteMessages(ctx context.Context, records []MessageRecord) error
    Close() error
}
```

这样可以避免大范围改动写入接口。

### 6. 扩展 MessageRecord

在 `internal/domain/domain.go` 中扩展：

```go
type MessageRecord struct {
    SyncedAt         int64           `json:"synced_at"`
    UserID           string          `json:"user_id"`
    AgentID          string          `json:"agent_id"`
    SessionID        string          `json:"session_id"`
    MessageID        string          `json:"message_id"`
    MessageCreatedAt int64           `json:"message_created_at"`
    Session          json.RawMessage `json:"session"`
    Message          json.RawMessage `json:"message"`

    SinkType          string `json:"-"`
    OutputRoot        string `json:"-"`
    OutputPath        string `json:"-"`
    OutputSessionFile string `json:"-"`
}
```

这些输出追踪字段不写入 JSONL 内容，只写入 SQLite。

### 7. Watcher 构造输出追踪字段

在 `syncSession` 构造 records 后：

1. 如果 Sink 实现了 `PathResolver`，为每条 record 填充：
   - `SinkType`
   - `OutputRoot`
   - `OutputPath`
   - `OutputSessionFile`
2. 再调用 `Sink.WriteMessages`。
3. Sink 成功后调用 `CommitSessionSync`，Store 把这些字段写入 `messages` 表。

示意：

```go
if resolver, ok := w.sink.(domain.PathResolver); ok {
    for i := range records {
        records[i].SinkType = resolver.SinkType()
        records[i].OutputRoot = resolver.OutputRoot()
        records[i].OutputPath = resolver.PathFor(records[i])
        records[i].OutputSessionFile = filepath.Base(records[i].OutputPath)
    }
}
```

### 8. Store 接口调整

当前：

```go
CommitSessionSync(ctx context.Context, session domain.Session, messages []domain.Message, syncedAt int64) error
```

调整为：

```go
CommitSessionSync(ctx context.Context, session domain.Session, records []domain.MessageRecord, syncedAt int64) error
```

原因：

- message 原始信息仍在 `record.Message` 中。
- `record` 包含 user/agent/sink/output_path 等 DB 需要的字段。
- watcher 不需要额外维护 message 到 output path 的映射。

对应 Store interface 和测试需要同步更新。

### 9. 数据写入规则

`messages` 插入逻辑改为：

```sql
INSERT OR IGNORE INTO messages(
  id,
  session_id,
  created_at,
  written_at,
  raw_json,
  user_id,
  agent_id,
  sink_type,
  output_root,
  output_path,
  output_session_file
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

如果 `INSERT OR IGNORE` 因主键存在而忽略：

- 不覆盖历史输出目标。
- 避免因为不同 output-dir 运行导致历史状态被改写。

`sessions` upsert 逻辑改为同时记录：

```sql
user_id = excluded.user_id,
agent_id = excluded.agent_id
```

### 10. 边界条件

- 旧 DB 没有 `schema_migrations`：启动时创建并执行缺失迁移。
- 旧 DB 已有 `sessions/messages`：不会删除，不会清空。
- 旧 DB 已有 message：迁移后仍视为已处理，不重新输出。
- Sink 不实现 `PathResolver`：DB 中 `sink_type/output_*` 写空值或 `unknown`，但同步仍可运行。
- 当前 JSONL Sink 实现 `PathResolver`，所以正常会写完整输出目标。
- output-dir 变化：新 message 会记录新 output_root/output_path；旧 message 不会被改写。

### 11. 测试计划

新增或更新测试：

- Store migration：
  - 新 DB 创建 `schema_migrations`。
  - 老 DB 只有基础表时能添加新字段。
  - migration 重复运行不会报错。
- Store output tracking：
  - `CommitSessionSync` 写入 `user_id/agent_id/sink_type/output_root/output_path/output_session_file`。
  - session 表写入 `user_id/agent_id`。
  - 已存在 message 不被覆盖。
- JSONL Sink：
  - `PathFor` 返回 `{output-dir}/{user_id}/{agent_id}/{session_id}.jsonl`。
  - `SinkType` 返回 `jsonl`。
  - `OutputRoot` 返回构造时传入的根目录。
- Watcher：
  - 写入 records 前能填充 output tracking 字段。
  - records 仍按时间升序写入。
- 端到端：
  - `go test ./...`。
  - 使用本地 open-code 服务跑 `-once`，检查 SQLite 中 output 字段与实际文件路径一致。

## 受影响文件

- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/domain/domain.go`
  - 新增 `PathResolver` 接口和 MessageRecord 输出追踪字段。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer.go`
  - 暴露 `PathFor`，增加 `SinkType`、`OutputRoot`。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/store/store.go`
  - 增加 migration 机制，扩展表字段，调整 `CommitSessionSync`。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/watcher/watcher.go`
  - 填充输出追踪字段，调用新的 Store 接口。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/**/*_test.go`
  - 更新相关测试。

## 预期结果

完成后，SQLite DB 会长期保留并完整表达：

- 每条 message 是否已处理；
- message 属于哪个 user；
- message 属于哪个 agent；
- message 属于哪个 session；
- message 使用哪个 Sink 写出；
- message 使用哪个输出根目录；
- message 实际写到了哪个 session JSONL 文件；
- 当前 DB 已经应用了哪些 schema migration。

多副本协调暂不实现，后续单独规划。
