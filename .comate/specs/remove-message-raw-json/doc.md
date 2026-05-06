# 移除 messages.raw_json 与所有 migration 设计文档

## 背景与目标

当前仍处于开发阶段，数据库没有版本兼容要求，所有开发都按一个当前 schema 版本处理。根据要求，本次不仅移除 `messages.raw_json`，还要把目前已经实现的 `schema_migrations` 机制整体去掉，后续版本稳定后再设计 migration。

当前 SQLite 的定位：

- 作为同步状态库；
- 作为 message 去重索引；
- 作为输出目标追踪索引；
- 不作为完整 message payload 存储；
- 不维护历史 schema 兼容。

目标：

- 删除 `schema_migrations` 表和所有 migration 执行逻辑。
- 使用单一当前 schema 初始化 `sessions` 和 `messages` 表。
- 当前 schema 的 `messages` 表不再包含 `raw_json` 字段。
- 写入 `messages` 时不再保存 `record.Message` 原始 JSON。
- 保留 `sessions.raw_json`，session 原始 JSON 仍保存。
- 保留 message 去重和输出路径追踪能力。
- 测试不再覆盖老 DB 迁移；只覆盖当前 schema。

## 当前相关实现

文件：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/store/store.go`

当前已经存在 migration 相关实现：

- `migration` 结构体；
- `migrate`；
- `migrationApplied`；
- `applyMigration`；
- `applyInitialSchema`；
- `applySessionMetadata`；
- `applyMessageOutputTracking`；
- `addColumnIfMissing`；
- `columnExists`。

当前 `messages` 表还包含 `raw_json`：

```sql
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT 0,
  written_at INTEGER NOT NULL,
  raw_json TEXT NOT NULL
);
```

当前 `CommitSessionSync` 写入 `raw_json`：

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
)
```

其中 `raw_json` 值为：

```go
string(record.Message)
```

## 设计方案

### 1. 删除 schema_migrations 和 migration 逻辑

移除以下内容：

- `schema_migrations` 表创建逻辑；
- migrations 列表；
- migration 版本记录；
- migration 事务执行逻辑；
- 字段存在性检查；
- 老 DB 兼容测试。

`Open` 逻辑改为：

```go
func Open(ctx context.Context, path string) (*Store, error) {
    // 创建 DB 目录
    // sql.Open
    // configure PRAGMA
    // init 当前 schema
}
```

即只调用：

```go
store.init(ctx)
```

不再调用：

```go
store.migrate(ctx)
```

### 2. 使用单一当前 schema

`init` 直接创建最终表结构。

#### sessions 表

```sql
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL DEFAULT 'default_user',
  agent_id TEXT NOT NULL DEFAULT 'default_agent',
  updated_at INTEGER NOT NULL DEFAULT 0,
  latest_message_id TEXT NOT NULL DEFAULT '',
  latest_message_created_at INTEGER NOT NULL DEFAULT 0,
  raw_json TEXT NOT NULL DEFAULT '',
  synced_at INTEGER NOT NULL DEFAULT 0
);
```

保留 `sessions.raw_json`，用于保存 session 详情原始 JSON。

#### messages 表

```sql
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT 0,
  written_at INTEGER NOT NULL,
  user_id TEXT NOT NULL DEFAULT 'default_user',
  agent_id TEXT NOT NULL DEFAULT 'default_agent',
  sink_type TEXT NOT NULL DEFAULT 'jsonl',
  output_root TEXT NOT NULL DEFAULT '',
  output_path TEXT NOT NULL DEFAULT '',
  output_session_file TEXT NOT NULL DEFAULT ''
);
```

不再包含 `raw_json`。

### 3. 保留当前索引

```sql
CREATE INDEX IF NOT EXISTS idx_sessions_user_agent ON sessions(user_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_messages_session_created ON messages(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_sink_output ON messages(sink_type, output_root, output_path);
CREATE INDEX IF NOT EXISTS idx_messages_user_agent_session ON messages(user_id, agent_id, session_id);
```

### 4. 写入逻辑不再引用 raw_json

`CommitSessionSync` 的 INSERT 改为：

```sql
INSERT OR IGNORE INTO messages(
  id,
  session_id,
  created_at,
  written_at,
  user_id,
  agent_id,
  sink_type,
  output_root,
  output_path,
  output_session_file
)
```

去掉：

- `raw_json` 列名；
- `string(record.Message)` 参数。

### 5. 测试调整

删除或改写以下测试思路：

- 老 DB migration 测试；
- `schema_migrations` 版本检查；
- 老表 `raw_json` 兼容测试。

保留和新增：

- 当前 schema 初始化测试；
- `messages` 表不存在 `raw_json` 列测试；
- `sessions` 表仍存在 `raw_json` 列测试；
- `CommitSessionSync` 输出追踪字段写入测试；
- 去重相关测试。

## 受影响文件

- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/store/store.go`
  - 删除 migration 相关代码；
  - 重写 `init` 为当前最终 schema；
  - 删除 `messages.raw_json`；
  - 调整 `CommitSessionSync`。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/store/store_test.go`
  - 删除 migration 测试；
  - 增加当前 schema 字段测试。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/.comate/specs/remove-message-raw-json/tasks.md`
  - 后续生成任务计划。

## 边界条件

- 由于不考虑 DB migration，已有旧 DB 不保证可用。
- 开发阶段如果 schema 变化，直接删除旧 DB 或换新的 `-db` 路径。
- 当前 JSONL 文件仍保存完整 message 内容。
- SQLite 中只保存 message 去重和输出追踪信息。

## 预期结果

完成后：

- 代码中不存在 `schema_migrations` 相关逻辑。
- 新 DB 中不存在 `schema_migrations` 表。
- 新 DB 的 `messages` 表不存在 `raw_json` 字段。
- `messages` 表仍能记录 message ID、session、创建时间、写入时间、user/agent、sink、输出根目录和输出路径。
- message 去重、增量同步和输出追踪不受影响。
