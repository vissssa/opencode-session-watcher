# 多副本安全持久化优化设计文档

## 背景与问题

当前实现已经可以将 SQLite DB 文件长期保留，用于记录 session/message 同步状态。但如果启动多个副本同时运行，现有实现存在冲突风险：

1. 多个副本会同时读取 `/session`，并对同一个 session 做 `shouldSync` 判断。
2. 多个副本可能同时发现同一批 message 在 SQLite 中不存在。
3. 多个副本可能同时向同一个 `{output-dir}/{user_id}/{agent_id}/{session_id}.jsonl` 文件追加写入。
4. 当前 `messages` 表只记录 message 是否已处理，不记录输出目标、处理状态、处理者和租约信息。
5. 当前 `sessions` 表没有租约字段，无法表达“某个 session 正在被哪个副本处理”。

因此，目前实现适合单进程/单副本长期运行；如果直接多副本运行，可能出现重复写 JSONL、状态竞争、文件追加顺序不稳定等问题。

## 目标

本次优化目标是：

- 保留 SQLite DB 文件作为长期持久化状态。
- 支持多个进程/副本共享同一个 SQLite DB 和同一个输出目录时协同工作。
- 确保同一时刻同一个 session 只被一个副本处理。
- 降低重复写 JSONL 的概率，并让状态表能记录输出路径、处理状态和处理者。
- 为未来接入集中式数据库或外部 Sink（ES/S3）保留接口扩展空间。

## 非目标与边界

### 不承诺跨机器共享 SQLite 的强一致

SQLite 适合单机本地文件或可靠 POSIX 文件锁语义的共享卷。对于 Kubernetes 多 Pod、多机器共享 PVC/NFS 的场景，SQLite 文件锁是否可靠取决于底层存储。

本次优化会让“多个本机进程或共享可靠 SQLite 文件锁的副本”更安全，但不把 SQLite 设计成真正的分布式协调服务。

如果目标是标准 K8s 多副本高可用，推荐后续将 `Store` 抽象扩展为 PostgreSQL/MySQL/Redis lease 等中心化存储。

### JSONL 追加无法与 SQLite 完全原子提交

当前输出是文件追加。即使引入 SQLite 租约，也无法让“写文件”和“更新 DB”形成真正分布式原子事务。

本次优化采用：

- session 级租约避免多个副本处理同一 session；
- message 状态记录降低重复输出；
- 输出成功后再标记 message 为 written；
- 如果进程在“文件写成功、DB 标记 written 前”崩溃，仍可能产生重复行。

这是文件 Sink 的天然限制。若需要更强一致性，后续应使用 outbox 模式或支持幂等写入的外部 Sink。

## 当前相关代码

- SQLite 初始化和状态写入：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/store/store.go`
  - `Open`：打开 DB，当前设置 `SetMaxOpenConns(1)`。
  - `init`：创建 `sessions`、`messages` 表。
  - `GetSessionState`：读取 session 游标。
  - `UnseenMessages`：逐条判断 message 是否存在。
  - `CommitSessionSync`：写入 message 和更新 session 游标。
- 同步编排：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/watcher/watcher.go`
  - `SyncOnce`：拉取 session 列表并启动 worker。
  - `shouldSync`：只基于 session 更新时间判断是否同步。
  - `syncSession`：拉取详情和消息，写 Sink，再提交 SQLite。
- JSONL Sink：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer.go`
  - 当前只有进程内 mutex，不能协调多个进程或多个副本。

## 设计方案

### 1. 增加副本身份

新增配置：

```bash
-instance-id auto
-session-lease-ttl 2m
-sqlite-busy-timeout 5s
```

参数说明：

- `-instance-id`：当前副本标识。默认 `auto`，启动时生成 `hostname-pid-random`。
- `-session-lease-ttl`：session 租约有效期，默认 `2m`。
- `-sqlite-busy-timeout`：SQLite 写锁等待时间，默认 `5s`。

配置结构扩展：

```go
type Config struct {
    // existing fields...
    InstanceID        string
    SessionLeaseTTL   time.Duration
    SQLiteBusyTimeout time.Duration
}
```

启动日志必须打印：

- `instance_id`
- `session_lease_ttl`
- `sqlite_busy_timeout`

### 2. SQLite 迁移机制

当前只使用 `CREATE TABLE IF NOT EXISTS`，不适合长期演进。新增轻量迁移：

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at INTEGER NOT NULL
);
```

每次启动执行迁移：

- 查询已应用版本。
- 按版本顺序执行未应用迁移。
- 每个迁移在事务中执行。

需要支持已有老 DB 平滑升级。SQLite 对 `ADD COLUMN` 支持较好，因此新字段通过 `ALTER TABLE ... ADD COLUMN ...` 添加，并忽略“duplicate column”错误或先通过 `PRAGMA table_info` 判断字段是否存在。

### 3. 扩展 sessions 表为 session 租约表

新增字段：

```sql
ALTER TABLE sessions ADD COLUMN user_id TEXT NOT NULL DEFAULT 'default_user';
ALTER TABLE sessions ADD COLUMN agent_id TEXT NOT NULL DEFAULT 'default_agent';
ALTER TABLE sessions ADD COLUMN lease_owner TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN lease_until INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN lease_updated_at INTEGER NOT NULL DEFAULT 0;
```

含义：

- `user_id` / `agent_id`：记录 session 元数据，便于追溯和后续输出定位。
- `lease_owner`：当前持有该 session 处理权的副本 ID。
- `lease_until`：租约过期时间，毫秒时间戳。
- `lease_updated_at`：租约更新时间。

新增索引：

```sql
CREATE INDEX IF NOT EXISTS idx_sessions_lease_until ON sessions(lease_until);
```

### 4. 扩展 messages 表记录输出状态

新增字段：

```sql
ALTER TABLE messages ADD COLUMN user_id TEXT NOT NULL DEFAULT 'default_user';
ALTER TABLE messages ADD COLUMN agent_id TEXT NOT NULL DEFAULT 'default_agent';
ALTER TABLE messages ADD COLUMN output_path TEXT NOT NULL DEFAULT '';
ALTER TABLE messages ADD COLUMN status TEXT NOT NULL DEFAULT 'written';
ALTER TABLE messages ADD COLUMN owner_id TEXT NOT NULL DEFAULT '';
ALTER TABLE messages ADD COLUMN claimed_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE messages ADD COLUMN claim_until INTEGER NOT NULL DEFAULT 0;
```

含义：

- `output_path`：该 message 写入的 JSONL 目标路径。
- `status`：`pending` 或 `written`。
- `owner_id`：处理该 message 的副本 ID。
- `claimed_at` / `claim_until`：message 级占用信息。

兼容老数据：

- 旧 message 行没有这些字段，迁移后默认 `status='written'`。
- 这表示旧数据被视为已完成处理，不会重新输出。

新增索引：

```sql
CREATE INDEX IF NOT EXISTS idx_messages_status_claim ON messages(status, claim_until);
CREATE INDEX IF NOT EXISTS idx_messages_output_path ON messages(output_path);
```

### 5. Session 级租约

新增 Store 方法：

```go
TryClaimSession(ctx context.Context, session domain.Session, ownerID string, nowMillis int64, ttl time.Duration) (bool, SessionState, error)
ReleaseSessionLease(ctx context.Context, sessionID string, ownerID string) error
```

领取规则：

- 如果 session 不存在，插入 session 行并设置租约。
- 如果 session 存在且 `lease_until <= now`，当前副本可抢占。
- 如果 session 存在且 `lease_owner == ownerID`，当前副本可续租。
- 否则说明其他副本正在处理，当前副本跳过该 session。

SQL 逻辑示意：

```sql
UPDATE sessions
SET lease_owner = ?, lease_until = ?, lease_updated_at = ?
WHERE id = ?
  AND (lease_until <= ? OR lease_owner = ?);
```

如果更新行数为 1，说明领取成功。否则领取失败。

处理流程变化：

1. `SyncOnce` 拉取 session 列表。
2. 对每个可能需要同步的 session，先调用 `TryClaimSession`。
3. 只有领取成功的 session 才进入 worker 队列。
4. session 完成后调用 `ReleaseSessionLease`。
5. 如果副本崩溃，租约到期后其他副本可继续处理。

### 6. Message 状态优化

由于 session 级租约已经保证同一 session 不会被多个副本同时处理，message 级冲突概率会显著降低。但为提升可观测性和异常恢复能力，仍记录 message 输出状态。

新增 Store 方法：

```go
ClaimUnseenMessages(ctx context.Context, session domain.Session, messages []domain.Message, ownerID string, outputPathFunc func(domain.Message) string, nowMillis int64, ttl time.Duration) ([]domain.Message, error)
MarkMessagesWritten(ctx context.Context, session domain.Session, messages []domain.Message, outputPaths map[string]string, writtenAt int64) error
```

建议实现顺序：

1. `ClaimUnseenMessages` 在事务中对未存在的 message 执行 `INSERT ... status='pending'`。
2. 已存在且 `status='written'` 的 message 跳过。
3. 已存在且 `status='pending'` 但 `claim_until <= now` 的 message 可被当前 owner 重新领取。
4. Sink 写入成功后调用 `MarkMessagesWritten`，将对应 message 标为 `written`。

注意：为了避免当前改造过大，可以第一步只实现 session 级租约，并扩展 message 字段记录 `output_path/status/owner_id`；message 级 claim 作为后续增强。但从多副本角度，session 级租约是必须实现的核心。

### 7. JSONL Sink 多副本策略

当前 `FileSink` 的 mutex 只保护单进程。多副本时依赖 session 租约保证同一 session 文件不会被多个副本同时写。

可选增强：增加文件锁。

方案：对每个 session 输出文件额外创建 lock 文件：

```text
{session_id}.jsonl.lock
```

写入前对 lock 文件执行 `flock`，写入后释放。

边界：

- `flock` 适合同机多进程或支持文件锁的文件系统。
- 对 NFS/PVC 是否可靠取决于存储实现。
- 如果后续接 S3/ES，应由对应 Sink 自身保证幂等或批量写安全。

本次建议：先实现 DB session lease，不强制增加 `flock`，因为当前代码的主要协调点应放在 Store 层。若用户明确要同机多个进程同时写同一 output-dir，再加文件锁。

### 8. SQLite 连接参数优化

打开 SQLite 时增加：

- `PRAGMA busy_timeout = {sqlite-busy-timeout}`
- `PRAGMA journal_mode = WAL`
- `PRAGMA synchronous = NORMAL`
- `PRAGMA foreign_keys = ON`

原因：

- `busy_timeout`：多个副本争抢写锁时等待一段时间，而不是立即失败。
- `WAL`：提高读写并发能力。
- `synchronous=NORMAL`：在 WAL 下性能与可靠性较平衡。
- `foreign_keys=ON`：为后续关系约束预留。

仍保留：

```go
db.SetMaxOpenConns(1)
```

这是单进程内减少 SQLite 写锁冲突。多进程之间依赖 SQLite 文件锁和 busy_timeout。

### 9. Watcher 流程调整

当前流程：

```text
ListSessions -> shouldSync -> worker -> GetSession -> ListMessages -> UnseenMessages -> Sink.Write -> CommitSessionSync
```

调整后：

```text
ListSessions
  -> Get local state
  -> shouldSync
  -> TryClaimSession
  -> worker
      -> GetSession
      -> merge metadata
      -> fetchUntilBoundary
      -> ClaimUnseenMessages
      -> sort claimed messages
      -> Sink.WriteMessages
      -> MarkMessagesWritten + update session cursor
      -> ReleaseSessionLease
```

如果任一 session 失败：

- 不更新 session 游标。
- 已经标记为 `pending` 的 message 等 `claim_until` 到期后可重新处理。
- session lease 等 `lease_until` 到期后可被其他副本接管，或当前副本在失败退出前主动释放。

### 10. 输出路径记录

当前 `FileSink` 内部计算输出路径，Store 不知道路径。为让 DB 记录 `output_path`，需要把路径计算能力抽出为接口或公开方法。

建议新增：

```go
type PathResolver interface {
    PathFor(record domain.MessageRecord) string
}
```

或让 Sink 返回写入结果：

```go
type WriteResult struct {
    MessageID  string
    OutputPath string
}

type Sink interface {
    WriteMessages(ctx context.Context, records []MessageRecord) ([]WriteResult, error)
    Close() error
}
```

为了减少接口破坏，本次建议采用 `PathResolver`，由 watcher 在写入前计算 `MessageRecord.OutputPath` 或由 Store 接收 `outputPath`。

也可扩展 `MessageRecord`：

```go
type MessageRecord struct {
    // existing fields...
    OutputPath string `json:"-"`
}
```

`json:"-"` 避免把本地路径写进 JSONL 内容，但 DB 可记录。

### 11. 预期多副本运行方式

同机或可靠共享卷场景：

```bash
session-watcher \
  -instance-id replica-1 \
  -db ./data/state.db \
  -output-dir ./data/messages

session-watcher \
  -instance-id replica-2 \
  -db ./data/state.db \
  -output-dir ./data/messages
```

Kubernetes 多副本建议：

- 不建议多个 Pod 直接共享 SQLite 文件作为强一致协调。
- 推荐后续扩展 PostgreSQL Store。
- JSONL 本地文件 Sink 也不适合作为多 Pod 输出目标，推荐 S3/ES Sink。

## 受影响文件

- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/config/config.go`
  - 新增 `instance-id`、`session-lease-ttl`、`sqlite-busy-timeout` 参数。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/cmd/session-watcher/main.go`
  - 生成/注入 instance ID，打印多副本相关启动日志。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/store/store.go`
  - 增加迁移机制、SQLite PRAGMA、session lease、message 状态字段和相关方法。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/watcher/watcher.go`
  - 在 session 处理前领取租约，完成后释放租约；调整 message claim/write/mark 流程。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer.go`
  - 暴露或复用输出路径计算逻辑，必要时支持文件锁。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/domain/domain.go`
  - 扩展 `MessageRecord` 或新增 `WriteResult` / `PathResolver` 相关结构。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/**/*_test.go`
  - 增加迁移、租约竞争、租约过期、重复 message claim、输出路径记录等测试。

## 测试计划

### 单元测试

- schema migration：老 DB 能升级到新 schema，已有 message 默认 `status='written'`。
- session lease：
  - 空 session 可被 replica-1 领取。
  - 未过期租约不能被 replica-2 领取。
  - 已过期租约可被 replica-2 抢占。
  - 同 owner 可续租。
- watcher：
  - 多个 session 能被不同 worker 处理。
  - 已被其他 owner 租约占用的 session 会跳过。
  - session 失败不推进游标。
- message 状态：
  - 新 message 被记录 output_path、owner_id、status。
  - written message 不会再次输出。
  - pending 过期后可重新领取。
- config：
  - 新参数默认值和非法值校验。

### 集成测试

- 启动两个 watcher 实例使用同一个临时 SQLite DB 和同一个临时 output-dir。
- 模拟同一批 session/message。
- 验证同一个 session 只被一个实例处理。
- 验证输出 JSONL 没有重复 message ID。
- 验证 DB 中 `messages.output_path` 与实际文件路径一致。

## 预期结果

完成后：

- 单副本行为保持兼容。
- 多个副本共享同一个 SQLite DB 时，不会同时处理同一个 session。
- SQLite DB 能长期保存同步状态、输出路径、处理状态和副本信息。
- 副本崩溃后，租约到期，其他副本可以接管。
- 对真正分布式多 Pod 场景，会明确建议使用中心化 Store/Sink，而不是直接依赖 SQLite 文件作为分布式锁。
