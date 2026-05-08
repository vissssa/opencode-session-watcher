# session_watcher 详细设计文档

**版本：** v2.0
**日期：** 2026-05-08
**状态：** 已发布

---

## 目录

1. [概述](#1-概述)
2. [系统边界与上下文](#2-系统边界与上下文)
3. [架构总览](#3-架构总览)
4. [模块详细设计](#4-模块详细设计)
   - 4.1 [config — 配置解析](#41-config--配置解析)
   - 4.2 [domain — 核心类型与接口](#42-domain--核心类型与接口)
   - 4.3 [source/opencode — HTTP 数据源](#43-sourceopencode--http-数据源)
   - 4.4 [watcher — 核心编排](#44-watcher--核心编排)
   - 4.5 [store — PostgreSQL 状态存储](#45-store--postgresql-状态存储)
   - 4.6 [sink/jsonl — JSONL 文件输出](#46-sinkjsonl--jsonl-文件输出)
   - 4.7 [status — 运行状态快照](#47-status--运行状态快照)
   - 4.8 [health — HTTP 健康探针](#48-health--http-健康探针)
5. [数据模型](#5-数据模型)
   - 5.1 [PostgreSQL Schema](#51-postgresql-schema)
   - 5.2 [JSONL 输出格式](#52-jsonl-输出格式)
6. [关键流程](#6-关键流程)
   - 6.1 [启动流程](#61-启动流程)
   - 6.2 [单轮同步（SyncOnce）](#62-单轮同步synconce)
   - 6.3 [增量边界探测（fetchUntilBoundary）](#63-增量边界探测fetchuntilboundary)
   - 6.4 [消息状态机](#64-消息状态机)
7. [并发模型](#7-并发模型)
8. [错误处理策略](#8-错误处理策略)
9. [部署与运维](#9-部署与运维)
   - 9.1 [命令行参数](#91-命令行参数)
   - 9.2 [目录布局](#92-目录布局)
   - 9.3 [健康检查 API](#93-健康检查-api)
   - 9.4 [日志](#94-日志)
10. [已知限制与风险](#10-已知限制与风险)
11. [扩展指南](#11-扩展指南)
12. [测试策略](#12-测试策略)
13. [外部服务集成指南](#13-外部服务集成指南)

---

## 1. 概述

`session_watcher` 是一个 Go 命令行服务，周期性地从 **open-code HTTP API** 拉取 AI 对话会话的消息，以**增量去重**方式追加写入本地 **JSONL 文件**，并用 **PostgreSQL** 维护同步游标与消息状态，供下游数据处理链路消费。

**核心目标：**

| 目标 | 描述 |
|------|------|
| 不重不漏 | 每条消息恰好写出一次（at-least-once，进程崩溃后重启最多重复写入最后一批） |
| 增量同步 | 仅拉取上次同步游标之后的新消息，避免全量扫描 |
| 高并发 | Worker Pool 模型，默认 8 个 Worker 并发处理多个 Session |
| 可观测 | 结构化日志 + HTTP `/healthz` `/status` 端点 |
| 可扩展 | Source / Sink 抽象，替换数据源或输出目标无需修改核心逻辑 |

---

## 2. 系统边界与上下文

```
┌─────────────────────────────────────────────┐
│               open-code 服务                  │
│  GET /session                                │
│  GET /session/{id}                           │
│  GET /session/{id}/message?limit=N           │
└───────────────────┬─────────────────────────┘
                    │ HTTP（可配置超时，最多 3 次重试）
                    ▼
┌─────────────────────────────────────────────┐
│              session_watcher                  │
│                                              │
│  PostgreSQL（状态 DB） JSONL 文件（输出）      │
└─────────────────────────────────────────────┘
                    │ 文件系统 / TCP
                    ▼
       下游数据处理链路（读取 JSONL）
```

**外部依赖：**

| 依赖 | 协议 | 说明 |
|------|------|------|
| open-code 服务 | HTTP GET | 提供 Session 列表、详情、消息列表接口 |
| PostgreSQL | TCP（pgx/v5） | 存储同步状态、消息去重标记、记忆标记 |
| 本地文件系统 | 文件 I/O | 存储 JSONL 输出文件 |

**不依赖：**

- 无消息队列（Kafka / RabbitMQ）
- 无外部缓存（Redis）

---

## 3. 架构总览

```
cmd/session-watcher/main.go
│
├── config.Parse()           — 参数解析与校验
├── status.NewReporter()     — 运行状态快照
├── health.Start()           — HTTP 健康探针
├── store.Open()             — PostgreSQL 连接池初始化
├── jsonl.NewFileSink()      — JSONL 文件输出
├── opencode.NewHTTPSource() — HTTP 数据拉取
└── watcher.New()            — 核心编排
    └── SyncOnce()
        ├── source.ListSessions()
        ├── store.GetSessionState()        — 读取游标
        └── [Worker Pool]
            └── syncSession()
                ├── source.GetSession()
                ├── fetchUntilBoundary()   — 增量探测
                │   └── source.ListMessages()
                │   └── store.AnyMessageExists()
                ├── store.UnseenMessages() — 去重
                ├── store.PrepareMessageRecords()
                ├── sink.WriteMessages()
                └── store.MarkMessagesWritten()
```

**接口分层（依赖方向）：**

```
watcher
  └── depends on →  domain.Source   (实现：opencode.HTTPSource)
  └── depends on →  domain.Sink     (实现：jsonl.FileSink)
  └── depends on →  watcher.Store   (实现：store.Store)
```

核心编排层（watcher）**不导入任何具体实现包**，全部通过接口通信。

---

## 4. 模块详细设计

### 4.1 config — 配置解析

**职责：** 解析命令行参数，做合法性校验，返回不可变的 `Config` 结构体。

**关键设计：**

- 所有字段均有默认值，生产环境至少需要覆盖 `-base-url`
- `Validate()` 在 `Parse()` 末尾自动调用，校验失败以退出码 `2` 终止进程
- `MessageLimit`（步长）不能大于 `MaxMessageFetch`（上限），否则 `fetchUntilBoundary` 逻辑会跳过扩展直接触顶

**参数列表：** 见 [第 9.1 节](#91-命令行参数)。

---

### 4.2 domain — 核心类型与接口

**职责：** 定义跨模块共用的值类型和接口约定，是整个系统的类型中心。

**核心类型：**

| 类型 | 说明 |
|------|------|
| `Session` | 会话元数据（ID、UserID、AgentID、UpdatedAt、原始 JSON） |
| `Message` | 消息元数据（ID、SessionID、CreatedAt、原始 JSON） |
| `MessageRecord` | 写入 Sink 的完整记录，含 `synced_at`、`session`、`message` 原始 JSON 及输出追踪字段 |

**接口定义：**

```go
// Source — 数据输入抽象
type Source interface {
    ListSessions(ctx context.Context) ([]Session, error)
    GetSession(ctx context.Context, sessionID string) (Session, error)
    ListMessages(ctx context.Context, sessionID string, limit int) ([]Message, error)
}

// Sink — 数据输出抽象
type Sink interface {
    WriteMessages(ctx context.Context, records []MessageRecord) error
    Close() error
}

// PathResolver — 可选接口，由 Sink 按需实现
type PathResolver interface {
    PathFor(record MessageRecord) string
    SinkType() string
    OutputRoot() string
}
```

**设计原则：** `Raw json.RawMessage` 字段透传原始 JSON，即使上游新增字段也不会丢失，做到对上游字段变更的零感知。

---

### 4.3 source/opencode — HTTP 数据源

**职责：** 实现 `domain.Source` 接口，通过 HTTP 访问 open-code 服务。

**API 对应关系：**

| 接口方法 | HTTP 请求 |
|----------|-----------|
| `ListSessions` | `GET /session` |
| `GetSession` | `GET /session/{sessionID}` |
| `ListMessages` | `GET /session/{sessionID}/message?limit=N` |

**重试策略：**

- 最多 3 次（含首次）
- 指数退避：第 1 次 100ms，第 2 次 200ms，加随机 jitter（避免惊群效应）
- **不重试条件：** `ctx` 已取消、4xx（429 除外）客户端错误
- **重试条件：** 5xx 服务端错误、429 限流、网络错误

**元数据合并（`mergeSessionMetadata`）：**

列表接口与详情接口返回的 `UserID`/`AgentID` 可能不一致，规则如下：
1. 详情接口有效值优先
2. 详情为空或为默认占位值时，取列表接口的值
3. 仍为空则填充全局默认值 `default_user` / `default_agent`

---

### 4.4 watcher — 核心编排

**职责：** 周期性执行 `SyncOnce`，协调 Source、Sink、Store 完成增量同步。

#### SyncOnce 流程

```
1. source.ListSessions()
2. 为每个 Session 读取本地状态（GetSessionState）
3. shouldSync() 过滤无需同步的 Session
4. 启动 Worker Pool（默认 8 个）
5. 每个 Worker 调用 syncSession()
6. 汇总 RoundResult 统计
```

#### shouldSync 判断逻辑

| 条件 | 结论 |
|------|------|
| 本地无记录（首次见到） | 同步 |
| 远端 `UpdatedAt == 0` | 保守同步 |
| 远端 `UpdatedAt > 本地记录值` | 同步 |
| 其他 | 跳过 |

#### syncSession 流程

```
1. GetSession() 拉取详情 → mergeSessionMetadata()
2. fetchUntilBoundary() 探测增量边界
3. UpdateSessionFetchStats() 记录本次拉取统计
4. UnseenMessages() 去重（排除已 written 的消息）
5. sortMessages() 按 CreatedAt 升序排列
6. fillOutputTracking() 填充输出路径（PathResolver 接口）
7. PrepareMessageRecords() → INSERT ... ON CONFLICT DO NOTHING 到 PostgreSQL
8. sink.WriteMessages() 写出到 JSONL
9. MarkMessagesWritten() 推进状态和游标
```

#### fetchUntilBoundary 算法

动态扩展 `limit`，以最小请求次数找到增量消息边界：

```
limit = min(MessageLimit, MaxMessageFetch)
loop:
    messages = ListMessages(sessionID, limit)
    if AnyMessageExists(messages):       // 找到边界，停止
        return messages, reachedMax=false
    if len(messages) < limit:            // 已取完，停止
        return messages, reachedMax=false
    if limit >= MaxMessageFetch:         // 触顶，记 warn，停止
        return messages, reachedMax=true
    limit = min(limit + MessageLimit, MaxMessageFetch)
```

**触顶风险：** `reachedMax=true` 时只写 warn 不失败，若两轮之间新增消息超过 `MaxMessageFetch` 条，这批消息将**永久遗漏**（无重试）。

---

### 4.5 store — PostgreSQL 状态存储

**职责：** 提供消息去重标记和 Session 游标管理，保证增量同步的可重启性。

**连接池配置（pgxpool）：**

| 参数 | 值 | 目的 |
|------|-----|------|
| `MaxConns` | 10 | 最大连接数，支持并发读写 |
| `MinConns` | 2 | 最小空闲连接，减少冷启动延迟 |
| `MaxConnLifetime` | 30min | 防止连接老化 |

**Schema 管理：**

启动时使用 `CREATE TABLE IF NOT EXISTS` 自动创建表和索引，无需手动迁移。通过 `information_schema.columns` 检查表结构完整性。

**临时 Schema（-once 模式）：**

`store.OpenTemp()` 创建独立临时 schema（命名格式 `tmp_<nanoseconds>`），通过 pgxpool 的 `AfterConnect` 钩子为每个连接设置 `search_path`，使所有 SQL 自动路由到临时 schema。`Close()` 时自动执行 `DROP SCHEMA CASCADE`，确保不留残余。正式模式（`store.Open()`）使用默认 public schema。

**核心操作：**

| 方法 | 说明 |
|------|------|
| `GetSessionState` | 读取 Session 的游标和拉取统计 |
| `AnyMessageExists` | 批量检查消息是否已 written（边界探测用） |
| `UnseenMessages` | 过滤出非 written 消息（写出前去重） |
| `PrepareMessageRecords` | 事务内 INSERT ... ON CONFLICT DO NOTHING + UPDATE（使用 pgx.Batch 批量发送） |
| `MarkMessagesWritten` | 事务内批量更新状态为 written，写入 output_line，推进 Session 游标和 file_size |
| `MarkMessagesFailed` | 批量记录错误，保留 pending 状态（供下轮重试） |
| `UpdateSessionFetchStats` | UPSERT 更新 Session 的拉取统计 |

**游标推进策略（`updateSessionCursor`）：**

采用"只前进不后退"策略——若已有游标比本批更新，保留已有游标，防止并发写入导致游标倒退。

**双重防重（PrepareMessageRecords）：**

```sql
-- 步骤 1：不存在时插入 pending（pgx.Batch 批量发送）
INSERT INTO messages(...) VALUES($1, $2, ...) ON CONFLICT (id) DO NOTHING

-- 步骤 2：存在且非 written 时重置为 pending（幂等重试）
UPDATE messages SET status='pending', ... WHERE id=$1 AND status <> 'written'

-- 步骤 3：查询最终状态，跳过 written 记录
SELECT id, status FROM messages WHERE id IN ($1, $2, ...)
```

---

### 4.6 sink/jsonl — JSONL 文件输出

**职责：** 实现 `domain.Sink` 和 `domain.PathResolver` 接口，将 `MessageRecord` 追加写入本地 JSONL 文件。

**输出路径格式：**

```
{output-dir}/{userID}/{agentID}/{sessionID}.jsonl
```

路径每个 segment 经过 `cleanSegment()` 过滤（白名单：字母、数字、`-`、`_`、`.`），防止路径遍历攻击。

**并发安全：**

- `locks map`：per-file `*pathLock`，保护同一文件的并发写入
- `locksMu`：保护 `locks map` 本身的并发读写
- 相同路径的消息在同一批次内串行写入；不同路径可并发写入

**锁的内存管理（TTL 清理）：**

空闲超过 10 分钟的锁在每分钟的清理检查中被回收，防止长期运行时 `locks map` 无界增长。

**写入过程：**

1. 按 `OutputPath` 分组 records
2. 为每个路径获取 `pathLock`
3. `os.OpenFile(O_CREATE|O_APPEND|O_WRONLY)` 打开文件
4. 若为首次写入该路径，调用 `countLines()` 和 `fileSize()` 初始化行号和字节偏移缓存
5. `bufio.Writer` 缓冲写入，每条记录一行 JSON，同时递增行号并回写到 `record.OutputLine`，记录写入前的字节偏移到 `record.OutputOffset`
6. `Flush()` 刷盘后释放锁

**行号与字节偏移追踪（OutputLine / OutputOffset）：**

每个 `pathLock` 维护内存行号计数 `lineCount` 和字节偏移 `byteOffset`。写入每条记录时：
- 写入 JSON + 换行后累加字节数到 `byteOffset`
- 递增 `lineCount` 赋值给 `record.OutputLine`
- 将当前 `byteOffset` 赋值给 `record.OutputOffset`（该消息写入后的文件末尾位置）

进程重启时通过 `countLines()` 和 `fileSize()` 从文件恢复缓存。

`MarkMessagesWritten` 取本批最后一条 record 的 `OutputOffset` 更新到 `sessions.file_size`，记录该 Session 的 JSONL 文件当前字节大小。

**外部服务增量读取：** 消费者从 `sessions.memorized_offset` 获取上次消费位置，`Seek(offset)` 后读取到文件末尾即可获取所有新增消息。通过 `file_size > memorized_offset` 判断是否有新内容（无需访问文件系统）。

---

### 4.7 status — 运行状态快照

**职责：** 线程安全地维护最近一轮同步的运行状态，供 `/status` 端点读取。

**快照字段：**

| 字段 | 说明 |
|------|------|
| `last_success_at` | 最近一次无错误完成的时间 |
| `last_error` | 最近一次错误信息 |
| `sessions_total` | 最近一轮发现的 Session 总数 |
| `sessions_synced` | 成功同步数 |
| `sessions_failed` | 失败数 |
| `messages_new` | 新写入消息数 |
| `last_max_fetch_reached` | 触及 MaxFetch 上限的 Session 数 |

**并发策略：** `sync.RWMutex`，写操作（`RecordRound`）加写锁，读操作（`Snapshot`）加读锁，适合频繁读取场景。

---

### 4.8 health — HTTP 健康探针

**职责：** 对外暴露两个 HTTP 端点，供 K8s 存活探针和监控系统使用。

| 端点 | 响应 | 说明 |
|------|------|------|
| `GET /healthz` | `{"status":"ok"}` | 存活探针，始终返回 200 |
| `GET /status` | `Snapshot` JSON | 最近一轮运行状态快照 |

**地址配置：** `-health-addr` 默认为 `127.0.0.1:0`（系统自动分配端口），设为空字符串时不启动服务。

**优雅关闭：** 监听 `ctx.Done()`，收到信号后最多等待 5 秒完成在途请求。

---

## 5. 数据模型

### 5.1 PostgreSQL Schema

#### sessions 表

```sql
CREATE TABLE IF NOT EXISTS sessions (
    id                        TEXT PRIMARY KEY,
    user_id                   TEXT NOT NULL DEFAULT 'default_user',
    agent_id                  TEXT NOT NULL DEFAULT 'default_agent',
    updated_at                BIGINT NOT NULL DEFAULT 0,    -- 远端最后更新时间（毫秒）
    latest_message_id         TEXT NOT NULL DEFAULT '',     -- 已写出最新消息 ID（游标）
    latest_message_created_at BIGINT NOT NULL DEFAULT 0,   -- 已写出最新消息创建时间（毫秒）
    raw_json                  TEXT NOT NULL DEFAULT '',
    synced_at                 BIGINT NOT NULL DEFAULT 0,    -- 最近同步时间（毫秒）
    last_fetch_reached_limit  BOOLEAN NOT NULL DEFAULT FALSE, -- 上次是否触及 MaxFetch
    last_fetch_count          INTEGER NOT NULL DEFAULT 0,
    last_fetch_limit          INTEGER NOT NULL DEFAULT 0,
    last_fetch_at             BIGINT NOT NULL DEFAULT 0,
    file_size                 BIGINT NOT NULL DEFAULT 0,    -- JSONL 文件当前字节大小（session_watcher 维护）
    memorized_offset          BIGINT NOT NULL DEFAULT 0,    -- 外部消费者已消费到的字节偏移（外部服务维护）
    memorized_at              BIGINT NOT NULL DEFAULT 0     -- 外部服务最后消费时间戳
);
```

#### messages 表

```sql
CREATE TABLE IF NOT EXISTS messages (
    id                   TEXT PRIMARY KEY,
    session_id           TEXT NOT NULL,
    created_at           BIGINT NOT NULL DEFAULT 0,    -- 消息创建时间（毫秒）
    prepared_at          BIGINT NOT NULL DEFAULT 0,
    written_at           BIGINT NOT NULL DEFAULT 0,
    status               TEXT NOT NULL DEFAULT 'pending', -- pending | written
    last_error           TEXT NOT NULL DEFAULT '',
    user_id              TEXT NOT NULL DEFAULT 'default_user',
    agent_id             TEXT NOT NULL DEFAULT 'default_agent',
    sink_type            TEXT NOT NULL DEFAULT 'jsonl',
    output_root          TEXT NOT NULL DEFAULT '',
    output_path          TEXT NOT NULL DEFAULT '',
    output_session_file  TEXT NOT NULL DEFAULT '',
    output_line          INTEGER NOT NULL DEFAULT 0    -- 消息在 JSONL 文件中的行号
);
```

**索引：**

```sql
CREATE INDEX IF NOT EXISTS idx_sessions_user_agent          ON sessions(user_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_messages_session_created     ON messages(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_sink_output         ON messages(sink_type, output_root, output_path);
CREATE INDEX IF NOT EXISTS idx_messages_user_agent_session  ON messages(user_id, agent_id, session_id);
CREATE INDEX IF NOT EXISTS idx_messages_status              ON messages(status);
```

---

### 5.2 JSONL 输出格式

每行一个 JSON 对象，字段如下：

```jsonc
{
  "synced_at": 1746460800000,        // 同步时间（Unix 毫秒）
  "user_id": "user_abc",
  "agent_id": "agent_xyz",
  "session_id": "session-001",
  "message_id": "msg-999",
  "message_created_at": 1746460790000, // 消息创建时间（Unix 毫秒）
  "session": { /* 原始 Session JSON，字段透传 */ },
  "message": { /* 原始 Message JSON，字段透传 */ }
}
```

**文件路径示例：**

```
data/messages/user_abc/agent_xyz/session-001.jsonl
```

---

## 6. 关键流程

### 6.1 启动流程

```
main()
  │
  ├─ config.Parse()              — 参数解析，失败退出码 2
  ├─ setupLogOutput()            — 初始化日志（stderr + lumberjack 轮转文件）
  ├─ signal.NotifyContext()      — 注册 SIGINT / SIGTERM
  ├─ status.NewReporter()
  ├─ health.Start()              — 启动 HTTP 健康端点
  ├─ store.Open()                — 连接 PostgreSQL，自动建表，失败退出码 1
  │   └─ [-once 模式] store.OpenTemp() — 创建临时 schema，隔离正式数据
  ├─ jsonl.NewFileSink()
  ├─ opencode.NewHTTPSource()
  └─ watcher.New()
      │
      ├─ [-once 模式] SyncOnce() → 退出
      └─ [持续模式]   for { SyncOnce(); select ctx.Done / time.After(interval) }
```

---

### 6.2 单轮同步（SyncOnce）

```
SyncOnce()
  │
  ├─ ListSessions()                     (HTTP, 带重试)
  │
  ├─ for each session:
  │    GetSessionState()                (PG 读)
  │    shouldSync()
  │
  ├─ Worker Pool (默认 8 workers)
  │    jobCh (unbuffered) ← goroutine 投递
  │    resultCh (buffered=len(jobs))
  │
  └─ for each job (via syncSession):
       │
       ├─ GetSession()                  (HTTP, 带重试)
       ├─ mergeSessionMetadata()
       ├─ fetchUntilBoundary()          (HTTP × N次, 动态 limit)
       ├─ UpdateSessionFetchStats()     (PG 写)
       ├─ UnseenMessages()              (PG 读)
       ├─ sortMessages()
       ├─ fillOutputTracking()
       ├─ PrepareMessageRecords()       (PG 事务写, pgx.Batch)
       ├─ sink.WriteMessages()          (文件 I/O + 行号追踪)
       └─ MarkMessagesWritten()         (PG 事务写, pgx.Batch)
```

---

### 6.3 增量边界探测（fetchUntilBoundary）

```
start: limit = min(MessageLimit, MaxMessageFetch)

┌────────────────────────────────────────────────┐
│  ListMessages(sessionID, limit)                │
│                                                │
│  AnyMessageExists(messages)?  ──Yes──▶ return  │
│                                                │
│  len(messages) < limit?       ──Yes──▶ return  │
│                                                │
│  limit >= MaxMessageFetch?    ──Yes──▶ warn    │
│                                   └──▶ return (reachedMax=true)
│                                                │
│  limit = min(limit + MessageLimit, MaxFetch)   │
└────────────────────────────────────────────────┘
         ↑ loop
```

---

### 6.4 消息状态机

```
                  下轮重试（last_error 被清空）
        ┌───────────────────────────┐
        │                           │
(新消息) ▼                          │
   [Unseen] ──PrepareMessageRecords──▶ [Pending]
                                            │
                                    WriteMessages 成功
                                            │
                                            ▼
                                        [Written]  ← 永久去重标记

              WriteMessages 失败
   [Pending] ──MarkMessagesFailed──▶ [Pending + last_error]
                                     (status 仍为 pending，
                                      last_error 记录错误信息)
```

数据库中只有两种 status 值：`pending` 和 `written`。失败时不改变 status，仅通过 `last_error` 字段区分正常 pending 和出错 pending。

写入语义为 **at-least-once**：进程崩溃重启后，`pending` 状态的消息会在下轮重新写出到 JSONL，导致少量重复行。

---

## 7. 并发模型

| 组件 | 并发机制 | 保护目标 |
|------|---------|----------|
| Worker Pool | `jobCh(unbuffered)` + `resultCh(buffered)` + `sync.WaitGroup` | Session 并发处理，背压控制 |
| PostgreSQL Store | `pgxpool`（MaxConns=10） | 连接池管理，支持并发读写 |
| JSONL FileSink | `locksMu` + per-file `pathLock.mu` | 同文件写入串行，不同文件并发 |
| status.Reporter | `sync.RWMutex` | 快照读写分离 |
| round 计数器 | `atomic.Int64` | 无锁递增 |

**背压流程：**

```
投递 goroutine ──▶ jobCh(unbuffered) ──▶ Worker goroutine
                        ↑
                  Worker 消费速率决定投递速率
```

Worker 处理慢时，投递 goroutine 自动阻塞，不会积压内存中的任务队列。

---

## 8. 错误处理策略

| 场景 | 处理方式 |
|------|---------|
| 配置解析失败 | 立即退出，退出码 2 |
| PostgreSQL 连接失败 | 立即退出，退出码 1 |
| `ListSessions` 失败 | 整轮失败，下轮重试 |
| 单个 Session `GetSession` 失败 | 该 Session 本轮失败，不影响其他 Session |
| `fetchUntilBoundary` HTTP 失败 | 该 Session 本轮失败，下轮重试（含重试退避） |
| `sink.WriteMessages` 失败 | `MarkMessagesFailed`，下轮重新 PrepareMessageRecords 再试 |
| `MarkMessagesWritten` 失败 | 该 Session 本轮失败，下轮重试（可能重复写入 JSONL） |
| `ctx` 取消 | 停止投递新任务，正在处理的 HTTP 请求尽快取消，优雅退出 |
| `GetSessionState` 读取失败 | 降级：仍加入同步队列（保守策略，不因读错误丢失 Session） |

---

## 9. 部署与运维

### 9.1 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-base-url` | `http://localhost:57811` | open-code 服务地址 |
| `-interval` | `10s` | 轮询间隔 |
| `-message-limit` | `100` | `fetchUntilBoundary` 的 limit 扩展步长 |
| `-max-message-fetch` | `1000` | 单 Session 每轮最多拉取消息数上限 |
| `-session-workers` | `8` | 并发 Worker 数 |
| `-pg-dsn` | *(必填)* | PostgreSQL 连接字符串（环境变量 `PG_DSN` 优先） |
| `-output-dir` | `./data/messages` | JSONL 输出根目录 |
| `-once` | `false` | 执行单轮同步后退出（使用临时 schema，不影响正式数据） |
| `-timeout` | `10s` | HTTP 请求超时时间 |
| `-log-level` | `info` | 日志级别：`debug`、`info`、`warn`、`error` |
| `-log-file` | `./data/session-watcher.log` | 日志文件路径，空字符串只写 stderr |
| `-health-addr` | `127.0.0.1:0` | 健康探针监听地址，空字符串不启动 |

**最小启动示例：**

```bash
PG_DSN="host=10.57.148.238 port=8432 user=repmgr password='xxx' dbname=memory sslmode=disable" \
  ./session_watcher \
  -base-url http://opencode-svc:57811 \
  -output-dir /data/messages \
  -interval 30s
```

**一次性同步（调试 / CI）：**

```bash
./session_watcher -once -log-level debug -base-url http://localhost:57811
```

> `-once` 模式使用独立临时 schema（`tmp_<nanoseconds>`），通过 `AfterConnect` 钩子设置 `search_path`，所有连接池连接自动路由到临时 schema。运行结束后执行 `DROP SCHEMA CASCADE` 自动清理，确保不影响正式 `sessions` / `messages` 表数据。

---

### 9.2 目录布局

```
data/
├── session-watcher.log         # 轮转日志
├── session-watcher.log.1.gz    # 历史日志（最多 10 个）
└── messages/
    └── {userID}/
        └── {agentID}/
            └── {sessionID}.jsonl
```

> 注：状态数据已迁移至外部 PostgreSQL，本地仅保留 JSONL 输出和日志文件。

---

### 9.3 健康检查 API

**存活探针：**

```http
GET /healthz
→ 200 OK
{"status":"ok"}
```

**运行状态：**

```http
GET /status
→ 200 OK
{
  "last_success_at": "2026-05-05 10:30:00",
  "last_error": "",
  "sessions_total": 42,
  "sessions_synced": 40,
  "sessions_failed": 0,
  "messages_new": 128,
  "last_max_fetch_reached": 0
}
```

`last_max_fetch_reached > 0` 时建议检查是否需要调大 `-max-message-fetch`。

---

### 9.4 日志

日志格式为结构化文本（`log/slog` TextHandler），关键字段均以 `key=value` 形式输出，便于 grep / 日志分析工具解析。

**关键日志事件：**

| 事件 | 级别 | 关键字段 |
|------|------|---------|
| 服务启动 | INFO | 所有配置项 |
| 轮次开始 | INFO | `round`, `total`, `sync`, `skip` |
| 轮次结束 | INFO | `round`, `duration`, `sessions_*`, `messages_new`, `max_fetch_reached` |
| Session 同步完成 | DEBUG | `round`, `session_id`, `new_messages`, `duration` |
| HTTP 重试 | WARN | `url`, `attempt`, `backoff`, `error` |
| MaxFetch 触顶 | WARN | `session_id`, `max_message_fetch`, `count` |
| Session 同步失败 | WARN | `round`, `session_id`, `error` |
| 服务启动失败 | ERROR | `error` |

**日志轮转配置（lumberjack）：**

- 单文件最大：100 MB
- 最多保留历史文件：10 个
- 压缩归档：启用（`.gz`）

---

## 10. 已知限制与风险

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| **MaxMessageFetch 触顶** | 两轮之间新增消息超过上限时，超出部分**永久遗漏**，且无告警 | 调大 `-max-message-fetch`；监控 `last_max_fetch_reached` 字段 |
| **at-least-once 重复** | 进程崩溃重启后，最后一批 `pending` 消息可能重复写入 JSONL | 下游消费时按 `message_id` 去重 |
| **locks map 内存增长** | 每个唯一路径创建一把锁，长期运行时 map 持续增长 | 已通过 TTL（10 分钟）定期回收空闲锁 |
| **PostgreSQL 可用性** | 服务强依赖 PostgreSQL，数据库不可达时无法启动或同步 | 部署时确保 PG 高可用（如 repmgr 主从） |
| **open-code 无认证** | HTTP 接口当前无鉴权 | 依赖网络隔离，不在公网暴露 open-code 服务端口 |

---

## 11. 扩展指南

### 新增 Source（替换数据源）

1. 在新包中实现 `domain.Source` 接口的三个方法
2. 在 `main.go` 中替换 `opencode.NewHTTPSource(...)` 的实例化

`watcher` 核心逻辑无需任何修改。

### 新增 Sink（替换输出目标）

1. 在新包中实现 `domain.Sink` 接口（`WriteMessages` + `Close`）
2. 可选实现 `domain.PathResolver` 接口，供 PostgreSQL 记录输出路径追踪
3. 在 `main.go` 中替换 `jsonl.NewFileSink(...)` 的实例化

### 新增健康端点

在 `health/server.go` 中向 `mux` 注册新路由即可，无需改动其他模块。

### 调整并发度

- 增加 `-session-workers`：提升 Session 并发处理能力
- 增加 `-max-message-fetch`：降低 MaxFetch 触顶频率
- 减小 `-interval`：提高消息实时性（同时增加 open-code 服务压力）

---

## 12. 测试策略

### 12.1 单元测试

所有包均有对应单元测试，覆盖核心逻辑边界条件：

```bash
# 运行全量单元测试（含竞态检测，需 PostgreSQL）
PG_TEST_DSN="postgres://user:pass@host:5432/dbname?sslmode=disable" \
  go test -race -timeout 120s ./...
```

**测试规范：**

- PostgreSQL 集成测试通过 `PG_TEST_DSN` 环境变量控制，未设置时自动 skip
- 每个测试使用独立 PostgreSQL schema 隔离，测试结束后自动 DROP
- 时钟、HTTP 客户端等外部依赖通过接口注入，支持 mock
- Sink / Source 通过接口定义，可替换为内存实现进行单元测试

### 12.2 端到端一致性验证（`scripts/verify_jsonl.sh`）

在真实 open-code 服务可用时，用于验证同步结果的正确性，**推荐在 `-once` 同步后作为集成测试环节运行**。

**脚本位置：** `scripts/verify_jsonl.sh`

**依赖：** `curl`、`jq`（macOS: `brew install jq`）

**校验逻辑：**

1. `GET /session` 获取全部 Session 列表
2. 对每个 Session 调用 `GET /session/{id}/message?limit=N` 拉取消息
3. 按路径规则 `{output-dir}/{user_id}/{agent_id}/{session_id}.jsonl` 定位本地文件
4. 逐条验证：API 中的所有消息均存在于 JSONL，且相对顺序（子序列关系）与 API 一致

**兼容的正常情况：**

| 场景 | 处理方式 |
|------|---------|
| JSONL 消息数 > API 返回数 | 通过（JSONL 是 API 的超集，多余行为历史存档） |
| JSONL 中存在重复消息 | 通过并输出 WARN（at-least-once 语义，取首次出现行号） |
| Session 在 API 中有消息但本地无文件 | 失败（说明同步未运行或文件丢失） |

**用法示例：**

```bash
# 校验全部 Session
./scripts/verify_jsonl.sh

# 校验单个 Session，开启详细输出
./scripts/verify_jsonl.sh -s <session_id> -v

# 自定义服务地址与输出目录
./scripts/verify_jsonl.sh -u http://localhost:57811 -d ./data/messages

# 增大消息拉取上限（Session 消息较多时）
./scripts/verify_jsonl.sh -l 5000
```

**退出码：**

- `0` — 全部 Session 校验通过
- `1` — 存在 Session 校验失败（输出具体 FAIL 信息）

**CI 集成示例：**

```bash
# 一次性同步后立即验证
./session_watcher -once && ./scripts/verify_jsonl.sh
```

### 12.3 路径清洗一致性

`verify_jsonl.sh` 中的 `clean_segment` 函数与 `sink/jsonl/writer.go` 中的 Go 实现逻辑完全一致（字符白名单、首尾点去除、空串兜底），已通过 15 组边界用例交叉验证。

---

## 13. 外部服务集成指南

### 13.1 增量读取 JSONL 消息

`sessions.file_size` 记录了每个 Session 的 JSONL 文件当前字节大小（由 session_watcher 维护）。`sessions.memorized_offset` 记录外部消费者已消费到的字节偏移（由外部服务维护）。通过 `file_size > memorized_offset` 即可判断是否有新内容。

```go
// 步骤 1：查询文件路径、文件大小和已消费偏移
var outputPath string
var fileSize, memorizedOffset int64
db.QueryRow(`
    SELECT m.output_path, s.file_size, s.memorized_offset
    FROM sessions s
    JOIN messages m ON m.session_id = s.id AND m.status = 'written'
    WHERE s.id = $1
    LIMIT 1`, sessionID).Scan(&outputPath, &fileSize, &memorizedOffset)

// 步骤 2：判断是否有新内容（无需打开文件）
if fileSize <= memorizedOffset {
    return // 无新内容
}

// 步骤 3：Seek 到消费偏移位置，读取所有新增内容
f, _ := os.Open(outputPath)
defer f.Close()
f.Seek(memorizedOffset, io.SeekStart)

scanner := bufio.NewScanner(f)
for scanner.Scan() {
    line := scanner.Bytes()
    // 每行是一条完整的 JSON 消息
    processMessage(line)
}

// 步骤 4：记录新偏移
newOffset, _ := f.Seek(0, io.SeekCurrent)
db.Exec(`UPDATE sessions SET memorized_offset = $1, memorized_at = EXTRACT(EPOCH FROM NOW()) * 1000 WHERE id = $2`,
    newOffset, sessionID)
```

**设计要点：**

| 特性 | 说明 |
|------|------|
| 定位方式 | 字节偏移 `Seek`，O(1) 无需扫描 |
| 消费粒度 | 文件级（每个 Session 一个 JSONL 文件） |
| 新内容判断 | `file_size > memorized_offset`（纯数据库查询，无需访问文件系统） |
| 职责分离 | session_watcher 写 `file_size`，外部服务写 `memorized_offset` + `memorized_at` |
| 并发安全 | 两者写不同字段，无写冲突 |

### 13.2 更新消费状态

外部服务处理完 Session 的消息后，更新消费偏移和时间戳：

```sql
-- 更新消费偏移和时间戳
UPDATE sessions
SET memorized_offset = <new_offset>, memorized_at = EXTRACT(EPOCH FROM NOW()) * 1000
WHERE id = 'ses_xxx';

-- 查询有未消费内容的 Session
SELECT id, file_size, memorized_offset, synced_at, memorized_at
FROM sessions
WHERE file_size > memorized_offset
ORDER BY synced_at;
```

**消费判断逻辑：**

`sessions.file_size > sessions.memorized_offset` 表示该 Session 有新写入尚未被外部服务消费。外部服务读取并处理完 JSONL 内容后，将 `memorized_offset` 更新为实际读取到的字节位置、`memorized_at` 更新为当前时间戳即可。

### 13.3 数据流全景

```
session_watcher                          外部记忆服务
─────────────────                        ──────────────────

1. 拉取 open-code 消息
2. 写入 JSONL 文件
3. 更新 messages.status = 'written'
4. 更新 sessions.file_size + synced_at
                                         5. 查询 file_size > memorized_offset 的 Session
                                         6. 读取 sessions.memorized_offset
                                         7. Seek + ReadAll 获取新消息
                                         8. 处理消息（向量化/摘要/...）
                                         9. UPDATE sessions SET memorized_offset, memorized_at
```

**职责边界：**

- session_watcher **只写** JSONL + messages.status + sessions.file_size + sessions.synced_at
- 外部服务 **只读** JSONL + **只写** sessions.memorized_offset + sessions.memorized_at
- 两者写不同字段，通过 PostgreSQL 行级隔离，互不阻塞
