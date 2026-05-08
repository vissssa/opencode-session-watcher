# session_watcher

周期性从 [open-code](http://localhost:57811) 服务同步 AI 对话会话消息，以增量去重方式追加输出到本地 JSONL 文件，并用 PostgreSQL 维护同步状态。

## 功能概述

- **增量同步**：每轮拉取会话列表，仅同步自上次更新后有变化的 Session
- **无遗漏消息**：动态扩展 `limit` 探测已处理消息边界，确保两轮之间新增的消息不遗漏
- **并发处理**：可配置 Worker Pool，多个 Session 并发同步，互不干扰
- **幂等写入**：基于 `message_id` 去重，重启后继续增量追加，不重复写入
- **可扩展架构**：`Source` / `Sink` 接口驱动，当前实现可替换为其他数据源或输出目标
- **可观测**：结构化日志（`log/slog`）+ HTTP `/healthz` / `/status` 端点

## 快速开始

### 构建

```bash
make build
# 产出：./session_watcher
```

### 运行

推荐使用 `.env` 文件配置（程序启动时自动加载，不覆盖已有环境变量）：

```env
# .env
PG_DSN=host=localhost port=5432 user=app password=secret dbname=memory sslmode=disable
BASE_URL=http://localhost:57811
OUTPUT_DIR=./data/messages
LOG_LEVEL=info
```

```bash
./session_watcher
```

也可通过 CLI flag 或环境变量直接指定（优先级：CLI flag > 环境变量 > `.env` 文件）：

```bash
./session_watcher \
  -base-url http://localhost:57811 \
  -interval 10s \
  -pg-dsn "host=localhost port=5432 user=app password=secret dbname=memory sslmode=disable" \
  -output-dir ./data/messages
```

### 单次同步（调试用）

```bash
./session_watcher -once
```

> `-once` 模式使用独立的临时 PostgreSQL schema，与正式数据完全隔离，运行结束后自动清理。适合调试和 CI 场景，不会污染线上状态。

## 配置

程序启动时按以下优先级读取配置：**CLI flag > 环境变量 > `.env` 文件 > 默认值**

`.env` 文件支持格式：`KEY=VALUE`、`KEY="VALUE"`、`KEY='VALUE'`、`export KEY=VALUE`，忽略注释和空行。

### 命令行参数与环境变量

| 参数 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `-base-url` | `BASE_URL` | `http://localhost:57811` | open-code 服务基础地址 |
| `-interval` | `INTERVAL` | `10s` | 轮询间隔，支持 Go duration 格式 |
| `-message-limit` | `MESSAGE_LIMIT` | `100` | 每次 limit 扩展的步长 |
| `-max-message-fetch` | `MAX_MESSAGE_FETCH` | `1000` | 单 Session 每轮最多拉取消息数 |
| `-session-workers` | `SESSION_WORKERS` | `8` | 最大并发 Session Worker 数 |
| `-pg-dsn` | `PG_DSN` | *(必填)* | PostgreSQL 连接字符串 |
| | `PG_HOST`/`PG_PORT`/`PG_USER`/`PG_PASSWORD`/`PG_DB`/`PG_SSLMODE` | | 分字段拼接 DSN（`PG_DSN` 优先） |
| `-output-dir` | `OUTPUT_DIR` | `./data/messages` | JSONL 输出根目录 |
| `-once` | | `false` | 执行单轮同步后退出（使用临时 schema） |
| `-timeout` | `TIMEOUT` | `10s` | HTTP 请求超时 |
| `-log-level` | `LOG_LEVEL` | `info` | 日志级别：`debug` / `info` / `warn` / `error` |
| `-log-file` | `LOG_FILE` | `./data/session-watcher.log` | 日志文件路径，空字符串禁用文件日志 |
| `-health-addr` | `HEALTH_ADDR` | `127.0.0.1:0` | Health 服务监听地址，空字符串禁用 |
| `-lease-path` | `LEASE_PATH` | *(空=禁用)* | Leader lease 文件路径（启用 HA 模式） |
| `-lease-id` | `LEASE_ID` | *自动生成* | 实例唯一标识（默认 hostname:pid） |
| `-lease-timeout` | `LEASE_TIMEOUT` | `30s` | Leader 超时时长 |
| `-lease-renew-interval` | `LEASE_RENEW_INTERVAL` | `10s` | Leader 续约间隔 |
| `-lease-poll-interval` | `LEASE_POLL_INTERVAL` | `5s` | Standby 轮询间隔 |

### .env 示例

```env
# PostgreSQL 连接
PG_DSN=host=10.57.148.238 port=8432 user=repmgr password='L#i_T^e^!@2025q' dbname=memory sslmode=disable

# 或分字段方式（PG_DSN 优先）
# PG_HOST=10.57.148.238
# PG_PORT=8432
# PG_USER=repmgr
# PG_PASSWORD=L#i_T^e^!@2025q
# PG_DB=memory

# 服务配置
BASE_URL=http://localhost:57811
INTERVAL=10s
OUTPUT_DIR=./data/messages
LOG_LEVEL=info
```

## 输出格式

消息按 `{output-dir}/{user_id}/{agent_id}/{session_id}.jsonl` 分文件存储，每行一条 JSON：

```json
{
  "synced_at": 1777000000000,
  "user_id": "user_abc",
  "agent_id": "default_agent",
  "session_id": "ses_xxx",
  "message_id": "msg_yyy",
  "message_created_at": 1776944844857,
  "session": { "id": "ses_xxx", "title": "...", "time": { "updated": 1776944851503 } },
  "message": { "info": { "id": "msg_yyy", "role": "assistant" }, "parts": [...] }
}
```

写入语义为 **at-least-once**：进程异常崩溃后重启，最后一批消息可能重复写入。可通过 `message_id` 字段进行后处理去重。

## 增量策略

单 Session 消息拉取采用动态 limit 扩展：

1. 以 `message-limit` 为步长，从 `limit=message-limit` 开始请求最近 N 条消息
2. 若返回结果中**包含已处理过的消息**，说明已到达边界，停止扩展
3. 若返回数量**小于 limit**，说明已拉取全部可见消息，停止扩展
4. 否则 `limit += message-limit`，继续扩展（上限为 `max-message-fetch`）

当 limit 触及 `max-message-fetch` 上限时，会记录 warn 日志并继续处理已拉取的消息。

## 架构

```
cmd/session-watcher/    程序入口，生命周期管理
internal/
  config/               CLI 参数解析与校验
  domain/               核心类型与接口（Source / Sink / PathResolver）
  source/opencode/      open-code HTTP Source 实现
  sink/jsonl/           JSONL FileSink 实现
  store/                PostgreSQL 状态存储
  watcher/              轮询调度与增量同步编排
  health/               HTTP health/status 服务
  status/               运行状态快照（线程安全）
```

### 扩展 Source

实现 `domain.Source` 接口（3 个方法），在 `main.go` 中替换即可：

```go
type Source interface {
    ListSessions(ctx context.Context) ([]Session, error)
    GetSession(ctx context.Context, sessionID string) (Session, error)
    ListMessages(ctx context.Context, sessionID string, limit int) ([]Message, error)
}
```

### 扩展 Sink

实现 `domain.Sink` 接口（2 个方法），可选实现 `PathResolver` 提供路径信息：

```go
type Sink interface {
    WriteMessages(ctx context.Context, records []MessageRecord) error
    Close() error
}
```

## 测试

### 单元测试

```bash
PG_TEST_DSN="postgres://user:pass@host:5432/dbname?sslmode=disable" \
  go test -race -timeout 120s ./...
```

### 端到端一致性验证

`scripts/verify_jsonl.sh` 是一个集成验证脚本，用于校验本地 JSONL 文件与 open-code API 数据的完整一致性，可作为集成测试的一环在 `-once` 同步后运行。

**依赖：** `curl`、`jq`（macOS: `brew install jq`）

**校验逻辑：**

1. 从 API 获取全部 Session 列表
2. 对每个 Session 拉取完整消息（自动限制上限）
3. 按路径规则 `{output-dir}/{user_id}/{agent_id}/{session_id}.jsonl` 定位本地文件
4. 验证 API 中的每条消息均存在于 JSONL，且相对顺序与 API 一致

**兼容场景：**

- JSONL 消息数多于 API（历史存档，正常）
- JSONL 存在重复消息（at-least-once 写入语义，输出 WARN 并忽略重复行）

```bash
# 校验全部 Session
./scripts/verify_jsonl.sh

# 校验单个 Session（详细输出）
./scripts/verify_jsonl.sh -s <session_id> -v

# 自定义 open-code 地址和输出目录
./scripts/verify_jsonl.sh -u http://localhost:57811 -d ./data/messages

# 增大消息拉取上限（消息较多时）
./scripts/verify_jsonl.sh -l 5000
```

校验全部通过时退出码为 `0`，任一 Session 失败时退出码为 `1`，可直接接入 CI 流水线：

```bash
# CI 中：一次性同步后立即验证
./session_watcher -once && ./scripts/verify_jsonl.sh
```

## 构建与打包

```bash
make all          # prepare + compile + package
make test         # 运行所有测试
make build        # 仅编译
make clean        # 清理产物
```

跨平台构建（ARM64）：

```bash
GOOS=linux GOARCH=arm64 make build
```

## 健康检查

启动后通过 `-health-addr` 参数指定的地址访问：

- `GET /healthz` — 存活探针，返回 `{"status": "ok"}`
- `GET /status`  — 运行状态快照，包含最近一轮统计信息

## 依赖

| 包 | 版本 | 用途 |
|----|------|------|
| `github.com/jackc/pgx/v5` | v5.7.4 | PostgreSQL 驱动 + 连接池 |
| `gopkg.in/natefinch/lumberjack.v2` | v2.2.1 | 日志文件轮转 |

## 外部服务集成

### 增量读取 JSONL 消息

外部服务（如记忆服务）可通过 `sessions.memorized_offset` 和 `sessions.file_size` 字段实现高效增量消费：

```go
// 1. 从 PostgreSQL 获取目标 Session 的文件路径、文件大小和已消费偏移
var outputPath string
var fileSize, memorizedOffset int64
db.QueryRow(`
    SELECT m.output_path, s.file_size, s.memorized_offset
    FROM sessions s
    JOIN messages m ON m.session_id = s.id
    WHERE s.id = $1
    LIMIT 1`, sessionID).Scan(&outputPath, &fileSize, &memorizedOffset)

// 2. 快速判断是否有新内容（无需打开文件）
if fileSize <= memorizedOffset {
    // 无新内容，跳过
    return
}

// 3. 从上次消费位置开始读取新消息（O(1) 定位，无需扫描历史内容）
f, _ := os.Open(outputPath)
defer f.Close()
f.Seek(memorizedOffset, io.SeekStart)

scanner := bufio.NewScanner(f)
for scanner.Scan() {
    var record map[string]interface{}
    json.Unmarshal(scanner.Bytes(), &record)
    // 处理新消息...
}

// 4. 处理完成后更新消费偏移和时间戳
newOffset, _ := f.Seek(0, io.SeekCurrent)
db.Exec(`UPDATE sessions SET memorized_offset = $1, memorized_at = EXTRACT(EPOCH FROM NOW()) * 1000 WHERE id = $2`,
    newOffset, sessionID)
```

> **说明：**
> - `sessions.file_size` 由 session_watcher 在每批消息写入后自动更新，表示该 Session 的 JSONL 文件当前字节大小
> - `sessions.memorized_offset` 由外部消费者维护，记录已消费到的字节位置
> - 判断是否有新内容：`file_size > memorized_offset`（无需访问文件系统）

### 更新消费状态

外部服务处理完 Session 的消息后，更新消费偏移和时间戳：

```sql
-- 更新消费偏移和时间戳
UPDATE sessions
SET memorized_offset = $1, memorized_at = EXTRACT(EPOCH FROM NOW()) * 1000
WHERE id = $2;

-- 查询有未消费内容的 Session（file_size > memorized_offset 表示有新写入尚未处理）
SELECT id, file_size, memorized_offset, synced_at, memorized_at
FROM sessions
WHERE file_size > memorized_offset
ORDER BY synced_at;
```

> **判断逻辑：** `sessions.file_size > sessions.memorized_offset` 表示该 Session 有新写入的消息尚未被外部服务消费。外部服务读取并处理完 JSONL 内容后，将 `memorized_offset` 更新为实际读取到的字节位置即可。

### 数据模型关系

```
sessions 表
├── file_size          → JSONL 文件当前字节大小（session_watcher 维护）
├── memorized_offset   → 外部服务已消费到的字节偏移（外部消费者维护）
├── memorized_at       → 外部服务最后消费的时间戳
├── synced_at          → 最后一次写入消息的时间戳
└── 1:N → messages 表
           └── output_line    → 该消息在 JSONL 文件中的行号
```
