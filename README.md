# session_watcher

周期性从 [open-code](http://localhost:57811) 服务同步 AI 对话会话消息，以增量去重方式追加输出到本地 JSONL 文件，并用 SQLite 维护同步状态。

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

```bash
./session_watcher \
  -base-url http://localhost:57811 \
  -interval 10s \
  -message-limit 100 \
  -max-message-fetch 1000 \
  -session-workers 8 \
  -db ./data/state.db \
  -output-dir ./data/messages \
  -log-level info
```

### 单次同步（调试用）

```bash
./session_watcher -once
```

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-base-url` | `http://localhost:57811` | open-code 服务基础地址 |
| `-interval` | `10s` | 轮询间隔，支持 Go duration 格式 |
| `-message-limit` | `100` | 每次 limit 扩展的步长 |
| `-max-message-fetch` | `1000` | 单 Session 每轮最多拉取消息数 |
| `-session-workers` | `8` | 最大并发 Session Worker 数 |
| `-db` | `./data/state.db` | SQLite 状态数据库路径 |
| `-output-dir` | `./data/messages` | JSONL 输出根目录 |
| `-once` | `false` | 执行单轮同步后退出 |
| `-timeout` | `10s` | HTTP 请求超时 |
| `-log-level` | `info` | 日志级别：`debug` / `info` / `warn` / `error` |
| `-log-file` | `./data/session-watcher.log` | 日志文件路径，空字符串禁用文件日志 |
| `-health-addr` | `127.0.0.1:0` | Health 服务监听地址，空字符串禁用 |

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
  store/                SQLite 状态存储
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
go test -race -timeout 30s ./...
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
| `modernc.org/sqlite` | v1.50.0 | Pure Go SQLite 驱动（免 CGO） |
| `gopkg.in/natefinch/lumberjack.v2` | v2.2.1 | 日志文件轮转 |
