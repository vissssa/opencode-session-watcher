# 可靠性与性能加固设计文档

## 背景与目标

当前 session watcher 已实现：

- 周期性拉取 session 列表；
- 对更新的 session 拉取 `/message?limit=N`；
- 使用 SQLite 记录 message 去重和输出追踪；
- 使用 JSONL Sink 按 `user_id/agent_id/session_id.jsonl` 写文件。

当前仍存在以下风险点：

1. `fetchUntilBoundary` 无上限保护，极端情况下 limit 会持续扩大。
2. `AnyMessageExists` / `UnseenMessages` 对每条 message 单独查 SQLite，存在 N+1 查询问题。
3. `FileSink` 使用全局 mutex，多个 session 文件写入被串行化。
4. HTTP 层无重试，瞬时网络错误或 open-code 短暂不可用会导致本轮 session 失败。
5. 日志仅输出 stderr，无持久化文件日志。

本次目标：

- 新增 `MaxMessageFetch`，默认最多拉最近 1000 条 message。
- 将 message 存在性检查改为批量 `WHERE id IN (...)` 查询。
- 将 JSONL Sink 全局锁改为 per-file mutex。
- HTTPSource.get 增加最多 3 次指数退避重试。
- 增加持久化日志文件输出，同时保留 stderr 输出。

## 当前相关实现位置

### Watcher limit 扩大逻辑

文件：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/watcher/watcher.go`

当前 `fetchUntilBoundary`：

```go
func (w *Watcher) fetchUntilBoundary(ctx context.Context, sessionID string) ([]domain.Message, error) {
    step := w.cfg.MessageLimit
    limit := step
    for {
        messages, err := w.source.ListMessages(ctx, sessionID, limit)
        ...
        if foundProcessed || len(messages) < limit {
            return messages, nil
        }
        limit += step
    }
}
```

风险：如果一直没有找到已处理 message，且接口始终返回 `len(messages) == limit`，limit 会持续增长。

### Store N+1 查询

文件：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/store/store.go`

当前：

```go
func (s *Store) AnyMessageExists(ctx context.Context, messages []domain.Message) (bool, error)
func (s *Store) UnseenMessages(ctx context.Context, messages []domain.Message) ([]domain.Message, int, error)
```

两个函数内部都会逐条调用：

```go
MessageExists(ctx, messageID)
```

风险：单 session 拉到 1000 条时会产生大量单行查询。

### JSONL Sink 全局锁

文件：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer.go`

当前：

```go
type FileSink struct {
    mu sync.Mutex
    rootDir string
    logger *slog.Logger
}
```

`WriteMessages` 全程持有 `s.mu`，导致不同 session 文件写入也串行。

### HTTP 无重试

文件：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/source/opencode/client.go`

当前 `get` 只请求一次。

### 日志只到 stderr

文件：`/Users/siegward/Developer/baidu/easydata/session_watcher/cmd/session-watcher/main.go`

当前：

```go
logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
```

## 设计方案

### 1. MaxMessageFetch 上限保护

#### 配置新增

在 `internal/config/config.go` 增加：

```go
DefaultMaxMessageFetch = 1000
```

`Config` 增加：

```go
MaxMessageFetch int
```

命令行参数：

```bash
-max-message-fetch 1000
```

校验规则：

- `max-message-fetch <= 0` 启动失败。
- 如果 `message-limit > max-message-fetch`，启动失败，提示需要让 `message-limit <= max-message-fetch`。

#### Watcher.Config 增加字段

```go
type Config struct {
    MessageLimit    int
    MaxMessageFetch int
    SessionWorkers  int
}
```

main 中传入：

```go
watcher.Config{
    MessageLimit: cfg.MessageLimit,
    MaxMessageFetch: cfg.MaxMessageFetch,
    SessionWorkers: cfg.SessionWorkers,
}
```

#### fetchUntilBoundary 调整

逻辑：

1. 初始 `limit = messageLimit`。
2. 每轮请求前确保 `limit <= maxMessageFetch`。
3. 如果下一次扩大超过 max，则使用 `maxMessageFetch` 做最后一次请求。
4. 如果在 `maxMessageFetch` 仍没有发现已处理 message 且返回数量等于上限，则停止并返回当前消息，同时打 warn 日志。

伪代码：

```go
limit := min(step, max)
for {
    messages := ListMessages(limit)
    foundProcessed := AnyMessageExists(messages)
    if foundProcessed || len(messages) < limit || limit == max {
        if !foundProcessed && len(messages) == max {
            logger.Warn("message fetch reached max limit", ...)
        }
        return messages, nil
    }
    limit = min(limit + step, max)
}
```

边界：

- 上限意味着如果某个 session 两轮之间新增超过 1000 条，超过最近 1000 之外的旧新增 message 可能暂时无法获取。
- 这是用户明确要求的保护策略：最大只获取最近 1000 条对话。
- 日志必须提示该风险。

### 2. 批量查询 message 是否存在

#### 新增 Store 批量方法

内部新增：

```go
func (s *Store) ExistingMessageIDs(ctx context.Context, ids []string) (map[string]struct{}, error)
```

实现：

- 去重并过滤空 ID。
- 使用 `SELECT id FROM messages WHERE id IN (?, ?, ...)`。
- SQLite 默认变量数有限，虽然本次 max 1000，仍建议分 chunk 查询。
- chunk 大小取 500，避免碰到 SQLite 变量上限。

伪代码：

```go
const sqliteInClauseChunkSize = 500
for each chunk:
    query := "SELECT id FROM messages WHERE id IN (" + placeholders + ")"
```

#### AnyMessageExists 调整

当前逐条查，改为：

```go
existing, err := s.ExistingMessageIDs(ctx, messageIDs(messages))
return len(existing) > 0, nil
```

#### UnseenMessages 调整

当前逐条查，改为：

```go
existing := ExistingMessageIDs(...)
for _, msg := range messages {
    if _, ok := existing[msg.ID]; ok { seen++ } else { unseen = append(...) }
}
```

好处：

- 1000 条 message 最多 2 次 SQL 查询。
- 减少 SQLite 往返和 query 编译开销。

### 3. JSONL Sink per-file mutex

#### 结构调整

当前：

```go
type FileSink struct {
    mu sync.Mutex
    rootDir string
    logger *slog.Logger
}
```

调整为：

```go
type FileSink struct {
    locksMu sync.Mutex
    locks map[string]*sync.Mutex
    rootDir string
    logger *slog.Logger
}
```

#### 获取文件锁

```go
func (s *FileSink) lockFor(path string) *sync.Mutex {
    s.locksMu.Lock()
    defer s.locksMu.Unlock()
    lock := s.locks[path]
    if lock == nil {
        lock = &sync.Mutex{}
        s.locks[path] = lock
    }
    return lock
}
```

#### WriteMessages 调整

仍然先按 path 分组，但写每个 path 时只锁该 path：

```go
for _, path := range paths {
    lock := s.lockFor(path)
    lock.Lock()
    err := s.writeFile(path, groups[path])
    lock.Unlock()
}
```

由于单次 `WriteMessages` 通常来自单个 session，仅一个 path。多个 worker 写不同 session 文件时可以并发执行。

注意：

- 这是进程内 per-file mutex，不解决多进程/多副本文件写冲突。
- 多副本问题已确认后续处理。

### 4. HTTPSource.get 增加指数退避重试

#### 配置策略

不新增命令行参数，先固定为最多 3 次，避免过度配置。

常量：

```go
const maxHTTPRetries = 3
```

含义：最多尝试 3 次总请求，包括首次请求。

退避：

- 第 1 次失败后等待 100ms。
- 第 2 次失败后等待 200ms。
- 第 3 次失败返回错误。

可加少量 jitter，但本次保持简单，不引入随机依赖。

#### 哪些错误重试

重试：

- 网络错误，如 connection refused、timeout。
- HTTP 5xx。
- HTTP 429。

不重试：

- HTTP 4xx（除 429）。
- 创建 request 失败。
- context canceled / deadline exceeded（由上层取消时直接返回）。

实现方式：

- 将单次请求封装为 `getOnce`。
- `get` 负责循环重试和 backoff。
- 非 2xx 返回时构造包含状态码的错误，或在 `getOnce` 中返回 status code 供 `get` 判断。

建议结构：

```go
type httpError struct {
    statusCode int
    message string
}
```

日志：

- warn：每次可重试失败时记录 attempt、url、error、backoff。
- error 仍由上层 session/list 逻辑记录。

### 5. 持久化日志文件

#### 配置新增

新增：

```go
DefaultLogFile = "./data/session-watcher.log"
```

`Config` 增加：

```go
LogFile string
```

命令行：

```bash
-log-file ./data/session-watcher.log
```

规则：

- 默认写入 `./data/session-watcher.log`。
- 如果用户传空字符串 `-log-file ""`，表示只输出 stderr，不写文件。
- 非空时自动创建父目录，以 append 模式打开文件。

#### main 日志初始化调整

当前只写 stderr。

调整为：

- 如果 `log-file` 为空：保持 stderr。
- 如果非空：使用 `io.MultiWriter(os.Stderr, file)` 同时写 stderr 和文件。
- 退出时关闭 log file。

伪代码：

```go
var logOutput io.Writer = os.Stderr
var logFile *os.File
if cfg.LogFile != "" {
    os.MkdirAll(filepath.Dir(cfg.LogFile), 0755)
    logFile, _ = os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
    logOutput = io.MultiWriter(os.Stderr, logFile)
}
logger := slog.New(slog.NewTextHandler(logOutput, ...))
```

边界：

- log file 打不开时启动失败，避免用户以为已持久化但实际没有。
- 不做日志轮转；后续如需要可以引入 `lumberjack` 或自行按大小切分。

## 受影响文件

- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/config/config.go`
  - 增加 `MaxMessageFetch`、`LogFile` 配置和校验。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/cmd/session-watcher/main.go`
  - 增加 log file 初始化和 MultiWriter。
  - 传递 `MaxMessageFetch` 到 watcher。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/watcher/watcher.go`
  - 增加 fetch 上限保护。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/store/store.go`
  - 增加批量 message ID 查询。
  - 改造 `AnyMessageExists` / `UnseenMessages`。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer.go`
  - 改为 per-file mutex。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/source/opencode/client.go`
  - 增加 HTTP retry/backoff。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/**/*_test.go`
  - 补充相关单元测试。

## 测试计划

### 配置测试

- 默认 `MaxMessageFetch=1000`。
- `message-limit > max-message-fetch` 报错。
- `max-message-fetch <= 0` 报错。
- 默认 `log-file=./data/session-watcher.log`。
- `-log-file ""` 允许禁用文件日志。

### Watcher 测试

- `fetchUntilBoundary` 达到 max 后停止。
- 达到 max 且仍未发现 processed 时返回最后一批结果并打 warn。
- `message-limit` 小于 max 时按 N、2N 扩大。

### Store 测试

- `ExistingMessageIDs` 能批量返回已有 ID。
- `AnyMessageExists` 使用批量结果判断。
- `UnseenMessages` 使用批量结果过滤。
- 1000 条消息场景下结果正确。

### JSONL Sink 测试

- 同一 session 文件并发写入仍保持合法 JSONL 行。
- 不同 session 文件并发写入都能成功。
- 可通过测试验证不同 path 不共享同一个锁对象或至少功能正确。

### HTTP Source 测试

- 500 后成功：会重试并最终成功。
- 429 后成功：会重试并最终成功。
- 400：不重试，直接失败。
- 网络错误：会重试。

### 日志测试/验证

- 配置 log-file 后运行一次，文件存在且包含启动日志。
- `-log-file ""` 时不创建日志文件。

### 端到端

- `gofmt`。
- `go test ./...`。
- 使用本地 open-code 服务运行一次 `-once`。
- 验证 JSONL 输出和日志文件生成。

## 预期结果

完成后：

- 单 session 最多拉取最近 1000 条 message，避免无限扩大 limit。
- 1000 条消息的去重查询从大量单行 SQL 降为少量 IN 查询。
- 不同 session JSONL 文件可并发写入，降低 worker 写入排队。
- HTTP 瞬时错误能在本轮内重试，减少偶发失败。
- 日志同时输出到 stderr 和本地文件，便于运行后排查。
