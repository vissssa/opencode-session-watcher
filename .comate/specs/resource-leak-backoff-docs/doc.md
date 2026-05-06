# resource-leak-backoff-docs 设计文档

## 需求分类

本次变更属于多文件 Bug Fix + 文档同步：

1. 修复 `FileSink` 中 `locks map[string]*sync.Mutex` 只增不减导致的长期内存增长问题。
2. 将 open-code HTTP 重试等待从短线性退避改为指数退避 + jitter，降低服务过载时的重试放大压力。
3. 为本次涉及的关键代码补充必要注释，解释并发锁生命周期、重试退避和 jitter 语义。
4. 同步更新项目 `README.md` 和 `CLAUDE.md`，记录修复后的行为、运行语义和维护约束。

## 当前项目上下文

项目是 Go CLI 服务 `session_watcher`，从 open-code HTTP API 周期性同步会话和消息，写入本地 JSONL，并用 SQLite 记录同步状态。

核心路径：

- `cmd/session-watcher/main.go` 初始化 source/sink/store/watcher。
- `internal/watcher/watcher.go:68` 的 `SyncOnce` 拉取 Session，分发 Worker。
- `internal/watcher/watcher.go:184` 的 `syncSession` 拉取消息、去重、构造 `MessageRecord`。
- `internal/watcher/watcher.go:232` 调用 `sink.WriteMessages`。
- `internal/sink/jsonl/writer.go:33` 的 `FileSink.WriteMessages` 按输出文件分组写入 JSONL。
- `internal/source/opencode/client.go:88` 的 `HTTPSource.get` 统一处理 HTTP GET 重试。

## 需求一：FileSink locks map 无界增长

### 现状

`internal/sink/jsonl/writer.go:19` 中 `FileSink` 持有：

```go
type FileSink struct {
    locksMu sync.Mutex
    locks   map[string]*sync.Mutex
    rootDir string
    logger  *slog.Logger
}
```

`internal/sink/jsonl/writer.go:79` 的 `lockFor(path string)` 为每个文件路径创建一个 `*sync.Mutex`，但没有删除逻辑：

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

输出路径由 `PathFor` 按 `{root}/{user_id}/{agent_id}/{session_id}.jsonl` 生成。Session 持续增长时，路径基数持续增长，`locks` 会永久保留历史 path，产生内存泄漏型增长。

### 根因假设

根因是 per-path mutex 的生命周期没有和写入活跃度绑定。锁对象被 map 强引用，Session 文件不再写入后也无法释放。

### 技术方案

采用标准库实现一个轻量的可清理锁表，避免引入第三方 expirable map：

```go
type fileLock struct {
    mu       sync.Mutex
    refs     int
    lastUsed time.Time
}
```

`FileSink` 调整为：

```go
type FileSink struct {
    locksMu sync.Mutex
    locks   map[string]*fileLock
    rootDir string
    logger  *slog.Logger
    now     func() time.Time
}
```

新增常量：

```go
const (
    lockIdleTTL      = 10 * time.Minute
    lockCleanupEvery = 256
)
```

或用类似命名表达：

- `fileLockIdleTTL`：锁在无引用且超过该空闲时长后可被移除。
- `fileLockCleanupInterval`：每创建/获取 N 次锁后触发一次 opportunistic cleanup。

锁获取与释放流程：

```go
func (s *FileSink) acquireLock(path string) *fileLock {
    s.locksMu.Lock()
    defer s.locksMu.Unlock()

    lock := s.locks[path]
    if lock == nil {
        lock = &fileLock{lastUsed: s.now()}
        s.locks[path] = lock
    }
    lock.refs++
    return lock
}

func (s *FileSink) releaseLock(path string, lock *fileLock) {
    s.locksMu.Lock()
    defer s.locksMu.Unlock()

    lock.refs--
    lock.lastUsed = s.now()
    if lock.refs < 0 {
        lock.refs = 0
    }
    s.cleanupExpiredLocksLocked(s.now())
}
```

写入改为：

```go
lock := s.acquireLock(path)
lock.mu.Lock()
err := s.writeFile(path, groups[path])
lock.mu.Unlock()
s.releaseLock(path, lock)
```

清理条件：

- `refs == 0`
- `now.Sub(lastUsed) >= fileLockIdleTTL`

为了测试和不依赖真实时间等待，给 `FileSink` 增加未导出的 `now func() time.Time`，生产默认 `time.Now`。测试可以直接替换 `sink.now`。

### 并发边界

- `locksMu` 只保护锁表元数据：map、refs、lastUsed。
- 单文件写入互斥仍由 `fileLock.mu` 保护。
- 正在等待或正在持有文件锁的 path，`refs > 0`，不会被 cleanup 删除。
- cleanup 只删除无引用且过期的锁，不会影响当前写入。

### 影响文件

- 修改：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer.go`
  - `FileSink` 字段。
  - `NewFileSink` 初始化。
  - `WriteMessages` 锁调用路径。
  - 替换 `lockFor` 为 `acquireLock` / `releaseLock` / `cleanupExpiredLocksLocked`。
- 修改：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer_test.go`
  - 增加锁清理测试。
  - 保留并发写入测试，验证无数据损坏。

### 预期结果

长期运行时，历史 Session 文件对应的锁会在空闲 TTL 后被清理，`locks` 不再随历史 path 永久增长。

## 需求二：HTTP 重试指数退避 + jitter

### 现状

`internal/source/opencode/client.go:88` 的 `get` 当前最多 3 次请求。失败后等待：

```go
backoff := time.Duration(attempt) * 100 * time.Millisecond
```

对于 3 次 attempt，只有两次 sleep：

- 第一次失败后：100ms
- 第二次失败后：200ms

总等待最多 300ms。open-code 过载或返回 429/5xx 时，短时间内集中重试会放大压力。

### 技术方案

改为指数退避 + jitter：

- 第 1 次失败后基础退避：100ms
- 第 2 次失败后基础退避：200ms
- 第 3 次失败后如果未来调整为更多 attempt，基础退避为 400ms

在当前 `maxHTTPAttempts = 3` 下，真实 sleep 发生在 attempt 1 和 attempt 2 后。仍实现通用函数，保证后续增加 attempts 时自然得到 400ms。

建议函数：

```go
const (
    baseHTTPBackoff = 100 * time.Millisecond
    maxHTTPJitter   = 50 * time.Millisecond
)

func retryBackoff(attempt int) time.Duration {
    if attempt < 1 {
        attempt = 1
    }
    backoff := baseHTTPBackoff << (attempt - 1)
    return backoff + time.Duration(rand.Int63n(int64(maxHTTPJitter)+1))
}
```

为了测试稳定性，避免全局 `math/rand` 不可控，优先使用 `HTTPSource` 的注入字段：

```go
type HTTPSource struct {
    baseURL string
    client  *http.Client
    logger  *slog.Logger
    jitter  func(time.Duration) time.Duration
}
```

生产默认：

```go
jitter: randomJitter,
```

测试可设置：

```go
source.jitter = func(time.Duration) time.Duration { return 0 }
```

最终 `get` 中：

```go
backoff := exponentialBackoff(attempt) + s.jitter(maxHTTPJitter)
```

并保持 `select` 监听 `ctx.Done()`，取消时不继续 sleep。

### 可重试条件保持不变

`internal/source/opencode/client.go:139` 的 `shouldRetry` 逻辑保持：

- context 取消/超时不重试。
- HTTP 429 重试。
- HTTP 5xx 重试。
- HTTP 4xx 除 429 外不重试。
- 网络错误默认重试。

### 影响文件

- 修改：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/source/opencode/client.go`
  - 新增退避常量和 jitter 函数。
  - `HTTPSource` 增加 jitter 注入字段。
  - `NewHTTPSource` 初始化 jitter。
  - `get` 使用 `retryBackoff`。
  - 为退避策略补充注释。
- 修改：`/Users/siegward/Developer/baidu/easydata/session_watcher/internal/source/opencode/client_test.go`
  - 增加 `retryBackoff` 单元测试，验证 100ms/200ms/400ms 基础序列。
  - 更新已有重试测试，避免真实 jitter 造成不稳定。

### 预期结果

open-code 过载时，请求不会在 300ms 内集中打满；指数退避配合 jitter 能降低多个 watcher 同时重试时的同步峰值。

## 需求三：给代码加注释

### 处理边界

用户表述为“给代码都加上注释”。为避免污染整个项目，不做机械式逐行注释。仅对本次修改涉及且行为不直观的地方添加注释：

- `fileLock`：解释 refs 与 lastUsed 共同控制锁生命周期。
- cleanup：解释只清理无引用锁，避免删除正在等待/持有的锁。
- HTTP retry backoff：解释指数退避和 jitter 的作用。

已有自解释代码不加冗余注释。

## 需求四：产出 README.md 和 CLAUDE.md

项目根目录已有：

- `/Users/siegward/Developer/baidu/easydata/session_watcher/README.md`
- `/Users/siegward/Developer/baidu/easydata/session_watcher/CLAUDE.md`

因此不新建，改为更新现有文件。

### README 更新点

- 功能概述补充：文件锁会自动清理，HTTP 重试采用指数退避 + jitter。
- 架构或运行语义部分补充：JSONL per-file mutex 是临时锁表，不再无限驻留。
- 可观测或注意事项中补充重试策略。

### CLAUDE 更新点

- 并发模型从“locks map 只增不减风险”改为“per-file mutex 带 idle cleanup”。
- 已知风险删除或改写 `locks map 增长` 风险，记录当前清理策略和维护约束。
- open-code HTTP Source 说明补充 429/5xx 指数退避 + jitter。

## 测试计划

运行：

```bash
go test ./...
go test -race -timeout 30s ./...
```

如果本地环境受 Go 版本或依赖下载限制影响，至少运行定向测试：

```bash
go test ./internal/sink/jsonl ./internal/source/opencode
```

## 风险与例外处理

- 不引入第三方 expirable map，降低依赖风险。
- cleanup 是 opportunistic，不启动后台 goroutine，避免 Close 生命周期复杂化。
- 锁 TTL 不影响文件内容写入，只影响内存中 mutex 的复用周期。
- HTTP jitter 不能超过 context deadline，当前 `select` 会在 `ctx.Done()` 时立刻返回。
- 当前 `maxHTTPAttempts = 3` 不变，避免扩大用户等待时间过多；本次只调整两次重试间隔策略。若未来改为 4 次，基础退避自然覆盖 400ms。

## 数据流路径

### JSONL 写入

`Watcher.syncSession` → `fillOutputTracking` → `FileSink.WriteMessages` → `PathFor` 分组 → `acquireLock(path)` → `writeFile` append JSONL → `releaseLock(path)` → opportunistic cleanup。

### HTTP 拉取

`Watcher.fetchUntilBoundary` → `HTTPSource.ListMessages` → `HTTPSource.get` → `getOnce` → 429/5xx/网络错误 → `retryBackoff(attempt)` + jitter → 再次 `getOnce`。

## 交付结果

- FileSink 锁表不再无界增长。
- HTTP 重试从线性短等待改为指数退避 + jitter。
- 涉及代码补充必要注释。
- README.md 与 CLAUDE.md 同步项目真实行为。
- 对应单元测试覆盖新增行为。
