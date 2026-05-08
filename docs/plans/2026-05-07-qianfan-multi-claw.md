# Qianfan 多 Claw 模式支持 实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 新增启动参数区分 opencode 单体模式与 qianfan 模式；qianfan 模式下通过 go-sdk 获取所有 claw_id，为每个 claw 创建独立的 HTTPSource（URL 加上 `/dumate/{claw_id}` 前缀），各自独立同步。支持定期刷新 claw 列表，动态增减 Watcher；被下线的 claw 在退出前执行最终一轮同步以确保不丢数据。

**Architecture:** 新增 `-mode` 参数（值为 `opencode` 或 `qianfan`）。引入 `ClawProvider` 接口抽象 claw_id 获取逻辑。qianfan 模式下引入 `ClawManager` 编排层：定期调用 ClawProvider 刷新 claw 列表，为新增 claw 启动 Watcher goroutine，为已移除 claw 触发 graceful shutdown（先执行最终一轮同步再退出）。

**Tech Stack:** Go 标准库、现有 HTTPSource（仅改变 baseURL 拼接方式）、新增 `internal/source/qianfan/` 包封装 ClawProvider 接口及 SDK 实现。

---

## 设计决策

### URL 拼接规则

- **单体模式（opencode）**：`baseURL + /session`、`baseURL + /session/{id}/message`（现有行为）
- **qianfan 模式**：`baseURL + /dumate/{claw_id} + /session`、`baseURL + /dumate/{claw_id} + /session/{id}/message`

也就是说，qianfan 模式下每个 claw 的 Source 等效于一个 baseURL 为 `{原始baseURL}/dumate/{claw_id}` 的单体 HTTPSource，**无需修改 HTTPSource 内部逻辑**。

### 多 Claw 并发模型 + 动态刷新

```
main.go
  └─ ClawManager.Run(ctx)
       │
       ├─ 初始: ClawProvider.ListClaws() → [claw_1, claw_2]
       │   ├─ 启动 watcher goroutine (claw_1)
       │   └─ 启动 watcher goroutine (claw_2)
       │
       ├─ 定时刷新 (每 claw-refresh-interval):
       │   ClawProvider.ListClaws() → [claw_1, claw_3]  (claw_2 下线, claw_3 新增)
       │   ├─ 启动 watcher goroutine (claw_3)           ← 新增
       │   └─ 通知 claw_2: "执行最终同步后退出"          ← 优雅下线
       │
       └─ ctx 取消 → 等待所有 watcher 退出
```

**优雅下线流程（被移除的 claw）：**
1. ClawManager 取消该 claw 的 watcher context（标记为"退出中"）
2. Watcher 感知到取消信号后，**不立即退出**，而是再执行一轮完整的 SyncOnce
3. 最终同步完成后，goroutine 退出

这确保即使 claw 被下线/删除，其最后时刻的消息也不会丢失。

**并发安全保证：**
- 所有 claw 共享同一个 SQLite Store 和 JSONL Sink（它们本身就是并发安全的）
- 每个 claw 的 Watcher 独立轮询，互不阻塞
- Session ID 在 open-code 中全局唯一，不会跨 claw 冲突
- ClawManager 持有 `map[string]cancelFunc`，对 map 的读写由 mutex 保护

### 认证参数

qianfan SDK 通常需要 AK/SK 或 IAM token，暂定通过环境变量传入（`QIANFAN_AK`、`QIANFAN_SK`），不在命令行暴露。接口先定义好，SDK 细节后续补充。

---

## Task 1: 新增 `-mode` 配置参数

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`（如果存在）

**Step 1: 在 Config 结构体中添加 Mode 字段**

```go
// Config 中新增：
Mode string // 运行模式："opencode"（默认，单体）或 "qianfan"（多 claw）
```

**Step 2: 在 Parse 中注册 flag**

```go
fs.StringVar(&cfg.Mode, "mode", "opencode", "service mode: opencode (standalone) or qianfan (multi-claw)")
```

**Step 3: 在 Validate 中校验 Mode 合法性**

```go
switch c.Mode {
case "opencode", "qianfan":
    // valid
default:
    return fmt.Errorf("unsupported mode %q, must be opencode or qianfan", c.Mode)
}
```

**Step 4: 编写单元测试验证 mode 参数解析和校验**

```go
func TestParseMode(t *testing.T) {
    // 测试默认值为 "opencode"
    cfg, err := Parse([]string{})
    if err != nil { t.Fatal(err) }
    if cfg.Mode != "opencode" { t.Fatalf("got %q", cfg.Mode) }

    // 测试 qianfan 模式
    cfg, err = Parse([]string{"-mode", "qianfan"})
    if err != nil { t.Fatal(err) }
    if cfg.Mode != "qianfan" { t.Fatalf("got %q", cfg.Mode) }

    // 测试非法 mode
    _, err = Parse([]string{"-mode", "invalid"})
    if err == nil { t.Fatal("expected error") }
}
```

**Step 5: 运行测试验证**

```bash
go test -race -timeout 30s ./internal/config/...
```

**Step 6: 提交**

```bash
git add internal/config/
git commit -m "feat: 新增 -mode 启动参数，支持 opencode/qianfan 两种运行模式"
```

---

## Task 2: 定义 ClawProvider 接口

**Files:**
- Create: `internal/source/qianfan/provider.go`
- Create: `internal/source/qianfan/provider_test.go`

**Step 1: 定义接口和类型**

```go
package qianfan

import "context"

// ClawProvider 抽象获取 claw_id 列表的能力。
// 当前由 SDK 实现；测试时可替换为 fake。
type ClawProvider interface {
    // ListClaws 返回当前可用的所有 claw_id 列表。
    ListClaws(ctx context.Context) ([]string, error)
}
```

**Step 2: 实现一个静态 Provider（用于测试和开发阶段）**

```go
// StaticProvider 返回固定的 claw_id 列表，用于开发调试和单元测试。
type StaticProvider struct {
    Claws []string
}

func (p *StaticProvider) ListClaws(ctx context.Context) ([]string, error) {
    return p.Claws, nil
}
```

**Step 3: 预留 SDK Provider 桩**

```go
// SDKProvider 通过 qianfan go-sdk 获取 claw_id 列表。
// TODO: 待 SDK 信息确认后实现。
type SDKProvider struct {
    // AK, SK 等认证信息从环境变量读取
}

func NewSDKProvider() (*SDKProvider, error) {
    // TODO: 初始化 SDK client
    return &SDKProvider{}, nil
}

func (p *SDKProvider) ListClaws(ctx context.Context) ([]string, error) {
    // TODO: 调用 SDK 获取 claw_id 列表
    return nil, fmt.Errorf("SDKProvider not yet implemented")
}
```

**Step 4: 编写 StaticProvider 的单元测试**

```go
func TestStaticProvider(t *testing.T) {
    p := &StaticProvider{Claws: []string{"claw_1", "claw_2"}}
    claws, err := p.ListClaws(context.Background())
    if err != nil { t.Fatal(err) }
    if len(claws) != 2 { t.Fatalf("got %d claws", len(claws)) }
    if claws[0] != "claw_1" || claws[1] != "claw_2" {
        t.Fatalf("unexpected claws: %v", claws)
    }
}
```

**Step 5: 运行测试**

```bash
go test -race -timeout 30s ./internal/source/qianfan/...
```

**Step 6: 提交**

```bash
git add internal/source/qianfan/
git commit -m "feat: 定义 ClawProvider 接口及 StaticProvider/SDKProvider 桩实现"
```

---

## Task 3: 新增 qianfan 相关 CLI 参数

**Files:**
- Modify: `internal/config/config.go`

**Step 1: 在 Config 中添加 qianfan 特有参数**

```go
// Qianfan 模式专用配置
QianfanClaws          []string      // 静态 claw_id 列表（逗号分隔），为空时通过 SDK 动态获取
ClawRefreshInterval   time.Duration // claw 列表刷新间隔，默认 5 分钟
```

**Step 2: 注册 flag（逗号分隔字符串 + 刷新间隔）**

```go
var clawsFlag string
fs.StringVar(&clawsFlag, "qianfan-claws", "", "comma-separated claw IDs for qianfan mode (empty = auto-discover via SDK)")
fs.DurationVar(&cfg.ClawRefreshInterval, "claw-refresh-interval", 5*time.Minute, "interval to refresh claw list in qianfan mode")
```

解析后处理：
```go
if clawsFlag != "" {
    for _, c := range strings.Split(clawsFlag, ",") {
        c = strings.TrimSpace(c)
        if c != "" {
            cfg.QianfanClaws = append(cfg.QianfanClaws, c)
        }
    }
}
```

**Step 3: Validate 中添加 qianfan 模式校验**

```go
if c.Mode == "qianfan" {
    if c.ClawRefreshInterval <= 0 {
        return fmt.Errorf("claw-refresh-interval must be positive: %s", c.ClawRefreshInterval)
    }
}
```

**Step 4: 运行测试**

```bash
go test -race -timeout 30s ./internal/config/...
```

**Step 5: 提交**

```bash
git add internal/config/
git commit -m "feat: 新增 -qianfan-claws 和 -claw-refresh-interval 参数"
```

---

## Task 4: 实现 ClawManager 编排层（动态刷新 + 优雅下线）

**Files:**
- Create: `internal/source/qianfan/manager.go`
- Create: `internal/source/qianfan/manager_test.go`

**Step 1: 定义 ClawManager 结构体**

```go
package qianfan

import (
    "context"
    "log/slog"
    "net/url"
    "sync"
    "time"

    "session_watcher/internal/domain"
    "session_watcher/internal/source/opencode"
)

// WatcherFactory 是创建并运行单个 claw Watcher 的工厂函数。
// 接收 claw 专属 Source 和 context；context 取消时 Watcher 应执行最终同步后退出。
type WatcherFactory func(ctx context.Context, clawID string, source domain.Source)

// ClawManager 负责 claw 列表的动态管理：
//   - 定期调用 ClawProvider 刷新 claw 列表
//   - 为新增 claw 启动 Watcher goroutine
//   - 为已移除 claw 触发优雅退出（最终同步后退出）
type ClawManager struct {
    provider        ClawProvider
    baseURL         string
    timeout         time.Duration
    refreshInterval time.Duration
    watcherFactory  WatcherFactory
    logger          *slog.Logger

    mu      sync.Mutex
    claws   map[string]context.CancelFunc // 活跃的 claw → 其 cancel 函数
    wg      sync.WaitGroup               // 追踪所有 watcher goroutine
}

// NewClawManager 创建 ClawManager。
func NewClawManager(
    provider ClawProvider,
    baseURL string,
    timeout time.Duration,
    refreshInterval time.Duration,
    watcherFactory WatcherFactory,
    logger *slog.Logger,
) *ClawManager {
    return &ClawManager{
        provider:        provider,
        baseURL:         baseURL,
        timeout:         timeout,
        refreshInterval: refreshInterval,
        watcherFactory:  watcherFactory,
        logger:          logger,
        claws:           make(map[string]context.CancelFunc),
    }
}
```

**Step 2: 实现 Run 方法（主循环）**

```go
// Run 启动 ClawManager 主循环：首次加载 claw 列表 + 定期刷新。
// ctx 取消时通知所有 Watcher 退出并等待完成。
func (m *ClawManager) Run(ctx context.Context) error {
    // 首次加载
    if err := m.refresh(ctx); err != nil {
        return fmt.Errorf("initial claw refresh: %w", err)
    }

    ticker := time.NewTicker(m.refreshInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            m.shutdownAll()
            m.wg.Wait()
            return nil
        case <-ticker.C:
            if err := m.refresh(ctx); err != nil {
                // 刷新失败不致命，保持现有 claw 继续运行
                m.logger.Error("claw refresh failed, keeping current set", "error", err)
            }
        }
    }
}
```

**Step 3: 实现 refresh 方法（diff + 启停）**

```go
// refresh 拉取最新 claw 列表，diff 出新增和移除的 claw，启动/停止对应 Watcher。
func (m *ClawManager) refresh(ctx context.Context) error {
    latest, err := m.provider.ListClaws(ctx)
    if err != nil {
        return err
    }
    latestSet := make(map[string]struct{}, len(latest))
    for _, c := range latest {
        latestSet[c] = struct{}{}
    }

    m.mu.Lock()
    defer m.mu.Unlock()

    // 启动新增的 claw
    for _, clawID := range latest {
        if _, exists := m.claws[clawID]; exists {
            continue
        }
        m.startClaw(ctx, clawID)
    }

    // 优雅停止已移除的 claw（cancel 后 watcher 会执行最终同步再退出）
    for clawID, cancel := range m.claws {
        if _, exists := latestSet[clawID]; !exists {
            m.logger.Info("claw removed, triggering final sync", "claw_id", clawID)
            cancel()
            delete(m.claws, clawID)
        }
    }

    m.logger.Info("claw refresh completed", "active_claws", len(m.claws), "total_discovered", len(latest))
    return nil
}

// startClaw 为指定 claw 启动 Watcher goroutine。
// 调用者须持有 m.mu。
func (m *ClawManager) startClaw(parentCtx context.Context, clawID string) {
    clawCtx, cancel := context.WithCancel(parentCtx)
    m.claws[clawID] = cancel

    clawBaseURL := m.baseURL + "/dumate/" + url.PathEscape(clawID)
    source := opencode.NewHTTPSource(clawBaseURL, m.timeout, m.logger.With("claw_id", clawID))

    m.wg.Add(1)
    go func() {
        defer m.wg.Done()
        m.logger.Info("claw watcher started", "claw_id", clawID, "base_url", clawBaseURL)
        m.watcherFactory(clawCtx, clawID, source)
        m.logger.Info("claw watcher exited", "claw_id", clawID)
    }()
}

// shutdownAll 取消所有活跃的 claw Watcher。
func (m *ClawManager) shutdownAll() {
    m.mu.Lock()
    defer m.mu.Unlock()
    for clawID, cancel := range m.claws {
        m.logger.Info("shutting down claw watcher", "claw_id", clawID)
        cancel()
    }
    m.claws = make(map[string]context.CancelFunc)
}
```

**Step 4: 编写单元测试**

```go
func TestClawManagerRefresh(t *testing.T) {
    // 测试场景：初始 [A, B] → 刷新后 [B, C]
    // 预期：A 被 cancel，C 被启动，B 保持不变
    calls := make(map[string]int) // clawID → 启动次数
    var mu sync.Mutex

    factory := func(ctx context.Context, clawID string, source domain.Source) {
        mu.Lock()
        calls[clawID]++
        mu.Unlock()
        <-ctx.Done() // 模拟运行直到被 cancel
    }

    provider := &sequenceProvider{
        rounds: [][]string{{"A", "B"}, {"B", "C"}},
    }

    mgr := NewClawManager(provider, "http://test", 5*time.Second, 100*time.Millisecond, factory, slog.Default())

    ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
    defer cancel()
    mgr.Run(ctx)

    mu.Lock()
    defer mu.Unlock()
    if calls["A"] != 1 { t.Errorf("A started %d times", calls["A"]) }
    if calls["B"] != 1 { t.Errorf("B started %d times", calls["B"]) }
    if calls["C"] != 1 { t.Errorf("C started %d times", calls["C"]) }
}
```

**Step 5: 运行测试**

```bash
go test -race -timeout 30s ./internal/source/qianfan/...
```

**Step 6: 提交**

```bash
git add internal/source/qianfan/
git commit -m "feat: 实现 ClawManager，支持 claw 列表动态刷新和优雅下线"
```

---

## Task 5: 重构 main.go 集成 ClawManager

**Files:**
- Modify: `cmd/session-watcher/main.go`

**Step 1: 修改 runWatcher 支持 qianfan 模式下使用 ClawManager**

```go
func runWatcher(ctx context.Context, cfg config.Config, reporter *status.Reporter, logger *slog.Logger) error {
    stateStore, err := store.Open(ctx, cfg.DBPath)
    if err != nil { /* ... */ }
    defer stateStore.Close()

    sink, err := jsonlsink.NewFileSink(cfg.OutputDir, logger)
    if err != nil { /* ... */ }
    defer sink.Close()

    watcherCfg := watcher.Config{
        MessageLimit:    cfg.MessageLimit,
        MaxMessageFetch: cfg.MaxMessageFetch,
        SessionWorkers:  cfg.SessionWorkers,
    }

    if cfg.Mode == "opencode" {
        // 单体模式：保持现有行为
        source := opencode.NewHTTPSource(cfg.BaseURL, cfg.Timeout, logger)
        w := watcher.New(source, sink, stateStore, watcherCfg, logger)
        return runSingleWatcher(ctx, cfg, w, reporter, logger)
    }

    // qianfan 模式：通过 ClawManager 管理多个 Watcher
    provider := resolveClawProvider(cfg, logger)

    // watcherFactory：ClawManager 为每个 claw 调用此函数启动 Watcher。
    // clawCtx 被 cancel 时，Watcher 需执行最终一轮同步后退出。
    factory := func(clawCtx context.Context, clawID string, source domain.Source) {
        w := watcher.New(source, sink, stateStore, watcherCfg, logger.With("claw_id", clawID))
        runClawWatcher(clawCtx, cfg, w, reporter, logger.With("claw_id", clawID))
    }

    mgr := qianfan.NewClawManager(provider, cfg.BaseURL, cfg.Timeout, cfg.ClawRefreshInterval, factory, logger)
    return mgr.Run(ctx)
}

// runClawWatcher 运行单个 claw 的 Watcher 循环。
// 当 ctx 被 cancel 时，执行最终一轮完整同步后退出（确保被下线 claw 不丢数据）。
func runClawWatcher(ctx context.Context, cfg config.Config, w *watcher.Watcher, reporter *status.Reporter, logger *slog.Logger) {
    for {
        started := time.Now()
        result, err := w.SyncOnce(ctx)
        reporter.RecordRound(status.RoundUpdate{/* ... */})

        if err != nil && ctx.Err() == nil {
            logger.Error("sync round failed", "error", err)
        }

        // ctx 取消后，已完成最终同步，可以退出
        if ctx.Err() != nil {
            logger.Info("claw watcher final sync completed, exiting")
            return
        }

        select {
        case <-ctx.Done():
            // ctx 在等待期间被取消 → 立即执行最终同步
            logger.Info("claw watcher received shutdown, performing final sync")
            finalResult, finalErr := w.SyncOnce(context.Background()) // 用独立 context 确保不被取消
            if finalErr != nil {
                logger.Error("final sync failed", "error", finalErr)
            } else {
                logger.Info("final sync completed", "messages_new", finalResult.MessagesNew)
            }
            return
        case <-time.After(cfg.Interval):
        }
    }
}

// resolveClawProvider 根据配置决定使用静态列表还是 SDK 动态发现。
func resolveClawProvider(cfg config.Config, logger *slog.Logger) qianfan.ClawProvider {
    if len(cfg.QianfanClaws) > 0 {
        logger.Info("using static claw provider", "claws", cfg.QianfanClaws)
        return &qianfan.StaticProvider{Claws: cfg.QianfanClaws}
    }
    // TODO: 返回 SDKProvider
    logger.Warn("SDK provider not yet implemented, falling back to empty static provider")
    return &qianfan.StaticProvider{Claws: nil}
}
```

**Step 2: 在启动日志中打印 mode 和 refresh interval**

```go
logger.Info("session watcher starting",
    "mode", cfg.Mode,
    "claw_refresh_interval", cfg.ClawRefreshInterval.String(),
    // ... 现有字段
)
```

**Step 3: 运行构建验证**

```bash
go build ./cmd/session-watcher
```

**Step 4: 提交**

```bash
git add cmd/session-watcher/
git commit -m "feat: 集成 ClawManager，qianfan 模式下支持 claw 动态刷新和优雅下线"
```

---

## Task 6: 端到端集成测试

**Files:**
- Create: `internal/source/qianfan/integration_test.go`
- 手动验证

**Step 1: 编写带 mock HTTP server 的集成测试**

验证 qianfan 模式下多个 claw 的 URL 拼接正确性：

```go
func TestQianfanURLConstruction(t *testing.T) {
    // 启动 mock HTTP server
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // 验证请求路径包含 /dumate/{claw_id}/ 前缀
        if !strings.HasPrefix(r.URL.Path, "/dumate/test_claw/") {
            t.Errorf("unexpected path: %s", r.URL.Path)
        }
        w.Write([]byte("[]"))
    }))
    defer server.Close()

    src := opencode.NewHTTPSource(server.URL+"/dumate/test_claw", 5*time.Second, slog.Default())
    sessions, err := src.ListSessions(context.Background())
    if err != nil { t.Fatal(err) }
    if len(sessions) != 0 { t.Fatalf("expected 0, got %d", len(sessions)) }
}
```

**Step 2: 编写 ClawManager 动态刷新 + 优雅下线集成测试**

```go
func TestClawManagerGracefulShutdown(t *testing.T) {
    // 验证被移除的 claw 在退出前确实收到了 context cancel 信号
    // 且 watcherFactory 有机会执行最终同步
    var finalSyncDone atomic.Bool

    factory := func(ctx context.Context, clawID string, source domain.Source) {
        <-ctx.Done()
        // 模拟最终同步
        finalSyncDone.Store(true)
    }

    provider := &sequenceProvider{
        rounds: [][]string{{"A"}, {}}, // 第二轮 A 被移除
    }

    mgr := NewClawManager(provider, "http://test", 5*time.Second, 50*time.Millisecond, factory, slog.Default())
    ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
    defer cancel()
    mgr.Run(ctx)

    if !finalSyncDone.Load() {
        t.Error("final sync was not performed for removed claw")
    }
}
```

**Step 3: 运行全量测试**

```bash
go test -race -timeout 30s ./...
```

**Step 4: 手动验证 qianfan 模式启动和刷新**

```bash
go run ./cmd/session-watcher -mode qianfan -qianfan-claws "claw_a,claw_b" -claw-refresh-interval 30s -once -log-level debug
```

**Step 5: 提交**

```bash
git add .
git commit -m "test: 添加 qianfan 模式 URL 拼接和动态刷新集成测试"
```

---

## Task 7: 更新文档和 CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

**Step 1: 更新项目概述和常用命令**

在 CLAUDE.md 的"常用命令"部分添加 qianfan 模式示例：

```bash
# qianfan 多 claw 模式（静态 claw 列表，每 5 分钟刷新）
go run ./cmd/session-watcher -mode qianfan -qianfan-claws "claw_1,claw_2" -claw-refresh-interval 5m -log-level debug

# qianfan 多 claw 模式（SDK 自动发现，需设置环境变量）
QIANFAN_AK=xxx QIANFAN_SK=yyy go run ./cmd/session-watcher -mode qianfan -log-level debug

# qianfan 单次同步（调试用）
go run ./cmd/session-watcher -mode qianfan -qianfan-claws "claw_1" -once -log-level debug
```

**Step 2: 更新项目结构和架构要点**

在项目结构中补充：
```
internal/source/qianfan/       ClawProvider 接口 + ClawManager 编排层
```

在架构要点中补充：
- 多 Source 并发模型
- ClawManager 动态刷新机制
- 优雅下线策略（最终同步）

**Step 3: 提交**

```bash
git add CLAUDE.md
git commit -m "docs: 更新 CLAUDE.md，补充 qianfan 多 claw 模式及动态刷新说明"
```

---

## 风险和注意事项

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| claw_id 包含特殊字符 | URL 拼接错误 | 对 claw_id 做 url.PathEscape |
| SDK 不可用 | 启动失败 | 支持 `-qianfan-claws` 静态列表兜底 |
| claw 数量极大 | goroutine 过多 | 考虑复用 SessionWorkers 配置或增加 max-claws 限制 |
| 多 claw 共享 SQLite | 写入压力增大 | SQLite 已 SetMaxOpenConns(1) 串行化，性能足够 |
| 刷新期间 Provider 返回错误 | 无法感知新 claw | 仅 log error，保持现有 claw 集不变 |
| 最终同步时 API 也已下线 | 最终同步失败 | 错误记录日志，不阻塞进程退出；at-least-once 语义下下次启动仍可重试 |
| 短时间内大量 claw 变更 | ClawManager 频繁启停 goroutine | refreshInterval 默认 5 分钟，足够平滑 |

## 后续迭代（不在本次范围）

- SDK Provider 真实实现（待 SDK 信息确认）
- 每个 claw 独立的 health/status 上报
- claw 粒度的错误隔离（单个 claw 失败不影响其他）
- `-once` 模式下 qianfan 的语义适配（所有 claw 各执行一轮后退出）
