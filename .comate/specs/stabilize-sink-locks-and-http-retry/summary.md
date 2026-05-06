# 修复 FileSink 锁增长与 HTTP 重试退避总结

## 完成内容

- 已移除本次“加注释”需求，仅处理两个稳定性修复点。
- 已修复 FileSink `locks` map 无界增长问题。
  - 将 path 锁从 `map[string]*sync.Mutex` 调整为 `map[string]*pathLock`。
  - `pathLock` 维护 `users` 与 `lastUsed`，避免清理仍在使用或等待使用的 path 锁。
  - 新增空闲锁清理逻辑，默认锁 TTL 为 10 分钟，清理间隔为 1 分钟。
  - 清理由写入流程触发，不新增后台 goroutine。
- 已修复 HTTP 重试退避过短问题。
  - 将原线性退避替换为指数退避基准：`100ms, 200ms, 400ms...`。
  - 增加 0%~50% jitter，降低并发 worker 集中重试概率。
  - 保持最大尝试次数为 3，不改变 429、5xx、网络错误重试和普通 4xx 不重试语义。
- 已补充测试覆盖：
  - FileSink 空闲 path 锁清理。
  - FileSink 并发写入继续保持正确。
  - HTTP retry backoff 的指数基准与 jitter 范围。
  - 原有 HTTP 重试次数语义保持不变。

## 修改文件

- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer.go`
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/sink/jsonl/writer_test.go`
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/source/opencode/client.go`
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/source/opencode/client_test.go`
- `/Users/siegward/Developer/baidu/easydata/session_watcher/.comate/specs/stabilize-sink-locks-and-http-retry/tasks.md`

## 验证结果

已执行并通过：

```bash
go test ./...
go test -race -timeout 30s ./...
```

补充执行并通过：

```bash
go test ./internal/sink/jsonl
go test ./internal/source/opencode
```

## 说明

FileSink 清理实现采用 `users` 引用计数而不是仅依赖 `TryLock()`。这样可以覆盖 goroutine 已拿到 path 锁对象但尚未进入 `Lock()` 的等待窗口，避免旧锁被删除后同一路径出现两个不同 mutex 的并发写入风险。
