# 可靠性与性能加固总结

## 完成内容

本次完成 5 项风险点加固：

1. `fetchUntilBoundary` 增加最大拉取上限。
2. `AnyMessageExists` / `UnseenMessages` 改为批量 SQLite 查询。
3. `FileSink` 从全局 mutex 改为 per-file mutex。
4. `HTTPSource.get` 增加最多 3 次指数退避重试。
5. 日志增加持久化文件输出，同时保留 stderr 输出。

## 主要改动

### MaxMessageFetch

新增配置：

```bash
-max-message-fetch 1000
```

默认值：`1000`。

校验规则：

- `max-message-fetch > 0`
- `message-limit <= max-message-fetch`

Watcher 现在会在 limit 扩大到 `max-message-fetch` 后停止继续扩大，并在达到上限但仍未发现已处理 message 时打印 warn 日志。

### SQLite 批量查询

新增：

```go
ExistingMessageIDs(ctx, ids)
```

`AnyMessageExists` / `UnseenMessages` 现在通过 `WHERE id IN (...)` 批量查询，并按 500 个 ID 一组拆分，避免 SQLite 参数过多。

### JSONL per-file mutex

`FileSink` 现在维护：

```go
locksMu sync.Mutex
locks map[string]*sync.Mutex
```

不同 session 文件可以并发写入；同一个 session 文件仍串行写入，保证 JSONL 行不交叉。

### HTTP 重试

`HTTPSource.get` 现在最多尝试 3 次：

- 第 1 次失败后等待 100ms
- 第 2 次失败后等待 200ms
- 第 3 次失败返回错误

重试场景：

- 网络错误
- HTTP 5xx
- HTTP 429

不重试：

- HTTP 4xx，429 除外
- context canceled / deadline exceeded

### 持久化日志

新增配置：

```bash
-log-file ./data/session-watcher.log
```

默认写入 `./data/session-watcher.log`，同时继续输出到 stderr。

可通过以下方式禁用文件日志：

```bash
-log-file ""
```

## 验证结果

已执行：

```bash
gofmt -w ...
go test ./...
go run ./cmd/session-watcher -once \
  -base-url http://localhost:57811 \
  -message-limit 50 \
  -max-message-fetch 100 \
  -session-workers 8 \
  -db ./data/verify-hardening.db \
  -output-dir ./data/messages_hardening_verify \
  -log-file ./data/session-watcher-verify.log
```

结果：

- `go test ./...` 全部通过。
- 端到端同步成功：8 个 session，100 条 message。
- `max_message_fetch` 出现在启动日志和 boundary probe 日志中。
- JSONL 输出仍按 session 分文件。
- 日志文件成功生成并包含启动日志。
- 验证 DB、JSONL 文件和日志文件已清理。

## 注意事项

- `max-message-fetch` 是硬上限；如果两轮之间新增超过该上限，超过最近上限之外的 message 可能不会在本轮被获取。
- per-file mutex 只解决单进程内并发写入，不解决多副本/多进程写同一文件的协调问题。
- 文件日志暂不支持轮转，长期运行后如果日志量较大，后续可增加按大小或日期切分。
- 当前 lint 仅有一个非阻塞 hint：`store.go` 中一个 if 可用 `min` 简化，不影响构建和运行。
