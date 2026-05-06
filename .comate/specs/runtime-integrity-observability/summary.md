# 运行完整性与可观测性改造总结

## 完成内容

本次完成 4 项改造：

1. 使用 SQLite outbox 改善 JSONL 写入与 DB 状态一致性。
2. 增加 health/status HTTP 服务。
3. 记录 `max-message-fetch` 达上限状态。
4. 增加开发期 schema sanity check。

## 主要实现

### SQLite outbox

`messages` 表新增：

- `status`：`pending` / `written`
- `prepared_at`
- `last_error`
- `written_at` 默认改为 0

Store 新增方法：

- `PrepareMessageRecords`
- `MarkMessagesWritten`
- `MarkMessagesFailed`

Watcher 写入流程改为：

```text
UnseenMessages
-> build records
-> fill output tracking
-> PrepareMessageRecords(status=pending)
-> Sink.WriteMessages
-> MarkMessagesWritten(status=written)
```

如果 Sink 写失败，会调用 `MarkMessagesFailed` 记录错误，不推进 session 游标。

### max-message-fetch 可观测

`sessions` 表新增：

- `last_fetch_reached_limit`
- `last_fetch_count`
- `last_fetch_limit`
- `last_fetch_at`

Watcher 每个 session fetch 完成后写入 fetch stats。

`RoundResult` 新增：

- `MaxFetchReached`

每轮日志会输出：

```text
max_fetch_reached
```

### Health/status 服务

新增：

- `internal/status`
- `internal/health`

新增配置：

```bash
-health-addr 127.0.0.1:0
```

默认启用本地随机端口，并在日志中打印实际地址。

可禁用：

```bash
-health-addr ""
```

接口：

```text
GET /healthz
GET /status
```

`/healthz` 返回：

```json
{"status":"ok"}
```

`/status` 返回运行快照，包括：

- started_at
- last_round_at
- last_round_duration_millis
- last_success_at
- last_error
- rounds_total
- sessions_total
- sessions_synced
- sessions_failed
- messages_new
- max_fetch_reached_total
- last_max_fetch_reached

### Schema sanity check

Store 启动时会检查当前开发期 schema：

- 必须包含当前 `sessions` 字段。
- 必须包含当前 `messages` 字段。
- 拒绝 `messages.raw_json`。
- 拒绝 `schema_migrations` 表。

如果发现旧 DB，会返回明确错误，提示删除旧 DB 或使用新 `-db` 路径。

## 主要文件

- `internal/store/store.go`
- `internal/watcher/watcher.go`
- `internal/status/status.go`
- `internal/health/server.go`
- `internal/config/config.go`
- `cmd/session-watcher/main.go`
- `internal/**/*_test.go`

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
  -db ./data/verify-integrity.db \
  -output-dir ./data/messages_integrity_verify \
  -log-file ./data/session-watcher-integrity-verify.log \
  -health-addr 127.0.0.1:0
```

验证结果：

- `go test ./...` 全部通过。
- lints 无诊断。
- 端到端同步成功：8 个 session，100 条 message。
- DB 中 `messages` 全部为 `written`。
- DB 中 8 个 session 均写入 fetch stats。
- health server 启动并在日志中打印实际监听地址。
- health/status handler 已由单元测试覆盖。
- 临时 DB、JSONL、日志文件均已清理。

## 注意事项

- outbox 不能实现文件系统和 SQLite 的真正原子事务；如果进程在 JSONL 写成功但 `MarkMessagesWritten` 前崩溃，pending 记录仍可能被重写，造成重复 JSONL 行。但相比原实现，DB 中已有明确 pending/last_error 状态，便于排查和后续修复。
- 当前开发阶段不做 migration；旧 DB 会被 schema sanity check 拒绝。
