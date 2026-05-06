# session JSONL 分目录与时间顺序输出改造总结

## 完成内容

本次变更已完成并验证：

- 将 CLI 输出参数从 `-output` 改为 `-output-dir`。
- 默认输出目录从 `./data/messages.jsonl` 改为 `./data/messages`。
- JSONL 输出从单文件改为按 session 分文件。
- 输出路径格式为 `{output-dir}/{user_id}/{agent_id}/{session_id}.jsonl`。
- 当 session 缺少 `user_id` 或 `agent_id` 时，分别使用 `default_user` 和 `default_agent`。
- 从 open-code session JSON 中提取 `user_id` / `userID` 与 `agent_id` / `agentID`。
- `MessageRecord` 中新增 `user_id`、`agent_id` 字段。
- 保持每个 session 文件内 message 按 `message_created_at` 升序写入，早的在上，新的在下。
- JSONL Sink 对路径片段做安全清洗，避免空值和路径穿越。
- 并发写入时通过 Sink 锁保证单行 JSON 不交叉。

## 主要修改文件

- `internal/config/config.go`：配置项改为 `OutputDir`，参数名改为 `-output-dir`。
- `cmd/session-watcher/main.go`：启动日志和 Sink 初始化改为使用 `cfg.OutputDir`。
- `internal/domain/domain.go`：新增默认 user/agent 常量，扩展 `Session` 和 `MessageRecord`。
- `internal/source/opencode/types.go`：提取 session 的 user/agent 元数据并设置默认值。
- `internal/sink/jsonl/writer.go`：按 `user_id/agent_id/session_id.jsonl` 分文件写入。
- `internal/watcher/watcher.go`：构造记录时写入 user/agent，并保留按时间升序排序逻辑。
- `internal/**/*_test.go`：更新配置、Source、Sink、Watcher 单元测试。

## 验证结果

已执行：

```bash
gofmt -w ...
go test ./...
go run ./cmd/session-watcher -once \
  -base-url http://localhost:57811 \
  -message-limit 100 \
  -session-workers 8 \
  -db ./data/verify-state.db \
  -output-dir ./data/messages_verify
```

结果：

- `go test ./...` 全部通过。
- 本地端到端同步成功：8 个 session，100 条 message。
- 生成 8 个 session JSONL 文件。
- 本地 open-code session 当前没有返回 `user_id` / `agent_id`，因此验证路径为：`data/messages_verify/default_user/default_agent/{session_id}.jsonl`。
- 已用脚本验证每个 session 文件内 `message_created_at` 升序。
- 本次验证临时产物 `data/messages_verify/*` 与 `data/verify-state.db` 已清理。

## 使用方式

```bash
go run ./cmd/session-watcher \
  -base-url http://localhost:57811 \
  -interval 10s \
  -message-limit 100 \
  -session-workers 8 \
  -db ./data/state.db \
  -output-dir ./data/messages
```

## 注意事项

- 之前用旧参数生成的 `data/messages.jsonl` 和 `data/state.db` 没有自动迁移；新输出会写入 `-output-dir` 下的分层目录。
- 如果使用已有 `state.db`，已处理过的 message 不会重新输出到新目录；如需重新导出，需要使用新的 SQLite 状态库路径或清理旧状态。
- lint 仅提示 `watcher.go` 中一个 `min` 可简化 hint，不影响构建、测试和运行。
