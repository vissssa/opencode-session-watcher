# DB 输出追踪与 schema migrations 优化总结

## 完成内容

本次优化已完成：

- 新增 `schema_migrations` 表，用于记录已应用的 DB schema 版本。
- 将基础建表纳入 migration version 1。
- 新增 migration version 2，为 `sessions` 表增加：
  - `user_id`
  - `agent_id`
  - `idx_sessions_user_agent` 索引
- 新增 migration version 3，为 `messages` 表增加：
  - `user_id`
  - `agent_id`
  - `sink_type`
  - `output_root`
  - `output_path`
  - `output_session_file`
  - `idx_messages_sink_output` 索引
  - `idx_messages_user_agent_session` 索引
- 增加字段存在性检查，已有老 DB 重复启动不会因为重复 `ALTER TABLE ADD COLUMN` 失败。
- 扩展 `MessageRecord`，增加不写入 JSONL 的输出追踪字段。
- 新增 `PathResolver` 接口，使 watcher 能从 Sink 获取输出路径信息。
- JSONL Sink 实现：
  - `PathFor`
  - `SinkType`
  - `OutputRoot`
- Store 的 `CommitSessionSync` 改为接收 `[]domain.MessageRecord`，并把输出目标写入 SQLite。
- Watcher 在写 Sink 前自动填充：
  - `SinkType`
  - `OutputRoot`
  - `OutputPath`
  - `OutputSessionFile`

## 主要修改文件

- `internal/domain/domain.go`
  - 新增 `PathResolver` 接口。
  - 扩展 `MessageRecord` 输出追踪字段。
- `internal/sink/jsonl/writer.go`
  - 暴露输出路径解析能力。
  - 保持 JSONL 内容不包含本地输出路径字段。
- `internal/store/store.go`
  - 新增 migration 机制。
  - 扩展 `sessions` 与 `messages` 表。
  - 持久化每条 message 的 sink 和 output path 信息。
- `internal/watcher/watcher.go`
  - 填充输出追踪字段。
  - 调整 `CommitSessionSync` 调用参数。
- `internal/store/store_test.go`
  - 增加 migration 和 output tracking 测试。
- `internal/sink/jsonl/writer_test.go`
  - 增加 `PathFor`、`SinkType`、`OutputRoot` 验证。
- `internal/watcher/watcher_test.go`
  - 验证 watcher 输出追踪字段。

## 验证结果

已执行：

```bash
gofmt -w ...
go test ./...
go run ./cmd/session-watcher -once \
  -base-url http://localhost:57811 \
  -message-limit 100 \
  -session-workers 8 \
  -db ./data/verify-output-tracking.db \
  -output-dir ./data/messages_output_tracking_verify
```

结果：

- `go test ./...` 全部通过。
- 端到端同步成功：8 个 session，100 条 message。
- `schema_migrations` 中记录版本：
  - `1 | initial_schema`
  - `2 | session_metadata`
  - `3 | message_output_tracking`
- `messages` 表中 100 条记录均包含非空：
  - `sink_type=jsonl`
  - `output_path`
  - `output_session_file`
- 已验证 DB 中记录的 100 条 `output_path` 都能对应到实际生成的 JSONL 文件。
- 端到端验证产生的临时 DB 和临时 JSONL 文件已清理。

## 注意事项

- 已有老 DB 会在下次启动时自动迁移。
- 旧 message 迁移后默认 `sink_type='jsonl'`，但 `output_root/output_path/output_session_file` 为空，因为历史数据无法反推出当时的输出目标。
- 已存在 message 使用 `INSERT OR IGNORE`，不会覆盖历史输出目标。
- 多副本协调本次未实现，后续单独设计。
- 当前仅剩一个非阻塞 lint hint：`watcher.go:93` 可用 `min` 简化，不影响构建、测试和运行。
