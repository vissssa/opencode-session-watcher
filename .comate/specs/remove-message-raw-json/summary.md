# 移除 messages.raw_json 与所有 migration 总结

## 完成内容

本次按开发阶段要求完成以下调整：

- 删除 Store 中所有 migration 相关实现。
- 不再创建 `schema_migrations` 表。
- Store 初始化改为直接创建当前唯一最终 schema。
- `messages` 表不再包含 `raw_json` 字段。
- `CommitSessionSync` 写入 `messages` 时不再保存 `record.Message` 原始 JSON。
- `sessions.raw_json` 保留不变，用于保存 session 原始 JSON。
- message 去重字段和 output tracking 字段保留不变。
- Store 单元测试改为只验证当前 schema，不再验证老 DB 迁移。

## 当前 messages 表字段

当前 `messages` 表字段为：

- `id`
- `session_id`
- `created_at`
- `written_at`
- `user_id`
- `agent_id`
- `sink_type`
- `output_root`
- `output_path`
- `output_session_file`

不再包含：

- `raw_json`

## 当前 sessions 表

`sessions` 表仍保留：

- `raw_json`

用于保存 session 详情原始 JSON。

## 验证结果

已执行：

```bash
gofmt -w internal/store/store.go internal/store/store_test.go
go test ./...
go run ./cmd/session-watcher -once \
  -base-url http://localhost:57811 \
  -message-limit 100 \
  -session-workers 8 \
  -db ./data/verify-no-migration.db \
  -output-dir ./data/messages_no_migration_verify
```

验证通过：

- `go test ./...` 全部通过。
- lint 无诊断。
- 临时 DB 中不存在 `schema_migrations` 表。
- 临时 DB 的 `messages` 表不存在 `raw_json` 字段。
- 临时 DB 的 `sessions` 表仍存在 `raw_json` 字段。
- 端到端同步成功。
- 临时验证 DB 和 JSONL 文件已清理。

## 注意事项

当前不考虑数据库版本兼容，因此已有旧 DB 不保证可用。开发阶段如遇 schema 不匹配，直接删除旧 DB 或换新的 `-db` 路径即可。
