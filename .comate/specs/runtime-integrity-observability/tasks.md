# 运行完整性与可观测性任务计划

- [x] Task 1: 扩展当前 SQLite schema 并增加 sanity check
    - 1.1: 在 `sessions` 表增加 `last_fetch_reached_limit`、`last_fetch_count`、`last_fetch_limit`、`last_fetch_at`
    - 1.2: 在 `messages` 表增加 `status`、`prepared_at`、`last_error`
    - 1.3: 将 `messages.written_at` 调整为默认 0，表示尚未确认写入
    - 1.4: 增加 `idx_messages_status` 索引
    - 1.5: 实现 schema sanity check，校验当前开发期 schema 必需字段
    - 1.6: sanity check 明确拒绝 `messages.raw_json`
    - 1.7: sanity check 明确拒绝 `schema_migrations` 表
    - 1.8: 在 `Open` 中 `init` 后执行 sanity check
    - 1.9: 更新 Store schema 单元测试

- [x] Task 2: 实现 SQLite outbox 状态写入流程
    - 2.1: 新增 `PrepareMessageRecords`，将待写 message 插入/更新为 `pending`
    - 2.2: 新增 `MarkMessagesWritten`，将 message 标记为 `written` 并更新 session 游标
    - 2.3: 新增 `MarkMessagesFailed`，写入 `last_error` 且不推进 session 游标
    - 2.4: 调整 `ExistingMessageIDs` 批量查询为返回 message status
    - 2.5: 调整 `AnyMessageExists` 只把 `written` 视为处理边界
    - 2.6: 调整 `UnseenMessages` 返回不存在或 `pending` 的 message，跳过 `written`
    - 2.7: 保留必要兼容方法或同步更新 watcher.Store 接口
    - 2.8: 增加 Store 单元测试覆盖 pending/written/failed 状态

- [x] Task 3: 改造 Watcher 写入链路为 outbox 流程
    - 3.1: 构造 records 后先调用 `PrepareMessageRecords`
    - 3.2: 仅将 `PrepareMessageRecords` 返回的 records 写入 Sink
    - 3.3: Sink 成功后调用 `MarkMessagesWritten`
    - 3.4: Sink 失败后调用 `MarkMessagesFailed`
    - 3.5: 移除旧 `CommitSessionSync` 调用
    - 3.6: 保持 message 时间排序和 output tracking 填充逻辑不变
    - 3.7: 更新 watcher 单元测试覆盖 Sink 成功和失败路径

- [x] Task 4: 记录 max-message-fetch 达上限状态
    - 4.1: 调整 `fetchUntilBoundary` 返回 reachedMax 标记和最终 limit
    - 4.2: 新增 Store 方法 `UpdateSessionFetchStats`
    - 4.3: 每个 session fetch 结束后写入 fetch stats
    - 4.4: `RoundResult` 增加 `MaxFetchReached`
    - 4.5: sessionResult 增加 max fetch reached 状态
    - 4.6: 每轮汇总日志输出 `max_fetch_reached`
    - 4.7: 更新 Store 和 Watcher 测试验证 fetch stats

- [x] Task 5: 增加运行状态 Reporter
    - 5.1: 新建 `internal/status` 包
    - 5.2: 定义 `Snapshot` 结构，包含启动时间、最近轮次、错误、session/message 统计和 max-fetch 统计
    - 5.3: 实现并发安全 `Reporter`
    - 5.4: main 启动时创建 reporter
    - 5.5: 每轮 SyncOnce 后更新 reporter
    - 5.6: 出错时记录 last error
    - 5.7: 增加 status 单元测试

- [x] Task 6: 增加 health/status HTTP 服务
    - 6.1: 在 config 中增加 `DefaultHealthAddr = 127.0.0.1:0`
    - 6.2: 增加 `-health-addr` 参数，空字符串表示禁用
    - 6.3: 新建 `internal/health` 包
    - 6.4: 实现 `/healthz` 返回 `{"status":"ok"}`
    - 6.5: 实现 `/status` 返回 reporter snapshot JSON
    - 6.6: 支持 `127.0.0.1:0` 并在日志中打印实际监听地址
    - 6.7: main 中启动 health server，并在退出时关闭
    - 6.8: 增加 health handler 单元测试

- [x] Task 7: 更新端到端验证与清理
    - 7.1: 运行 `gofmt` 格式化所有变更文件
    - 7.2: 运行 `go test ./...`
    - 7.3: 使用临时 DB 和输出目录运行 `go run ./cmd/session-watcher -once`
    - 7.4: 查询 DB 验证 messages 全部为 `written`
    - 7.5: 查询 DB 验证 sessions fetch stats 已写入
    - 7.6: 启动非 once 模式或测试 server 验证 `/healthz` 与 `/status`
    - 7.7: 清理端到端验证产生的临时 DB、JSONL、日志文件
