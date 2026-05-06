# 移除 messages.raw_json 与所有 migration 任务计划

- [x] Task 1: 简化 Store 初始化为单一当前 schema
    - 1.1: 删除 `migration` 结构体和 migration 列表
    - 1.2: 删除 `migrate`、`migrationApplied`、`applyMigration` 等 migration 执行逻辑
    - 1.3: 删除 `applyInitialSchema`、`applySessionMetadata`、`applyMessageOutputTracking`
    - 1.4: 删除 `addColumnIfMissing`、`columnExists` 等兼容老 DB 的字段检查逻辑
    - 1.5: 将 `Open` 调整为 configure 后直接调用 `init`

- [x] Task 2: 重写当前最终 schema
    - 2.1: 在 `init` 中直接创建最终 `sessions` 表
    - 2.2: 保留 `sessions.raw_json`
    - 2.3: 在 `init` 中直接创建最终 `messages` 表
    - 2.4: 从 `messages` 表中移除 `raw_json`
    - 2.5: 在 `init` 中直接创建 sessions/messages 相关索引
    - 2.6: 确保不再创建 `schema_migrations` 表

- [x] Task 3: 调整 message 写入逻辑
    - 3.1: 修改 `CommitSessionSync` 的 `INSERT INTO messages` 字段列表
    - 3.2: 移除 `raw_json` 列名
    - 3.3: 移除 `string(record.Message)` 参数
    - 3.4: 保持 `INSERT OR IGNORE` 去重语义不变
    - 3.5: 保持 output tracking 字段写入不变
    - 3.6: 保持 latest message 游标计算逻辑不变

- [x] Task 4: 更新 Store 单元测试
    - 4.1: 删除老 DB migration 测试
    - 4.2: 删除 `schema_migrations` 版本断言
    - 4.3: 增加当前 schema 字段检查测试
    - 4.4: 验证 `messages` 表不存在 `raw_json` 列
    - 4.5: 验证 `sessions` 表仍存在 `raw_json` 列
    - 4.6: 验证 message output tracking 字段仍正确写入

- [x] Task 5: 格式化与验证
    - 5.1: 运行 `gofmt` 格式化变更文件
    - 5.2: 运行 `go test ./...`
    - 5.3: 使用临时 DB 运行 `go run ./cmd/session-watcher -once`
    - 5.4: 检查临时 DB 不存在 `schema_migrations` 表
    - 5.5: 检查临时 DB 的 `messages` 表不存在 `raw_json` 字段
    - 5.6: 检查临时 DB 的 `sessions` 表仍存在 `raw_json` 字段
    - 5.7: 清理端到端验证产生的临时 DB 和临时输出文件
