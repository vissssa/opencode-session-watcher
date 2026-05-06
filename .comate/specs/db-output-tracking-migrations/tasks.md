# DB 输出追踪与 schema migrations 优化任务计划

- [x] Task 1: 扩展领域模型和 Sink 路径解析接口
    - 1.1: 在 `MessageRecord` 中增加 `SinkType`、`OutputRoot`、`OutputPath`、`OutputSessionFile` 非 JSON 输出字段
    - 1.2: 新增 `PathResolver` 接口，包含 `PathFor`、`SinkType`、`OutputRoot`
    - 1.3: 保持现有 `Sink` 接口不变，避免影响未来 ES/S3 Sink 扩展
    - 1.4: 更新相关测试构造的 `MessageRecord`

- [x] Task 2: 暴露 JSONL Sink 输出路径信息
    - 2.1: 将 JSONL Sink 的路径计算方法从私有 `pathFor` 调整为公开 `PathFor`
    - 2.2: 实现 `SinkType()` 返回 `jsonl`
    - 2.3: 实现 `OutputRoot()` 返回 Sink 构造时传入的输出根目录
    - 2.4: 保持路径清洗和分目录写入逻辑不变
    - 2.5: 增加 JSONL Sink 单元测试验证 `PathFor`、`SinkType`、`OutputRoot`

- [x] Task 3: 实现 SQLite schema migrations 机制
    - 3.1: 创建 `schema_migrations` 表，字段包括 `version`、`name`、`applied_at`
    - 3.2: 将基础建表逻辑纳入 migration version 1
    - 3.3: 实现 migration version 2，为 `sessions` 增加 `user_id`、`agent_id` 和索引
    - 3.4: 实现 migration version 3，为 `messages` 增加输出追踪字段和索引
    - 3.5: 实现字段存在性检查，保证老 DB 重复启动不会因重复 `ADD COLUMN` 失败
    - 3.6: 每个 migration 使用事务执行，成功后写入 `schema_migrations`
    - 3.7: 增加 migration 单元测试覆盖新 DB、老 DB、重复迁移

- [x] Task 4: 调整 Store 写入接口和输出追踪持久化
    - 4.1: 将 `CommitSessionSync` 入参从 `[]domain.Message` 改为 `[]domain.MessageRecord`
    - 4.2: 写入 `messages` 表时保存 `user_id`、`agent_id`、`sink_type`、`output_root`、`output_path`、`output_session_file`
    - 4.3: 写入 `sessions` 表时保存 `user_id`、`agent_id`
    - 4.4: 保持 `INSERT OR IGNORE` 语义，已存在 message 不覆盖历史输出目标
    - 4.5: 保持 latest message 游标计算逻辑正确
    - 4.6: 更新 Store 单元测试验证输出追踪字段和老数据兼容

- [x] Task 5: 调整 Watcher 填充输出追踪字段
    - 5.1: 在构造 `MessageRecord` 后检测 Sink 是否实现 `PathResolver`
    - 5.2: 为每条 record 填充 `SinkType`、`OutputRoot`、`OutputPath`、`OutputSessionFile`
    - 5.3: Sink 不支持 `PathResolver` 时填充安全默认值，不阻断同步
    - 5.4: 调整调用 `CommitSessionSync` 的参数为 records
    - 5.5: 更新 Watcher 单元测试验证 record 输出追踪字段和时间顺序

- [x] Task 6: 格式化、测试和端到端验证
    - 6.1: 运行 `gofmt` 格式化所有变更 Go 文件
    - 6.2: 运行 `go test ./...` 验证全部测试
    - 6.3: 使用本地 open-code 服务运行 `go run ./cmd/session-watcher -once` 到临时 DB 和临时输出目录
    - 6.4: 查询 SQLite，验证 `schema_migrations` 已记录版本 1、2、3
    - 6.5: 查询 SQLite，验证 `messages` 表已记录 sink 和 output path 字段
    - 6.6: 检查 SQLite 中的 `output_path` 与实际 JSONL 文件路径一致
    - 6.7: 清理端到端验证产生的临时输出文件和临时 DB
