# session JSONL 分目录与时间顺序输出改造任务计划

- [x] Task 1: 调整命令行输出配置
    - 1.1: 将 `-output` 参数替换为 `-output-dir`
    - 1.2: 将默认输出从 `./data/messages.jsonl` 改为 `./data/messages`
    - 1.3: 更新配置结构、校验逻辑和启动日志字段
    - 1.4: 更新 main 中 JSONL Sink 初始化参数
    - 1.5: 更新配置单元测试覆盖 `output-dir`

- [x] Task 2: 扩展 session 元数据提取
    - 2.1: 在 `domain.Session` 中增加 `UserID` 和 `AgentID`
    - 2.2: 在 `domain.MessageRecord` 中增加 `user_id` 和 `agent_id` 输出字段
    - 2.3: 在 open-code session 解析中提取 `user_id` / `userID`
    - 2.4: 在 open-code session 解析中提取 `agent_id` / `agentID`
    - 2.5: 当字段缺失时使用 `default_user` 和 `default_agent`
    - 2.6: 更新 HTTP Source 单元测试验证字段提取和默认值

- [x] Task 3: 重构 JSONL Sink 为按 session 分文件输出
    - 3.1: 将 Sink 构造参数语义改为输出根目录
    - 3.2: 根据 `MessageRecord.UserID`、`AgentID`、`SessionID` 生成输出路径
    - 3.3: 输出路径格式为 `{output-dir}/{user_id}/{agent_id}/{session_id}.jsonl`
    - 3.4: 对路径片段进行安全清洗，避免空值和路径穿越
    - 3.5: 写入前自动创建对应的 user/agent 目录
    - 3.6: 为并发 session 写入维护文件级锁或 Sink 全局锁，保证单行 JSON 不交叉
    - 3.7: 更新 JSONL Sink 单元测试验证按 session 分文件写入

- [x] Task 4: 保证每个 session 文件内按时间顺序追加
    - 4.1: 保持 watcher 对未处理消息按 `createdAt` 升序排序
    - 4.2: 相同 `createdAt` 时继续按 message ID 稳定排序
    - 4.3: 构造 `MessageRecord` 时写入 `user_id`、`agent_id`、`session_id`
    - 4.4: 确保同一 session 的消息批次一次性按排序后顺序写入该 session 文件
    - 4.5: 更新 watcher 单元测试验证 JSONL 输出顺序为早的在上、新的在下

- [x] Task 5: 更新测试与端到端验证
    - 5.1: 更新所有因 `-output` 改为 `-output-dir` 受影响的测试
    - 5.2: 增加缺少 `user_id` / `agent_id` 时路径使用默认值的测试
    - 5.3: 增加存在 `user_id` / `agent_id` 时路径使用真实字段的测试
    - 5.4: 运行 `gofmt` 格式化变更文件
    - 5.5: 运行 `go test ./...` 验证全部单元测试
    - 5.6: 使用本地 open-code 服务运行 `go run ./cmd/session-watcher -once` 验证输出目录结构
    - 5.7: 检查生成文件路径和文件内 JSONL 时间顺序
