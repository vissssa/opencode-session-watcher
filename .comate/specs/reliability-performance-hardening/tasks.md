# 可靠性与性能加固任务计划

- [x] Task 1: 增加 MaxMessageFetch 配置和 fetch 上限保护
    - 1.1: 在 `internal/config` 增加 `DefaultMaxMessageFetch = 1000`
    - 1.2: 在 `Config` 中增加 `MaxMessageFetch`
    - 1.3: 增加 `-max-message-fetch` 命令行参数
    - 1.4: 校验 `max-message-fetch > 0`
    - 1.5: 校验 `message-limit <= max-message-fetch`
    - 1.6: 在 main 启动日志中打印 `max_message_fetch`
    - 1.7: 在 `watcher.Config` 中增加 `MaxMessageFetch`
    - 1.8: 修改 `fetchUntilBoundary`，limit 扩大到上限后停止
    - 1.9: 达到上限仍未发现已处理 message 时打印 warn 日志
    - 1.10: 更新配置和 watcher 单元测试

- [x] Task 2: 将 message 存在性检查改为批量查询
    - 2.1: 在 Store 中新增 `ExistingMessageIDs(ctx, ids)` 批量查询方法
    - 2.2: 对输入 ID 去重并过滤空字符串
    - 2.3: 使用 `WHERE id IN (?,...)` 批量查询
    - 2.4: 按固定 chunk 大小拆分查询，避免 SQLite 参数数量过多
    - 2.5: 改造 `AnyMessageExists` 使用批量查询结果
    - 2.6: 改造 `UnseenMessages` 使用批量查询结果
    - 2.7: 保留 `MessageExists` 供单点查询和测试使用
    - 2.8: 增加 1000 条消息批量去重测试

- [x] Task 3: 将 JSONL Sink 全局锁改为 per-file mutex
    - 3.1: 将 `FileSink.mu` 改为 `locksMu + map[string]*sync.Mutex`
    - 3.2: 增加 `lockFor(path)` 获取单文件锁
    - 3.3: `WriteMessages` 仍按 path 分组，但每个 path 单独加锁
    - 3.4: 保持同一文件内单批记录顺序写入
    - 3.5: 保持不同 session 文件可并发写入
    - 3.6: 更新并发写入测试，覆盖同文件和不同文件场景

- [x] Task 4: 为 HTTPSource.get 增加指数退避重试
    - 4.1: 将单次 HTTP 请求提取为 `getOnce`
    - 4.2: `get` 实现最多 3 次总尝试
    - 4.3: 网络错误、HTTP 5xx、HTTP 429 触发重试
    - 4.4: HTTP 4xx 除 429 外不重试
    - 4.5: context canceled / deadline exceeded 不重试
    - 4.6: 退避间隔使用 100ms、200ms
    - 4.7: 每次重试打印 warn 日志，包含 attempt、url、backoff、error
    - 4.8: 增加 500/429/400/网络错误相关单元测试

- [x] Task 5: 增加持久化日志文件输出
    - 5.1: 在 `internal/config` 增加 `DefaultLogFile = ./data/session-watcher.log`
    - 5.2: 在 `Config` 中增加 `LogFile`
    - 5.3: 增加 `-log-file` 命令行参数
    - 5.4: 允许 `-log-file ""` 禁用文件日志
    - 5.5: main 中非空 log-file 自动创建父目录并 append 打开文件
    - 5.6: 使用 `io.MultiWriter(os.Stderr, file)` 同时写 stderr 和日志文件
    - 5.7: log file 打开失败时启动失败
    - 5.8: 退出时关闭 log file
    - 5.9: 增加配置测试，端到端验证日志文件生成

- [x] Task 6: 格式化、测试和端到端验证
    - 6.1: 运行 `gofmt` 格式化所有变更 Go 文件
    - 6.2: 运行 `go test ./...`
    - 6.3: 使用本地 open-code 服务运行 `go run ./cmd/session-watcher -once`
    - 6.4: 验证 `max-message-fetch` 参数生效
    - 6.5: 验证 JSONL 输出仍按 session 分文件
    - 6.6: 验证日志文件生成且包含启动日志
    - 6.7: 清理端到端验证产生的临时 DB、JSONL 文件和日志文件
