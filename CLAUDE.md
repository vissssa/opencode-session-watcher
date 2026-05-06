# CLAUDE.md — session_watcher

## 项目概述

session_watcher 是一个 Go 命令行服务，周期性从 open-code HTTP API 拉取 AI 对话会话消息，以增量去重方式写入本地 JSONL 文件，并用 SQLite 维护同步状态。
- 每次新增、修改、删除代码，都要保证相应代码的注释得到更新
- 和我的对话以及运行中的plan等都需要使用中文
- 每次生成可执行文件，结束后都要自动删除

## 项目结构

```
cmd/session-watcher/main.go     程序入口、生命周期、信号处理
internal/config/config.go       CLI flag 解析与参数校验
internal/domain/domain.go       核心类型 + Source/Sink/PathResolver 接口
internal/source/opencode/       open-code HTTP Source（重试、JSON 解析）
internal/sink/jsonl/            JSONL FileSink（per-file mutex、路径生成）
internal/store/store.go         SQLite 状态存储（WAL、Schema 检查、事务写入）
internal/watcher/watcher.go     核心编排：Worker Pool、增量拉取、去重写入
internal/health/server.go       HTTP /healthz /status 端点
internal/status/status.go       线程安全运行状态快照
internal/lease/lease.go         基于文件时间戳的 Leader 选举（Active-Standby HA）
```

## 常用命令

```bash
# 构建
make build

# 运行测试（含竞态检测）
go test -race -timeout 30s ./...

# 本地运行（需要 open-code 服务在 localhost:57811）
go run ./cmd/session-watcher -once -log-level debug

# HA 多副本模式（需要 GlusterFS 共享挂载）
go run ./cmd/session-watcher \
  -lease-path /mnt/glusterfs/leader.lease \
  -lease-id $(hostname):$$ \
  -lease-timeout 30s \
  -lease-renew-interval 10s

# 完整构建（下载依赖 + 编译 + 打包到 output/）
make all

# 端到端一致性验证：校验本地 JSONL 与 open-code API 数据是否完全吻合
# 需要 open-code 服务可访问，依赖 curl + jq
./scripts/verify_jsonl.sh                              # 校验全部 Session
./scripts/verify_jsonl.sh -s <session_id> -v           # 校验单个 Session（详细输出）
./scripts/verify_jsonl.sh -u http://host:57811 -d ./data/messages  # 自定义地址/目录
```

## 架构要点

### 接口边界

核心编排（watcher）完全依赖接口，**不依赖任何具体实现**：

- `domain.Source` — 数据输入（当前实现：opencode.HTTPSource）
- `domain.Sink` — 数据输出（当前实现：jsonl.FileSink）
- `domain.PathResolver` — 输出路径策略（由 Sink 可选实现）

新增 Source 或 Sink 只需实现对应接口，在 `main.go` 替换即可，watcher 核心无需修改。

### 增量策略（fetchUntilBoundary）

动态扩展 `limit`，步长为 `MessageLimit`，上限为 `MaxMessageFetch`：
- 遇到已处理消息 → 停止扩展（找到边界）
- 返回数 < limit → 停止扩展（已取完）
- limit 触顶 → 记录 warn，继续处理已拉取数据（**可能遗漏消息**）

### 并发模型

- Worker Pool：jobCh（unbuffered）+ resultCh（buffered）+ sync.WaitGroup
- SQLite：`SetMaxOpenConns(1)` 串行化所有写入，避免锁冲突
- JSONL：`locksMu` 保护的 per-file `sync.Mutex`，确保每行记录完整
- status.Reporter：`sync.RWMutex` 保护快照读写
- round 计数：`atomic.Int64`

### 消息状态机

```
Unseen → Pending（PrepareMessageRecords, INSERT OR IGNORE）
       → Written（MarkMessagesWritten，WriteMessages 成功后）
       → PendingError（MarkMessagesFailed，WriteMessages 失败）
PendingError → Pending（下轮重试）
```

写入语义为 **at-least-once**。

## 存储模型

两张表：`sessions`（Session 状态与游标）、`messages`（消息去重与输出追踪）

SQLite PRAGMA：`journal_mode=WAL`、`synchronous=NORMAL`、`busy_timeout=5000`

Schema 检查在启动时进行，不兼容时拒绝启动并提示删除旧 DB（无自动迁移）。

## 代码规范

- **依赖原则**：优先使用 Go 标准库，非必要不引入第三方包
- **错误处理**：错误向上传播；单 Session 失败不影响其他 Session
- **日志**：使用 `log/slog`，结构化键值对，禁止裸字符串格式化
- **测试**：新功能需有对应单元测试；使用 `t.TempDir()` 隔离 SQLite 测试数据库
- **接口优先**：watcher 核心通过接口依赖 Source/Sink/Store，不直接依赖具体类型

## 已知风险与注意事项

- **MaxMessageFetch 触顶**：`reachedMax=true` 时仅 warn，不会失败重试——若新增消息超出上限，可能永久遗漏
- **at-least-once**：进程崩溃后重启，最后一批消息可能重复写入 JSONL
- **locks map 增长**：FileSink 的 per-file mutex map 只增不减，长期运行内存会持续增长
- **无 Schema 迁移**：DB 结构不兼容时，需手动删除旧 DB 或指定新 `-db` 路径
- **脑裂窗口**：write-then-verify 的 jitter+verifyDelay 约 250~400ms，极端情况下两个实例可能同时认为自己是 Leader，窗口极小且最终会收敛
- **renew() 读改写脑裂路径**：`renew()` 采用 readLease→修改→writeLease 的非原子操作，若 Standby 在此窗口内完成竞争写入，Leader 的续约会无声覆盖 Standby 写入，产生双主。窗口通常 < 10ms，但在高延迟 GlusterFS 下可能更宽。最终通过下一轮 lease 文件内容不一致自动收敛，但收敛前存在短暂双主。
- **GlusterFS rename 原子性**：lease 写入使用 tmp+rename，在部分 GlusterFS 版本上 rename 可能非原子，建议测试验证

## 外部依赖

| 包 | 版本 | 用途 |
|----|------|------|
| `modernc.org/sqlite` | v1.50.0 | Pure Go SQLite（免 CGO） |
| `gopkg.in/natefinch/lumberjack.v2` | v2.2.1 | 日志文件轮转 |

## CI / 部署

- CI：百度 bcloud，同时产出 x86_64 和 ARM64 二进制
- 容器：`build/Dockerfile`（基于 Alpine），入口脚本：`scripts/docker_entry.sh`
- 产出目录：`output/bin/session_watcher`
