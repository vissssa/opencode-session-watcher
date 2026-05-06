# 默认压缩日志滚动设计文档

## 背景与目标

当前日志文件实现位于：

- `/Users/siegward/Developer/baidu/easydata/session_watcher/cmd/session-watcher/main.go`

当前逻辑：

```go
file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
return io.MultiWriter(os.Stderr, file), func() { _ = file.Close() }, nil
```

该实现仅将日志追加写入 `-log-file`，没有按时间或大小滚动，也不会压缩历史日志。

本次目标：

- 不新增启动参数。
- 保持 `-log-file` 作为日志文件路径配置。
- 默认自动按大小和日期滚动。
- 历史日志直接 gzip 压缩存储。
- 保留 stderr 输出。
- 只使用 Go 标准库，不引入第三方依赖。

## 默认策略

使用内置默认值，不暴露为 CLI 参数：

```go
const (
    defaultLogMaxSizeBytes = 100 * 1024 * 1024 // 100MB
    defaultLogMaxBackups   = 10
)
```

滚动触发条件：

1. 当前日期变化：按天滚动。
2. 当前日志文件大小超过 100MB：按大小滚动。

历史文件命名：

```text
session-watcher.log.20260501-120000.gz
session-watcher.log.20260501-130000.gz
```

当前活跃文件仍然是：

```text
session-watcher.log
```

历史保留策略：

- 最多保留 10 个 `.gz` 归档文件。
- 超出数量后删除最旧的归档文件。

## 技术方案

### 1. 新增 rolling log writer

新增文件：

- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/logging/rolling_writer.go`

核心结构：

```go
type RollingWriter struct {
    mu sync.Mutex
    path string
    maxSizeBytes int64
    maxBackups int
    file *os.File
    currentDay string
    size int64
}
```

对外方法：

```go
func NewRollingWriter(path string, maxSizeBytes int64, maxBackups int) (*RollingWriter, error)
func (w *RollingWriter) Write(p []byte) (int, error)
func (w *RollingWriter) Close() error
```

`RollingWriter` 实现 `io.Writer`，可直接传给 `slog.NewTextHandler`。

### 2. 写入流程

`Write` 内部：

1. 加锁。
2. 确保文件已打开。
3. 判断是否需要 rotate：
   - 当前日期不同于 `currentDay`；
   - 或 `size + len(p) > maxSizeBytes`。
4. 如需 rotate：
   - 关闭当前文件；
   - 将当前文件压缩为 `.gz` 历史文件；
   - 删除原始未压缩历史文件；
   - 重新打开活跃文件；
   - 清理超出 `maxBackups` 的旧 `.gz` 文件。
5. 写入当前活跃文件。
6. 更新 `size`。

### 3. 压缩流程

使用标准库：

- `compress/gzip`
- `io`
- `os`

压缩函数：

```go
func compressFile(srcPath, dstPath string) error
```

流程：

1. 打开源文件。
2. 创建目标 `.gz` 文件。
3. `gzip.NewWriter`。
4. `io.Copy`。
5. close gzip writer 和文件。
6. 删除源文件。

为了避免覆盖，历史文件名使用时间戳到秒。如果同一秒发生多次 rotate，追加序号：

```text
session-watcher.log.20260501-120000.gz
session-watcher.log.20260501-120000.1.gz
```

### 4. 日期滚动

记录 `currentDay`：

```go
time.Now().Format("20060102")
```

如果写入时当天值变化，则 rotate。

注意：日期按本地时区处理，符合本地命令行工具预期。

### 5. 大小滚动

每次写入前检查：

```go
if size > 0 && size + int64(len(p)) > maxSizeBytes { rotate }
```

如果单条日志本身超过 `maxSizeBytes`，允许写入当前新文件，不拆分单条日志。

### 6. 历史日志清理

扫描日志目录中匹配：

```text
basename + ".*.gz"
```

按修改时间或文件名排序，保留最新 10 个，删除更旧文件。

### 7. main 接入

当前 `setupLogOutput` 位于 `cmd/session-watcher/main.go`。

改为：

```go
func setupLogOutput(path string) (io.Writer, func(), error) {
    if path == "" { return os.Stderr, func(){}, nil }
    writer, err := logging.NewRollingWriter(path, defaultLogMaxSizeBytes, defaultLogMaxBackups)
    if err != nil { return nil, nil, err }
    return io.MultiWriter(os.Stderr, writer), func(){ _ = writer.Close() }, nil
}
```

默认仍然写入 `./data/session-watcher.log`，并自动滚动压缩。

### 8. 错误处理

- 初始打开日志文件失败：启动失败。
- rotate 压缩失败：本次写入返回错误，slog 会丢弃该条或向 handler 返回错误；由于 slog handler 不会中断主流程，后续写入仍会再次尝试。
- 清理旧归档失败：不影响当前写入，尽量返回或记录不可行；由于 writer 自身不应递归使用 logger，建议清理失败直接忽略。

### 9. 测试计划

新增测试文件：

- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/logging/rolling_writer_test.go`

测试内容：

- 正常写入会创建活跃日志文件。
- 文件超过小尺寸阈值后触发 rotate。
- rotate 后历史文件为 `.gz`。
- `.gz` 解压后包含历史日志内容。
- 超过 `maxBackups` 后只保留指定数量的 `.gz`。
- `Close` 后文件句柄关闭。

main 层测试可不新增，端到端验证通过 `go run` 检查日志文件生成即可。

## 受影响文件

- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/logging/rolling_writer.go`
  - 新增 rolling writer。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/internal/logging/rolling_writer_test.go`
  - 新增单元测试。
- `/Users/siegward/Developer/baidu/easydata/session_watcher/cmd/session-watcher/main.go`
  - `setupLogOutput` 改为使用 rolling writer。

## 预期结果

完成后：

- 当前活跃日志仍写入 `./data/session-watcher.log`。
- 日志超过 100MB 或跨天后自动滚动。
- 历史日志以 `.gz` 压缩文件保存。
- 默认最多保留 10 个历史压缩日志。
- 不新增任何启动参数。
- stderr 日志输出保持不变。
