package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"session_watcher/internal/config"
	"session_watcher/internal/health"
	"session_watcher/internal/lease"
	jsonlsink "session_watcher/internal/sink/jsonl"
	"session_watcher/internal/source/opencode"
	"session_watcher/internal/status"
	"session_watcher/internal/store"
	"session_watcher/internal/watcher"

	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {
	// 自动加载 .env 文件中的环境变量（文件不存在时静默跳过）
	loadEnvFile(".env")

	// 解析命令行参数，校验失败时以退出码 2 退出（与 flag 包约定一致）
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse config: %v\n", err)
		os.Exit(2)
	}

	level, err := config.LogLevel(cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse log level: %v\n", err)
		os.Exit(2)
	}
	logOutput, closeLog, err := setupLogOutput(cfg.LogFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open log file: %v\n", err)
		os.Exit(1)
	}
	defer closeLog()
	// 初始化结构化日志，并设为全局默认 logger
	logger := slog.New(slog.NewTextHandler(logOutput, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)
	logger.Info("session watcher starting",
		"base_url", cfg.BaseURL,
		"interval", cfg.Interval.String(),
		"message_limit", cfg.MessageLimit,
		"max_message_fetch", cfg.MaxMessageFetch,
		"session_workers", cfg.SessionWorkers,
		"pg_dsn", maskDSN(cfg.PGDSN),
		"output_dir", cfg.OutputDir,
		"once", cfg.Once,
		"timeout", cfg.Timeout.String(),
		"log_level", cfg.LogLevel,
		"log_file", cfg.LogFile,
		"health_addr", cfg.HealthAddr,
	)

	// 注册 SIGINT / SIGTERM 信号，ctx 取消会传播到所有下游组件
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 启动 status HTTP 服务（可选，addr 为空时不启动）
	reporter := status.NewReporter()
	healthServer, err := health.Start(cfg.HealthAddr, reporter, logger)
	if err != nil {
		logger.Error("start health server failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := healthServer.Close(shutdownCtx); err != nil {
			logger.Warn("close health server failed", "error", err)
		}
	}()
	if healthServer != nil {
		logger.Info("health server listening", "addr", healthServer.Addr())
	}

	// 构建 leaseID：优先使用配置值，否则自动生成 hostname:pid
	leaseID := cfg.LeaseID
	if leaseID == "" {
		hostname, _ := os.Hostname()
		leaseID = fmt.Sprintf("%s:%d", hostname, os.Getpid())
	}

	if cfg.LeasePath == "" {
		// 单实例模式：lease 未启用，直接运行 watcher（向后兼容）
		reporter.SetMode("standalone")
		logger.Info("lease disabled, running in standalone mode")
		if err := runWatcher(ctx, cfg, reporter, logger); err != nil && cfg.Once {
			// -once 模式下同步失败以非零码退出，方便 CI 感知
			os.Exit(1)
		}
	} else {
		// HA 模式：通过 lease 文件选举，只有 Leader 才运行 watcher
		reporter.SetMode("ha")
		logger.Info("lease enabled, entering HA mode",
			"lease_path", cfg.LeasePath,
			"lease_id", leaseID,
			"lease_timeout", cfg.LeaseTimeout,
			"renew_interval", cfg.LeaseRenewInterval,
			"poll_interval", cfg.LeasePollInterval,
		)
		leaseMgr := lease.New(cfg.LeasePath, leaseID, lease.Config{
			LeaseTimeout:  cfg.LeaseTimeout,
			RenewInterval: cfg.LeaseRenewInterval,
			PollInterval:  cfg.LeasePollInterval,
		}, logger)
		leaseMgr.Run(ctx, func(leaderCtx context.Context) {
			reporter.SetLeaderState(true, leaseID)
			defer reporter.SetLeaderState(false, leaseID)
			if err := runWatcher(leaderCtx, cfg, reporter, logger); err != nil && cfg.Once {
				// -once 模式下同步失败以非零码退出，方便 CI 感知
				stop()
				os.Exit(1)
			}
			// -once 模式下 runWatcher 完成即退出整个进程；
			// 取消 ctx 使 lease.Run 循环感知到退出信号，避免进程永久阻塞。
			if cfg.Once {
				stop()
			}
		})
	}

	logger.Info("session watcher exited")
}

// runWatcher 初始化并运行 watcher 核心逻辑（PostgreSQL 存储、JSONL Sink、HTTP Source）。
// 单次模式（-once）完成后返回，同步失败时返回非 nil error；
// 持续轮询模式在 ctx 取消后返回 nil。
// HA 模式下由 lease Leader 回调调用；单实例模式下由 main 直接调用。
func runWatcher(ctx context.Context, cfg config.Config, reporter *status.Reporter, logger *slog.Logger) error {
	// 连接 PostgreSQL 状态存储：
	// -once 模式使用临时 schema（与正式数据隔离，完成后自动清理），适合调试/CI
	// 持续模式使用正式表，保留同步状态用于增量去重
	var stateStore *store.Store
	var err error
	if cfg.Once {
		stateStore, err = store.OpenTemp(ctx, cfg.PGDSN)
		if err != nil {
			logger.Error("open temp postgres store failed", "error", err)
			return err
		}
		logger.Info("using temporary schema for once mode (auto-cleanup on exit)")
	} else {
		stateStore, err = store.Open(ctx, cfg.PGDSN)
		if err != nil {
			logger.Error("open postgres store failed", "error", err)
			return err
		}
	}
	defer func() {
		if err := stateStore.Close(); err != nil {
			logger.Warn("close postgres store failed", "error", err)
		}
	}()

	// 初始化 JSONL Sink
	sink, err := jsonlsink.NewFileSink(cfg.OutputDir, logger)
	if err != nil {
		logger.Error("open jsonl sink failed", "error", err)
		return err
	}
	defer func() {
		if err := sink.Close(); err != nil {
			logger.Warn("close sink failed", "error", err)
		}
	}()

	source := opencode.NewHTTPSource(cfg.BaseURL, cfg.Timeout, logger)
	w := watcher.New(source, sink, stateStore, watcher.Config{MessageLimit: cfg.MessageLimit, MaxMessageFetch: cfg.MaxMessageFetch, SessionWorkers: cfg.SessionWorkers}, logger)

	// -once 模式：执行单轮同步后退出，主要用于调试和 CI 验证
	if cfg.Once {
		started := time.Now()
		result, err := w.SyncOnce(ctx)
		reporter.RecordRound(status.RoundUpdate{
			SessionsTotal:   result.SessionsTotal,
			SessionsSynced:  result.SessionsSynced,
			SessionsFailed:  result.SessionsFailed,
			MessagesNew:     result.MessagesNew,
			MaxFetchReached: result.MaxFetchReached,
			Duration:        time.Since(started),
			Err:             err,
		})
		if err != nil {
			logger.Error("sync once failed", "error", err)
			return err
		}
		return nil
	}

	// 持续轮询模式：每轮 SyncOnce 后等待 interval，收到退出信号时优雅退出
	for {
		started := time.Now()
		result, err := w.SyncOnce(ctx)
		reporter.RecordRound(status.RoundUpdate{
			SessionsTotal:   result.SessionsTotal,
			SessionsSynced:  result.SessionsSynced,
			SessionsFailed:  result.SessionsFailed,
			MessagesNew:     result.MessagesNew,
			MaxFetchReached: result.MaxFetchReached,
			Duration:        time.Since(started),
			Err:             err,
		})
		// ctx 已取消时不打印错误（属于正常退出），否则记录错误日志
		if err != nil && ctx.Err() == nil {
			logger.Error("sync round failed", "error", err)
		}
		select {
		case <-ctx.Done():
			logger.Info("session watcher received shutdown signal")
			return nil
		case <-timeAfter(cfg.Interval):
		}
	}
}

// setupLogOutput 根据 path 配置日志输出目标。
// path 为空时只写 stderr；非空时同时写 stderr 和 lumberjack 文件（自动轮转）。
// 返回 Writer、关闭函数和错误。
func setupLogOutput(path string) (io.Writer, func(), error) {
	if path == "" {
		return os.Stderr, func() {}, nil
	}
	// 最多保留 10 个历史日志文件，单文件上限 100MB，压缩归档
	logger := &lumberjack.Logger{
		Filename:   path,
		MaxSize:    100,
		MaxBackups: 10,
		Compress:   true,
	}
	return io.MultiWriter(os.Stderr, logger), func() { _ = logger.Close() }, nil
}

// timeAfter 是对 time.After 的可替换封装，便于测试时注入 fake 时钟。
var timeAfter = func(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// maskDSN 隐藏 DSN 中的密码部分，用于日志输出。
// 支持 keyword/value 格式（password='xxx'）和 URL 格式（user:pass@host）。
func maskDSN(dsn string) string {
	// keyword/value 格式：password='xxx' 或 password=xxx
	if idx := strings.Index(dsn, "password="); idx >= 0 {
		end := idx + len("password=")
		// 跳过可能的引号
		if end < len(dsn) && dsn[end] == '\'' {
			closeQuote := strings.Index(dsn[end+1:], "'")
			if closeQuote >= 0 {
				return dsn[:end] + "'***'" + dsn[end+1+closeQuote+1:]
			}
		}
		// 无引号，找下一个空格
		spaceIdx := strings.Index(dsn[end:], " ")
		if spaceIdx >= 0 {
			return dsn[:end] + "***" + dsn[end+spaceIdx:]
		}
		return dsn[:end] + "***"
	}
	// URL 格式：scheme://user:pass@host
	if idx := strings.Index(dsn, "://"); idx >= 0 {
		rest := dsn[idx+3:]
		atIdx := strings.Index(rest, "@")
		if atIdx >= 0 {
			colonIdx := strings.Index(rest[:atIdx], ":")
			if colonIdx >= 0 {
				return dsn[:idx+3] + rest[:colonIdx+1] + "***" + "@" + rest[atIdx+1:]
			}
		}
	}
	return dsn
}

// loadEnvFile 读取指定的 .env 文件并将其中的键值对设置为环境变量。
// 仅设置当前进程中尚未定义的变量（不覆盖已有环境变量）。
// 文件不存在时静默跳过，不影响程序启动。
// 支持格式：KEY=VALUE、KEY="VALUE"、KEY='VALUE'、export KEY=VALUE，忽略注释和空行。
func loadEnvFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return // 文件不存在或无权限，静默跳过
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// 跳过空行和注释
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// 去掉 export 前缀
		line = strings.TrimPrefix(line, "export ")
		line = strings.TrimSpace(line)

		// 按第一个 = 分割
		eqIdx := strings.Index(line, "=")
		if eqIdx <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:eqIdx])
		value := strings.TrimSpace(line[eqIdx+1:])

		// 去掉值两端的引号
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') ||
				(value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}

		// 不覆盖已有的环境变量
		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, value)
		}
	}
}
