package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
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
		"db", cfg.DBPath,
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

// runWatcher 初始化并运行 watcher 核心逻辑（SQLite 存储、JSONL Sink、HTTP Source）。
// 单次模式（-once）完成后返回，同步失败时返回非 nil error；
// 持续轮询模式在 ctx 取消后返回 nil。
// HA 模式下由 lease Leader 回调调用；单实例模式下由 main 直接调用。
func runWatcher(ctx context.Context, cfg config.Config, reporter *status.Reporter, logger *slog.Logger) error {
	// 打开 SQLite 状态存储，失败时无法保证增量去重，直接返回
	stateStore, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		logger.Error("open sqlite store failed", "error", err)
		return err
	}
	defer func() {
		if err := stateStore.Close(); err != nil {
			logger.Warn("close sqlite store failed", "error", err)
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
