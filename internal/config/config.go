package config

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// 各配置项的默认值。
const (
	DefaultBaseURL         = "http://localhost:57811"
	DefaultInterval        = 10 * time.Second
	DefaultMessageLimit    = 100
	DefaultMaxMessageFetch = 1000
	DefaultSessionWorkers  = 8
	DefaultDBPath          = "./data/state.db"
	DefaultOutputDir       = "./data/messages"
	DefaultTimeout         = 10 * time.Second
	DefaultLogLevel        = "info"
	DefaultLogFile         = "./data/session-watcher.log"
	DefaultHealthAddr      = "127.0.0.1:0"
)

// Config 保存所有命令行配置项。
type Config struct {
	BaseURL         string
	Interval        time.Duration
	MessageLimit    int           // 每次 limit 扩展的步长
	MaxMessageFetch int           // 单 Session 每轮最多拉取的消息条数上限
	SessionWorkers  int           // 并发 Worker 数
	DBPath          string
	OutputDir       string
	Once            bool          // 是否只执行一轮同步后退出
	Timeout         time.Duration // HTTP 请求超时
	LogLevel        string
	LogFile         string
	HealthAddr      string

	// Lease 相关配置（-lease-path 为空时禁用 Leader 选举，退化为单实例模式）
	LeasePath          string        // GlusterFS 上的 lease 文件路径，空=单实例模式
	LeaseID            string        // 本实例唯一标识，空时在 main.go 中自动生成（hostname:pid）
	LeaseTimeout       time.Duration // Leader 超时时长，默认 30s
	LeaseRenewInterval time.Duration // Leader 续约间隔，默认 10s
	LeasePollInterval  time.Duration // Standby 轮询间隔，默认 5s
}

// Parse 解析命令行参数并返回校验后的 Config。
// 会对 BaseURL、DBPath 等字符串字段做 trim/normalize 处理。
func Parse(args []string) (Config, error) {
	cfg := Config{}
	fs := flag.NewFlagSet("session-watcher", flag.ContinueOnError)
	fs.StringVar(&cfg.BaseURL, "base-url", DefaultBaseURL, "open-code service base URL")
	fs.DurationVar(&cfg.Interval, "interval", DefaultInterval, "poll interval")
	fs.IntVar(&cfg.MessageLimit, "message-limit", DefaultMessageLimit, "message fetch limit growth step")
	fs.IntVar(&cfg.MaxMessageFetch, "max-message-fetch", DefaultMaxMessageFetch, "max messages fetched per session per round")
	fs.IntVar(&cfg.SessionWorkers, "session-workers", DefaultSessionWorkers, "max concurrent session workers")
	fs.StringVar(&cfg.DBPath, "db", DefaultDBPath, "sqlite state database path")
	fs.StringVar(&cfg.OutputDir, "output-dir", DefaultOutputDir, "jsonl output root directory")
	fs.BoolVar(&cfg.Once, "once", false, "run one sync round and exit")
	fs.DurationVar(&cfg.Timeout, "timeout", DefaultTimeout, "HTTP request timeout")
	fs.StringVar(&cfg.LogLevel, "log-level", DefaultLogLevel, "log level: debug, info, warn, error")
	fs.StringVar(&cfg.LogFile, "log-file", DefaultLogFile, "log file path, empty disables file logging")
	fs.StringVar(&cfg.HealthAddr, "health-addr", DefaultHealthAddr, "health/status listen address, empty disables health server")
	fs.StringVar(&cfg.LeasePath, "lease-path", "", "leader lease file path on shared fs (empty disables HA mode)")
	fs.StringVar(&cfg.LeaseID, "lease-id", "", "unique instance id for leader election (default: auto hostname:pid)")
	fs.DurationVar(&cfg.LeaseTimeout, "lease-timeout", 30*time.Second, "leader lease timeout")
	fs.DurationVar(&cfg.LeaseRenewInterval, "lease-renew-interval", 10*time.Second, "leader lease renew interval")
	fs.DurationVar(&cfg.LeasePollInterval, "lease-poll-interval", 5*time.Second, "standby lease poll interval")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	// 统一去掉首尾空白和 BaseURL 末尾的斜杠，确保拼接路径时不重复
	cfg.BaseURL = strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	cfg.DBPath = strings.TrimSpace(cfg.DBPath)
	cfg.OutputDir = strings.TrimSpace(cfg.OutputDir)
	cfg.LogLevel = strings.TrimSpace(cfg.LogLevel)
	cfg.LogFile = strings.TrimSpace(cfg.LogFile)
	cfg.HealthAddr = strings.TrimSpace(cfg.HealthAddr)
	return cfg, cfg.Validate()
}

// Validate 校验配置项的合法性，任何一项不满足条件则返回错误。
func (c Config) Validate() error {
	if c.BaseURL == "" {
		return errors.New("base-url cannot be empty")
	}
	if c.Interval <= 0 {
		return fmt.Errorf("interval must be positive: %s", c.Interval)
	}
	if c.MessageLimit <= 0 {
		return fmt.Errorf("message-limit must be positive: %d", c.MessageLimit)
	}
	if c.MaxMessageFetch <= 0 {
		return fmt.Errorf("max-message-fetch must be positive: %d", c.MaxMessageFetch)
	}
	// MessageLimit 是扩展步长，不能大于上限
	if c.MessageLimit > c.MaxMessageFetch {
		return fmt.Errorf("message-limit (%d) must be less than or equal to max-message-fetch (%d)", c.MessageLimit, c.MaxMessageFetch)
	}
	if c.SessionWorkers <= 0 {
		return fmt.Errorf("session-workers must be positive: %d", c.SessionWorkers)
	}
	if c.DBPath == "" {
		return errors.New("db path cannot be empty")
	}
	if c.OutputDir == "" {
		return errors.New("output-dir cannot be empty")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive: %s", c.Timeout)
	}
	if _, err := LogLevel(c.LogLevel); err != nil {
		return err
	}
	if c.LeasePath != "" {
		if c.LeaseTimeout <= 0 {
			return fmt.Errorf("lease-timeout must be positive: %s", c.LeaseTimeout)
		}
		if c.LeaseRenewInterval <= 0 {
			return fmt.Errorf("lease-renew-interval must be positive: %s", c.LeaseRenewInterval)
		}
		if c.LeaseRenewInterval >= c.LeaseTimeout {
			return fmt.Errorf("lease-renew-interval (%s) must be less than lease-timeout (%s)", c.LeaseRenewInterval, c.LeaseTimeout)
		}
		if c.LeasePollInterval <= 0 {
			return fmt.Errorf("lease-poll-interval must be positive: %s", c.LeasePollInterval)
		}
	}
	return nil
}

// LogLevel 将字符串日志级别转换为 slog.Level。
// 空字符串默认返回 Info 级别。
func LogLevel(level string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug, nil
	case "", "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unsupported log-level %q", level)
	}
}
