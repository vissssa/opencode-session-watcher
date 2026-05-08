package config

import (
	"testing"
	"time"
)

// testPGDSNArgs 返回包含有效 pg-dsn 参数的切片，确保测试中 Validate 不会因缺少 DSN 而失败。
func testPGDSNArgs(extra ...string) []string {
	return append([]string{"-pg-dsn", "postgres://user:pass@localhost:5432/testdb"}, extra...)
}

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse(testPGDSNArgs())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BaseURL != DefaultBaseURL {
		t.Fatalf("BaseURL = %q", cfg.BaseURL)
	}
	if cfg.MessageLimit != DefaultMessageLimit {
		t.Fatalf("MessageLimit = %d", cfg.MessageLimit)
	}
	if cfg.MaxMessageFetch != DefaultMaxMessageFetch {
		t.Fatalf("MaxMessageFetch = %d", cfg.MaxMessageFetch)
	}
	if cfg.OutputDir != DefaultOutputDir {
		t.Fatalf("OutputDir = %q", cfg.OutputDir)
	}
	if cfg.LogFile != DefaultLogFile {
		t.Fatalf("LogFile = %q", cfg.LogFile)
	}
	if cfg.HealthAddr != DefaultHealthAddr {
		t.Fatalf("HealthAddr = %q", cfg.HealthAddr)
	}
}

func TestParseNormalizesBaseURL(t *testing.T) {
	cfg, err := Parse(testPGDSNArgs("-base-url", "http://localhost:57811///"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BaseURL != "http://localhost:57811" {
		t.Fatalf("BaseURL = %q", cfg.BaseURL)
	}
}

func TestParseOutputDir(t *testing.T) {
	cfg, err := Parse(testPGDSNArgs("-output-dir", "./custom/messages"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OutputDir != "./custom/messages" {
		t.Fatalf("OutputDir = %q", cfg.OutputDir)
	}
}

func TestParseLogFileCanBeDisabled(t *testing.T) {
	cfg, err := Parse(testPGDSNArgs("-log-file", ""))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LogFile != "" {
		t.Fatalf("LogFile = %q", cfg.LogFile)
	}
}

func TestParseHealthAddrCanBeDisabled(t *testing.T) {
	cfg, err := Parse(testPGDSNArgs("-health-addr", ""))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HealthAddr != "" {
		t.Fatalf("HealthAddr = %q", cfg.HealthAddr)
	}
}

func TestParseRejectsInvalidValues(t *testing.T) {
	tests := [][]string{
		testPGDSNArgs("-interval", "0s"),
		testPGDSNArgs("-message-limit", "0"),
		testPGDSNArgs("-max-message-fetch", "0"),
		testPGDSNArgs("-message-limit", "1001", "-max-message-fetch", "1000"),
		testPGDSNArgs("-session-workers", "0"),
		testPGDSNArgs("-base-url", ""),
		testPGDSNArgs("-output-dir", ""),
		{"-pg-dsn", ""},  // 空 DSN 也应报错
	}
	for _, tt := range tests {
		if _, err := Parse(tt); err == nil {
			t.Fatalf("Parse(%v) expected error", tt)
		}
	}
}

func TestConfig_LeaseFlags(t *testing.T) {
	cfg, err := Parse(testPGDSNArgs(
		"-lease-path", "/data/leader.lease",
		"-lease-id", "pod-a:1234",
		"-lease-timeout", "45s",
		"-lease-renew-interval", "15s",
		"-lease-poll-interval", "8s",
	))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LeasePath != "/data/leader.lease" {
		t.Errorf("LeasePath = %q, want /data/leader.lease", cfg.LeasePath)
	}
	if cfg.LeaseID != "pod-a:1234" {
		t.Errorf("LeaseID = %q, want pod-a:1234", cfg.LeaseID)
	}
	if cfg.LeaseTimeout != 45*time.Second {
		t.Errorf("LeaseTimeout = %v, want 45s", cfg.LeaseTimeout)
	}
	if cfg.LeaseRenewInterval != 15*time.Second {
		t.Errorf("LeaseRenewInterval = %v, want 15s", cfg.LeaseRenewInterval)
	}
	if cfg.LeasePollInterval != 8*time.Second {
		t.Errorf("LeasePollInterval = %v, want 8s", cfg.LeasePollInterval)
	}
}

func TestConfig_NoLeaseIsValid(t *testing.T) {
	_, err := Parse(testPGDSNArgs())
	if err != nil {
		t.Fatalf("default config (no lease) should be valid: %v", err)
	}
}

func TestConfig_LeaseValidation_RenewIntervalTooLarge(t *testing.T) {
	_, err := Parse(testPGDSNArgs(
		"-lease-path", "/tmp/test.lease",
		"-lease-timeout", "10s",
		"-lease-renew-interval", "15s", // 大于 timeout，应报错
	))
	if err == nil {
		t.Fatal("expected error when lease-renew-interval >= lease-timeout")
	}
}

func TestConfig_EnvVarOverridesDefaults(t *testing.T) {
	t.Setenv("PG_DSN", "postgres://env@localhost/testdb")
	t.Setenv("BASE_URL", "http://env-host:9999")
	t.Setenv("INTERVAL", "30s")
	t.Setenv("MESSAGE_LIMIT", "200")
	t.Setenv("MAX_MESSAGE_FETCH", "2000")
	t.Setenv("SESSION_WORKERS", "16")
	t.Setenv("OUTPUT_DIR", "/tmp/env-output")
	t.Setenv("TIMEOUT", "20s")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FILE", "/tmp/env.log")
	t.Setenv("HEALTH_ADDR", "0.0.0.0:8080")

	// 不传任何 CLI flag，全部依赖环境变量
	cfg, err := Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PGDSN != "postgres://env@localhost/testdb" {
		t.Fatalf("PGDSN = %q", cfg.PGDSN)
	}
	if cfg.BaseURL != "http://env-host:9999" {
		t.Fatalf("BaseURL = %q", cfg.BaseURL)
	}
	if cfg.Interval != 30*time.Second {
		t.Fatalf("Interval = %v", cfg.Interval)
	}
	if cfg.MessageLimit != 200 {
		t.Fatalf("MessageLimit = %d", cfg.MessageLimit)
	}
	if cfg.MaxMessageFetch != 2000 {
		t.Fatalf("MaxMessageFetch = %d", cfg.MaxMessageFetch)
	}
	if cfg.SessionWorkers != 16 {
		t.Fatalf("SessionWorkers = %d", cfg.SessionWorkers)
	}
	if cfg.OutputDir != "/tmp/env-output" {
		t.Fatalf("OutputDir = %q", cfg.OutputDir)
	}
	if cfg.Timeout != 20*time.Second {
		t.Fatalf("Timeout = %v", cfg.Timeout)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("LogLevel = %q", cfg.LogLevel)
	}
	if cfg.LogFile != "/tmp/env.log" {
		t.Fatalf("LogFile = %q", cfg.LogFile)
	}
	if cfg.HealthAddr != "0.0.0.0:8080" {
		t.Fatalf("HealthAddr = %q", cfg.HealthAddr)
	}
}

func TestConfig_CLIFlagOverridesEnvVar(t *testing.T) {
	t.Setenv("PG_DSN", "postgres://env@localhost/testdb")
	t.Setenv("BASE_URL", "http://env-host:9999")

	// CLI flag 显式传参应覆盖环境变量默认值
	cfg, err := Parse([]string{"-pg-dsn", "postgres://cli@localhost/clidb", "-base-url", "http://cli-host:1234"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PGDSN != "postgres://cli@localhost/clidb" {
		t.Fatalf("PGDSN = %q, want cli value", cfg.PGDSN)
	}
	if cfg.BaseURL != "http://cli-host:1234" {
		t.Fatalf("BaseURL = %q, want cli value", cfg.BaseURL)
	}
}

func TestConfig_EnvVarLeaseFields(t *testing.T) {
	t.Setenv("PG_DSN", "postgres://user@localhost/db")
	t.Setenv("LEASE_PATH", "/shared/leader.lease")
	t.Setenv("LEASE_ID", "node-1")
	t.Setenv("LEASE_TIMEOUT", "60s")
	t.Setenv("LEASE_RENEW_INTERVAL", "20s")
	t.Setenv("LEASE_POLL_INTERVAL", "10s")

	cfg, err := Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LeasePath != "/shared/leader.lease" {
		t.Fatalf("LeasePath = %q", cfg.LeasePath)
	}
	if cfg.LeaseID != "node-1" {
		t.Fatalf("LeaseID = %q", cfg.LeaseID)
	}
	if cfg.LeaseTimeout != 60*time.Second {
		t.Fatalf("LeaseTimeout = %v", cfg.LeaseTimeout)
	}
	if cfg.LeaseRenewInterval != 20*time.Second {
		t.Fatalf("LeaseRenewInterval = %v", cfg.LeaseRenewInterval)
	}
	if cfg.LeasePollInterval != 10*time.Second {
		t.Fatalf("LeasePollInterval = %v", cfg.LeasePollInterval)
	}
}
