package config

import (
	"testing"
	"time"
)

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse(nil)
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
	cfg, err := Parse([]string{"-base-url", "http://localhost:57811///"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BaseURL != "http://localhost:57811" {
		t.Fatalf("BaseURL = %q", cfg.BaseURL)
	}
}

func TestParseOutputDir(t *testing.T) {
	cfg, err := Parse([]string{"-output-dir", "./custom/messages"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OutputDir != "./custom/messages" {
		t.Fatalf("OutputDir = %q", cfg.OutputDir)
	}
}

func TestParseLogFileCanBeDisabled(t *testing.T) {
	cfg, err := Parse([]string{"-log-file", ""})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LogFile != "" {
		t.Fatalf("LogFile = %q", cfg.LogFile)
	}
}

func TestParseHealthAddrCanBeDisabled(t *testing.T) {
	cfg, err := Parse([]string{"-health-addr", ""})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HealthAddr != "" {
		t.Fatalf("HealthAddr = %q", cfg.HealthAddr)
	}
}

func TestParseRejectsInvalidValues(t *testing.T) {
	tests := [][]string{
		{"-interval", "0s"},
		{"-message-limit", "0"},
		{"-max-message-fetch", "0"},
		{"-message-limit", "1001", "-max-message-fetch", "1000"},
		{"-session-workers", "0"},
		{"-base-url", ""},
		{"-output-dir", ""},
	}
	for _, tt := range tests {
		if _, err := Parse(tt); err == nil {
			t.Fatalf("Parse(%v) expected error", tt)
		}
	}
}

func TestConfig_LeaseFlags(t *testing.T) {
	cfg, err := Parse([]string{
		"-lease-path", "/data/leader.lease",
		"-lease-id", "pod-a:1234",
		"-lease-timeout", "45s",
		"-lease-renew-interval", "15s",
		"-lease-poll-interval", "8s",
	})
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
	_, err := Parse([]string{})
	if err != nil {
		t.Fatalf("default config (no lease) should be valid: %v", err)
	}
}

func TestConfig_LeaseValidation_RenewIntervalTooLarge(t *testing.T) {
	_, err := Parse([]string{
		"-lease-path", "/tmp/test.lease",
		"-lease-timeout", "10s",
		"-lease-renew-interval", "15s", // 大于 timeout，应报错
	})
	if err == nil {
		t.Fatal("expected error when lease-renew-interval >= lease-timeout")
	}
}
