package status

import (
	"errors"
	"testing"
)

func TestReporterRecordRound(t *testing.T) {
	reporter := NewReporter()
	reporter.RecordRound(RoundUpdate{SessionsTotal: 3, SessionsSynced: 2, SessionsFailed: 1, MessagesNew: 5, MaxFetchReached: 1})
	snapshot := reporter.Snapshot()
	if snapshot.SessionsTotal != 3 || snapshot.MessagesNew != 5 || snapshot.LastMaxFetchReached != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if snapshot.LastSuccessAt == "" || snapshot.LastError != "" {
		t.Fatalf("success status = %#v", snapshot)
	}
	reporter.RecordRound(RoundUpdate{Err: errors.New("failed")})
	snapshot = reporter.Snapshot()
	if snapshot.LastError != "failed" {
		t.Fatalf("error snapshot = %#v", snapshot)
	}
}

// TestReporterSetMode 验证 SetMode 写入后通过 Snapshot 可读取，且直接反映在 snapshot.Mode。
func TestReporterSetMode(t *testing.T) {
	reporter := NewReporter()

	reporter.SetMode("standalone")
	if got := reporter.Snapshot().Mode; got != "standalone" {
		t.Fatalf("Mode = %q, want %q", got, "standalone")
	}

	reporter.SetMode("ha")
	if got := reporter.Snapshot().Mode; got != "ha" {
		t.Fatalf("Mode = %q, want %q", got, "ha")
	}
}

// TestReporterSetLeaderState 验证 SetLeaderState 写入后通过 Snapshot 可正确读取。
func TestReporterSetLeaderState(t *testing.T) {
	reporter := NewReporter()

	// 初始状态：非 Leader，leaseID 为空
	snap := reporter.Snapshot()
	if snap.IsLeader || snap.LeaseID != "" {
		t.Fatalf("initial state: IsLeader=%v LeaseID=%q, want false/empty", snap.IsLeader, snap.LeaseID)
	}

	// 成为 Leader
	reporter.SetLeaderState(true, "host1:1234")
	snap = reporter.Snapshot()
	if !snap.IsLeader || snap.LeaseID != "host1:1234" {
		t.Fatalf("after SetLeaderState(true): IsLeader=%v LeaseID=%q", snap.IsLeader, snap.LeaseID)
	}

	// 失去 Leader
	reporter.SetLeaderState(false, "host1:1234")
	snap = reporter.Snapshot()
	if snap.IsLeader {
		t.Fatalf("after SetLeaderState(false): IsLeader should be false, got %v", snap.IsLeader)
	}
}

// TestReporterSetMode_StandaloneImpliesLeader 验证 standalone 模式下 is_leader 自动为 true，
// 避免监控系统误判单实例节点为"未工作"。
func TestReporterSetMode_StandaloneImpliesLeader(t *testing.T) {
	reporter := NewReporter()
	reporter.SetMode("standalone")
	snap := reporter.Snapshot()
	if !snap.IsLeader {
		t.Fatalf("standalone mode should imply is_leader=true, got false")
	}
}

// TestReporterSetMode_HaModeDoesNotImplyLeader 验证 ha 模式下 is_leader 不由 SetMode 决定。
func TestReporterSetMode_HaModeDoesNotImplyLeader(t *testing.T) {
	reporter := NewReporter()
	reporter.SetMode("ha")
	snap := reporter.Snapshot()
	if snap.IsLeader {
		t.Fatalf("ha mode should not auto-set is_leader=true, got true")
	}
}

// Mode/IsLeader/LeaseID 与业务数据来自同一把锁保护下的同一时刻读取。
func TestReporterSnapshotConsistency(t *testing.T) {
	reporter := NewReporter()
	reporter.SetMode("ha")
	reporter.SetLeaderState(true, "host:99")
	reporter.RecordRound(RoundUpdate{SessionsTotal: 5, MessagesNew: 10})

	snap := reporter.Snapshot()
	if snap.Mode != "ha" {
		t.Errorf("Mode = %q, want ha", snap.Mode)
	}
	if !snap.IsLeader {
		t.Errorf("IsLeader = false, want true")
	}
	if snap.LeaseID != "host:99" {
		t.Errorf("LeaseID = %q, want host:99", snap.LeaseID)
	}
	if snap.SessionsTotal != 5 || snap.MessagesNew != 10 {
		t.Errorf("business fields: %+v", snap)
	}
}
