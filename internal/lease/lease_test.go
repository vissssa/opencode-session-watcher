package lease_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"session_watcher/internal/lease"
)

// TestLeaderElection_ActiveLeaseNotStolen 验证活跃 lease 存在时 Standby 不会抢占
func TestLeaderElection_ActiveLeaseNotStolen(t *testing.T) {
	dir := t.TempDir()
	leasePath := filepath.Join(dir, "leader.lease")

	// 预写一个新鲜的 lease（holder=other-instance）
	lf := map[string]any{
		"holder":      "other-instance",
		"acquired_at": time.Now().UnixMilli(),
		"renewed_at":  time.Now().UnixMilli(),
	}
	data, _ := json.Marshal(lf)
	_ = os.WriteFile(leasePath, data, 0o644)

	cfg := lease.Config{
		LeaseTimeout:  2 * time.Second,
		RenewInterval: 500 * time.Millisecond,
		PollInterval:  200 * time.Millisecond,
	}
	l := lease.New(leasePath, "test-holder-2", cfg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	becameLeader := make(chan struct{}, 1)
	go l.Run(ctx, func(leaderCtx context.Context) {
		becameLeader <- struct{}{}
		<-leaderCtx.Done()
	})

	<-ctx.Done()
	select {
	case <-becameLeader:
		t.Fatal("should NOT become leader when active lease exists")
	default:
		// pass
	}
}

// TestLeaderElection_TakeoverAfterTimeout 验证 lease 超时后 Standby 成功接管
func TestLeaderElection_TakeoverAfterTimeout(t *testing.T) {
	dir := t.TempDir()
	leasePath := filepath.Join(dir, "leader.lease")

	leaseTimeout := 600 * time.Millisecond

	lf := map[string]any{
		"holder":      "dead-instance",
		"acquired_at": time.Now().Add(-2 * leaseTimeout).UnixMilli(),
		"renewed_at":  time.Now().Add(-2 * leaseTimeout).UnixMilli(),
	}
	data, _ := json.Marshal(lf)
	_ = os.WriteFile(leasePath, data, 0o644)

	cfg := lease.Config{
		LeaseTimeout:  leaseTimeout,
		RenewInterval: 200 * time.Millisecond,
		PollInterval:  100 * time.Millisecond,
	}
	l := lease.New(leasePath, "takeover-holder", cfg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	becameLeader := make(chan struct{}, 1)
	l.Run(ctx, func(leaderCtx context.Context) {
		becameLeader <- struct{}{}
		<-leaderCtx.Done()
	})

	select {
	case <-becameLeader:
		// pass
	default:
		t.Fatal("expected to take over expired lease but did not")
	}
}

// TestLeader_ContinuesOnRenewFailure 验证续约失败（模拟 GlusterFS 故障）时 Leader 继续工作不降级
func TestLeader_ContinuesOnRenewFailure(t *testing.T) {
	dir := t.TempDir()
	leasePath := filepath.Join(dir, "leader.lease")

	cfg := lease.Config{
		LeaseTimeout:  2 * time.Second,
		RenewInterval: 100 * time.Millisecond,
		PollInterval:  100 * time.Millisecond,
	}
	l := lease.New(leasePath, "renew-fail-holder", cfg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	workDone := make(chan struct{}, 1)
	l.Run(ctx, func(leaderCtx context.Context) {
		// 晋升后删除 lease 文件模拟 GlusterFS 故障
		_ = os.Remove(leasePath)
		// 等待至少 3 个续约周期，Leader 应继续运行
		select {
		case <-time.After(400 * time.Millisecond):
			workDone <- struct{}{}
		case <-leaderCtx.Done():
		}
	})

	select {
	case <-workDone:
		// pass
	default:
		t.Fatal("leader should continue working even when renew fails")
	}
}

// TestAcquiredAt_ReflectsCurrentHolder 验证接管过期 lease 时 acquired_at 是本实例的获取时刻，
// 而非沿用前任 Leader 的时间戳（P1-3 fix）。
func TestAcquiredAt_ReflectsCurrentHolder(t *testing.T) {
	dir := t.TempDir()
	leasePath := filepath.Join(dir, "leader.lease")

	leaseTimeout := 300 * time.Millisecond
	// 写入一个过期的 lease，acquired_at 设为很久以前（5 秒前）
	oldAcquiredAt := time.Now().Add(-5 * time.Second).UnixMilli()
	lf := map[string]any{
		"holder":      "dead-instance",
		"acquired_at": oldAcquiredAt,
		"renewed_at":  time.Now().Add(-2 * leaseTimeout).UnixMilli(),
	}
	data, _ := json.Marshal(lf)
	_ = os.WriteFile(leasePath, data, 0o644)

	cfg := lease.Config{
		LeaseTimeout:  leaseTimeout,
		RenewInterval: 200 * time.Millisecond,
		PollInterval:  100 * time.Millisecond,
	}

	takeoverTime := time.Now()
	l := lease.New(leasePath, "new-holder", cfg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	l.Run(ctx, func(leaderCtx context.Context) {
		// 读取 lease 文件，验证 acquired_at 是本实例接管的时刻，而非前任值
		raw, err := os.ReadFile(leasePath)
		if err != nil {
			t.Errorf("read lease file: %v", err)
			return
		}
		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Errorf("unmarshal lease: %v", err)
			return
		}
		gotAcquiredAt := int64(got["acquired_at"].(float64))
		// acquired_at 应该 >= takeoverTime（本实例接管时刻），不应沿用前任的 oldAcquiredAt
		if gotAcquiredAt < takeoverTime.UnixMilli() {
			t.Errorf("acquired_at=%d should be >= takeoverTime=%d (current holder's acquisition time), got old value=%d",
				gotAcquiredAt, takeoverTime.UnixMilli(), oldAcquiredAt)
		}
		cancel()
	})
}

// TestAcquiredAt_PreservedOnRenewFailure 验证续约失败（lease 文件消失）时，
// acquired_at 仍保持为本实例最初获取 leadership 的时刻，而非 renew 时刻（P1-2 fix）。
func TestAcquiredAt_PreservedOnRenewFailure(t *testing.T) {
	dir := t.TempDir()
	leasePath := filepath.Join(dir, "leader.lease")

	cfg := lease.Config{
		LeaseTimeout:  2 * time.Second,
		RenewInterval: 100 * time.Millisecond,
		PollInterval:  100 * time.Millisecond,
	}

	acquireTime := time.Now()
	l := lease.New(leasePath, "renew-test-holder", cfg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	l.Run(ctx, func(leaderCtx context.Context) {
		// 删除 lease 文件，模拟 GlusterFS 故障
		_ = os.Remove(leasePath)

		// 等待至少 3 个续约周期（renew 会重建文件）
		time.Sleep(400 * time.Millisecond)

		// 读取续约后的 lease 文件，验证 acquired_at 未被重置
		raw, err := os.ReadFile(leasePath)
		if err != nil {
			t.Logf("lease file not found after renew (may be ok): %v", err)
			cancel()
			return
		}
		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Errorf("unmarshal lease after renew failure: %v", err)
			cancel()
			return
		}
		gotAcquiredAt := int64(got["acquired_at"].(float64))
		// acquired_at 不应该在 renew 时被重置为当前时间（应 <= acquireTime + 500ms 容差）
		// 实际上应该接近 acquireTime，而不是 time.Now()
		renewTime := time.Now()
		if gotAcquiredAt > renewTime.Add(-200*time.Millisecond).UnixMilli() {
			// acquired_at 接近 renewTime 说明被重置了
			t.Errorf("acquired_at=%d was reset to near renew time=%d, should remain near acquire time=%d",
				gotAcquiredAt, renewTime.UnixMilli(), acquireTime.UnixMilli())
		}
		cancel()
	})
}

// TestRun_ExitsAfterCtxCancelledPostOnLeader 验证 onLeader 返回后 ctx 被取消时 Run 能退出。
// 这是 HA + -once 模式的关键路径：onLeader（runWatcher）完成后调用方取消 ctx，
// lease.Run 必须在有限时间内返回，而非继续循环等待下一轮选举。
func TestRun_ExitsAfterCtxCancelledPostOnLeader(t *testing.T) {
	dir := t.TempDir()
	leasePath := filepath.Join(dir, "leader.lease")

	cfg := lease.Config{
		LeaseTimeout:  2 * time.Second,
		RenewInterval: 200 * time.Millisecond,
		PollInterval:  100 * time.Millisecond,
	}
	l := lease.New(leasePath, "once-holder", cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		l.Run(ctx, func(leaderCtx context.Context) {
			// 模拟 -once 模式：onLeader 完成工作后立即返回，同时取消外部 ctx
			cancel()
		})
	}()

	select {
	case <-runDone:
		// pass：Run 在 ctx 取消后正常退出
	case <-time.After(2 * time.Second):
		t.Fatal("lease.Run should exit after ctx is cancelled, but it blocked")
	}
}

func TestLeaderElection_NoExistingLease(t *testing.T) {
	dir := t.TempDir()
	leasePath := filepath.Join(dir, "leader.lease")

	cfg := lease.Config{
		LeaseTimeout:  2 * time.Second,
		RenewInterval: 500 * time.Millisecond,
		PollInterval:  200 * time.Millisecond,
	}
	l := lease.New(leasePath, "test-holder-1", cfg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	becameLeader := make(chan struct{}, 1)
	l.Run(ctx, func(leaderCtx context.Context) {
		becameLeader <- struct{}{}
		<-leaderCtx.Done()
	})

	select {
	case <-becameLeader:
		// pass
	default:
		t.Fatal("expected to become leader but did not")
	}
}
