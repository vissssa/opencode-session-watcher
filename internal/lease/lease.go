package lease

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strings"
	"time"
)

// leaseFile 是写入磁盘的 JSON 结构。
type leaseFile struct {
	Holder     string `json:"holder"`
	AcquiredAt int64  `json:"acquired_at"`
	RenewedAt  int64  `json:"renewed_at"`
}

// Config 包含 Lease 的运行时参数。
type Config struct {
	LeaseTimeout  time.Duration // renewed_at 超出此时长认为 Leader 已死，默认 30s
	RenewInterval time.Duration // Leader 续约间隔，默认 10s
	PollInterval  time.Duration // Standby 轮询间隔，默认 5s
}

func (c *Config) setDefaults() {
	if c.LeaseTimeout <= 0 {
		c.LeaseTimeout = 30 * time.Second
	}
	if c.RenewInterval <= 0 {
		c.RenewInterval = 10 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Second
	}
}

// Lease 管理基于文件时间戳的 Leader 选举。
type Lease struct {
	path       string
	holderID   string
	cfg        Config
	logger     *slog.Logger
	acquiredAt int64 // 本实例最近一次获取 leadership 的 Unix 时间戳（毫秒），仅 Leader 状态下有效
}

// New 创建 Lease 管理器。
// leasePath 是 GlusterFS 上的共享 lease 文件路径；
// holderID 是本实例唯一标识（建议格式: hostname:pid），不能为空且不能包含空字节（\x00）；
// logger 为 nil 时使用全局默认 logger。
func New(leasePath, holderID string, cfg Config, logger *slog.Logger) *Lease {
	cfg.setDefaults()
	if holderID == "" {
		panic("lease.New: holderID must not be empty")
	}
	if strings.ContainsRune(holderID, 0) {
		panic("lease.New: holderID must not contain null bytes")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Lease{
		path:     leasePath,
		holderID: holderID,
		cfg:      cfg,
		logger:   logger,
	}
}

// Run 阻塞运行选举循环：
//   - Standby 状态：轮询检查 lease 是否超时，超时后尝试竞争晋升
//   - Leader 状态：调用 onLeader(ctx)，同时后台定期续约
//   - onLeader 返回后重新进入 Standby 等待下一轮选举
//   - ctx 取消时 Run 返回
func (l *Lease) Run(ctx context.Context, onLeader func(ctx context.Context)) {
	for {
		if ctx.Err() != nil {
			return
		}
		if l.waitUntilCandidate(ctx) {
			l.runAsLeader(ctx, onLeader)
		}
	}
}

// waitUntilCandidate 在 Standby 状态等待，直到本实例可以尝试晋升。
// 返回 true 表示晋升成功（自己是新 Leader）；false 表示 ctx 已取消。
func (l *Lease) waitUntilCandidate(ctx context.Context) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		lf, err := l.readLease()
		if err != nil {
			if !os.IsNotExist(err) {
				// 读取失败：GlusterFS 可能故障，保守等待，不抢占
				l.logger.Warn("lease: read failed, standby waiting", "error", err)
				if !sleepWithContext(ctx, l.cfg.PollInterval) {
					return false
				}
				continue
			}
			// lease 文件不存在，可以尝试竞争
		} else {
			age := time.Duration(nowMillis()-lf.RenewedAt) * time.Millisecond
			if age < l.cfg.LeaseTimeout {
				// Leader 仍活跃，继续等待
				if !sleepWithContext(ctx, l.cfg.PollInterval) {
					return false
				}
				continue
			}
			l.logger.Info("lease: lease expired, attempting takeover",
				"expired_holder", lf.Holder,
				"age", age.Round(time.Millisecond),
			)
		}

		if l.tryAcquire(ctx) {
			return true
		}
		if !sleepWithContext(ctx, l.cfg.PollInterval) {
			return false
		}
	}
}

// tryAcquire 执行 write-then-verify 竞争写入：
//  1. 覆盖写入 lease，holder 设为自己，AcquiredAt 设为本实例当前时刻
//  2. 随机 jitter 等待（50-200ms），拉开与其他竞争者的写入窗口
//  3. verifyDelay 200ms，等待文件系统同步稳定
//  4. 读回验证 holder == 自己
func (l *Lease) tryAcquire(ctx context.Context) bool {
	now := nowMillis()
	// 始终用本实例的竞争时刻作为 AcquiredAt，不继承前任 Leader 的值。
	// 继承前任值会导致 /status 中显示的 leader 持有时长虚高（记录的是前任获取时间）。
	lf := leaseFile{
		Holder:     l.holderID,
		AcquiredAt: now,
		RenewedAt:  now,
	}
	if err := l.writeLease(lf); err != nil {
		l.logger.Warn("lease: write failed during acquire", "error", err)
		return false
	}

	jitter := time.Duration(50+rand.Intn(151)) * time.Millisecond
	if !sleepWithContext(ctx, jitter) {
		return false
	}
	if !sleepWithContext(ctx, 200*time.Millisecond) {
		return false
	}

	got, err := l.readLease()
	if err != nil {
		l.logger.Warn("lease: verify read failed after acquire", "error", err)
		return false
	}
	if got.Holder != l.holderID {
		l.logger.Info("lease: lost election to another instance", "winner", got.Holder)
		return false
	}
	// 记录本实例获取 leadership 的时刻，供 renew 在文件读取失败时使用
	l.acquiredAt = now
	l.logger.Info("lease: acquired leadership", "holder", l.holderID)
	return true
}

// runAsLeader 在 Leader 状态运行 onLeader，同时后台定期续约。
// 续约失败时记录 warn 日志但不降级（宽松续约策略）。
func (l *Lease) runAsLeader(ctx context.Context, onLeader func(ctx context.Context)) {
	leaderCtx, cancelLeader := context.WithCancel(ctx)
	defer cancelLeader()

	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(l.cfg.RenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-leaderCtx.Done():
				return
			case <-ticker.C:
				if err := l.renew(); err != nil {
					l.logger.Warn("lease: renew failed, continuing as leader", "error", err)
				} else {
					l.logger.Debug("lease: renewed", "holder", l.holderID)
				}
			}
		}
	}()

	onLeader(leaderCtx)
	cancelLeader()
	<-renewDone
	l.logger.Info("lease: released leadership", "holder", l.holderID)
}

// renew 更新 lease 文件的 renewed_at 为当前时间。
// 文件读取失败时（如 GlusterFS 故障）重建文件，AcquiredAt 使用本实例最初获取 leadership 的时刻，
// 防止将"leader 持有开始时间"重置为续约时刻。
//
// ⚠️ 已知限制：renew 采用读改写而非原子 CAS，存在极小的脑裂窗口。
// 若 Standby 恰在 readLease 和 writeLease 之间完成竞争写入（即判断当前 Leader 的 lease
// 已超时并写入自己的 lease），本实例的 writeLease 会无声覆盖 Standby 的写入，
// 导致双主状态。该窗口的宽度取决于 readLease + writeLease 的 I/O 延迟，通常 < 10ms，
// 但在 GlusterFS 高延迟下可能更宽。最终两者会因 lease 文件内容不一致而收敛（其中一方
// 在下一次续约/轮询时发现自己不再是 holder 而退出），但在收敛前存在双主风险。
// 详见 CLAUDE.md「已知风险」章节。
func (l *Lease) renew() error {
	lf, err := l.readLease()
	if err != nil {
		// 用本实例记录的 acquiredAt，而非 nowMillis()，保持 AcquiredAt 语义正确
		lf = &leaseFile{
			Holder:     l.holderID,
			AcquiredAt: l.acquiredAt,
		}
	}
	lf.RenewedAt = nowMillis()
	lf.Holder = l.holderID
	return l.writeLease(*lf)
}

// readLease 读取并解析 lease 文件。
func (l *Lease) readLease() (*leaseFile, error) {
	data, err := os.ReadFile(l.path)
	if err != nil {
		return nil, err
	}
	var lf leaseFile
	if err := json.Unmarshal(data, &lf); err != nil {
		return nil, fmt.Errorf("lease: parse lease file: %w", err)
	}
	return &lf, nil
}

// writeLease 将 leaseFile 原子写入 lease 文件（先写临时文件再 rename）。
// tmp 文件路径包含经过路径安全处理的 holderID，防止同机多实例竞争时相互覆盖临时文件。
// holderID 中的 "/" 替换为 "_"（防止被误解析为目录分隔符）；
// holderID 的非空和无空字节已在 New() 中保证。
func (l *Lease) writeLease(lf leaseFile) error {
	data, err := json.Marshal(lf)
	if err != nil {
		return fmt.Errorf("lease: marshal lease: %w", err)
	}
	// 对 holderID 做路径安全处理，将 "/" 替换为 "_"，防止含斜杠的 ID 写到错误目录。
	safeHolder := strings.ReplaceAll(l.holderID, "/", "_")
	tmp := l.path + ".tmp." + safeHolder
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("lease: write tmp file: %w", err)
	}
	if err := os.Rename(tmp, l.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("lease: rename lease file: %w", err)
	}
	return nil
}

// nowMillis 返回当前 Unix 时间戳（毫秒）。
func nowMillis() int64 {
	return time.Now().UnixMilli()
}

// sleepWithContext 等待 d 时长或 ctx 取消，返回 false 表示 ctx 已取消。
// 使用 time.NewTimer 而非 time.After，确保 ctx 先取消时 timer 资源被立即释放。
func sleepWithContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
