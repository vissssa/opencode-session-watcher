package status

import (
	"sync"
	"time"
)

// Snapshot 是某一时刻的运行状态快照，通过 /status 端点对外暴露。
// 只保留关键运行状态，便于快速判断服务健康度。
type Snapshot struct {
	Mode                string `json:"mode"`                   // 运行模式："standalone"（单实例）或 "ha"（多副本）
	LastSuccessAt       string `json:"last_success_at"`        // 最近一次无错误完成的时间（RFC3339 UTC）
	LastError           string `json:"last_error"`             // 最近一次错误信息，无错误时为空
	SessionsTotal       int    `json:"sessions_total"`         // 最近一轮发现的 Session 总数
	SessionsSynced      int    `json:"sessions_synced"`        // 最近一轮成功同步的 Session 数
	SessionsFailed      int    `json:"sessions_failed"`        // 最近一轮失败的 Session 数
	MessagesNew         int    `json:"messages_new"`           // 最近一轮新写入的消息数
	LastMaxFetchReached int    `json:"last_max_fetch_reached"` // 最近一轮触及 MaxMessageFetch 上限的 Session 数
	IsLeader            bool   `json:"is_leader"`              // 当前实例是否为活跃工作节点；standalone 模式下恒为 true，ha 模式下由选举结果决定
	LeaseID             string `json:"lease_id,omitempty"`     // 当前持有 lease 的节点标识；非 Leader 时为空
}

// Reporter 线程安全地维护一份运行状态快照。
// 写操作通过互斥锁保护，读操作通过读锁保护，适合高频读取场景。
// Mode/IsLeader/LeaseID 与业务字段统一存储在 snapshot 中，Snapshot() 直接返回副本。
type Reporter struct {
	mu       sync.RWMutex
	snapshot Snapshot
}

// RoundUpdate 携带单轮同步的统计数据，用于更新 Reporter。
type RoundUpdate struct {
	SessionsTotal   int
	SessionsSynced  int
	SessionsFailed  int
	MessagesNew     int
	MaxFetchReached int
	Duration        time.Duration
	Err             error
}

// NewReporter 创建一个新的 Reporter。
func NewReporter() *Reporter {
	return &Reporter{}
}

// Snapshot 返回当前运行状态的只读副本。
func (r *Reporter) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snapshot
}

// RecordRound 根据本轮同步结果更新快照。
// 若本轮有错误，记录错误信息并跳过 LastSuccessAt 的更新。
func (r *Reporter) RecordRound(update RoundUpdate) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshot.SessionsTotal = update.SessionsTotal
	r.snapshot.SessionsSynced = update.SessionsSynced
	r.snapshot.SessionsFailed = update.SessionsFailed
	r.snapshot.MessagesNew = update.MessagesNew
	r.snapshot.LastMaxFetchReached = update.MaxFetchReached
	if update.Err != nil {
		// 有错误时只更新错误信息，不推进 LastSuccessAt
		r.snapshot.LastError = update.Err.Error()
		return
	}
	r.snapshot.LastError = ""
	r.snapshot.LastSuccessAt = time.Now().UTC().Format(time.RFC3339)
}

// SetLeaderState 更新 Leader 状态，供 main.go 在选举结果变化时调用。
func (r *Reporter) SetLeaderState(isLeader bool, leaseID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshot.IsLeader = isLeader
	r.snapshot.LeaseID = leaseID
}

// SetMode 设置运行模式（"standalone" 或 "ha"），在程序启动时调用一次。
// standalone 模式下实例始终是"唯一工作节点"，因此自动将 is_leader 置为 true，
// 避免监控系统将单实例节点误判为"未工作"。
func (r *Reporter) SetMode(mode string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshot.Mode = mode
	if mode == "standalone" {
		r.snapshot.IsLeader = true
	}
}
