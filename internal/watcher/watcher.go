package watcher

import (
	"context"
	"log/slog"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"session_watcher/internal/domain"
	"session_watcher/internal/store"
)

// Store 是 watcher 对 PostgreSQL 状态存储的接口抽象，便于测试时替换为 fake 实现。
type Store interface {
	GetSessionState(ctx context.Context, sessionID string) (store.SessionState, bool, error)
	AnyMessageExists(ctx context.Context, messages []domain.Message) (bool, error)
	UnseenMessages(ctx context.Context, messages []domain.Message) ([]domain.Message, int, error)
	PrepareMessageRecords(ctx context.Context, session domain.Session, records []domain.MessageRecord, preparedAt int64) ([]domain.MessageRecord, error)
	MarkMessagesWritten(ctx context.Context, session domain.Session, records []domain.MessageRecord, writtenAt int64) error
	MarkMessagesFailed(ctx context.Context, records []domain.MessageRecord, errText string) error
	UpdateSessionFetchStats(ctx context.Context, sessionID string, reachedLimit bool, count int, limit int, fetchedAt int64) error
}

// Config 包含 Watcher 的运行时配置。
type Config struct {
	MessageLimit    int // fetchUntilBoundary 的 limit 扩展步长
	MaxMessageFetch int // 单 Session 每轮最多拉取的消息条数上限
	SessionWorkers  int // Worker Pool 大小
}

// Watcher 负责周期性地从 Source 拉取会话消息，并通过 Sink 输出新消息。
// 内部维护一个 atomic 轮次计数器，便于日志追踪和并发调试。
type Watcher struct {
	source domain.Source
	sink   domain.Sink
	store  Store
	cfg    Config
	logger *slog.Logger
	round  atomic.Int64 // 当前同步轮次，原子递增
}

// RoundResult 汇总单轮同步的统计数据。
type RoundResult struct {
	Round           int64
	SessionsTotal   int // 本轮发现的 Session 总数
	SessionsSkipped int // 因无更新而跳过的 Session 数
	SessionsSynced  int // 成功完成同步的 Session 数
	SessionsFailed  int // 同步失败的 Session 数
	MessagesNew     int // 本轮新写出的消息总数
	MaxFetchReached int // 触及 MaxMessageFetch 上限的 Session 数
}

// sessionJob 是分发给 Worker 的同步任务，包含 Session 信息和本地状态。
type sessionJob struct {
	session    domain.Session
	state      store.SessionState
	stateFound bool // false 表示该 Session 在本地尚无记录
}

// sessionResult 是 Worker 完成单个 Session 同步后返回的结果。
type sessionResult struct {
	sessionID       string
	newCount        int
	maxFetchReached bool
	err             error
}

// New 创建一个新的 Watcher 实例。
func New(source domain.Source, sink domain.Sink, store Store, cfg Config, logger *slog.Logger) *Watcher {
	return &Watcher{source: source, sink: sink, store: store, cfg: cfg, logger: logger}
}

// SyncOnce 执行一轮完整的增量同步：
//  1. 拉取 Session 列表，过滤出需要同步的 Session
//  2. 启动 Worker Pool 并发处理每个 Session
//  3. 汇总统计结果后返回
//
// ctx 取消时会中断新任务分发，正在处理的 HTTP 请求也会尽快取消。
func (w *Watcher) SyncOnce(ctx context.Context) (RoundResult, error) {
	round := w.round.Add(1)
	started := time.Now()

	sessions, err := w.source.ListSessions(ctx)
	if err != nil {
		w.logger.Error("list sessions failed", "round", round, "error", err)
		return RoundResult{Round: round}, err
	}

	// 为每个 Session 读取本地状态，过滤出需要同步的任务
	jobs := make([]sessionJob, 0, len(sessions))
	for _, session := range sessions {
		state, found, err := w.store.GetSessionState(ctx, session.ID)
		if err != nil {
			// 读取状态失败时降级处理：仍加入同步队列（保守策略）
			w.logger.Warn("read session state failed", "round", round, "session_id", session.ID, "error", err)
			jobs = append(jobs, sessionJob{session: session})
			continue
		}
		if shouldSync(session, state, found) {
			jobs = append(jobs, sessionJob{session: session, state: state, stateFound: found})
		}
	}
	result := RoundResult{Round: round, SessionsTotal: len(sessions), SessionsSkipped: len(sessions) - len(jobs)}

	// 无需同步的轮次静默跳过，不打印日志避免刷屏
	if len(jobs) == 0 {
		return result, nil
	}
	w.logger.Info("sync round started", "round", round, "total", len(sessions), "sync", len(jobs), "skip", result.SessionsSkipped)

	// Worker 数量不超过实际任务数，避免创建空闲 goroutine
	workers := w.cfg.SessionWorkers
	if workers > len(jobs) {
		workers = len(jobs)
	}
	// jobCh 为 unbuffered，发送方受接收方速率背压控制
	jobCh := make(chan sessionJob)
	// resultCh 预分配容量，保证 Worker 不会因发送结果而阻塞
	resultCh := make(chan sessionResult, len(jobs))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for job := range jobCh {
				newCount, reachedMax, err := w.syncSession(ctx, round, job)
				resultCh <- sessionResult{sessionID: job.session.ID, newCount: newCount, maxFetchReached: reachedMax, err: err}
			}
		}(i + 1)
	}

	// 在独立 goroutine 中向 jobCh 投递任务，ctx 取消时停止投递
	go func() {
		defer close(jobCh)
		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return
			case jobCh <- job:
			}
		}
	}()

	// 等待所有 Worker 完成后关闭 resultCh，再汇总统计
	wg.Wait()
	close(resultCh)
	for res := range resultCh {
		if res.maxFetchReached {
			result.MaxFetchReached++
		}
		if res.err != nil {
			result.SessionsFailed++
			w.logger.Warn("session sync failed", "round", round, "session_id", res.sessionID, "error", res.err)
			continue
		}
		result.SessionsSynced++
		result.MessagesNew += res.newCount
	}
	w.logger.Info("sync round finished",
		"round", round,
		"duration", time.Since(started),
		"sessions_total", result.SessionsTotal,
		"sessions_synced", result.SessionsSynced,
		"sessions_failed", result.SessionsFailed,
		"messages_new", result.MessagesNew,
		"max_fetch_reached", result.MaxFetchReached,
	)
	// ctx 取消属于正常关闭信号，不作为错误返回，避免污染 /status 的 last_error
	return result, nil
}

// shouldSync 判断指定 Session 是否需要本轮同步。
// 决策逻辑：
//   - 本地未见过的 Session（found=false）：一定同步（首次需要建立基线）
//   - Session.UpdatedAt == 0：远端 API 未返回更新时间，保守处理每轮强制同步。
//     ⚠️ 注意：若 open-code API 大量 Session 的 updated 字段均为 0，所有 Session
//     将每轮发起完整的 GetSession + ListMessages HTTP 请求，可能显著放大 API 压力。
//     运维时若发现 API 请求量过高，应检查 Source 返回的 UpdatedAt 字段是否正常。
//   - 远端 UpdatedAt > 本地记录：有新更新，同步
func shouldSync(session domain.Session, state store.SessionState, found bool) bool {
	if !found {
		return true // 首次见到该 Session
	}
	if session.UpdatedAt == 0 {
		return true // 远端无更新时间，保守处理
	}
	return session.UpdatedAt > state.UpdatedAt
}

// mergeSessionMetadata 合并详情接口和列表接口返回的 Session 元数据。
// 当详情接口返回的 UserID/AgentID 为空或默认值时，优先使用列表接口的值。
// 最终兜底：若仍为空则填充 DefaultUserID / DefaultAgentID。
func mergeSessionMetadata(detail domain.Session, listed domain.Session) domain.Session {
	if detail.UserID == "" || detail.UserID == domain.DefaultUserID {
		if listed.UserID != "" {
			detail.UserID = listed.UserID
		}
	}
	if detail.AgentID == "" || detail.AgentID == domain.DefaultAgentID {
		if listed.AgentID != "" {
			detail.AgentID = listed.AgentID
		}
	}
	if detail.UserID == "" {
		detail.UserID = domain.DefaultUserID
	}
	if detail.AgentID == "" {
		detail.AgentID = domain.DefaultAgentID
	}
	return detail
}

// syncSession 同步单个 Session 的完整流程：
//  1. 拉取 Session 详情并合并元数据
//  2. fetchUntilBoundary 拉取增量消息
//  3. 去重、排序、填充输出追踪字段
//  4. PrepareMessageRecords → WriteMessages → MarkMessagesWritten
//
// 返回本轮新写出的消息数、是否触及 MaxFetch 上限，以及错误（若有）。
func (w *Watcher) syncSession(ctx context.Context, round int64, job sessionJob) (int, bool, error) {
	started := time.Now()
	w.logger.Debug("session sync started",
		"round", round,
		"session_id", job.session.ID,
		"remote_updated_at", job.session.UpdatedAt,
		"local_updated_at", job.state.UpdatedAt,
	)

	session, err := w.source.GetSession(ctx, job.session.ID)
	if err != nil {
		return 0, false, err
	}
	// 详情接口和列表接口的元数据可能不一致，合并后使用最准确的值
	session = mergeSessionMetadata(session, job.session)

	messages, reachedMax, fetchLimit, err := w.fetchUntilBoundary(ctx, session.ID)
	if err != nil {
		return 0, reachedMax, err
	}
	fetchedAt := store.NowMillis()
	if err := w.store.UpdateSessionFetchStats(ctx, session.ID, reachedMax, len(messages), fetchLimit, fetchedAt); err != nil {
		return 0, reachedMax, err
	}

	unseen, seenCount, err := w.store.UnseenMessages(ctx, messages)
	if err != nil {
		return 0, reachedMax, err
	}
	// 按创建时间升序排列，相同时间按 ID 稳定排序，保证输出顺序一致
	sortMessages(unseen)
	w.logger.Debug("message dedupe completed", "round", round, "session_id", session.ID, "new", len(unseen), "seen", seenCount)

	syncedAt := store.NowMillis()
	records := make([]domain.MessageRecord, 0, len(unseen))
	for _, msg := range unseen {
		records = append(records, domain.MessageRecord{
			SyncedAt:         syncedAt,
			UserID:           session.UserID,
			AgentID:          session.AgentID,
			SessionID:        session.ID,
			MessageID:        msg.ID,
			MessageCreatedAt: msg.CreatedAt,
			Session:          session.Raw,
			Message:          msg.Raw,
		})
	}
	// 填充输出追踪字段（SinkType/OutputRoot/OutputPath），供 PostgreSQL 记录
	w.fillOutputTracking(records)

	// PrepareMessageRecords：在 PostgreSQL 中登记 pending 状态（双重防重）
	prepared, err := w.store.PrepareMessageRecords(ctx, session, records, syncedAt)
	if err != nil {
		return 0, reachedMax, err
	}
	// WriteMessages：写出到 Sink（at-least-once 语义）
	if err := w.sink.WriteMessages(ctx, prepared); err != nil {
		// 写出失败：记录错误到 PostgreSQL，下轮可重试
		if markErr := w.store.MarkMessagesFailed(ctx, prepared, err.Error()); markErr != nil {
			w.logger.Warn("mark messages failed error", "session_id", session.ID, "error", markErr)
		}
		return 0, reachedMax, err
	}
	// MarkMessagesWritten：写出成功后推进状态，确保状态不领先于实际输出
	if err := w.store.MarkMessagesWritten(ctx, session, prepared, syncedAt); err != nil {
		return 0, reachedMax, err
	}

	latestID := ""
	latestCreatedAt := int64(0)
	if len(prepared) > 0 {
		last := prepared[len(prepared)-1]
		latestID = last.MessageID
		latestCreatedAt = last.MessageCreatedAt
	}
	w.logger.Debug("session sync completed",
		"round", round,
		"session_id", session.ID,
		"new_messages", len(prepared),
		"latest_message_id", latestID,
		"latest_message_created_at", latestCreatedAt,
		"max_fetch_reached", reachedMax,
		"duration", time.Since(started),
	)
	return len(prepared), reachedMax, nil
}

// fillOutputTracking 利用 PathResolver 接口填充 records 的输出追踪字段。
// 若 Sink 未实现 PathResolver，SinkType 设为 "unknown"，其余路径字段留空。
func (w *Watcher) fillOutputTracking(records []domain.MessageRecord) {
	resolver, ok := w.sink.(domain.PathResolver)
	for i := range records {
		if !ok {
			records[i].SinkType = "unknown"
			continue
		}
		outputPath := resolver.PathFor(records[i])
		records[i].SinkType = resolver.SinkType()
		records[i].OutputRoot = resolver.OutputRoot()
		records[i].OutputPath = outputPath
		records[i].OutputSessionFile = filepath.Base(outputPath)
	}
}

// fetchUntilBoundary 通过动态扩展 limit 探测增量消息的边界，确保不遗漏两轮间的新消息。
//
// 算法：
//  1. 从 limit=min(MessageLimit, MaxMessageFetch) 开始请求最近 N 条消息
//  2. 若返回消息中包含已处理过的消息 → 找到边界，停止扩展
//  3. 若返回数量 < limit → 已取完全部可见消息，停止扩展
//  4. 若 limit 已达 MaxMessageFetch → 触顶，记录 warn 并停止（可能遗漏消息）
//  5. 否则 limit += MessageLimit，继续扩展
//
// 返回消息列表、是否触及上限，以及实际使用的 limit 值。
func (w *Watcher) fetchUntilBoundary(ctx context.Context, sessionID string) ([]domain.Message, bool, int, error) {
	step := w.cfg.MessageLimit
	maxFetch := w.cfg.MaxMessageFetch
	if maxFetch <= 0 {
		maxFetch = step
	}
	limit := min(step, maxFetch)
	for {
		messages, err := w.source.ListMessages(ctx, sessionID, limit)
		if err != nil {
			return nil, false, limit, err
		}
		foundProcessed, err := w.store.AnyMessageExists(ctx, messages)
		if err != nil {
			return nil, false, limit, err
		}
		w.logger.Debug("message boundary probe", "session_id", sessionID, "limit", limit, "max_message_fetch", maxFetch, "count", len(messages), "found_processed", foundProcessed)
		// 找到边界（含已处理消息）或已取完全部消息，停止扩展
		if foundProcessed || len(messages) < limit {
			return messages, false, limit, nil
		}
		// limit 触顶：warn 并返回，调用方应处理 reachedMax=true 的情况
		if limit >= maxFetch {
			w.logger.Warn("message fetch reached max limit", "session_id", sessionID, "max_message_fetch", maxFetch, "count", len(messages), "found_processed", foundProcessed)
			return messages, true, limit, nil
		}
		limit = min(limit+step, maxFetch)
	}
}

// sortMessages 按 CreatedAt 升序排列消息，CreatedAt 相同时按 ID 稳定排序。
// 确保输出顺序与消息创建时间一致，便于下游按时序处理。
func sortMessages(messages []domain.Message) {
	sort.SliceStable(messages, func(i, j int) bool {
		if messages[i].CreatedAt == messages[j].CreatedAt {
			return messages[i].ID < messages[j].ID
		}
		return messages[i].CreatedAt < messages[j].CreatedAt
	})
}
