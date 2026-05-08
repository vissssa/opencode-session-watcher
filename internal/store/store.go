package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"session_watcher/internal/domain"
)

// 消息状态常量。
const (
	MessageStatusPending = "pending" // 已写入 PostgreSQL 但尚未写出到 Sink
	MessageStatusWritten = "written" // 已成功写出到 Sink，永久去重标记
)

// SessionState 表示从 PostgreSQL 读出的 Session 同步状态，包含游标和最近一次拉取统计。
type SessionState struct {
	ID                     string
	UserID                 string
	AgentID                string
	UpdatedAt              int64  // 最后一次同步时 Session 的远端更新时间
	LatestMessageID        string // 本地已写出的最新消息 ID（游标）
	LatestMessageCreatedAt int64  // 本地已写出的最新消息创建时间
	RawJSON                string
	SyncedAt               int64
	LastFetchReachedLimit  bool  // 上次拉取是否触及 MaxMessageFetch 上限
	LastFetchCount         int   // 上次实际拉取的消息数
	LastFetchLimit         int   // 上次使用的 limit 值
	LastFetchAt            int64
	FileSize               int64 // session_watcher 维护的 JSONL 文件当前字节大小
	MemorizedOffset        int64 // 外部消费者维护的消费偏移（消费者读到哪就写到哪）
	MemorizedAt            int64 // 外部服务最后一次消费该 Session 的时间戳（毫秒）
}

// Store 封装 PostgreSQL 连接池，提供 Session 状态管理和消息去重能力。
type Store struct {
	pool       *pgxpool.Pool
	tempSchema string // 非空时表示使用临时 schema，Close 时自动 DROP CASCADE
}

// Open 连接 PostgreSQL 数据库，完成连接池配置、Schema 初始化和版本检查。
// 任何步骤失败都会关闭连接池并返回错误。
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := newPool(ctx, dsn)
	if err != nil {
		return nil, err
	}
	store := &Store{pool: pool}
	if err := store.init(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := store.checkSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return store, nil
}

// OpenTemp 连接 PostgreSQL 并在独立的临时 schema 中创建表。
// 适用于 -once 单次测试模式：与正式数据完全隔离，Close 时自动清理。
func OpenTemp(ctx context.Context, dsn string) (*Store, error) {
	schema := fmt.Sprintf("tmp_%d", time.Now().UnixNano())

	// 第一步：用临时单连接创建 schema
	setupPool, err := newPool(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if _, err := setupPool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		setupPool.Close()
		return nil, fmt.Errorf("create temp schema: %w", err)
	}
	setupPool.Close()

	// 第二步：创建带 AfterConnect 的连接池，确保所有连接自动使用临时 schema
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pg dsn: %w", err)
	}
	poolCfg.MaxConns = 10
	poolCfg.MinConns = 2
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET search_path TO "+schema)
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pg pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping pg: %w", err)
	}

	store := &Store{pool: pool, tempSchema: schema}
	if err := store.init(ctx); err != nil {
		store.cleanup(ctx)
		pool.Close()
		return nil, err
	}
	return store, nil
}

// newPool 创建并 ping PostgreSQL 连接池。
func newPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pg dsn: %w", err)
	}
	poolCfg.MaxConns = 10
	poolCfg.MinConns = 2
	poolCfg.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pg pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping pg: %w", err)
	}
	return pool, nil
}

// Close 关闭连接池。若使用了临时 schema，先自动 DROP CASCADE 清理。
func (s *Store) Close() error {
	if s.tempSchema != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.cleanup(ctx)
	}
	s.pool.Close()
	return nil
}

// cleanup 删除临时 schema 及其所有对象。
func (s *Store) cleanup(ctx context.Context) {
	if s.tempSchema != "" {
		s.pool.Exec(ctx, "DROP SCHEMA "+s.tempSchema+" CASCADE")
	}
}

// init 创建核心表和索引（幂等，已存在时跳过）。
// sessions 表记录 Session 级别的同步状态和拉取统计；
// messages 表记录消息级别的去重标记和输出追踪信息。
func (s *Store) init(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS sessions (
			id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL DEFAULT 'default_user',
			agent_id TEXT NOT NULL DEFAULT 'default_agent',
			updated_at BIGINT NOT NULL DEFAULT 0,
			latest_message_id TEXT NOT NULL DEFAULT '',
			latest_message_created_at BIGINT NOT NULL DEFAULT 0,
			raw_json TEXT NOT NULL DEFAULT '',
			synced_at BIGINT NOT NULL DEFAULT 0,
			last_fetch_reached_limit BOOLEAN NOT NULL DEFAULT FALSE,
			last_fetch_count INTEGER NOT NULL DEFAULT 0,
			last_fetch_limit INTEGER NOT NULL DEFAULT 0,
			last_fetch_at BIGINT NOT NULL DEFAULT 0,
			file_size BIGINT NOT NULL DEFAULT 0,
			memorized_offset BIGINT NOT NULL DEFAULT 0,
			memorized_at BIGINT NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS messages (
			id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			created_at BIGINT NOT NULL DEFAULT 0,
			prepared_at BIGINT NOT NULL DEFAULT 0,
			written_at BIGINT NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'pending',
			last_error TEXT NOT NULL DEFAULT '',
			user_id TEXT NOT NULL DEFAULT 'default_user',
			agent_id TEXT NOT NULL DEFAULT 'default_agent',
			sink_type TEXT NOT NULL DEFAULT 'jsonl',
			output_root TEXT NOT NULL DEFAULT '',
			output_path TEXT NOT NULL DEFAULT '',
			output_session_file TEXT NOT NULL DEFAULT '',
			output_line INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_sessions_user_agent ON sessions(user_id, agent_id)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_session_created ON messages(session_id, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_sink_output ON messages(sink_type, output_root, output_path)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_user_agent_session ON messages(user_id, agent_id, session_id)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status)`,
	}
	for _, stmt := range stmts {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}
	return nil
}

// checkSchema 检查当前数据库 Schema 是否与本版本程序兼容。
// 检测到缺少必需列时拒绝启动，防止新版程序在旧格式数据库上静默写错。
func (s *Store) checkSchema(ctx context.Context) error {
	sessionsColumns, err := s.tableColumns(ctx, "sessions")
	if err != nil {
		return err
	}
	messagesColumns, err := s.tableColumns(ctx, "messages")
	if err != nil {
		return err
	}
	// 检查 sessions 表所有必需列
	for _, column := range []string{"id", "user_id", "agent_id", "updated_at", "latest_message_id", "latest_message_created_at", "raw_json", "synced_at", "last_fetch_reached_limit", "last_fetch_count", "last_fetch_limit", "last_fetch_at", "file_size", "memorized_offset", "memorized_at"} {
		if _, ok := sessionsColumns[column]; !ok {
			return fmt.Errorf("incompatible database schema: sessions.%s missing", column)
		}
	}
	// 检查 messages 表所有必需列
	for _, column := range []string{"id", "session_id", "created_at", "prepared_at", "written_at", "status", "last_error", "user_id", "agent_id", "sink_type", "output_root", "output_path", "output_session_file", "output_line"} {
		if _, ok := messagesColumns[column]; !ok {
			return fmt.Errorf("incompatible database schema: messages.%s missing", column)
		}
	}
	return nil
}

// tableColumns 返回指定表的所有列名集合（用于 Schema 版本检查）。
// 使用 current_schema() 自动适配当前 search_path，确保在非 public schema 下也能正确查询。
func (s *Store) tableColumns(ctx context.Context, table string) (map[string]struct{}, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1`, table)
	if err != nil {
		return nil, fmt.Errorf("query columns for %s: %w", table, err)
	}
	defer rows.Close()
	columns := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("table %q not found or has no columns", table)
	}
	return columns, nil
}

// GetSessionState 查询指定 Session 的同步状态，未找到时返回 (zero, false, nil)。
func (s *Store) GetSessionState(ctx context.Context, sessionID string) (SessionState, bool, error) {
	var state SessionState
	err := s.pool.QueryRow(ctx,
		`SELECT id, user_id, agent_id, updated_at, latest_message_id, latest_message_created_at, raw_json, synced_at, last_fetch_reached_limit, last_fetch_count, last_fetch_limit, last_fetch_at, file_size, memorized_offset, memorized_at
		FROM sessions WHERE id = $1`, sessionID).
		Scan(&state.ID, &state.UserID, &state.AgentID, &state.UpdatedAt, &state.LatestMessageID, &state.LatestMessageCreatedAt, &state.RawJSON, &state.SyncedAt, &state.LastFetchReachedLimit, &state.LastFetchCount, &state.LastFetchLimit, &state.LastFetchAt, &state.FileSize, &state.MemorizedOffset, &state.MemorizedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SessionState{}, false, nil
		}
		return SessionState{}, false, err
	}
	return state, true, nil
}

// MessageExists 检查指定消息是否已处于 written 状态（已成功写出到 Sink）。
func (s *Store) MessageExists(ctx context.Context, messageID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM messages WHERE id = $1 AND status = $2)`, messageID, MessageStatusWritten).
		Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// ExistingMessageStatuses 批量查询指定消息 ID 列表的状态。
// 为避免超大 IN 列表，内部按 chunkSize=500 分批查询。
func (s *Store) ExistingMessageStatuses(ctx context.Context, ids []string) (map[string]string, error) {
	// 去重输入 ID，过滤空值
	unique := make([]string, 0, len(ids))
	seenInput := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seenInput[id]; ok {
			continue
		}
		seenInput[id] = struct{}{}
		unique = append(unique, id)
	}

	statuses := make(map[string]string)
	const chunkSize = 500
	for start := 0; start < len(unique); start += chunkSize {
		end := min(start+chunkSize, len(unique))
		chunk := unique[start:end]
		// 构造 IN 占位符列表 $1, $2, ...
		placeholders := make([]string, len(chunk))
		args := make([]any, len(chunk))
		for i, id := range chunk {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = id
		}
		query := `SELECT id, status FROM messages WHERE id IN (` + strings.Join(placeholders, ",") + `)`
		rows, err := s.pool.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			var status string
			if err := rows.Scan(&id, &status); err != nil {
				rows.Close()
				return nil, err
			}
			statuses[id] = status
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return statuses, nil
}

// ExistingMessageIDs 返回已处于 written 状态的消息 ID 集合。
func (s *Store) ExistingMessageIDs(ctx context.Context, ids []string) (map[string]struct{}, error) {
	statuses, err := s.ExistingMessageStatuses(ctx, ids)
	if err != nil {
		return nil, err
	}
	existing := make(map[string]struct{}, len(statuses))
	for id, status := range statuses {
		if status == MessageStatusWritten {
			existing[id] = struct{}{}
		}
	}
	return existing, nil
}

// AnyMessageExists 判断消息列表中是否有任意一条已处于 written 状态。
// 用于 fetchUntilBoundary 的边界探测：一旦发现已处理消息，说明已找到增量边界。
func (s *Store) AnyMessageExists(ctx context.Context, messages []domain.Message) (bool, error) {
	existing, err := s.ExistingMessageIDs(ctx, messageIDs(messages))
	if err != nil {
		return false, err
	}
	return len(existing) > 0, nil
}

// UnseenMessages 从消息列表中过滤出尚未写出（非 written）的消息。
// 返回未见过的消息列表和已见过（written）的消息数量。
func (s *Store) UnseenMessages(ctx context.Context, messages []domain.Message) ([]domain.Message, int, error) {
	statuses, err := s.ExistingMessageStatuses(ctx, messageIDs(messages))
	if err != nil {
		return nil, 0, err
	}
	unseen := make([]domain.Message, 0, len(messages))
	seen := 0
	for _, msg := range messages {
		if msg.ID == "" {
			continue
		}
		if statuses[msg.ID] == MessageStatusWritten {
			seen++
			continue
		}
		unseen = append(unseen, msg)
	}
	return unseen, seen, nil
}

// messageIDs 从消息列表中提取非空 ID 列表。
func messageIDs(messages []domain.Message) []string {
	ids := make([]string, 0, len(messages))
	for _, msg := range messages {
		if msg.ID != "" {
			ids = append(ids, msg.ID)
		}
	}
	return ids
}

// PrepareMessageRecords 在事务中将消息记录写入 messages 表（状态设为 pending），
// 同时更新 sessions 表的元数据。返回实际需要写出到 Sink 的记录（排除已 written 的）。
//
// 采用"INSERT ON CONFLICT DO NOTHING + UPDATE WHERE status <> written"的双重防重策略：
// - INSERT ON CONFLICT DO NOTHING：并发安全地插入新记录，已存在时跳过
// - UPDATE：仅更新非 written 状态的记录，防止覆盖已完成的记录
// - 最终批量 SELECT status 确认状态，跳过已 written 的记录
//
// 使用 pgx Batch 将所有 INSERT 和 UPDATE 合并发送，减少网络往返。
func (s *Store) PrepareMessageRecords(ctx context.Context, session domain.Session, records []domain.MessageRecord, preparedAt int64) ([]domain.MessageRecord, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// 过滤空 ID 的记录
	validRecords := make([]domain.MessageRecord, 0, len(records))
	for _, record := range records {
		if record.MessageID != "" {
			validRecords = append(validRecords, record)
		}
	}
	if len(validRecords) == 0 {
		if err := upsertSessionMetadata(ctx, tx, session, preparedAt); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}

	// 批量 INSERT + UPDATE：使用 pgx.Batch 减少网络往返
	batch := &pgx.Batch{}
	for _, record := range validRecords {
		batch.Queue(`INSERT INTO messages(
			id, session_id, created_at, prepared_at, written_at, status, last_error,
			user_id, agent_id, sink_type, output_root, output_path, output_session_file,
			output_line
		) VALUES ($1, $2, $3, $4, 0, $5, '', $6, $7, $8, $9, $10, $11, 0)
		ON CONFLICT (id) DO NOTHING`,
			record.MessageID,
			record.SessionID,
			record.MessageCreatedAt,
			preparedAt,
			MessageStatusPending,
			defaultString(record.UserID, domain.DefaultUserID),
			defaultString(record.AgentID, domain.DefaultAgentID),
			record.SinkType,
			record.OutputRoot,
			record.OutputPath,
			record.OutputSessionFile,
		)
		batch.Queue(`UPDATE messages SET
			prepared_at = $1, status = $2, last_error = '',
			user_id = $3, agent_id = $4, sink_type = $5,
			output_root = $6, output_path = $7, output_session_file = $8
		WHERE id = $9 AND status <> $10`,
			preparedAt,
			MessageStatusPending,
			defaultString(record.UserID, domain.DefaultUserID),
			defaultString(record.AgentID, domain.DefaultAgentID),
			record.SinkType,
			record.OutputRoot,
			record.OutputPath,
			record.OutputSessionFile,
			record.MessageID,
			MessageStatusWritten,
		)
	}
	br := tx.SendBatch(ctx, batch)
	// 消费所有 INSERT+UPDATE 结果
	for range len(validRecords) * 2 {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return nil, err
		}
	}
	if err := br.Close(); err != nil {
		return nil, err
	}

	// 批量 SELECT 最终状态，确定哪些记录需要写出
	ids := make([]string, len(validRecords))
	for i, r := range validRecords {
		ids[i] = r.MessageID
	}
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	query := `SELECT id, status FROM messages WHERE id IN (` + strings.Join(placeholders, ",") + `)`
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	statusMap := make(map[string]string, len(ids))
	for rows.Next() {
		var id, status string
		if err := rows.Scan(&id, &status); err != nil {
			rows.Close()
			return nil, err
		}
		statusMap[id] = status
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	prepared := make([]domain.MessageRecord, 0, len(validRecords))
	for _, record := range validRecords {
		if statusMap[record.MessageID] != MessageStatusWritten {
			prepared = append(prepared, record)
		}
	}

	if err := upsertSessionMetadata(ctx, tx, session, preparedAt); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return prepared, nil
}

// MarkMessagesWritten 在事务中将消息状态更新为 written，并推进 Session 的消息游标和文件大小。
// 同时写入 output_line（行号），并将本批最后一条消息写入后的文件末尾字节大小更新到 sessions.file_size。
// 必须在 Sink.WriteMessages 成功后调用，确保状态领先于输出。
func (s *Store) MarkMessagesWritten(ctx context.Context, session domain.Session, records []domain.MessageRecord, writtenAt int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	latestID := ""
	latestCreatedAt := int64(0)

	// 使用 pgx.Batch 批量发送 UPDATE，减少网络往返
	batch := &pgx.Batch{}
	for _, record := range records {
		if record.MessageID == "" {
			continue
		}
		batch.Queue(
			`UPDATE messages SET status = $1, written_at = $2, last_error = '', output_line = $3 WHERE id = $4`,
			MessageStatusWritten, writtenAt, record.OutputLine, record.MessageID)
		// 找出本批中时间最新的消息，用于推进 Session 游标
		if record.MessageCreatedAt > latestCreatedAt || (record.MessageCreatedAt == latestCreatedAt && record.MessageID > latestID) {
			latestID = record.MessageID
			latestCreatedAt = record.MessageCreatedAt
		}
	}
	if batch.Len() > 0 {
		br := tx.SendBatch(ctx, batch)
		for range batch.Len() {
			if _, err := br.Exec(); err != nil {
				br.Close()
				return err
			}
		}
		if err := br.Close(); err != nil {
			return err
		}
	}
	// 取本批最后一条 record 的 OutputOffset 作为 session 的 JSONL 文件大小
	var fileSize int64
	if len(records) > 0 {
		fileSize = records[len(records)-1].OutputOffset
	}
	if err := updateSessionCursor(ctx, tx, session, latestID, latestCreatedAt, writtenAt, fileSize); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// MarkMessagesFailed 将消息状态保留为 pending 并记录错误信息，供下轮重试。
// errText 超过 512 字节时以 UTF-8 字符边界截断，确保写入 PostgreSQL 的始终是合法 UTF-8。
func (s *Store) MarkMessagesFailed(ctx context.Context, records []domain.MessageRecord, errText string) error {
	if len(errText) > 512 {
		// 从字节位置 512 向前回退，找到合法的 UTF-8 字符边界，
		// 确保写入 PostgreSQL TEXT 字段的始终是合法 UTF-8（防止截断多字节字符如中文）
		b := errText[:512]
		for !utf8.ValidString(b) && len(b) > 0 {
			b = b[:len(b)-1]
		}
		errText = b
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 使用 pgx.Batch 批量发送 UPDATE，减少网络往返
	batch := &pgx.Batch{}
	for _, record := range records {
		if record.MessageID == "" {
			continue
		}
		// 只更新仍为 pending 状态的记录，防止覆盖已 written 的记录
		batch.Queue(
			`UPDATE messages SET last_error = $1 WHERE id = $2 AND status = $3`,
			errText, record.MessageID, MessageStatusPending)
	}
	if batch.Len() > 0 {
		br := tx.SendBatch(ctx, batch)
		for range batch.Len() {
			if _, err := br.Exec(); err != nil {
				br.Close()
				return err
			}
		}
		if err := br.Close(); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// UpdateSessionFetchStats 记录本次 Session 消息拉取的统计信息。
// 使用 UPSERT 语义：Session 不存在时插入，已存在时只更新拉取统计字段。
func (s *Store) UpdateSessionFetchStats(ctx context.Context, sessionID string, reachedLimit bool, count int, limit int, fetchedAt int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO sessions(id, last_fetch_reached_limit, last_fetch_count, last_fetch_limit, last_fetch_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT(id) DO UPDATE SET
			last_fetch_reached_limit = EXCLUDED.last_fetch_reached_limit,
			last_fetch_count = EXCLUDED.last_fetch_count,
			last_fetch_limit = EXCLUDED.last_fetch_limit,
			last_fetch_at = EXCLUDED.last_fetch_at`,
		sessionID, reachedLimit, count, limit, fetchedAt)
	return err
}

// upsertSessionMetadata 在事务中更新 Session 的元数据字段（不含游标）。
// raw_json 无效时存空字符串，避免写入损坏数据。
func upsertSessionMetadata(ctx context.Context, tx pgx.Tx, session domain.Session, syncedAt int64) error {
	raw := string(session.Raw)
	if !json.Valid(session.Raw) {
		raw = ""
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO sessions(id, user_id, agent_id, updated_at, raw_json, synced_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT(id) DO UPDATE SET
			user_id = EXCLUDED.user_id,
			agent_id = EXCLUDED.agent_id,
			updated_at = EXCLUDED.updated_at,
			raw_json = EXCLUDED.raw_json,
			synced_at = EXCLUDED.synced_at`,
		session.ID, defaultString(session.UserID, domain.DefaultUserID), defaultString(session.AgentID, domain.DefaultAgentID), session.UpdatedAt, raw, syncedAt)
	return err
}

// updateSessionCursor 在事务中推进 Session 的消息游标（latest_message_id / latest_message_created_at）和文件大小。
// 采用"只前进不后退"策略：若已有游标比本批更新，保留已有游标，防止并发写入导致游标倒退。
// fileSize 为本批写入后的 JSONL 文件字节大小，只增不减。
func updateSessionCursor(ctx context.Context, tx pgx.Tx, session domain.Session, latestID string, latestCreatedAt int64, syncedAt int64, fileSize int64) error {
	prev, ok, err := getSessionStateTx(ctx, tx, session.ID)
	if err != nil {
		return err
	}
	// latestID 为空说明本批无新消息，保留已有游标
	if latestID == "" && ok {
		latestID = prev.LatestMessageID
		latestCreatedAt = prev.LatestMessageCreatedAt
	}
	// 已有游标更新则保留，防止游标倒退
	if ok && (prev.LatestMessageCreatedAt > latestCreatedAt || (prev.LatestMessageCreatedAt == latestCreatedAt && prev.LatestMessageID > latestID)) {
		latestID = prev.LatestMessageID
		latestCreatedAt = prev.LatestMessageCreatedAt
	}
	// file_size 只前进不后退（JSONL 为 append-only）
	if ok && prev.FileSize > fileSize {
		fileSize = prev.FileSize
	}
	raw := string(session.Raw)
	if !json.Valid(session.Raw) {
		raw = ""
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO sessions(id, user_id, agent_id, updated_at, latest_message_id, latest_message_created_at, raw_json, synced_at, file_size)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT(id) DO UPDATE SET
			user_id = EXCLUDED.user_id,
			agent_id = EXCLUDED.agent_id,
			updated_at = EXCLUDED.updated_at,
			latest_message_id = EXCLUDED.latest_message_id,
			latest_message_created_at = EXCLUDED.latest_message_created_at,
			raw_json = EXCLUDED.raw_json,
			synced_at = EXCLUDED.synced_at,
			file_size = EXCLUDED.file_size`,
		session.ID, defaultString(session.UserID, domain.DefaultUserID), defaultString(session.AgentID, domain.DefaultAgentID), session.UpdatedAt, latestID, latestCreatedAt, raw, syncedAt, fileSize)
	return err
}

// CommitSessionSync 将 PrepareMessageRecords 和 MarkMessagesWritten 合并为单次原子操作。
// 适用于不需要分离 prepare/write 阶段的场景（如测试初始化）。
func (s *Store) CommitSessionSync(ctx context.Context, session domain.Session, records []domain.MessageRecord, syncedAt int64) error {
	prepared, err := s.PrepareMessageRecords(ctx, session, records, syncedAt)
	if err != nil {
		return err
	}
	return s.MarkMessagesWritten(ctx, session, prepared, syncedAt)
}

// getSessionStateTx 在事务中查询 Session 状态，未找到时返回 (zero, false, nil)。
func getSessionStateTx(ctx context.Context, tx pgx.Tx, sessionID string) (SessionState, bool, error) {
	var state SessionState
	err := tx.QueryRow(ctx,
		`SELECT id, user_id, agent_id, updated_at, latest_message_id, latest_message_created_at, raw_json, synced_at, last_fetch_reached_limit, last_fetch_count, last_fetch_limit, last_fetch_at, file_size, memorized_offset, memorized_at
		FROM sessions WHERE id = $1`, sessionID).
		Scan(&state.ID, &state.UserID, &state.AgentID, &state.UpdatedAt, &state.LatestMessageID, &state.LatestMessageCreatedAt, &state.RawJSON, &state.SyncedAt, &state.LastFetchReachedLimit, &state.LastFetchCount, &state.LastFetchLimit, &state.LastFetchAt, &state.FileSize, &state.MemorizedOffset, &state.MemorizedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SessionState{}, false, nil
		}
		return SessionState{}, false, err
	}
	return state, true, nil
}

// defaultString 若 value 为空则返回 fallback，否则返回 value。
func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// NowMillis 返回当前 Unix 时间戳（毫秒）。
func NowMillis() int64 {
	return time.Now().UnixMilli()
}
