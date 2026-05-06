package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"session_watcher/internal/domain"

	_ "modernc.org/sqlite"
)

// 消息状态常量。
const (
	MessageStatusPending = "pending" // 已写入 SQLite 但尚未写出到 Sink
	MessageStatusWritten = "written" // 已成功写出到 Sink，永久去重标记
)

// SessionState 表示从 SQLite 读出的 Session 同步状态，包含游标和最近一次拉取统计。
type SessionState struct {
	ID                     string
	UserID                 string
	AgentID                string
	UpdatedAt              int64  // 最后一次同步时 Session 的远端更新时间
	LatestMessageID        string // 本地已写出的最新消息 ID（游标）
	LatestMessageCreatedAt int64  // 本地已写出的最新消息创建时间
	RawJSON                string
	SyncedAt               int64
	LastFetchReachedLimit  bool // 上次拉取是否触及 MaxMessageFetch 上限
	LastFetchCount         int  // 上次实际拉取的消息数
	LastFetchLimit         int  // 上次使用的 limit 值
	LastFetchAt            int64
}

// Store 封装 SQLite 状态数据库，提供 Session 状态管理和消息去重能力。
type Store struct {
	db *sql.DB
}

// Open 打开（或创建）SQLite 数据库，完成 PRAGMA 配置、Schema 初始化和版本检查。
// 任何步骤失败都会关闭数据库连接并返回错误。
func Open(ctx context.Context, path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// 限制为单连接，避免 SQLite 写锁竞争；WAL 模式下读不阻塞写
	db.SetMaxOpenConns(1)
	store := &Store{db: db}
	if err := store.configure(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := store.init(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := store.checkSchema(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := store.initIndexes(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

// Close 关闭数据库连接。
func (s *Store) Close() error {
	return s.db.Close()
}

// configure 设置 SQLite PRAGMA，在任何 DDL/DML 操作前执行。
// WAL 模式提升读写并发；NORMAL 同步级别在性能与持久性间取得平衡；busy_timeout 防止写锁永久等待。
func (s *Store) configure(ctx context.Context) error {
	stmts := []string{
		`PRAGMA busy_timeout = 5000;`,
		`PRAGMA journal_mode = WAL;`,
		`PRAGMA synchronous = NORMAL;`,
		`PRAGMA foreign_keys = ON;`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// init 创建核心表（幂等，已存在时跳过）。
// sessions 表记录 Session 级别的同步状态和拉取统计；
// messages 表记录消息级别的去重标记和输出追踪信息。
func (s *Store) init(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS sessions (
			id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL DEFAULT 'default_user',
			agent_id TEXT NOT NULL DEFAULT 'default_agent',
			updated_at INTEGER NOT NULL DEFAULT 0,
			latest_message_id TEXT NOT NULL DEFAULT '',
			latest_message_created_at INTEGER NOT NULL DEFAULT 0,
			raw_json TEXT NOT NULL DEFAULT '',
			synced_at INTEGER NOT NULL DEFAULT 0,
			last_fetch_reached_limit INTEGER NOT NULL DEFAULT 0,
			last_fetch_count INTEGER NOT NULL DEFAULT 0,
			last_fetch_limit INTEGER NOT NULL DEFAULT 0,
			last_fetch_at INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE IF NOT EXISTS messages (
			id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			created_at INTEGER NOT NULL DEFAULT 0,
			prepared_at INTEGER NOT NULL DEFAULT 0,
			written_at INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'pending',
			last_error TEXT NOT NULL DEFAULT '',
			user_id TEXT NOT NULL DEFAULT 'default_user',
			agent_id TEXT NOT NULL DEFAULT 'default_agent',
			sink_type TEXT NOT NULL DEFAULT 'jsonl',
			output_root TEXT NOT NULL DEFAULT '',
			output_path TEXT NOT NULL DEFAULT '',
			output_session_file TEXT NOT NULL DEFAULT ''
		);`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// initIndexes 创建查询优化索引（幂等，已存在时跳过）。
func (s *Store) initIndexes(ctx context.Context) error {
	stmts := []string{
		`CREATE INDEX IF NOT EXISTS idx_sessions_user_agent ON sessions(user_id, agent_id);`,
		`CREATE INDEX IF NOT EXISTS idx_messages_session_created ON messages(session_id, created_at);`,
		`CREATE INDEX IF NOT EXISTS idx_messages_sink_output ON messages(sink_type, output_root, output_path);`,
		`CREATE INDEX IF NOT EXISTS idx_messages_user_agent_session ON messages(user_id, agent_id, session_id);`,
		`CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status);`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// checkSchema 检查当前数据库 Schema 是否与本版本程序兼容。
// 检测到旧版 schema_migrations 表、缺少必需列或存在已删除列时拒绝启动，
// 防止新版程序在旧格式数据库上静默写错。
func (s *Store) checkSchema(ctx context.Context) error {
	// 存在 schema_migrations 表说明是早期版本的旧 DB，直接拒绝
	if exists, err := s.tableExists(ctx, "schema_migrations"); err != nil {
		return err
	} else if exists {
		return errors.New("incompatible database schema: schema_migrations table exists; remove old db or use another -db path")
	}
	sessionsColumns, err := s.tableColumns(ctx, "sessions")
	if err != nil {
		return err
	}
	messagesColumns, err := s.tableColumns(ctx, "messages")
	if err != nil {
		return err
	}
	// 检查 sessions 表所有必需列
	for _, column := range []string{"id", "user_id", "agent_id", "updated_at", "latest_message_id", "latest_message_created_at", "raw_json", "synced_at", "last_fetch_reached_limit", "last_fetch_count", "last_fetch_limit", "last_fetch_at"} {
		if _, ok := sessionsColumns[column]; !ok {
			return fmt.Errorf("incompatible database schema: sessions.%s missing; remove old db or use another -db path", column)
		}
	}
	// 检查 messages 表所有必需列
	for _, column := range []string{"id", "session_id", "created_at", "prepared_at", "written_at", "status", "last_error", "user_id", "agent_id", "sink_type", "output_root", "output_path", "output_session_file"} {
		if _, ok := messagesColumns[column]; !ok {
			return fmt.Errorf("incompatible database schema: messages.%s missing; remove old db or use another -db path", column)
		}
	}
	// messages.raw_json 是已删除的旧版字段，存在则说明 DB 是旧版本
	if _, ok := messagesColumns["raw_json"]; ok {
		return errors.New("incompatible database schema: messages.raw_json exists; remove old db or use another -db path")
	}
	return nil
}

// tableExists 检查指定表是否存在于 sqlite_master 中。
func (s *Store) tableExists(ctx context.Context, table string) (bool, error) {
	var name string
	err := s.db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// tableColumns 返回指定表的所有列名集合（用于 Schema 版本检查）。
// table 参数仅接受白名单内的表名，防止 PRAGMA 拼接时发生注入（SQLite PRAGMA 不支持参数化）。
func (s *Store) tableColumns(ctx context.Context, table string) (map[string]struct{}, error) {
	switch table {
	case "sessions", "messages":
		// 白名单内，继续执行
	default:
		return nil, fmt.Errorf("tableColumns: unknown table %q", table)
	}
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := make(map[string]struct{})
	for rows.Next() {
		var name string
		var unused [5]any
		if err := rows.Scan(&unused[0], &name, &unused[1], &unused[2], &unused[3], &unused[4]); err != nil {
			return nil, err
		}
		columns[name] = struct{}{}
	}
	return columns, rows.Err()
}

// GetSessionState 查询指定 Session 的同步状态，未找到时返回 (zero, false, nil)。
func (s *Store) GetSessionState(ctx context.Context, sessionID string) (SessionState, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, user_id, agent_id, updated_at, latest_message_id, latest_message_created_at, raw_json, synced_at, last_fetch_reached_limit, last_fetch_count, last_fetch_limit, last_fetch_at FROM sessions WHERE id = ?`, sessionID)
	var state SessionState
	var reachedLimit int
	if err := row.Scan(&state.ID, &state.UserID, &state.AgentID, &state.UpdatedAt, &state.LatestMessageID, &state.LatestMessageCreatedAt, &state.RawJSON, &state.SyncedAt, &reachedLimit, &state.LastFetchCount, &state.LastFetchLimit, &state.LastFetchAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SessionState{}, false, nil
		}
		return SessionState{}, false, err
	}
	state.LastFetchReachedLimit = reachedLimit != 0
	return state, true, nil
}

// MessageExists 检查指定消息是否已处于 written 状态（已成功写出到 Sink）。
func (s *Store) MessageExists(ctx context.Context, messageID string) (bool, error) {
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM messages WHERE id = ? AND status = ?`, messageID, MessageStatusWritten).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
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
		// 构造 IN 占位符列表
		placeholders := strings.TrimRight(strings.Repeat("?,", len(chunk)), ",")
		query := `SELECT id, status FROM messages WHERE id IN (` + placeholders + `)`
		args := make([]any, len(chunk))
		for i, id := range chunk {
			args[i] = id
		}
		rows, err := s.db.QueryContext(ctx, query, args...)
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
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
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
// 采用"INSERT OR IGNORE + UPDATE WHERE status <> written"的双重防重策略：
// - INSERT OR IGNORE：并发安全地插入新记录，已存在时跳过
// - UPDATE：仅更新非 written 状态的记录，防止覆盖已完成的记录
// - 最终 SELECT status 确认状态，跳过已 written 的记录
func (s *Store) PrepareMessageRecords(ctx context.Context, session domain.Session, records []domain.MessageRecord, preparedAt int64) ([]domain.MessageRecord, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	prepared := make([]domain.MessageRecord, 0, len(records))
	for _, record := range records {
		if record.MessageID == "" {
			continue
		}
		// 步骤 1：不存在时插入 pending 状态的新记录
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO messages(
			id,
			session_id,
			created_at,
			prepared_at,
			written_at,
			status,
			last_error,
			user_id,
			agent_id,
			sink_type,
			output_root,
			output_path,
			output_session_file
		) VALUES (?, ?, ?, ?, 0, ?, '', ?, ?, ?, ?, ?, ?)`,
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
		); err != nil {
			return nil, err
		}
		// 步骤 2：记录已存在且状态非 written 时，更新元数据并重置为 pending（幂等重试）
		if _, err := tx.ExecContext(ctx, `UPDATE messages SET
			prepared_at = ?,
			status = ?,
			last_error = '',
			user_id = ?,
			agent_id = ?,
			sink_type = ?,
			output_root = ?,
			output_path = ?,
			output_session_file = ?
		WHERE id = ? AND status <> ?`,
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
		); err != nil {
			return nil, err
		}
		// 步骤 3：查询最终状态，仅将非 written 的记录加入待输出列表
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM messages WHERE id = ?`, record.MessageID).Scan(&status); err != nil {
			return nil, err
		}
		if status != MessageStatusWritten {
			prepared = append(prepared, record)
		}
	}
	if err := upsertSessionMetadata(ctx, tx, session, preparedAt); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return prepared, nil
}

// MarkMessagesWritten 在事务中将消息状态更新为 written，并推进 Session 的消息游标。
// 必须在 Sink.WriteMessages 成功后调用，确保状态领先于输出。
func (s *Store) MarkMessagesWritten(ctx context.Context, session domain.Session, records []domain.MessageRecord, writtenAt int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	latestID := ""
	latestCreatedAt := int64(0)
	for _, record := range records {
		if record.MessageID == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE messages SET status = ?, written_at = ?, last_error = '' WHERE id = ?`, MessageStatusWritten, writtenAt, record.MessageID); err != nil {
			return err
		}
		// 找出本批中时间最新的消息，用于推进 Session 游标
		if record.MessageCreatedAt > latestCreatedAt || (record.MessageCreatedAt == latestCreatedAt && record.MessageID > latestID) {
			latestID = record.MessageID
			latestCreatedAt = record.MessageCreatedAt
		}
	}
	if err := updateSessionCursor(ctx, tx, session, latestID, latestCreatedAt, writtenAt); err != nil {
		return err
	}
	return tx.Commit()
}

// MarkMessagesFailed 将消息状态保留为 pending 并记录错误信息，供下轮重试。
// errText 超过 512 字节时以 UTF-8 字符边界截断，确保写入 SQLite 的始终是合法 UTF-8。
func (s *Store) MarkMessagesFailed(ctx context.Context, records []domain.MessageRecord, errText string) error {
	if len(errText) > 512 {
		// 从字节位置 512 向前回退，找到合法的 UTF-8 字符边界，
		// 确保写入 SQLite TEXT 字段的始终是合法 UTF-8（防止截断多字节字符如中文）
		b := errText[:512]
		for !utf8.ValidString(b) && len(b) > 0 {
			b = b[:len(b)-1]
		}
		errText = b
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, record := range records {
		if record.MessageID == "" {
			continue
		}
		// 只更新仍为 pending 状态的记录，防止覆盖已 written 的记录
		if _, err := tx.ExecContext(ctx, `UPDATE messages SET last_error = ? WHERE id = ? AND status = ?`, errText, record.MessageID, MessageStatusPending); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// UpdateSessionFetchStats 记录本次 Session 消息拉取的统计信息。
// 使用 UPSERT 语义：Session 不存在时插入，已存在时只更新拉取统计字段。
func (s *Store) UpdateSessionFetchStats(ctx context.Context, sessionID string, reachedLimit bool, count int, limit int, fetchedAt int64) error {
	reached := 0
	if reachedLimit {
		reached = 1
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO sessions(id, last_fetch_reached_limit, last_fetch_count, last_fetch_limit, last_fetch_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			last_fetch_reached_limit = excluded.last_fetch_reached_limit,
			last_fetch_count = excluded.last_fetch_count,
			last_fetch_limit = excluded.last_fetch_limit,
			last_fetch_at = excluded.last_fetch_at`, sessionID, reached, count, limit, fetchedAt)
	return err
}

// upsertSessionMetadata 在事务中更新 Session 的元数据字段（不含游标）。
// raw_json 无效时存空字符串，避免写入损坏数据。
func upsertSessionMetadata(ctx context.Context, tx *sql.Tx, session domain.Session, syncedAt int64) error {
	raw := string(session.Raw)
	if !json.Valid(session.Raw) {
		raw = ""
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO sessions(id, user_id, agent_id, updated_at, raw_json, synced_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			user_id = excluded.user_id,
			agent_id = excluded.agent_id,
			updated_at = excluded.updated_at,
			raw_json = excluded.raw_json,
			synced_at = excluded.synced_at`, session.ID, defaultString(session.UserID, domain.DefaultUserID), defaultString(session.AgentID, domain.DefaultAgentID), session.UpdatedAt, raw, syncedAt)
	return err
}

// updateSessionCursor 在事务中推进 Session 的消息游标（latest_message_id / latest_message_created_at）。
// 采用"只前进不后退"策略：若已有游标比本批更新，保留已有游标，防止并发写入导致游标倒退。
func updateSessionCursor(ctx context.Context, tx *sql.Tx, session domain.Session, latestID string, latestCreatedAt int64, syncedAt int64) error {
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
	raw := string(session.Raw)
	if !json.Valid(session.Raw) {
		raw = ""
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO sessions(id, user_id, agent_id, updated_at, latest_message_id, latest_message_created_at, raw_json, synced_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			user_id = excluded.user_id,
			agent_id = excluded.agent_id,
			updated_at = excluded.updated_at,
			latest_message_id = excluded.latest_message_id,
			latest_message_created_at = excluded.latest_message_created_at,
			raw_json = excluded.raw_json,
			synced_at = excluded.synced_at`, session.ID, defaultString(session.UserID, domain.DefaultUserID), defaultString(session.AgentID, domain.DefaultAgentID), session.UpdatedAt, latestID, latestCreatedAt, raw, syncedAt)
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
func getSessionStateTx(ctx context.Context, tx *sql.Tx, sessionID string) (SessionState, bool, error) {
	row := tx.QueryRowContext(ctx, `SELECT id, user_id, agent_id, updated_at, latest_message_id, latest_message_created_at, raw_json, synced_at, last_fetch_reached_limit, last_fetch_count, last_fetch_limit, last_fetch_at FROM sessions WHERE id = ?`, sessionID)
	var state SessionState
	var reachedLimit int
	if err := row.Scan(&state.ID, &state.UserID, &state.AgentID, &state.UpdatedAt, &state.LatestMessageID, &state.LatestMessageCreatedAt, &state.RawJSON, &state.SyncedAt, &reachedLimit, &state.LastFetchCount, &state.LastFetchLimit, &state.LastFetchAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SessionState{}, false, nil
		}
		return SessionState{}, false, err
	}
	state.LastFetchReachedLimit = reachedLimit != 0
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
