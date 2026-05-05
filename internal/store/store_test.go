package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"session_watcher/internal/domain"
)

func TestStoreSessionAndMessageState(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	assertColumnExists(t, ctx, st.db, "sessions", "raw_json", true)
	assertColumnExists(t, ctx, st.db, "sessions", "last_fetch_reached_limit", true)
	assertColumnExists(t, ctx, st.db, "messages", "raw_json", false)
	assertColumnExists(t, ctx, st.db, "messages", "status", true)
	assertTableExists(t, ctx, st.db, "schema_migrations", false)

	_, found, err := st.GetSessionState(ctx, "ses_1")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("unexpected existing session")
	}

	session := domain.Session{ID: "ses_1", UserID: "user_1", AgentID: "agent_1", UpdatedAt: 10, Raw: []byte(`{"id":"ses_1","time":{"updated":10}}`)}
	records := []domain.MessageRecord{{
		SyncedAt:          30,
		UserID:            "user_1",
		AgentID:           "agent_1",
		SessionID:         "ses_1",
		MessageID:         "msg_1",
		MessageCreatedAt:  20,
		Session:           session.Raw,
		Message:           []byte(`{"info":{"id":"msg_1"}}`),
		SinkType:          "jsonl",
		OutputRoot:        "./data/messages",
		OutputPath:        "data/messages/user_1/agent_1/ses_1.jsonl",
		OutputSessionFile: "ses_1.jsonl",
	}}
	prepared, err := st.PrepareMessageRecords(ctx, session, records, 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared) != 1 {
		t.Fatalf("prepared = %d", len(prepared))
	}
	exists, err := st.MessageExists(ctx, "msg_1")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("pending message should not be considered written")
	}
	if err := st.MarkMessagesWritten(ctx, session, prepared, 40); err != nil {
		t.Fatal(err)
	}

	state, found, err := st.GetSessionState(ctx, "ses_1")
	if err != nil || !found {
		t.Fatalf("state found=%v err=%v", found, err)
	}
	if state.LatestMessageID != "msg_1" || state.LatestMessageCreatedAt != 20 {
		t.Fatalf("state = %#v", state)
	}
	if state.UserID != "user_1" || state.AgentID != "agent_1" {
		t.Fatalf("state metadata = %#v", state)
	}
	if state.RawJSON == "" {
		t.Fatal("session raw_json should be retained")
	}
	exists, err = st.MessageExists(ctx, "msg_1")
	if err != nil || !exists {
		t.Fatalf("MessageExists = %v, %v", exists, err)
	}
	unseen, seen, err := st.UnseenMessages(ctx, []domain.Message{{ID: "msg_1", SessionID: "ses_1", CreatedAt: 20}})
	if err != nil {
		t.Fatal(err)
	}
	if len(unseen) != 0 || seen != 1 {
		t.Fatalf("unseen=%d seen=%d", len(unseen), seen)
	}
	existing, err := st.ExistingMessageIDs(ctx, []string{"msg_1", "msg_1", "missing", ""})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := existing["msg_1"]; !ok || len(existing) != 1 {
		t.Fatalf("existing ids = %#v", existing)
	}

	row := st.db.QueryRowContext(ctx, `SELECT user_id, agent_id, sink_type, output_root, output_path, output_session_file, status, written_at FROM messages WHERE id = ?`, "msg_1")
	var userID, agentID, sinkType, outputRoot, outputPath, outputSessionFile, status string
	var writtenAt int64
	if err := row.Scan(&userID, &agentID, &sinkType, &outputRoot, &outputPath, &outputSessionFile, &status, &writtenAt); err != nil {
		t.Fatal(err)
	}
	if userID != "user_1" || agentID != "agent_1" || sinkType != "jsonl" || outputRoot != "./data/messages" || outputPath != "data/messages/user_1/agent_1/ses_1.jsonl" || outputSessionFile != "ses_1.jsonl" || status != MessageStatusWritten || writtenAt != 40 {
		t.Fatalf("output tracking = %q %q %q %q %q %q %q %d", userID, agentID, sinkType, outputRoot, outputPath, outputSessionFile, status, writtenAt)
	}
}

func TestStorePendingMessagesAreRetried(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	session := domain.Session{ID: "ses_1", UserID: "user", AgentID: "agent", UpdatedAt: 1, Raw: []byte(`{"id":"ses_1"}`)}
	record := domain.MessageRecord{UserID: "user", AgentID: "agent", SessionID: "ses_1", MessageID: "msg_pending", MessageCreatedAt: 1, SinkType: "jsonl"}
	if _, err := st.PrepareMessageRecords(ctx, session, []domain.MessageRecord{record}, 10); err != nil {
		t.Fatal(err)
	}
	unseen, seen, err := st.UnseenMessages(ctx, []domain.Message{{ID: "msg_pending", SessionID: "ses_1", CreatedAt: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if seen != 0 || len(unseen) != 1 {
		t.Fatalf("seen=%d unseen=%d", seen, len(unseen))
	}
	if err := st.MarkMessagesFailed(ctx, []domain.MessageRecord{record}, strings.Repeat("x", 600)); err != nil {
		t.Fatal(err)
	}
	var lastError string
	if err := st.db.QueryRowContext(ctx, `SELECT last_error FROM messages WHERE id = ?`, "msg_pending").Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if len(lastError) != 512 {
		t.Fatalf("last_error len = %d", len(lastError))
	}
}

func TestStoreBatchExistingMessageIDs(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	session := domain.Session{ID: "ses_bulk", UserID: "user", AgentID: "agent", UpdatedAt: 1, Raw: []byte(`{"id":"ses_bulk"}`)}
	records := make([]domain.MessageRecord, 0, 1000)
	for i := 0; i < 1000; i++ {
		id := "msg_bulk_" + strconv.Itoa(i)
		records = append(records, domain.MessageRecord{UserID: "user", AgentID: "agent", SessionID: session.ID, MessageID: id, MessageCreatedAt: int64(i), SinkType: "jsonl"})
	}
	prepared, err := st.PrepareMessageRecords(ctx, session, records, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMessagesWritten(ctx, session, prepared, 2); err != nil {
		t.Fatal(err)
	}
	messages := make([]domain.Message, 0, 1000)
	for i, record := range records {
		messages = append(messages, domain.Message{ID: record.MessageID, SessionID: session.ID, CreatedAt: int64(i)})
	}
	unseen, seen, err := st.UnseenMessages(ctx, messages)
	if err != nil {
		t.Fatal(err)
	}
	if seen != 1000 || len(unseen) != 0 {
		t.Fatalf("seen=%d unseen=%d", seen, len(unseen))
	}
}

// TestStoreAnyMessageExists 验证 AnyMessageExists 在消息存在/不存在时返回正确结果。
// 这是 fetchUntilBoundary 边界探测的核心调用路径，必须有独立测试。
func TestStoreAnyMessageExists(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	session := domain.Session{ID: "ses_any", UserID: "u", AgentID: "a", UpdatedAt: 1, Raw: []byte(`{"id":"ses_any"}`)}
	msgs := []domain.Message{
		{ID: "msg_any_1", SessionID: session.ID, CreatedAt: 1},
		{ID: "msg_any_2", SessionID: session.ID, CreatedAt: 2},
	}

	// 初始状态：消息未写入，AnyMessageExists 应返回 false
	exists, err := st.AnyMessageExists(ctx, msgs)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("AnyMessageExists should be false before any messages are written")
	}

	// 写入第一条消息
	rec := []domain.MessageRecord{{UserID: "u", AgentID: "a", SessionID: session.ID, MessageID: "msg_any_1", MessageCreatedAt: 1, SinkType: "jsonl"}}
	prepared, err := st.PrepareMessageRecords(ctx, session, rec, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMessagesWritten(ctx, session, prepared, 2); err != nil {
		t.Fatal(err)
	}

	// 写入后：列表中含已处理消息，AnyMessageExists 应返回 true
	exists, err = st.AnyMessageExists(ctx, msgs)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("AnyMessageExists should be true after msg_any_1 is written")
	}

	// 空列表：AnyMessageExists 应返回 false
	exists, err = st.AnyMessageExists(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("AnyMessageExists should be false for empty message list")
	}
}

func TestStoreFetchStats(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.UpdateSessionFetchStats(ctx, "ses_1", true, 1000, 1000, 123); err != nil {
		t.Fatal(err)
	}
	state, found, err := st.GetSessionState(ctx, "ses_1")
	if err != nil || !found {
		t.Fatalf("state found=%v err=%v", found, err)
	}
	if !state.LastFetchReachedLimit || state.LastFetchCount != 1000 || state.LastFetchLimit != 1000 || state.LastFetchAt != 123 {
		t.Fatalf("state fetch stats = %#v", state)
	}
}

func TestStoreSchemaSanityRejectsOldSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE messages (id TEXT PRIMARY KEY, raw_json TEXT NOT NULL);`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Open(ctx, path)
	if err == nil || !strings.Contains(err.Error(), "messages.session_id missing") {
		t.Fatalf("expected schema error, got %v", err)
	}
}

func assertColumnExists(t *testing.T, ctx context.Context, db *sql.DB, table, column string, want bool) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := false
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			t.Fatal(err)
		}
		if name == column {
			got = true
			break
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("column %s.%s exists = %v, want %v", table, column, got, want)
	}
}

// TestTableColumnsRejectsUnknownTable 验证 tableColumns 对非白名单表名返回错误，
// 防御未来调用方传入非受信字符串时发生 PRAGMA 注入。
func TestTableColumnsRejectsUnknownTable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, err = s.tableColumns(ctx, "unknown_table")
	if err == nil {
		t.Fatal("expected error for unknown table name, got nil")
	}
	if !strings.Contains(err.Error(), "unknown table") {
		t.Fatalf("error message should mention 'unknown table', got: %v", err)
	}
}

// TestMarkMessagesFailed_UTF8Truncation 验证含多字节 UTF-8 字符的错误信息被截断时，
// 结果仍是合法 UTF-8，防止写入 SQLite 损坏数据（I-2 fix）。
func TestMarkMessagesFailed_UTF8Truncation(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	session := domain.Session{ID: "ses_utf8", UserID: "u", AgentID: "a", UpdatedAt: 1, Raw: []byte(`{"id":"ses_utf8"}`)}
	record := domain.MessageRecord{UserID: "u", AgentID: "a", SessionID: "ses_utf8", MessageID: "msg_utf8", MessageCreatedAt: 1, SinkType: "jsonl"}
	if _, err := st.PrepareMessageRecords(ctx, session, []domain.MessageRecord{record}, 1); err != nil {
		t.Fatal(err)
	}
	// 构造含中文字符（每个 3 字节）且总长度 > 512 字节的错误信息
	// 171 个"错"字 = 513 字节 > 512，必定触发截断；
	// 截断后最后一个"错"被丢弃，结果应为 170 个"错" = 510 字节，仍为合法 UTF-8。
	longUTF8 := strings.Repeat("错", 171) // 171*3 = 513 字节，必定触发截断
	if err := st.MarkMessagesFailed(ctx, []domain.MessageRecord{record}, longUTF8); err != nil {
		t.Fatal(err)
	}
	var lastError string
	if err := st.db.QueryRowContext(ctx, `SELECT last_error FROM messages WHERE id = ?`, "msg_utf8").Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if len(lastError) > 512 {
		t.Fatalf("last_error byte length = %d, want <= 512", len(lastError))
	}
	if !utf8.ValidString(lastError) {
		t.Fatalf("last_error is not valid UTF-8: %q", lastError)
	}
	want := strings.Repeat("错", 170)
	if lastError != want {
		t.Fatalf("last_error = %q (len=%d), want %q (len=%d)", lastError, len(lastError), want, len(want))
	}
}

func assertTableExists(t *testing.T, ctx context.Context, db *sql.DB, table string, want bool) {
	t.Helper()
	var name string
	err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) && !want {
			return
		}
		t.Fatal(err)
	}
	if !want {
		t.Fatalf("table %s exists unexpectedly", table)
	}
}
