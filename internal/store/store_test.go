package store

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"session_watcher/internal/domain"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testDSN 从环境变量获取测试 PostgreSQL 连接字符串，未设置时 skip。
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set, skipping PostgreSQL integration tests")
	}
	return dsn
}

// setupTestStore 为每个测试创建独立的 schema 并初始化 Store。
// 测试结束后自动 DROP schema 清理。
func setupTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	dsn := testDSN(t)

	// 连接默认 schema 创建测试专用 schema
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect pg: %v", err)
	}
	// 用测试名生成唯一 schema 名（替换非法字符）
	schemaName := "test_" + strings.ReplaceAll(
		strings.ReplaceAll(t.Name(), "/", "_"),
		"-", "_",
	)
	// schema 名长度限制，取前 60 字符
	if len(schemaName) > 60 {
		schemaName = schemaName[:60]
	}
	schemaName = strings.ToLower(schemaName)
	if _, err := pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName)); err != nil {
		pool.Close()
		t.Fatalf("drop schema: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", schemaName)); err != nil {
		pool.Close()
		t.Fatalf("create schema: %v", err)
	}
	pool.Close()

	// 使用独立 schema 的 DSN 打开 Store
	schemaDSN := dsn
	if strings.Contains(dsn, "?") {
		schemaDSN += "&search_path=" + schemaName
	} else {
		schemaDSN += "?search_path=" + schemaName
	}
	st, err := Open(ctx, schemaDSN)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		st.Close()
		// 清理 schema
		cleanPool, err := pgxpool.New(context.Background(), dsn)
		if err == nil {
			cleanPool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
			cleanPool.Close()
		}
	})
	return st
}

func TestStoreSessionAndMessageState(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	// 验证核心列存在
	cols, err := st.tableColumns(ctx, "messages")
	if err != nil {
		t.Fatal(err)
	}
	for _, col := range []string{"output_line", "status"} {
		if _, ok := cols[col]; !ok {
			t.Fatalf("messages.%s column missing", col)
		}
	}

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

	// 模拟 Sink 写入后设置 OutputLine
	prepared[0].OutputLine = 5
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

	// 验证输出追踪字段和 output_line
	var userID, agentID, sinkType, outputRoot, outputPath, outputSessionFile, msgStatus string
	var writtenAt int64
	var outputLine int
	err = st.pool.QueryRow(ctx,
		`SELECT user_id, agent_id, sink_type, output_root, output_path, output_session_file, status, written_at, output_line FROM messages WHERE id = $1`, "msg_1").
		Scan(&userID, &agentID, &sinkType, &outputRoot, &outputPath, &outputSessionFile, &msgStatus, &writtenAt, &outputLine)
	if err != nil {
		t.Fatal(err)
	}
	if userID != "user_1" || agentID != "agent_1" || sinkType != "jsonl" || outputRoot != "./data/messages" || outputPath != "data/messages/user_1/agent_1/ses_1.jsonl" || outputSessionFile != "ses_1.jsonl" || msgStatus != MessageStatusWritten || writtenAt != 40 || outputLine != 5 {
		t.Fatalf("output tracking = %q %q %q %q %q %q %q %d %d", userID, agentID, sinkType, outputRoot, outputPath, outputSessionFile, msgStatus, writtenAt, outputLine)
	}
}

func TestStorePendingMessagesAreRetried(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

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
	if err := st.pool.QueryRow(ctx, `SELECT last_error FROM messages WHERE id = $1`, "msg_pending").Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if len(lastError) != 512 {
		t.Fatalf("last_error len = %d", len(lastError))
	}
}

func TestStoreBatchExistingMessageIDs(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	session := domain.Session{ID: "ses_bulk", UserID: "user", AgentID: "agent", UpdatedAt: 1, Raw: []byte(`{"id":"ses_bulk"}`)}
	const batchSize = 100
	records := make([]domain.MessageRecord, 0, batchSize)
	for i := 0; i < batchSize; i++ {
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
	messages := make([]domain.Message, 0, batchSize)
	for i, record := range records {
		messages = append(messages, domain.Message{ID: record.MessageID, SessionID: session.ID, CreatedAt: int64(i)})
	}
	unseen, seen, err := st.UnseenMessages(ctx, messages)
	if err != nil {
		t.Fatal(err)
	}
	if seen != batchSize || len(unseen) != 0 {
		t.Fatalf("seen=%d unseen=%d", seen, len(unseen))
	}
}

// TestStoreAnyMessageExists 验证 AnyMessageExists 在消息存在/不存在时返回正确结果。
// 这是 fetchUntilBoundary 边界探测的核心调用路径，必须有独立测试。
func TestStoreAnyMessageExists(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

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
	st := setupTestStore(t)

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

// TestMarkMessagesFailed_UTF8Truncation 验证含多字节 UTF-8 字符的错误信息被截断时，
// 结果仍是合法 UTF-8，防止写入 PostgreSQL 损坏数据。
func TestMarkMessagesFailed_UTF8Truncation(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

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
	if err := st.pool.QueryRow(ctx, `SELECT last_error FROM messages WHERE id = $1`, "msg_utf8").Scan(&lastError); err != nil {
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

// TestStoreMemorizedAt 验证 sessions 表的 memorized_at 字段默认值为 0。
func TestStoreMemorizedAt(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	session := domain.Session{ID: "ses_mem", UserID: "u", AgentID: "a", UpdatedAt: 1, Raw: []byte(`{"id":"ses_mem"}`)}
	record := domain.MessageRecord{UserID: "u", AgentID: "a", SessionID: "ses_mem", MessageID: "msg_mem", MessageCreatedAt: 1, SinkType: "jsonl"}
	if _, err := st.PrepareMessageRecords(ctx, session, []domain.MessageRecord{record}, 1); err != nil {
		t.Fatal(err)
	}

	var memorizedAt int64
	err := st.pool.QueryRow(ctx, `SELECT memorized_at FROM sessions WHERE id = $1`, "ses_mem").Scan(&memorizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if memorizedAt != 0 {
		t.Fatal("sessions.memorized_at should default to 0")
	}
}
