package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"session_watcher/internal/domain"
	"session_watcher/internal/store"
)

// testDSN 从环境变量获取测试 PostgreSQL 连接字符串，未设置时 skip。
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set, skipping watcher integration tests")
	}
	return dsn
}

// setupTestStore 为 watcher 测试创建独立的 PG schema 并初始化 store。
func setupTestStore(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()
	dsn := testDSN(t)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect pg: %v", err)
	}
	schemaName := "test_watcher_" + strings.ReplaceAll(
		strings.ReplaceAll(t.Name(), "/", "_"),
		"-", "_",
	)
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

	schemaDSN := dsn
	if strings.Contains(dsn, "?") {
		schemaDSN += "&search_path=" + schemaName
	} else {
		schemaDSN += "?search_path=" + schemaName
	}
	st, err := store.Open(ctx, schemaDSN)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		st.Close()
		cleanPool, err := pgxpool.New(context.Background(), dsn)
		if err == nil {
			cleanPool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
			cleanPool.Close()
		}
	})
	return st
}

type fakeSource struct {
	mu       sync.Mutex
	sessions []domain.Session
	details  map[string]domain.Session
	messages map[string][]domain.Message
	limits   []int
	fail     map[string]bool
}

func (f *fakeSource) ListSessions(ctx context.Context) ([]domain.Session, error) {
	return append([]domain.Session(nil), f.sessions...), nil
}

func (f *fakeSource) GetSession(ctx context.Context, sessionID string) (domain.Session, error) {
	if f.fail[sessionID] {
		return domain.Session{}, errors.New("failed")
	}
	return f.details[sessionID], nil
}

func (f *fakeSource) ListMessages(ctx context.Context, sessionID string, limit int) ([]domain.Message, error) {
	f.mu.Lock()
	f.limits = append(f.limits, limit)
	f.mu.Unlock()
	messages := f.messages[sessionID]
	if len(messages) > limit {
		return append([]domain.Message(nil), messages[:limit]...), nil
	}
	return append([]domain.Message(nil), messages...), nil
}

type memorySink struct {
	mu      sync.Mutex
	records []domain.MessageRecord
}

func (s *memorySink) WriteMessages(ctx context.Context, records []domain.MessageRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, records...)
	return nil
}

func (s *memorySink) Close() error { return nil }

func (s *memorySink) PathFor(record domain.MessageRecord) string {
	return filepath.Join("memory-root", record.UserID, record.AgentID, record.SessionID+".jsonl")
}

func (s *memorySink) SinkType() string { return "memory" }

func (s *memorySink) OutputRoot() string { return "memory-root" }

func TestWatcherDedupAndLimitGrowth(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	seenSession := session("ses_1", 1)
	seenMessage := message("ses_1", "msg_seen", 1)
	if err := st.CommitSessionSync(ctx, seenSession, []domain.MessageRecord{recordFromMessage(seenSession, seenMessage)}, 1); err != nil {
		t.Fatal(err)
	}

	source := &fakeSource{
		sessions: []domain.Session{session("ses_1", 2)},
		details:  map[string]domain.Session{"ses_1": session("ses_1", 2)},
		messages: map[string][]domain.Message{"ses_1": {
			message("ses_1", "msg_4", 4),
			message("ses_1", "msg_3", 3),
			message("ses_1", "msg_2", 2),
			message("ses_1", "msg_seen", 1),
		}},
	}
	sink := &memorySink{}
	w := New(source, sink, st, Config{MessageLimit: 2, MaxMessageFetch: 10, SessionWorkers: 1}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	result, err := w.SyncOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.MessagesNew != 3 {
		t.Fatalf("MessagesNew = %d", result.MessagesNew)
	}
	if len(sink.records) != 3 {
		t.Fatalf("records = %d", len(sink.records))
	}
	if sink.records[0].MessageID != "msg_2" || sink.records[2].MessageID != "msg_4" {
		t.Fatalf("records order = %#v", sink.records)
	}
	if sink.records[0].UserID != "user_ses_1" || sink.records[0].AgentID != "agent_ses_1" {
		t.Fatalf("record metadata = %#v", sink.records[0])
	}
	if sink.records[0].SinkType != "memory" || sink.records[0].OutputRoot != "memory-root" || sink.records[0].OutputSessionFile != "ses_1.jsonl" {
		t.Fatalf("record output tracking = %#v", sink.records[0])
	}
	if len(source.limits) != 2 || source.limits[0] != 2 || source.limits[1] != 4 {
		t.Fatalf("limits = %v", source.limits)
	}

	result, err = w.SyncOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.MessagesNew != 0 {
		t.Fatalf("second MessagesNew = %d", result.MessagesNew)
	}
}

func TestWatcherStopsAtMaxMessageFetch(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	msgs := []domain.Message{
		message("ses_1", "msg_5", 5),
		message("ses_1", "msg_4", 4),
		message("ses_1", "msg_3", 3),
		message("ses_1", "msg_2", 2),
		message("ses_1", "msg_1", 1),
	}
	source := &fakeSource{
		sessions: []domain.Session{session("ses_1", 2)},
		details:  map[string]domain.Session{"ses_1": session("ses_1", 2)},
		messages: map[string][]domain.Message{"ses_1": msgs},
	}
	sink := &memorySink{}
	w := New(source, sink, st, Config{MessageLimit: 2, MaxMessageFetch: 3, SessionWorkers: 1}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	result, err := w.SyncOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.MessagesNew != 3 {
		t.Fatalf("MessagesNew = %d", result.MessagesNew)
	}
	if len(source.limits) != 2 || source.limits[0] != 2 || source.limits[1] != 3 {
		t.Fatalf("limits = %v", source.limits)
	}
}

func TestWatcherSessionFailureDoesNotBlockOthers(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	source := &fakeSource{
		sessions: []domain.Session{session("bad", 1), session("good", 1)},
		details:  map[string]domain.Session{"good": session("good", 1)},
		messages: map[string][]domain.Message{"good": {message("good", "msg_good", 1)}},
		fail:     map[string]bool{"bad": true},
	}
	sink := &memorySink{}
	w := New(source, sink, st, Config{MessageLimit: 2, MaxMessageFetch: 10, SessionWorkers: 2}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	result, err := w.SyncOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.SessionsFailed != 1 || result.SessionsSynced != 1 || result.MessagesNew != 1 {
		t.Fatalf("result = %#v", result)
	}
}

func session(id string, updated int64) domain.Session {
	userID := "user_" + id
	agentID := "agent_" + id
	raw, _ := json.Marshal(map[string]any{"id": id, "user_id": userID, "agent_id": agentID, "time": map[string]any{"updated": updated}})
	return domain.Session{ID: id, UserID: userID, AgentID: agentID, UpdatedAt: updated, Raw: raw}
}

func message(sessionID, id string, created int64) domain.Message {
	raw, _ := json.Marshal(map[string]any{"info": map[string]any{"id": id, "sessionID": sessionID, "time": map[string]any{"created": created}}, "parts": []any{}})
	return domain.Message{ID: id, SessionID: sessionID, CreatedAt: created, Raw: raw}
}

func recordFromMessage(session domain.Session, message domain.Message) domain.MessageRecord {
	return domain.MessageRecord{
		SyncedAt:         1,
		UserID:           session.UserID,
		AgentID:          session.AgentID,
		SessionID:        session.ID,
		MessageID:        message.ID,
		MessageCreatedAt: message.CreatedAt,
		Session:          session.Raw,
		Message:          message.Raw,
		SinkType:         "test",
		OutputRoot:       "",
		OutputPath:       "",
	}
}

// TestMergeSessionMetadata 覆盖 mergeSessionMetadata 的各个 fallback 分支。
func TestMergeSessionMetadata(t *testing.T) {
	cases := []struct {
		name        string
		detail      domain.Session
		listed      domain.Session
		wantUserID  string
		wantAgentID string
	}{
		{
			name:        "detail 有值：直接使用 detail 的字段",
			detail:      domain.Session{UserID: "u_detail", AgentID: "a_detail"},
			listed:      domain.Session{UserID: "u_listed", AgentID: "a_listed"},
			wantUserID:  "u_detail",
			wantAgentID: "a_detail",
		},
		{
			name:        "detail UserID 为空：用 listed 的 UserID",
			detail:      domain.Session{UserID: "", AgentID: "a_detail"},
			listed:      domain.Session{UserID: "u_listed", AgentID: "a_listed"},
			wantUserID:  "u_listed",
			wantAgentID: "a_detail",
		},
		{
			name:        "detail AgentID 为空：用 listed 的 AgentID",
			detail:      domain.Session{UserID: "u_detail", AgentID: ""},
			listed:      domain.Session{UserID: "u_listed", AgentID: "a_listed"},
			wantUserID:  "u_detail",
			wantAgentID: "a_listed",
		},
		{
			name:        "detail UserID 为 DefaultUserID：用 listed 的 UserID",
			detail:      domain.Session{UserID: domain.DefaultUserID, AgentID: "a_detail"},
			listed:      domain.Session{UserID: "u_listed", AgentID: "a_listed"},
			wantUserID:  "u_listed",
			wantAgentID: "a_detail",
		},
		{
			name:        "detail AgentID 为 DefaultAgentID：用 listed 的 AgentID",
			detail:      domain.Session{UserID: "u_detail", AgentID: domain.DefaultAgentID},
			listed:      domain.Session{UserID: "u_listed", AgentID: "a_listed"},
			wantUserID:  "u_detail",
			wantAgentID: "a_listed",
		},
		{
			name:        "detail 和 listed 都为空：使用 DefaultUserID/DefaultAgentID 兜底",
			detail:      domain.Session{},
			listed:      domain.Session{},
			wantUserID:  domain.DefaultUserID,
			wantAgentID: domain.DefaultAgentID,
		},
		{
			name:        "detail UserID 为空且 listed 也为空：兜底为 DefaultUserID",
			detail:      domain.Session{UserID: "", AgentID: "a_detail"},
			listed:      domain.Session{UserID: "", AgentID: "a_listed"},
			wantUserID:  domain.DefaultUserID,
			wantAgentID: "a_detail",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mergeSessionMetadata(tc.detail, tc.listed)
			if got.UserID != tc.wantUserID {
				t.Errorf("UserID = %q, want %q", got.UserID, tc.wantUserID)
			}
			if got.AgentID != tc.wantAgentID {
				t.Errorf("AgentID = %q, want %q", got.AgentID, tc.wantAgentID)
			}
		})
	}
}
