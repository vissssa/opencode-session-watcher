package jsonl

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"session_watcher/internal/domain"
)

func TestFileSinkWritesPerSessionFile(t *testing.T) {
	root := t.TempDir()
	sink, err := NewFileSink(root, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if sink.SinkType() != "jsonl" {
		t.Fatalf("SinkType = %q", sink.SinkType())
	}
	if sink.OutputRoot() != root {
		t.Fatalf("OutputRoot = %q", sink.OutputRoot())
	}

	records := []domain.MessageRecord{
		{SyncedAt: 1, UserID: "user_1", AgentID: "agent_1", SessionID: "ses_1", MessageID: "msg_1", Session: []byte(`{"id":"ses_1"}`), Message: []byte(`{"info":{"id":"msg_1"}}`)},
		{SyncedAt: 2, UserID: "user_1", AgentID: "agent_1", SessionID: "ses_2", MessageID: "msg_2", Session: []byte(`{"id":"ses_2"}`), Message: []byte(`{"info":{"id":"msg_2"}}`)},
	}
	if err := sink.WriteMessages(context.Background(), records); err != nil {
		t.Fatal(err)
	}

	if got := sink.PathFor(records[0]); got != filepath.Join(root, "user_1", "agent_1", "ses_1.jsonl") {
		t.Fatalf("PathFor = %q", got)
	}
	assertLineCount(t, filepath.Join(root, "user_1", "agent_1", "ses_1.jsonl"), 1)
	assertLineCount(t, filepath.Join(root, "user_1", "agent_1", "ses_2.jsonl"), 1)
}

func TestFileSinkUsesDefaultMetadataAndCleansPath(t *testing.T) {
	root := t.TempDir()
	sink, err := NewFileSink(root, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	record := domain.MessageRecord{SyncedAt: 1, SessionID: "../ses/1", MessageID: "msg_1", Session: []byte(`{"id":"ses"}`), Message: []byte(`{"info":{"id":"msg_1"}}`)}
	if err := sink.WriteMessages(context.Background(), []domain.MessageRecord{record}); err != nil {
		t.Fatal(err)
	}
	assertLineCount(t, filepath.Join(root, domain.DefaultUserID, domain.DefaultAgentID, "_ses_1.jsonl"), 1)
}

func TestFileSinkConcurrentWrites(t *testing.T) {
	root := t.TempDir()
	sink, err := NewFileSink(root, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sessionID := "ses_a"
			if i%2 == 0 {
				sessionID = "ses_b"
			}
			record := domain.MessageRecord{SyncedAt: int64(i), UserID: "user", AgentID: "agent", SessionID: sessionID, MessageID: "msg", Session: []byte(`{"id":"ses"}`), Message: []byte(`{"info":{"id":"msg"}}`)}
			if err := sink.WriteMessages(context.Background(), []domain.MessageRecord{record}); err != nil {
				t.Errorf("WriteMessages: %v", err)
			}
		}(i)
	}
	wg.Wait()
	assertLineCount(t, filepath.Join(root, "user", "agent", "ses_a.jsonl"), 10)
	assertLineCount(t, filepath.Join(root, "user", "agent", "ses_b.jsonl"), 10)
}

func TestFileSinkCleansIdleLocks(t *testing.T) {
	root := t.TempDir()
	sink, err := NewFileSink(root, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	now := time.Unix(100, 0)
	sink.now = func() time.Time { return now }
	sink.lockTTL = time.Second
	sink.lockCleanupInterval = 0

	first := domain.MessageRecord{SyncedAt: 1, UserID: "user", AgentID: "agent", SessionID: "old", MessageID: "msg_1", Session: []byte(`{"id":"old"}`), Message: []byte(`{"info":{"id":"msg_1"}}`)}
	if err := sink.WriteMessages(context.Background(), []domain.MessageRecord{first}); err != nil {
		t.Fatal(err)
	}
	if got := sinkLockCount(sink); got != 1 {
		t.Fatalf("lock count = %d, want 1", got)
	}

	now = now.Add(2 * time.Second)
	second := domain.MessageRecord{SyncedAt: 2, UserID: "user", AgentID: "agent", SessionID: "new", MessageID: "msg_2", Session: []byte(`{"id":"new"}`), Message: []byte(`{"info":{"id":"msg_2"}}`)}
	if err := sink.WriteMessages(context.Background(), []domain.MessageRecord{second}); err != nil {
		t.Fatal(err)
	}
	if got := sinkLockCount(sink); got != 1 {
		t.Fatalf("lock count = %d, want 1", got)
	}
	if sinkHasLock(sink, sink.PathFor(first)) {
		t.Fatalf("old path lock was not cleaned")
	}
	assertLineCount(t, filepath.Join(root, "user", "agent", "old.jsonl"), 1)
	assertLineCount(t, filepath.Join(root, "user", "agent", "new.jsonl"), 1)
}

func sinkLockCount(sink *FileSink) int {
	sink.locksMu.Lock()
	defer sink.locksMu.Unlock()
	return len(sink.locks)
}

func sinkHasLock(sink *FileSink, path string) bool {
	sink.locksMu.Lock()
	defer sink.locksMu.Unlock()
	_, ok := sink.locks[path]
	return ok
}

func assertLineCount(t *testing.T, path string, want int) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		count++
		if !json.Valid(scanner.Bytes()) {
			t.Fatalf("invalid json line: %s", scanner.Text())
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("line count for %s = %d, want %d", path, count, want)
	}
}

// TestWriteFile_NormalWriteSucceeds 验证正常写入时 writeFile 不返回错误。
func TestWriteFile_NormalWriteSucceeds(t *testing.T) {
	root := t.TempDir()
	sink, err := NewFileSink(root, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "test.jsonl")
	records := []domain.MessageRecord{{SyncedAt: 1, UserID: "u", AgentID: "a", SessionID: "s", MessageID: "m",
		Session: []byte(`{"id":"s"}`), Message: []byte(`{"info":{"id":"m"}}`)}}
	lock := &pathLock{lineCount: -1}
	if err := sink.writeFile(path, lock, records, []int{0}); err != nil {
		t.Fatalf("unexpected error on normal write: %v", err)
	}
}

// TestWriteFile_ErrorPathsPropagated 验证 writeFile 的错误传播机制整体有效，
// 覆盖 MkdirAll 失败路径（父路径是普通文件，无法作为目录）——这是 I-1 fix 的回归保护。
func TestWriteFile_ErrorPathsPropagated(t *testing.T) {
	root := t.TempDir()
	sink, err := NewFileSink(root, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	// 先创建一个同名普通文件，再尝试将其子路径作为目录写入，
	// MkdirAll 会因为父路径是普通文件而失败，触发错误提前返回路径。
	conflictFile := filepath.Join(root, "conflict")
	if err := os.WriteFile(conflictFile, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(conflictFile, "subdir", "test.jsonl")
	records := []domain.MessageRecord{{SyncedAt: 1, UserID: "u", AgentID: "a", SessionID: "s", MessageID: "m",
		Session: []byte(`{"id":"s"}`), Message: []byte(`{"info":{"id":"m"}}`)}}
	lock := &pathLock{lineCount: -1}
	err = sink.writeFile(path, lock, records, []int{0})
	if err == nil {
		t.Fatal("expected error when MkdirAll fails (parent is a regular file), got nil")
	}
}

// TestWriteFile_WriteErrorReturned 验证写入过程中发生 I/O 错误时，writeFile 正确返回该错误。
func TestWriteFile_WriteErrorReturned(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}
	root := t.TempDir()
	sink, err := NewFileSink(root, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	// 先创建目标文件，再将其设为只读，触发 OpenFile O_WRONLY 时的 permission denied
	path := filepath.Join(root, "readonly.jsonl")
	if err := os.WriteFile(path, []byte{}, 0o444); err != nil {
		t.Fatal(err)
	}
	records := []domain.MessageRecord{{SyncedAt: 1, UserID: "u", AgentID: "a", SessionID: "s", MessageID: "m",
		Session: []byte(`{"id":"s"}`), Message: []byte(`{"info":{"id":"m"}}`)}}
	lock := &pathLock{lineCount: -1}
	err = sink.writeFile(path, lock, records, []int{0})
	if err == nil {
		t.Fatal("expected error when opening read-only file for writing, got nil")
	}
}

// TestFileSinkTracksOutputLine 验证 WriteMessages 正确填充 OutputLine 字段。
func TestFileSinkTracksOutputLine(t *testing.T) {
	root := t.TempDir()
	sink, err := NewFileSink(root, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	// 第一批写入 3 条记录
	records1 := []domain.MessageRecord{
		{SyncedAt: 1, UserID: "u", AgentID: "a", SessionID: "ses_line", MessageID: "m1", Session: []byte(`{"id":"s"}`), Message: []byte(`{"id":"m1"}`)},
		{SyncedAt: 2, UserID: "u", AgentID: "a", SessionID: "ses_line", MessageID: "m2", Session: []byte(`{"id":"s"}`), Message: []byte(`{"id":"m2"}`)},
		{SyncedAt: 3, UserID: "u", AgentID: "a", SessionID: "ses_line", MessageID: "m3", Session: []byte(`{"id":"s"}`), Message: []byte(`{"id":"m3"}`)},
	}
	if err := sink.WriteMessages(context.Background(), records1); err != nil {
		t.Fatal(err)
	}
	// 验证行号从 1 开始递增
	if records1[0].OutputLine != 1 {
		t.Fatalf("records1[0].OutputLine = %d, want 1", records1[0].OutputLine)
	}
	if records1[1].OutputLine != 2 {
		t.Fatalf("records1[1].OutputLine = %d, want 2", records1[1].OutputLine)
	}
	if records1[2].OutputLine != 3 {
		t.Fatalf("records1[2].OutputLine = %d, want 3", records1[2].OutputLine)
	}

	// 第二批再写入 2 条记录，行号应续接
	records2 := []domain.MessageRecord{
		{SyncedAt: 4, UserID: "u", AgentID: "a", SessionID: "ses_line", MessageID: "m4", Session: []byte(`{"id":"s"}`), Message: []byte(`{"id":"m4"}`)},
		{SyncedAt: 5, UserID: "u", AgentID: "a", SessionID: "ses_line", MessageID: "m5", Session: []byte(`{"id":"s"}`), Message: []byte(`{"id":"m5"}`)},
	}
	if err := sink.WriteMessages(context.Background(), records2); err != nil {
		t.Fatal(err)
	}
	if records2[0].OutputLine != 4 {
		t.Fatalf("records2[0].OutputLine = %d, want 4", records2[0].OutputLine)
	}
	if records2[1].OutputLine != 5 {
		t.Fatalf("records2[1].OutputLine = %d, want 5", records2[1].OutputLine)
	}
}

// TestFileSinkOutputLineAfterRestart 验证 pathLock 被回收后重新初始化时，
// 通过 countLines 恢复行号缓存，后续写入的 OutputLine 仍然正确续接。
func TestFileSinkOutputLineAfterRestart(t *testing.T) {
	root := t.TempDir()
	sink, err := NewFileSink(root, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	// 先写入 2 条
	records := []domain.MessageRecord{
		{SyncedAt: 1, UserID: "u", AgentID: "a", SessionID: "ses_restart", MessageID: "m1", Session: []byte(`{"id":"s"}`), Message: []byte(`{"id":"m1"}`)},
		{SyncedAt: 2, UserID: "u", AgentID: "a", SessionID: "ses_restart", MessageID: "m2", Session: []byte(`{"id":"s"}`), Message: []byte(`{"id":"m2"}`)},
	}
	if err := sink.WriteMessages(context.Background(), records); err != nil {
		t.Fatal(err)
	}

	// 手动清除 lock 缓存，模拟 lock 被回收或进程重启
	sink.locksMu.Lock()
	sink.locks = make(map[string]*pathLock)
	sink.locksMu.Unlock()

	// 再写入 1 条，应该通过 countLines 恢复，行号续接为 3
	records2 := []domain.MessageRecord{
		{SyncedAt: 3, UserID: "u", AgentID: "a", SessionID: "ses_restart", MessageID: "m3", Session: []byte(`{"id":"s"}`), Message: []byte(`{"id":"m3"}`)},
	}
	if err := sink.WriteMessages(context.Background(), records2); err != nil {
		t.Fatal(err)
	}
	if records2[0].OutputLine != 3 {
		t.Fatalf("records2[0].OutputLine = %d, want 3", records2[0].OutputLine)
	}
}
