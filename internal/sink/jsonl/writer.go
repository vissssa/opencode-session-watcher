package jsonl

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"

	"session_watcher/internal/domain"
)

// 文件锁的默认 TTL 和清理间隔。
// 超过 TTL 且无 goroutine 使用的锁会在下次清理时被回收，防止长期运行时 locks map 无界增长。
const (
	defaultPathLockTTL      = 10 * time.Minute
	pathLockCleanupInterval = time.Minute
)

// FileSink 实现 domain.Sink 和 domain.PathResolver 接口，
// 将消息记录按 {rootDir}/{userID}/{agentID}/{sessionID}.jsonl 的路径追加写入本地文件。
type FileSink struct {
	locksMu             sync.Mutex          // 保护 locks map 的并发访问
	locks               map[string]*pathLock // 每个输出文件路径对应一把锁
	rootDir             string
	logger              *slog.Logger
	lockTTL             time.Duration // 空闲锁的存活时间
	lockCleanupInterval time.Duration // 清理检查的最小间隔
	lastLockCleanup     time.Time     // 上次执行清理的时间
	now                 func() time.Time // 可注入的时钟函数，便于测试
}

// pathLock 是单个文件路径的带引用计数的互斥锁。
// users 记录当前持有或等待该锁的 goroutine 数量，用于安全回收。
// lineCount 缓存该文件当前的行数，避免每次写入都全量扫描文件。
// byteOffset 缓存该文件当前的字节大小，用于记录每条消息写入后的文件大小（更新到 sessions.file_size）。
type pathLock struct {
	mu         sync.Mutex
	users      int       // 当前使用该锁的 goroutine 数量
	lastUsed   time.Time // 最近一次使用时间，用于 TTL 判断
	lineCount  int       // 该文件当前行数缓存，-1 表示未初始化
	byteOffset int64     // 该文件当前字节大小缓存，-1 表示未初始化
}

// NewFileSink 创建 FileSink，rootDir 不存在时会自动创建。
func NewFileSink(rootDir string, logger *slog.Logger) (*FileSink, error) {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output root directory: %w", err)
	}
	return &FileSink{
		rootDir:             rootDir,
		logger:              logger,
		locks:               make(map[string]*pathLock),
		lockTTL:             defaultPathLockTTL,
		lockCleanupInterval: pathLockCleanupInterval,
		now:                 time.Now,
	}, nil
}

// WriteMessages 将 records 批量追加写入对应的 JSONL 文件。
// 相同输出路径的记录会被分组后串行写入，不同路径可并发写入（各自持有独立锁）。
// 写入时自动填充每条 record 的 OutputLine（行号从 1 开始）和 OutputOffset（写入后文件字节大小）字段。
// 每次写完后尝试定期清理空闲的文件锁，防止 locks map 无界增长。
func (s *FileSink) WriteMessages(ctx context.Context, records []domain.MessageRecord) error {
	if len(records) == 0 {
		return nil
	}
	started := time.Now()

	// 按输出路径分组，用 paths 切片保持原始插入顺序。
	// groupIndices 记录每条 record 在原始 records 切片中的索引，用于回写 OutputLine。
	groups := make(map[string][]int)
	paths := make([]string, 0)
	for i, record := range records {
		path := s.PathFor(record)
		if _, ok := groups[path]; !ok {
			paths = append(paths, path)
		}
		groups[path] = append(groups[path], i)
	}

	for _, path := range paths {
		// 检查 ctx 是否已取消，避免在关闭时继续写入
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		lock := s.lockFor(path)
		lock.mu.Lock()
		err := s.writeFile(path, lock, records, groups[path])
		lock.mu.Unlock()
		s.releaseLock(path, lock)
		// 每次写完后触发一次清理检查（受 cleanupInterval 限频）
		s.cleanupIdleLocks()
		if err != nil {
			return err
		}
		s.logger.Debug("jsonl sink wrote messages", "path", path, "count", len(groups[path]))
	}
	s.logger.Debug("jsonl sink write batch completed", "count", len(records), "duration", time.Since(started))
	return nil
}

// Close 释放资源，当前实现无需关闭操作。
func (s *FileSink) Close() error {
	return nil
}

// SinkType 返回 Sink 类型标识，实现 domain.PathResolver 接口。
func (s *FileSink) SinkType() string {
	return "jsonl"
}

// OutputRoot 返回输出根目录，实现 domain.PathResolver 接口。
func (s *FileSink) OutputRoot() string {
	return s.rootDir
}

// lockFor 返回指定路径对应的 pathLock，若不存在则创建，并将引用计数加一。
// 新建的 pathLock lineCount 和 byteOffset 初始为 -1，表示未初始化。
func (s *FileSink) lockFor(path string) *pathLock {
	now := s.now()
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	lock := s.locks[path]
	if lock == nil {
		lock = &pathLock{lastUsed: now, lineCount: -1, byteOffset: -1}
		s.locks[path] = lock
	}
	lock.users++
	lock.lastUsed = now
	return lock
}

// releaseLock 将指定路径的 pathLock 引用计数减一，并更新最后使用时间。
func (s *FileSink) releaseLock(path string, lock *pathLock) {
	now := s.now()
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	// 若 map 中的锁已被替换（理论上不会发生），直接返回
	if s.locks[path] != lock {
		return
	}
	if lock.users > 0 {
		lock.users--
	}
	lock.lastUsed = now
}

// cleanupIdleLocks 清理 locks map 中空闲超过 TTL 的文件锁。
// 受 lockCleanupInterval 限频，不会每次写入都触发全量扫描。
func (s *FileSink) cleanupIdleLocks() {
	now := s.now()
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	// 距上次清理未到间隔，跳过
	if !s.lastLockCleanup.IsZero() && now.Sub(s.lastLockCleanup) < s.lockCleanupInterval {
		return
	}
	s.lastLockCleanup = now
	for path, lock := range s.locks {
		// 只回收无 goroutine 使用且空闲超过 TTL 的锁
		if lock.users == 0 && now.Sub(lock.lastUsed) > s.lockTTL {
			delete(s.locks, path)
		}
	}
}

// writeFile 将指定索引的 records 追加写入 JSONL 文件，文件不存在时自动创建。
// 使用 bufio.Writer 减少系统调用次数，每条记录占一行。
// 写入前初始化行数和字节大小缓存（若需要），写入后更新 records 的 OutputLine/OutputOffset 字段。
// Flush 和 Close 的错误均会被捕获并返回，确保写入失败时调用方能感知。
func (s *FileSink) writeFile(path string, lock *pathLock, records []domain.MessageRecord, indices []int) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	// 首次使用时初始化行数和字节偏移缓存
	if lock.lineCount < 0 || lock.byteOffset < 0 {
		lock.lineCount = countLines(path)
		lock.byteOffset = fileSize(path)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(file)
	for _, idx := range indices {
		line, err := json.Marshal(records[idx])
		if err != nil {
			_ = file.Close()
			return err
		}
		if _, err := writer.Write(line); err != nil {
			_ = file.Close()
			return err
		}
		if err := writer.WriteByte('\n'); err != nil {
			_ = file.Close()
			return err
		}
		// 更新字节偏移缓存：JSON 内容 + 换行符
		lock.byteOffset += int64(len(line)) + 1
		// 填充行号（从 1 开始），并推进缓存计数
		lock.lineCount++
		records[idx].OutputLine = lock.lineCount
		// 填充写入后的文件末尾偏移（下次增量读取的起始位置）
		records[idx].OutputOffset = lock.byteOffset
	}
	if err := writer.Flush(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// countLines 统计文件当前行数。文件不存在或无法打开时返回 0。
func countLines(path string) int {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		count++
	}
	return count
}

// fileSize 返回文件当前字节大小。文件不存在或无法 stat 时返回 0。
func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// PathFor 根据消息记录生成输出文件路径，格式为：
//
//	{rootDir}/{userID}/{agentID}/{sessionID}.jsonl
//
// 每个 segment 都经过 cleanSegment 过滤，防止路径遍历攻击。
// 实现 domain.PathResolver 接口。
func (s *FileSink) PathFor(record domain.MessageRecord) string {
	userID := record.UserID
	if userID == "" {
		userID = domain.DefaultUserID
	}
	agentID := record.AgentID
	if agentID == "" {
		agentID = domain.DefaultAgentID
	}
	sessionID := record.SessionID
	if sessionID == "" {
		sessionID = "unknown_session"
	}
	return filepath.Join(s.rootDir, cleanSegment(userID), cleanSegment(agentID), cleanSegment(sessionID)+".jsonl")
}

// cleanSegment 对路径 segment 做字符白名单过滤，防止路径遍历攻击。
// 只允许字母、数字、连字符、下划线、点；其余字符替换为下划线；
// 空字符串、"."、".." 返回 "unknown"。
func cleanSegment(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || value == "." || value == ".." {
		return "unknown"
	}
	var builder strings.Builder
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || r == '.' {
			builder.WriteRune(r)
			continue
		}
		builder.WriteByte('_')
	}
	// 去掉首尾的点，防止生成隐藏文件或相对路径（如 ".foo" 或 "foo."）
	cleaned := strings.Trim(builder.String(), ".")
	if cleaned == "" || cleaned == ".." {
		return "unknown"
	}
	return cleaned
}
