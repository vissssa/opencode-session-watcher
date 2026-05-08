package domain

import (
	"context"
	"encoding/json"
)

// 当 session 或 message 缺少对应字段时使用的默认占位值。
const (
	DefaultUserID  = "default_user"
	DefaultAgentID = "default_agent"
)

// Session 表示一个 AI 对话会话的元数据。
// Raw 保存从 Source 获取的原始 JSON，避免字段增加时丢失信息。
type Session struct {
	ID        string
	UserID    string
	AgentID   string
	UpdatedAt int64           // 远端最后更新时间（毫秒时间戳），用于判断是否需要重新同步
	Raw       json.RawMessage // 原始 JSON，透传给 Sink
}

// Message 表示一条会话消息的元数据。
// Raw 保存完整的原始 JSON，供 Sink 写出。
type Message struct {
	ID        string
	SessionID string
	CreatedAt int64           // 消息创建时间（毫秒时间戳），用于排序
	Raw       json.RawMessage // 原始 JSON，透传给 Sink
}

// MessageRecord 是写入 Sink 的完整记录，包含元数据字段和原始 JSON。
// SinkType / OutputRoot / OutputPath / OutputSessionFile / OutputLine / OutputOffset 为输出追踪字段，
// 不序列化到 JSON（json:"-"），由 PathResolver 和 Sink 填充后写入 PostgreSQL。
type MessageRecord struct {
	SyncedAt         int64           `json:"synced_at"`
	UserID           string          `json:"user_id"`
	AgentID          string          `json:"agent_id"`
	SessionID        string          `json:"session_id"`
	MessageID        string          `json:"message_id"`
	MessageCreatedAt int64           `json:"message_created_at"`
	Session          json.RawMessage `json:"session"`
	Message          json.RawMessage `json:"message"`

	// 以下字段不写入 JSONL，仅用于 PostgreSQL 输出追踪
	SinkType          string `json:"-"`
	OutputRoot        string `json:"-"`
	OutputPath        string `json:"-"`
	OutputSessionFile string `json:"-"`
	OutputLine        int    `json:"-"` // 消息在 JSONL 文件中的行号（从 1 开始），由 Sink 写入时填充
	OutputOffset      int64  `json:"-"` // 该消息写入后的文件字节大小（用于更新 sessions.file_size），由 Sink 写入时填充
}

// Source 是数据输入抽象，当前由 opencode.HTTPSource 实现。
// 后续可替换为其他数据源（Kafka、数据库等），无需修改 watcher 核心逻辑。
type Source interface {
	// ListSessions 返回所有会话列表。
	ListSessions(ctx context.Context) ([]Session, error)
	// GetSession 返回指定会话的详情。
	GetSession(ctx context.Context, sessionID string) (Session, error)
	// ListMessages 返回指定会话最近 limit 条消息。
	ListMessages(ctx context.Context, sessionID string, limit int) ([]Message, error)
}

// Sink 是数据输出抽象，当前由 jsonl.FileSink 实现。
// 后续可替换为 ES、S3 等输出目标，无需修改 watcher 核心逻辑。
type Sink interface {
	// WriteMessages 批量写出消息记录，实现应保证并发安全。
	WriteMessages(ctx context.Context, records []MessageRecord) error
	// Close 释放 Sink 持有的资源。
	Close() error
}

// PathResolver 是可选的路径信息提供接口，由 Sink 实现。
// watcher 在填充 MessageRecord 的输出追踪字段时会做类型断言检查，
// 若 Sink 未实现此接口，SinkType 将被设为 "unknown"。
type PathResolver interface {
	// PathFor 根据消息记录返回完整输出路径。
	PathFor(record MessageRecord) string
	// SinkType 返回 Sink 类型标识（如 "jsonl"）。
	SinkType() string
	// OutputRoot 返回输出根目录。
	OutputRoot() string
}
