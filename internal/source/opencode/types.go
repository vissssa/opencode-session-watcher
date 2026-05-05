package opencode

import (
	"encoding/json"

	"session_watcher/internal/domain"
)

// rawSession 是 open-code /session API 的原始响应结构。
// 同时兼容 snake_case（user_id/agent_id）和 camelCase（userID/agentID）两种字段命名风格。
type rawSession struct {
	ID           string `json:"id"`
	UserID       string `json:"user_id"`
	UserIDCamel  string `json:"userID"`
	AgentID      string `json:"agent_id"`
	AgentIDCamel string `json:"agentID"`
	Time         struct {
		Updated int64 `json:"updated"`
	} `json:"time"`
}

// rawMessage 是 open-code /message API 的原始响应结构，仅解析 info 中的元数据字段。
// 完整的消息内容通过 json.RawMessage 原样保留，不做字段映射。
type rawMessage struct {
	Info struct {
		ID        string `json:"id"`
		SessionID string `json:"sessionID"`
		Time      struct {
			Created int64 `json:"created"`
		} `json:"time"`
	} `json:"info"`
}

// decodeSessions 将 JSON 数组解码为 []domain.Session。
func decodeSessions(data []byte) ([]domain.Session, error) {
	var raws []json.RawMessage
	if err := json.Unmarshal(data, &raws); err != nil {
		return nil, err
	}
	sessions := make([]domain.Session, 0, len(raws))
	for _, raw := range raws {
		s, err := decodeSession(raw)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, s)
	}
	return sessions, nil
}

// decodeSession 将单个 Session JSON 解码为 domain.Session。
// UserID/AgentID 优先取 snake_case 字段，其次 camelCase，最后兜底为 DefaultUserID/DefaultAgentID。
// Raw 字段保留原始 JSON 的完整副本，供 Sink 透传输出。
func decodeSession(data []byte) (domain.Session, error) {
	var s rawSession
	if err := json.Unmarshal(data, &s); err != nil {
		return domain.Session{}, err
	}
	return domain.Session{
		ID:        s.ID,
		UserID:    firstNonEmpty(s.UserID, s.UserIDCamel, domain.DefaultUserID),
		AgentID:   firstNonEmpty(s.AgentID, s.AgentIDCamel, domain.DefaultAgentID),
		UpdatedAt: s.Time.Updated,
		Raw:       append(json.RawMessage(nil), data...), // 深拷贝，避免外部修改影响此字段
	}, nil
}

// decodeMessages 将 JSON 数组解码为 []domain.Message。
func decodeMessages(data []byte) ([]domain.Message, error) {
	var raws []json.RawMessage
	if err := json.Unmarshal(data, &raws); err != nil {
		return nil, err
	}
	messages := make([]domain.Message, 0, len(raws))
	for _, raw := range raws {
		var m rawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, err
		}
		messages = append(messages, domain.Message{
			ID:        m.Info.ID,
			SessionID: m.Info.SessionID,
			CreatedAt: m.Info.Time.Created,
			Raw:       append(json.RawMessage(nil), raw...), // 深拷贝原始 JSON
		})
	}
	return messages, nil
}

// firstNonEmpty 返回 values 中第一个非空字符串，全为空时返回空字符串。
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
