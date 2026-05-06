package opencode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"session_watcher/internal/domain"
)

// maxHTTPAttempts 是单次请求的最大重试次数（含首次）。
const maxHTTPAttempts = 3

// HTTPSource 实现 domain.Source 接口，通过 HTTP 访问 open-code 服务。
type HTTPSource struct {
	baseURL string
	client  *http.Client
	logger  *slog.Logger
}

// httpStatusError 表示 HTTP 非 2xx 响应错误，携带状态码和响应体片段。
type httpStatusError struct {
	url        string
	statusCode int
	preview    string // 响应体前 512 字节，便于快速定位问题
}

// Error 实现 error 接口。
func (e httpStatusError) Error() string {
	return fmt.Sprintf("GET %s returned %d: %s", e.url, e.statusCode, e.preview)
}

// NewHTTPSource 创建一个 HTTPSource，baseURL 末尾的斜杠会被去除。
func NewHTTPSource(baseURL string, timeout time.Duration, logger *slog.Logger) *HTTPSource {
	return &HTTPSource{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: timeout},
		logger:  logger,
	}
}

// ListSessions 请求 GET /session，返回所有会话列表。
func (s *HTTPSource) ListSessions(ctx context.Context) ([]domain.Session, error) {
	started := time.Now()
	body, err := s.get(ctx, "/session", nil)
	if err != nil {
		return nil, err
	}
	sessions, err := decodeSessions(body)
	if err != nil {
		return nil, fmt.Errorf("decode sessions: %w", err)
	}
	s.logger.Debug("list sessions completed", "count", len(sessions), "duration", time.Since(started))
	return sessions, nil
}

// GetSession 请求 GET /session/{sessionID}，返回指定会话详情。
func (s *HTTPSource) GetSession(ctx context.Context, sessionID string) (domain.Session, error) {
	started := time.Now()
	body, err := s.get(ctx, "/session/"+url.PathEscape(sessionID), nil)
	if err != nil {
		return domain.Session{}, err
	}
	session, err := decodeSession(body)
	if err != nil {
		return domain.Session{}, fmt.Errorf("decode session %s: %w", sessionID, err)
	}
	s.logger.Debug("get session completed", "session_id", sessionID, "duration", time.Since(started))
	return session, nil
}

// ListMessages 请求 GET /session/{sessionID}/message?limit=N，返回最近 limit 条消息。
func (s *HTTPSource) ListMessages(ctx context.Context, sessionID string, limit int) ([]domain.Message, error) {
	started := time.Now()
	query := url.Values{}
	query.Set("limit", strconv.Itoa(limit))
	body, err := s.get(ctx, "/session/"+url.PathEscape(sessionID)+"/message", query)
	if err != nil {
		return nil, err
	}
	messages, err := decodeMessages(body)
	if err != nil {
		return nil, fmt.Errorf("decode messages for %s: %w", sessionID, err)
	}
	s.logger.Debug("list messages completed", "session_id", sessionID, "limit", limit, "count", len(messages), "duration", time.Since(started))
	return messages, nil
}

// get 发送带重试的 HTTP GET 请求。
// 重试策略：最多 maxHTTPAttempts 次，指数退避 + jitter，
// ctx 取消、客户端错误（4xx，429 除外）不重试。
func (s *HTTPSource) get(ctx context.Context, path string, query url.Values) ([]byte, error) {
	u := s.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	for attempt := 1; attempt <= maxHTTPAttempts; attempt++ {
		body, err := s.getOnce(ctx, u)
		if err == nil {
			return body, nil
		}
		// 不可重试的错误直接返回，避免无效重试
		if !shouldRetry(ctx, err) || attempt == maxHTTPAttempts {
			return nil, err
		}
		backoff := retryBackoff(attempt)
		s.logger.Warn("http request failed, retrying", "url", u, "attempt", attempt, "max_attempts", maxHTTPAttempts, "backoff", backoff, "error", err)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	// 循环内所有路径均已 return，此处理论上不可达；
	// 用 error 代替 panic，避免极端情况下进程崩溃。
	return nil, fmt.Errorf("http get %s: exceeded max attempts without result", u)
}

// getOnce 执行单次 HTTP GET 请求，非 2xx 响应返回 httpStatusError。
func (s *HTTPSource) getOnce(ctx context.Context, u string) ([]byte, error) {
	s.logger.Debug("http request", "url", u)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// 截取响应体前 512 字节作为错误预览，避免日志过大
		preview := string(body)
		if len(preview) > 512 {
			preview = preview[:512]
		}
		return nil, httpStatusError{url: u, statusCode: resp.StatusCode, preview: preview}
	}
	return body, nil
}

// retryBackoff 计算第 attempt 次重试的等待时间。
// 采用指数退避（base * 2^(attempt-1)）加随机 jitter，以避免多个实例同时重试时的惊群效应。
func retryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := 100 * time.Millisecond
	// 指数增长：第 1 次 100ms，第 2 次 200ms，第 3 次 400ms，以此类推
	for i := 1; i < attempt; i++ {
		base *= 2
	}
	// 在 [0, base/2] 范围内加随机 jitter，打散重试时间窗口
	jitter := time.Duration(rand.Int63n(int64(base/2) + 1))
	return base + jitter
}

// shouldRetry 判断指定错误是否应当重试。
// ctx 取消/超时、客户端错误（4xx，429 除外）不重试；
// 服务端错误（5xx）、429 限流、网络错误均可重试。
func shouldRetry(ctx context.Context, err error) bool {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var statusErr httpStatusError
	if errors.As(err, &statusErr) {
		return statusErr.statusCode == http.StatusTooManyRequests || statusErr.statusCode >= 500
	}
	return true
}
