package health

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"time"

	"session_watcher/internal/status"
)

// Server 封装 HTTP health/status 服务实例。
type Server struct {
	server *http.Server
	addr   string // 实际监听地址（addr:0 时由系统分配端口）
}

// Start 启动 HTTP health/status 服务。
// addr 为空时直接返回 nil，不启动服务。
// 调用方负责在适当时机调用 Close 关闭服务。
// 设置了 ReadTimeout=5s / WriteTimeout=5s / IdleTimeout=30s，防止异常客户端长期占用连接。
// 暴露两个端点：
//   - GET /healthz：存活探针，返回 {"status":"ok"}
//   - GET /status：运行状态快照，返回 Reporter.Snapshot() 的 JSON
func Start(addr string, reporter *status.Reporter, logger *slog.Logger) (*Server, error) {
	if addr == "" {
		return nil, nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, reporter.Snapshot())
	})

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
	hs := &Server{server: srv, addr: listener.Addr().String()}
	logger.Info("health server started", "addr", hs.addr)

	// 在独立 goroutine 中启动 HTTP 服务，ErrServerClosed 是正常关闭时的预期错误。
	// 关闭由调用方通过 Close 统一管控，避免与外部 defer 形成双重 Shutdown。
	go func() {
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error("health server failed", "error", err)
		}
	}()
	return hs, nil
}

// Addr 返回服务实际监听的地址，s 为 nil 时返回空字符串。
func (s *Server) Addr() string {
	if s == nil {
		return ""
	}
	return s.addr
}

// Close 优雅关闭 HTTP 服务，s 为 nil 时直接返回 nil。
func (s *Server) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

// writeJSON 将 value 序列化为 JSON 并写入响应。
func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
