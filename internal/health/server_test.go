package health

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"session_watcher/internal/status"
)

func TestHealthServer(t *testing.T) {
	reporter := status.NewReporter()
	reporter.RecordRound(status.RoundUpdate{SessionsTotal: 1, MessagesNew: 2})
	server, err := Start("127.0.0.1:0", reporter, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close(context.Background())

	client := http.Client{Timeout: time.Second}
	resp, err := client.Get("http://" + server.Addr() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", resp.StatusCode)
	}
	var health map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatal(err)
	}
	if health["status"] != "ok" {
		t.Fatalf("health = %#v", health)
	}

	resp, err = client.Get("http://" + server.Addr() + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var snapshot status.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.SessionsTotal != 1 || snapshot.MessagesNew != 2 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}
