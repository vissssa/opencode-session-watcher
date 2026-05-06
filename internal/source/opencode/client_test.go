package opencode

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"session_watcher/internal/domain"
)

func TestHTTPSource(t *testing.T) {
	var messagePath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			w.Write([]byte(`[{"id":"ses_1","user_id":"user_1","agent_id":"agent_1","time":{"updated":10}},{"id":"ses_2","time":{"updated":11}}]`))
		case "/session/ses_1":
			w.Write([]byte(`{"id":"ses_1","userID":"user_camel","agentID":"agent_camel","time":{"updated":10}}`))
		case "/session/ses_1/message":
			messagePath = r.URL.RawQuery
			w.Write([]byte(`[{"info":{"id":"msg_1","sessionID":"ses_1","time":{"created":20}},"parts":[]}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	source := NewHTTPSource(server.URL, time.Second, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	sessions, err := source.ListSessions(context.Background())
	if err != nil || len(sessions) != 2 || sessions[0].ID != "ses_1" {
		t.Fatalf("ListSessions = %#v, %v", sessions, err)
	}
	if sessions[0].UserID != "user_1" || sessions[0].AgentID != "agent_1" {
		t.Fatalf("session metadata = %#v", sessions[0])
	}
	if sessions[1].UserID != domain.DefaultUserID || sessions[1].AgentID != domain.DefaultAgentID {
		t.Fatalf("default metadata = %#v", sessions[1])
	}
	session, err := source.GetSession(context.Background(), "ses_1")
	if err != nil || session.UpdatedAt != 10 {
		t.Fatalf("GetSession = %#v, %v", session, err)
	}
	if session.UserID != "user_camel" || session.AgentID != "agent_camel" {
		t.Fatalf("camel metadata = %#v", session)
	}
	messages, err := source.ListMessages(context.Background(), "ses_1", 7)
	if err != nil || len(messages) != 1 || messages[0].ID != "msg_1" {
		t.Fatalf("ListMessages = %#v, %v", messages, err)
	}
	if messagePath != "limit=7" {
		t.Fatalf("message query = %q", messagePath)
	}
}

func TestHTTPSourceRetriesServerError(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			http.Error(w, "temporary", http.StatusInternalServerError)
			return
		}
		w.Write([]byte(`[]`))
	}))
	defer server.Close()

	source := NewHTTPSource(server.URL, time.Second, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if _, err := source.ListSessions(context.Background()); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d", attempts)
	}
}

func TestHTTPSourceRetriesTooManyRequests(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		w.Write([]byte(`[]`))
	}))
	defer server.Close()

	source := NewHTTPSource(server.URL, time.Second, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if _, err := source.ListSessions(context.Background()); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d", attempts)
	}
}

func TestRetryBackoffUsesExponentialBaseWithJitter(t *testing.T) {
	cases := []struct {
		attempt int
		base    time.Duration
	}{
		{attempt: 1, base: 100 * time.Millisecond},
		{attempt: 2, base: 200 * time.Millisecond},
		{attempt: 3, base: 400 * time.Millisecond},
		{attempt: 0, base: 100 * time.Millisecond},
	}
	for _, tc := range cases {
		for range 100 {
			got := retryBackoff(tc.attempt)
			if got < tc.base || got > tc.base+tc.base/2 {
				t.Fatalf("retryBackoff(%d) = %s, want [%s, %s]", tc.attempt, got, tc.base, tc.base+tc.base/2)
			}
		}
	}
}

func TestHTTPSourceDoesNotRetryBadRequest(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		http.Error(w, "bad", http.StatusBadRequest)
	}))
	defer server.Close()

	source := NewHTTPSource(server.URL, time.Second, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if _, err := source.ListSessions(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d", attempts)
	}
}

func TestHTTPSourceRetriesNetworkError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[]`))
	}))
	url := server.URL
	server.Close()

	source := NewHTTPSource(url, 50*time.Millisecond, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if _, err := source.ListSessions(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}
