package systemeventslisten

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestPushClientSendEventRetriesTransientFailures(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want %q", r.Method, http.MethodPost)
		}
		if r.URL.Path != "/events" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/events")
		}
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := CreatePushClient(time.Second, server.URL, "prod", "gateway", RetryConfig{
		MaxRetries:     2,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
	})
	event := &Event{Event: "created"}

	if err := client.SendEvent(context.Background(), event); err != nil {
		t.Fatalf("SendEvent() error = %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
	if event.Namespace != "prod" {
		t.Errorf("event namespace = %q, want %q", event.Namespace, "prod")
	}
	if event.App != "gateway" {
		t.Errorf("event app = %q, want %q", event.App, "gateway")
	}
	if event.UniqueKey == "" {
		t.Error("event unique key was not generated")
	}
}

func TestPushClientSendNotificationDoesNotRetryClientErrors(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/notifications" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/notifications")
		}
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	client := CreatePushClient(time.Second, server.URL, "prod", "gateway", RetryConfig{
		MaxRetries:     3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
	})

	if err := client.SendNotification(context.Background(), &Event{}); err == nil {
		t.Fatal("SendNotification() error = nil, want non-nil")
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1", got)
	}
}

func TestPushClientRetryDelayIsExponentiallyCapped(t *testing.T) {
	client := CreatePushClient(time.Second, "https://example.com", "prod", "gateway", RetryConfig{
		MaxRetries:     4,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     25 * time.Millisecond,
	})

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 10 * time.Millisecond},
		{attempt: 2, want: 20 * time.Millisecond},
		{attempt: 3, want: 25 * time.Millisecond},
	}
	for _, tt := range tests {
		if got := client.retryDelay(tt.attempt); got != tt.want {
			t.Errorf("retryDelay(%d) = %s, want %s", tt.attempt, got, tt.want)
		}
	}
}
