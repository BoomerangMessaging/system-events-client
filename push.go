package systemeventslisten

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

type Event struct {
	// unique per namespace
	UniqueKey string `json:"unique_key"`
	// prod/staging/dev/mps
	Namespace string `json:"namespace"`
	// ui/gateway/contacts/reporting/logic/carrier
	App string `json:"app"`
	// login/sms/sent/receive/updated/deleted/suspended
	Event string `json:"event"`
	// contact/user/account etc
	ResourceType string `json:"resource_type"`
	ResourceID   string `json:"resource_id"`
	CustomerID   string `json:"customer_id"`
	ClientID     string `json:"client_id"`
	// info/warning/error
	Level   string          `json:"level"`
	Message json.RawMessage `json:"message"`
	// if CountMinutes > 0, defines the time frame in minutes for counting
	CountMinutes int `json:"count_minutes"`
}

func (e *Event) GenerateUniqueKey() {
	e.UniqueKey = fmt.Sprintf("%s:%s:%s:%s:%d:%d",
		e.Namespace, e.App, e.Event, e.ResourceType,
		rand.Intn(10000), time.Now().UnixNano(),
	)
}

// namespace - prod/staging/dev/mps
// app - ui/gateway/contacts/reporting/logic/carrier
func CreatePushClient(timeout time.Duration, baseUrl, namespace, appName string) *PushClient {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	return &PushClient{
		baseURL:   strings.TrimRight(strings.TrimSpace(baseUrl), "/"),
		timeout:   timeout,
		namespace: namespace,
		app:       appName,
	}
}

type PushClient struct {
	// Add fields if necessary
	baseURL   string
	timeout   time.Duration
	namespace string
	app       string
}

func (pc *PushClient) SendEvent(ctx context.Context, event *Event) error {

	// Implement the logic to send the event to the external system
	if pc.baseURL == "" {
		slog.Info("push_client_base_url_is_empty_skipping_event_send", "event", event)
		return nil
	}

	if event.UniqueKey == "" {
		event.GenerateUniqueKey()
	}

	if event.Namespace == "" {
		event.Namespace = pc.namespace
	}

	slog.Debug("sending_event", "event", event)
	// marshal event to JSON
	// send HTTP POST request to pc.baseURL with the event data
	bytesBuffer := &bytes.Buffer{}
	err := json.NewEncoder(bytesBuffer).Encode(event)
	if err != nil {
		slog.Error("failed_to_marshal_event_to_json", "err", err)
		return err
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, pc.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctxWithTimeout, "POST", pc.baseURL+"/events", bytesBuffer)
	if err != nil {
		slog.Error("failed_to_create_http_request", "err", err)
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Error("failed_to_send_http_request", "err", err)
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err2 := io.ReadAll(resp.Body)
		if err2 != nil {
			slog.Error("failed_to_read_response_body", "err", err2)
		}

		slog.Error("received_non_ok_response", "status_code", resp.StatusCode, "body", string(body))
		return fmt.Errorf("non-ok response: %d", resp.StatusCode)
	}

	slog.Info("event_sent_successfully", "event", event.UniqueKey)
	return nil
}
