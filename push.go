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
	CountMinutes int      `json:"count_minutes"`
	Channels     []string `json:"channels"`
	DoNotStore   bool     `json:"do_not_store"`
	// event is addressed to this email
	Email string `json:"email"`
	// front-end or source of the event
	Info      string    `json:"info"`
	CreatedAt time.Time `json:"created_at"`
}

func (e *Event) UnmarshalJSON(data []byte) error {
	type eventAlias struct {
		UniqueKey    string          `json:"unique_key"`
		Namespace    string          `json:"namespace"`
		App          string          `json:"app"`
		Event        string          `json:"event"`
		ResourceType string          `json:"resource_type"`
		ResourceID   string          `json:"resource_id"`
		CustomerID   string          `json:"customer_id"`
		ClientID     string          `json:"client_id"`
		Level        string          `json:"level"`
		Message      json.RawMessage `json:"message"`
		CountMinutes int             `json:"count_minutes"`
		Channels     []string        `json:"channels"`
		DoNotStore   bool            `json:"do_not_store"`
		Email        string          `json:"email"`
		Info         string          `json:"info"`
		CreatedAt    json.RawMessage `json:"created_at"`
	}

	var aux eventAlias
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	e.UniqueKey = aux.UniqueKey
	e.Namespace = aux.Namespace
	e.App = aux.App
	e.Event = aux.Event
	e.ResourceType = aux.ResourceType
	e.ResourceID = aux.ResourceID
	e.CustomerID = aux.CustomerID
	e.ClientID = aux.ClientID
	e.Level = aux.Level
	e.Message = aux.Message
	e.CountMinutes = aux.CountMinutes
	e.Channels = aux.Channels
	e.DoNotStore = aux.DoNotStore
	e.Email = aux.Email
	e.Info = aux.Info

	if len(aux.CreatedAt) == 0 || string(aux.CreatedAt) == "null" {
		e.CreatedAt = time.Time{}
		return nil
	}

	var createdAt time.Time
	if err := json.Unmarshal(aux.CreatedAt, &createdAt); err == nil {
		e.CreatedAt = createdAt
		return nil
	}

	var createdAtStr string
	if err := json.Unmarshal(aux.CreatedAt, &createdAtStr); err == nil && createdAtStr != "" {
		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02T15:04:05",
			"2006-01-02T15:04:05.999999999",
			"2006-01-02 15:04:05Z07:00",
		}
		for _, layout := range layouts {
			parsed, err := time.Parse(layout, createdAtStr)
			if err == nil {
				e.CreatedAt = parsed
				return nil
			}
		}
	}

	var unixSeconds int64
	if err := json.Unmarshal(aux.CreatedAt, &unixSeconds); err == nil {
		e.CreatedAt = time.Unix(unixSeconds, 0)
		return nil
	}

	var unixMilliseconds int64
	if err := json.Unmarshal(aux.CreatedAt, &unixMilliseconds); err == nil && unixMilliseconds > 1e12 {
		e.CreatedAt = time.UnixMilli(unixMilliseconds)
		return nil
	}

	return fmt.Errorf("invalid created_at value: %s", string(aux.CreatedAt))
}

func (e *Event) GenerateUniqueKey() {
	e.UniqueKey = fmt.Sprintf("%s:%s:%s:%s:%d:%d",
		e.Namespace, e.App, e.Event, e.ResourceType,
		rand.Intn(1000), time.Now().UnixMilli(),
	)

	// make sure that UniqueKey is not longer than 64 characters
	if len(e.UniqueKey) > 64 {
		// shuffle string and truncate
		shuffled := []rune(e.UniqueKey)
		rand.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})

		e.UniqueKey = string(shuffled)[:64]
	}
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

	if event.Namespace == "" {
		event.Namespace = pc.namespace
	}

	if event.UniqueKey == "" {
		event.GenerateUniqueKey()
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
