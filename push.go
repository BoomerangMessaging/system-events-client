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

	var unixTimestamp int64
	if err := json.Unmarshal(aux.CreatedAt, &unixTimestamp); err == nil {
		if unixTimestamp > 1e12 {
			e.CreatedAt = time.UnixMilli(unixTimestamp)
		} else {
			e.CreatedAt = time.Unix(unixTimestamp, 0)
		}
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
// An optional RetryConfig enables retries for transient delivery failures.
func CreatePushClient(timeout time.Duration, baseURL, namespace, appName string, retryConfigs ...RetryConfig) *PushClient {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	retryConfig := RetryConfig{}
	if len(retryConfigs) > 0 {
		retryConfig = retryConfigs[0].normalized()
	}

	return &PushClient{
		baseURL:     strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		timeout:     timeout,
		namespace:   namespace,
		app:         appName,
		retryConfig: retryConfig,
	}
}

// RetryConfig controls retry behavior for transient push delivery failures.
// MaxRetries is the number of attempts after the initial request. A zero value
// disables retries. InitialBackoff and MaxBackoff default to 100ms and 5s when
// retries are enabled and their respective values are not positive.
type RetryConfig struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func (rc RetryConfig) normalized() RetryConfig {
	if rc.MaxRetries <= 0 {
		return RetryConfig{}
	}
	if rc.InitialBackoff <= 0 {
		rc.InitialBackoff = 100 * time.Millisecond
	}
	if rc.MaxBackoff <= 0 {
		rc.MaxBackoff = 5 * time.Second
	}
	if rc.MaxBackoff < rc.InitialBackoff {
		rc.MaxBackoff = rc.InitialBackoff
	}
	return rc
}

type PushClient struct {
	baseURL     string
	timeout     time.Duration
	namespace   string
	app         string
	retryConfig RetryConfig
}

func (pc *PushClient) SendEvent(ctx context.Context, event *Event) error {
	return pc.send(ctx, event, "/events", "event")
}

func (pc *PushClient) SendNotification(ctx context.Context, event *Event) error {
	return pc.send(ctx, event, "/notifications", "notification")
}

func (pc *PushClient) send(ctx context.Context, event *Event, endpoint, kind string) error {
	if pc.baseURL == "" {
		slog.Info("push_client_base_url_is_empty_skipping_send", "kind", kind, "event", event)
		return nil
	}

	if event == nil {
		return fmt.Errorf("event must not be nil")
	}
	if event.Namespace == "" {
		event.Namespace = pc.namespace
	}
	if event.App == "" {
		event.App = pc.app
	}
	if event.UniqueKey == "" {
		event.GenerateUniqueKey()
	}

	slog.Debug("sending_push", "kind", kind, "event", event)
	payload, err := json.Marshal(event)
	if err != nil {
		slog.Error("failed_to_marshal_event_to_json", "err", err)
		return err
	}

	var lastErr error
	for attempt := 0; attempt <= pc.retryConfig.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := pc.retryDelay(attempt)
			slog.Warn("retrying_push", "kind", kind, "attempt", attempt+1, "delay", delay, "err", lastErr)
			if err := waitForRetry(ctx, delay); err != nil {
				return err
			}
		}

		retryable, err := pc.doRequest(ctx, endpoint, payload)
		if err == nil {
			slog.Info("push_sent_successfully", "kind", kind, "event", event.UniqueKey, "attempt", attempt+1)
			return nil
		}
		if !retryable || attempt == pc.retryConfig.MaxRetries {
			return err
		}
		lastErr = err
	}

	return lastErr
}

func (pc *PushClient) doRequest(ctx context.Context, endpoint string, payload []byte) (bool, error) {
	requestCtx, cancel := context.WithTimeout(ctx, pc.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, pc.baseURL+endpoint, bytes.NewReader(payload))
	if err != nil {
		slog.Error("failed_to_create_http_request", "err", err)
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Error("failed_to_send_http_request", "err", err)
		return true, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return false, nil
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		slog.Error("failed_to_read_response_body", "err", readErr)
	}
	err = fmt.Errorf("non-ok response: %d", resp.StatusCode)
	slog.Error("received_non_ok_response", "status_code", resp.StatusCode, "body", string(body))
	return resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError, err
}

func (pc *PushClient) retryDelay(attempt int) time.Duration {
	delay := pc.retryConfig.InitialBackoff
	for retry := 1; retry < attempt && delay < pc.retryConfig.MaxBackoff; retry++ {
		if delay > pc.retryConfig.MaxBackoff/2 {
			return pc.retryConfig.MaxBackoff
		}
		delay *= 2
	}
	if delay > pc.retryConfig.MaxBackoff {
		return pc.retryConfig.MaxBackoff
	}
	return delay
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
