# system-events-client

`system-events-client` provides a RabbitMQ worker for consuming and publishing system events, plus an HTTP client for sending events and notifications to a system-events service.

```sh
go get github.com/BoomerangMessaging/system-events-client
```

```go
import systemevents "github.com/BoomerangMessaging/system-events-client"
```

## Event

`Event` is the payload sent by `PushClient`. `Message` is `json.RawMessage`, so it can hold any valid JSON value.

```go
event := &systemevents.Event{
	Namespace:    "prod",
	App:          "gateway",
	Event:        "created",
	ResourceType: "contact",
	ResourceID:   "contact-123",
	CustomerID:   "customer-456",
	Level:        "info",
	Message:      json.RawMessage(`{"source":"api"}`),
	CreatedAt:    time.Now().UTC(),
}
event.GenerateUniqueKey()
```

### `(*Event).GenerateUniqueKey`

Generates a unique key when the event does not already have one. The generated value is limited to 64 characters.

```go
event := &systemevents.Event{Namespace: "prod", App: "gateway", Event: "created"}
event.GenerateUniqueKey()
```

### `(*Event).UnmarshalJSON`

`Event` implements `json.Unmarshaler`. In addition to RFC 3339 timestamps, `created_at` accepts common timestamp strings, Unix seconds, Unix milliseconds, or `null`.

```go
var event systemevents.Event
err := json.Unmarshal([]byte(`{
  "namespace":"prod",
  "app":"gateway",
  "event":"created",
  "created_at":1710000000
}`), &event)
if err != nil {
	return err
}
```

## RabbitMQ worker

### `New`

Creates a RabbitMQ connection and a durable topic consumer. Each host must be in `host:port` form. Call `Close` when the worker is no longer needed.

```go
worker, err := systemevents.New(
	"guest",
	"guest",
	[]string{"localhost:5672"},
	"contacts-worker",
	"system-events",
	[]string{"contact.*"},
	4,
)
if err != nil {
	return err
}
defer worker.Close()
```

### `NewWithConnection`

Creates a consumer using an existing `go-rabbitmq` connection. Once passed to the worker, `Worker.Close` closes that connection.

```go
conn, err := rabbitmq.NewConn("amqp://guest:guest@localhost:5672/")
if err != nil {
	return err
}

worker, err := systemevents.NewWithConnection(
	conn,
	"contacts-worker",
	"system-events",
	[]string{"contact.*"},
	4,
)
if err != nil {
	conn.Close()
	return err
}
defer worker.Close()
```

### `(*Worker).Run`

Starts consuming messages. The handler returns the action to apply to every delivery.

```go
err := worker.Run(func(delivery rabbitmq.Delivery) rabbitmq.Action {
	var event systemevents.Event
	if err := json.Unmarshal(delivery.Body, &event); err != nil {
		return rabbitmq.NackDiscard
	}

	// Process event.
	return rabbitmq.Ack
})
if err != nil {
	return err
}
```

### `(*Worker).Close`

Closes the consumer, cached publisher, and RabbitMQ connection. It is safe to call more than once.

```go
if err := worker.Close(); err != nil {
	return err
}
```

## Publishing

All publishing methods lazily create and reuse a RabbitMQ publisher. The optional `rabbitmq.PublishOptions` functions can customize publishing behavior.

### `(*Worker).PublishQueue`

Publishes raw bytes to one or more queues through RabbitMQ's default exchange.

```go
err := worker.PublishQueue(
	[]byte("plain text"),
	[]string{"system-events"},
	rabbitmq.WithPublishOptionsPersistentDelivery,
)
if err != nil {
	return err
}
```

### `(*Worker).PublishQueueWithContext`

The context-aware equivalent of `PublishQueue`.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := worker.PublishQueueWithContext(ctx, []byte("plain text"), []string{"system-events"}); err != nil {
	return err
}
```

### `(*Worker).PublishTopic`

Publishes raw bytes to a routing key on a named exchange.

```go
if err := worker.PublishTopic([]byte("plain text"), "system-events", "contact.created"); err != nil {
	return err
}
```

### `(*Worker).PublishTopicWithContext`

The context-aware equivalent of `PublishTopic`.

```go
ctx := context.Background()
if err := worker.PublishTopicWithContext(ctx, []byte("plain text"), "system-events", "contact.created"); err != nil {
	return err
}
```

### `(*Worker).PublishJSONQueue`

Marshals a value as JSON and publishes it to one or more queues. It sets the content type to `application/json`.

```go
payload := map[string]string{"event": "contact.created"}
if err := worker.PublishJSONQueue(payload, []string{"system-events"}); err != nil {
	return err
}
```

### `(*Worker).PublishJSONQueueWithContext`

The context-aware equivalent of `PublishJSONQueue`.

```go
ctx := context.Background()
payload := map[string]string{"event": "contact.created"}
if err := worker.PublishJSONQueueWithContext(ctx, payload, []string{"system-events"}); err != nil {
	return err
}
```

### `(*Worker).PublishJSONTopic`

Marshals a value as JSON and publishes it to a routing key on a named exchange.

```go
payload := map[string]string{"event": "contact.created"}
if err := worker.PublishJSONTopic(payload, "system-events", "contact.created"); err != nil {
	return err
}
```

### `(*Worker).PublishJSONTopicWithContext`

The context-aware equivalent of `PublishJSONTopic`.

```go
ctx := context.Background()
payload := map[string]string{"event": "contact.created"}
if err := worker.PublishJSONTopicWithContext(ctx, payload, "system-events", "contact.created"); err != nil {
	return err
}
```

## HTTP push client

### `CreatePushClient`

Creates a client that sends `POST` requests to `{baseURL}/events` and `{baseURL}/notifications`. A non-positive timeout defaults to five seconds. An empty base URL makes sends no-ops, which is useful when push delivery is disabled.

The optional `RetryConfig` configures retries for connection failures, `429 Too Many Requests`, and `5xx` responses. Other `4xx` responses are returned immediately. `MaxRetries` excludes the initial attempt; zero disables retries. When retries are enabled, omitted/non-positive backoff values default to 100ms initially and a 5s maximum.

```go
client := systemevents.CreatePushClient(
	5*time.Second,
	"https://system-events.example.com",
	"prod",
	"gateway",
	systemevents.RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
	},
)
```

### `(*PushClient).SendEvent`

Sends an event to `/events`. Missing `Namespace` and `App` values are populated from the client configuration, and a missing `UniqueKey` is generated.

```go
event := &systemevents.Event{
	Event:        "created",
	ResourceType: "contact",
	ResourceID:   "contact-123",
	Level:        "info",
	Message:      json.RawMessage(`{"source":"api"}`),
}
if err := client.SendEvent(context.Background(), event); err != nil {
	return err
}
```

### `(*PushClient).SendNotification`

Sends an event-shaped notification payload to `/notifications`, with the same defaulting and retry behavior as `SendEvent`.

```go
notification := &systemevents.Event{
	Event:    "password-reset",
	Level:    "warning",
	Email:    "person@example.com",
	Channels: []string{"email"},
	Message:  json.RawMessage(`{"reset_url":"https://example.com/reset"}`),
}
if err := client.SendNotification(context.Background(), notification); err != nil {
	return err
}
```