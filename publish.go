package systemeventslisten

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wagslane/go-rabbitmq"
)

func (w *Worker) createPublisher() (*rabbitmq.Publisher, error) {
	w.publisherMutex.Lock()
	defer w.publisherMutex.Unlock()

	if w.publisher != nil {
		return w.publisher, nil
	}
	if w.conn == nil {
		return nil, fmt.Errorf("worker connection is not initialized")
	}

	publisher, err := rabbitmq.NewPublisher(w.conn)
	if err != nil {
		return nil, err
	}

	w.publisher = publisher
	return publisher, nil
}

// Publish sends data to the provided queue names via RabbitMQ's default exchange.
// Callers can override the exchange through optionFuncs when needed.
func (w *Worker) Publish(data []byte, queueNames []string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	return w.PublishWithContext(context.Background(), data, queueNames, optionFuncs...)
}

// PublishWithContext sends data to the provided queue names via RabbitMQ's default exchange.
// Callers can override the exchange through optionFuncs when needed.
func (w *Worker) PublishWithContext(ctx context.Context, data []byte, queueNames []string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	if len(queueNames) == 0 {
		return fmt.Errorf("at least one queue name must be provided")
	}

	publisher, err := w.createPublisher()
	if err != nil {
		return err
	}

	return publisher.PublishWithContext(ctx, data, queueNames, optionFuncs...)
}

// PublishJSON marshals payload as JSON and sends it to the provided queue names.
func (w *Worker) PublishJSON(payload any, queueNames []string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	return w.PublishJSONWithContext(context.Background(), payload, queueNames, optionFuncs...)
}

// PublishJSONWithContext marshals payload as JSON and sends it to the provided queue names.
func (w *Worker) PublishJSONWithContext(ctx context.Context, payload any, queueNames []string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	options := append([]func(*rabbitmq.PublishOptions){
		rabbitmq.WithPublishOptionsContentType("application/json"),
	}, optionFuncs...)

	return w.PublishWithContext(ctx, data, queueNames, options...)
}
