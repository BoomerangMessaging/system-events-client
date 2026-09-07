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
	return w.createPublisherLocked()
}

func (w *Worker) createPublisherLocked() (*rabbitmq.Publisher, error) {
	if w.closed {
		return nil, fmt.Errorf("worker is closed")
	}
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

// PublishQueue sends data to the provided queue names via RabbitMQ's default exchange.
func (w *Worker) PublishQueue(data []byte, queueNames []string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	return w.PublishQueueWithContext(context.Background(), data, queueNames, optionFuncs...)
}

// PublishQueueWithContext sends data to the provided queue names via RabbitMQ's default exchange.
func (w *Worker) PublishQueueWithContext(ctx context.Context, data []byte, queueNames []string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	if len(queueNames) == 0 {
		return fmt.Errorf("at least one queue name must be provided")
	}
	for _, queueName := range queueNames {
		if queueName == "" {
			return fmt.Errorf("queue name must be provided")
		}
	}

	w.publisherMutex.Lock()
	defer w.publisherMutex.Unlock()
	publisher, err := w.createPublisherLocked()
	if err != nil {
		return err
	}

	return publisher.PublishWithContext(ctx, data, queueNames, optionFuncs...)
}

// PublishTopic sends data to the provided routing key on the named exchange.
func (w *Worker) PublishTopic(data []byte, exchangeName, routingKey string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	return w.PublishTopicWithContext(context.Background(), data, exchangeName, routingKey, optionFuncs...)
}

// PublishTopicWithContext sends data to the provided routing key on the named exchange.
func (w *Worker) PublishTopicWithContext(ctx context.Context, data []byte, exchangeName, routingKey string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	if exchangeName == "" {
		return fmt.Errorf("exchange name must be provided")
	}
	if routingKey == "" {
		return fmt.Errorf("routing key must be provided")
	}

	w.publisherMutex.Lock()
	defer w.publisherMutex.Unlock()
	publisher, err := w.createPublisherLocked()
	if err != nil {
		return err
	}

	options := append([]func(*rabbitmq.PublishOptions){
		rabbitmq.WithPublishOptionsExchange(exchangeName),
	}, optionFuncs...)

	return publisher.PublishWithContext(ctx, data, []string{routingKey}, options...)
}

// PublishJSONQueue marshals payload as JSON and sends it to the provided queue names.
func (w *Worker) PublishJSONQueue(payload any, queueNames []string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	return w.PublishJSONQueueWithContext(context.Background(), payload, queueNames, optionFuncs...)
}

// PublishJSONQueueWithContext marshals payload as JSON and sends it to the provided queue names.
func (w *Worker) PublishJSONQueueWithContext(ctx context.Context, payload any, queueNames []string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	options := append([]func(*rabbitmq.PublishOptions){
		rabbitmq.WithPublishOptionsContentType("application/json"),
	}, optionFuncs...)

	return w.PublishQueueWithContext(ctx, data, queueNames, options...)
}

// PublishJSONTopic marshals payload as JSON and sends it to the provided routing key on the named exchange.
func (w *Worker) PublishJSONTopic(payload any, exchangeName, routingKey string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	return w.PublishJSONTopicWithContext(context.Background(), payload, exchangeName, routingKey, optionFuncs...)
}

// PublishJSONTopicWithContext marshals payload as JSON and sends it to the provided routing key on the named exchange.
func (w *Worker) PublishJSONTopicWithContext(ctx context.Context, payload any, exchangeName, routingKey string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	options := append([]func(*rabbitmq.PublishOptions){
		rabbitmq.WithPublishOptionsContentType("application/json"),
	}, optionFuncs...)

	return w.PublishTopicWithContext(ctx, data, exchangeName, routingKey, options...)
}
