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

	publisher, err := rabbitmq.NewPublisher(
		w.conn,
		rabbitmq.WithPublisherOptionsExchangeName(w.exchange),
		rabbitmq.WithPublisherOptionsExchangeKind("topic"),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
		rabbitmq.WithPublisherOptionsExchangeDurable,
	)
	if err != nil {
		return nil, err
	}

	w.publisher = publisher
	return publisher, nil
}

func (w *Worker) Publish(data []byte, routingKeys []string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	return w.PublishWithContext(context.Background(), data, routingKeys, optionFuncs...)
}

func (w *Worker) PublishWithContext(ctx context.Context, data []byte, routingKeys []string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	if len(routingKeys) == 0 {
		return fmt.Errorf("at least one routing key must be provided")
	}
	if ctx == nil {
		return fmt.Errorf("context must not be nil")
	}

	publisher, err := w.createPublisher()
	if err != nil {
		return err
	}

	options := append([]func(*rabbitmq.PublishOptions){
		rabbitmq.WithPublishOptionsExchange(w.exchange),
	}, optionFuncs...)

	return publisher.PublishWithContext(ctx, data, routingKeys, options...)
}

func (w *Worker) PublishJSON(payload any, routingKeys []string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	return w.PublishJSONWithContext(context.Background(), payload, routingKeys, optionFuncs...)
}

func (w *Worker) PublishJSONWithContext(ctx context.Context, payload any, routingKeys []string, optionFuncs ...func(*rabbitmq.PublishOptions)) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	options := append([]func(*rabbitmq.PublishOptions){
		rabbitmq.WithPublishOptionsContentType("application/json"),
	}, optionFuncs...)

	return w.PublishWithContext(ctx, data, routingKeys, options...)
}
