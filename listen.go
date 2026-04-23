package systemeventslisten

import (
	"fmt"
	"sync"

	"github.com/wagslane/go-rabbitmq"
)

type Worker struct {
	conn           *rabbitmq.Conn
	consumer       *rabbitmq.Consumer
	publisher      *rabbitmq.Publisher
	publisherMutex sync.Mutex
	exchange       string
}

// Close consumer and connection.
// It is important to close the consumer before closing the connection
// to ensure all messages are properly acknowledged and resources are released.
// This will block until all pending messages are processed.
func (w *Worker) Close() error {
	if w.consumer != nil {
		w.consumer.Close()
	}

	w.publisherMutex.Lock()
	publisher := w.publisher
	w.publisher = nil
	w.publisherMutex.Unlock()

	if publisher != nil {
		publisher.Close()
	}

	return w.conn.Close()
}

func (w *Worker) Run(handler func(d rabbitmq.Delivery) rabbitmq.Action) error {
	return w.consumer.Run(handler)
}

func (worker *Worker) createConsumer(workerName, exchange string, routingKeys []string, concurrency int) (err error) {
	worker.exchange = exchange
	opts := []func(*rabbitmq.ConsumerOptions){
		rabbitmq.WithConsumerOptionsQueueDurable,
		rabbitmq.WithConsumerOptionsConsumerName("system-events-listen-" + workerName),
		rabbitmq.WithConsumerOptionsExchangeName(exchange),
		rabbitmq.WithConsumerOptionsExchangeKind("topic"),
		rabbitmq.WithConsumerOptionsExchangeDeclare,
		rabbitmq.WithConsumerOptionsExchangeDurable}
	for _, key := range routingKeys {
		opts = append(opts, rabbitmq.WithConsumerOptionsRoutingKey(key))
	}

	if concurrency > 0 {
		opts = append(opts, rabbitmq.WithConsumerOptionsConcurrency(concurrency))
	}

	worker.consumer, err = rabbitmq.NewConsumer(
		worker.conn,
		"system_events_listen_"+workerName,
		opts...,
	)

	if err != nil && worker.conn != nil {
		worker.conn.Close()
	}

	return
}

func NewWithConnection(conn *rabbitmq.Conn, workerName, exchange string, routingKeys []string, concurrency int) (worker *Worker, err error) {
	worker = &Worker{conn: conn}
	err = worker.createConsumer(workerName, exchange, routingKeys, concurrency)
	if err != nil {
		return nil, err
	}

	return worker, nil
}

func New(user, password string, hosts []string, workerName, exchange string, routingKeys []string, concurrency int) (worker *Worker, err error) {

	if len(routingKeys) == 0 {
		return nil, fmt.Errorf("at least one routing key must be provided")
	}

	if len(hosts) == 0 {
		return nil, fmt.Errorf("at least one host must be provided")
	}

	dsns := make([]string, 0, len(hosts))
	for _, host := range hosts {
		dsns = append(dsns, fmt.Sprintf("amqp://%s:%s@%s/", user, password, host))
	}

	worker = &Worker{}
	if len(hosts) == 1 {
		worker.conn, err = rabbitmq.NewConn(
			dsns[0],
			rabbitmq.WithConnectionOptionsLogging,
		)
	} else {
		worker.conn, err = rabbitmq.NewClusterConn(
			rabbitmq.NewStaticResolver(dsns, false /* shuffle */),
			rabbitmq.WithConnectionOptionsLogging,
		)
	}

	if err != nil {
		return nil, err
	}

	err = worker.createConsumer(workerName, exchange, routingKeys, concurrency)
	if err != nil {
		return nil, err
	}

	return
}
