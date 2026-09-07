package systemeventslisten

import (
	"strings"
	"testing"
)

func TestCreatePublisherRejectsClosedWorker(t *testing.T) {
	worker := &Worker{closed: true}

	if _, err := worker.createPublisher(); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("createPublisher() = %v, want an error indicating the worker is closed", err)
	}
}

func TestPublishQueueAndTopicValidation(t *testing.T) {
	worker := &Worker{}

	if err := worker.PublishQueue([]byte("hello"), nil); err == nil || !strings.Contains(err.Error(), "at least one queue name") {
		t.Fatalf("PublishQueue() = %v, want queue validation error", err)
	}
	if err := worker.PublishTopic([]byte("hello"), "", "orders.created"); err == nil || !strings.Contains(err.Error(), "exchange name") {
		t.Fatalf("PublishTopic() = %v, want exchange validation error", err)
	}
	if err := worker.PublishTopic([]byte("hello"), "events", ""); err == nil || !strings.Contains(err.Error(), "routing key") {
		t.Fatalf("PublishTopic() = %v, want routing key validation error", err)
	}
}
