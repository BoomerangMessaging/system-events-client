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
