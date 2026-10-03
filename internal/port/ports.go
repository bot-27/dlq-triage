package port

import (
	"context"

	"dlq-triage/internal/model"
)

// JevClient defines the interface for calling the Jev AI Decision API Choice primitive.
type JevClient interface {
	Classify(ctx context.Context, msg model.DLQMessage) (*model.TriageDecision, error)
}

// KafkaProducer defines the interface for producing messages to target Kafka topics.
type KafkaProducer interface {
	Publish(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) error
	Close() error
}

// Alerter defines the interface for dispatching alerts to external notification channels.
type Alerter interface {
	Alert(ctx context.Context, alert model.AlertPayload) error
}

// TriageUsecase represents the domain service for evaluating and routing DLQ messages.
type TriageUsecase interface {
	Triage(ctx context.Context, msg model.DLQMessage) (*model.TriageResult, error)
}
