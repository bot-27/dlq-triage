package service

import (
	"context"
	"errors"
	"testing"

	"dlq-triage/internal/config"
	"dlq-triage/internal/model"
	"dlq-triage/internal/observability"

	"go.uber.org/zap"
)

type mockJevClient struct {
	classifyFunc func(ctx context.Context, msg model.DLQMessage) (*model.TriageDecision, error)
}

func (m *mockJevClient) Classify(ctx context.Context, msg model.DLQMessage) (*model.TriageDecision, error) {
	if m.classifyFunc != nil {
		return m.classifyFunc(ctx, msg)
	}
	return nil, errors.New("classifyFunc not set")
}

type mockKafkaProducer struct {
	published []publishedMessage
}

type publishedMessage struct {
	topic   string
	key     []byte
	value   []byte
	headers map[string]string
}

func (m *mockKafkaProducer) Publish(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) error {
	m.published = append(m.published, publishedMessage{
		topic:   topic,
		key:     key,
		value:   value,
		headers: headers,
	})
	return nil
}

func (m *mockKafkaProducer) Close() error {
	return nil
}

type mockAlerter struct {
	alerts []model.AlertPayload
}

func (m *mockAlerter) Alert(ctx context.Context, alert model.AlertPayload) error {
	m.alerts = append(m.alerts, alert)
	return nil
}

func setupTestService(decision model.Classification, reasoning string) (*TriageService, *mockKafkaProducer, *mockAlerter) {
	cfg := config.Config{
		Kafka: config.KafkaConfig{
			MainTopic:    "main_topic",
			DeadEndTopic: "dead_end_topic",
		},
		Triage: config.TriageConfig{
			MaxRetries:  3,
			RetryHeader: "X-Retry-Count",
		},
	}

	mockJev := &mockJevClient{
		classifyFunc: func(ctx context.Context, msg model.DLQMessage) (*model.TriageDecision, error) {
			return &model.TriageDecision{
				Classification: decision,
				Confidence:     0.95,
				Reasoning:      reasoning,
			}, nil
		},
	}

	mockProd := &mockKafkaProducer{}
	mockAlert := &mockAlerter{}
	metrics := observability.NewTestMetrics()
	logger := zap.NewNop()

	svc := NewTriageService(cfg, mockJev, mockProd, mockAlert, metrics, logger)
	return svc, mockProd, mockAlert
}

func TestTriage_TransientNetworkError_RequeuesWithHeader(t *testing.T) {
	svc, mockProd, _ := setupTestService(model.ClassificationTransientNetworkError, "DB timeout")

	msg := model.DLQMessage{
		ID:            "msg-1",
		OriginalTopic: "orders",
		Payload:       []byte(`{"order_id": 123}`),
		Headers:       map[string]string{"X-Retry-Count": "0"},
	}

	res, err := svc.Triage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ActionTaken != "REQUEUED_TO_MAIN_TOPIC" {
		t.Errorf("expected REQUEUED_TO_MAIN_TOPIC, got %s", res.ActionTaken)
	}

	if len(mockProd.published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(mockProd.published))
	}

	pub := mockProd.published[0]
	if pub.topic != "main_topic" {
		t.Errorf("expected target topic main_topic, got %s", pub.topic)
	}
	if pub.headers["X-Retry-Count"] != "1" {
		t.Errorf("expected X-Retry-Count 1, got %s", pub.headers["X-Retry-Count"])
	}
}

func TestTriage_TransientNetworkError_MaxRetriesExceeded(t *testing.T) {
	svc, mockProd, _ := setupTestService(model.ClassificationTransientNetworkError, "DB timeout")

	msg := model.DLQMessage{
		ID:            "msg-2",
		OriginalTopic: "orders",
		Payload:       []byte(`{"order_id": 123}`),
		Headers:       map[string]string{"X-Retry-Count": "3"},
	}

	res, err := svc.Triage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ActionTaken != "MAX_RETRIES_EXCEEDED_MOVED_TO_DEAD_END" {
		t.Errorf("expected MAX_RETRIES_EXCEEDED_MOVED_TO_DEAD_END, got %s", res.ActionTaken)
	}

	if mockProd.published[0].topic != "dead_end_topic" {
		t.Errorf("expected dead_end_topic, got %s", mockProd.published[0].topic)
	}
}

func TestTriage_MalformedJSON(t *testing.T) {
	svc, mockProd, _ := setupTestService(model.ClassificationMalformedJSON, "Syntax error at line 1")

	msg := model.DLQMessage{
		ID:      "msg-3",
		Payload: []byte(`{"broken: "json"`),
	}

	res, err := svc.Triage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ActionTaken != "ROUTED_TO_DEAD_END_TOPIC" {
		t.Errorf("expected ROUTED_TO_DEAD_END_TOPIC, got %s", res.ActionTaken)
	}

	if mockProd.published[0].topic != "dead_end_topic" {
		t.Errorf("expected dead_end_topic, got %s", mockProd.published[0].topic)
	}
}

func TestTriage_LogicBug_TriggersAlertAndDrops(t *testing.T) {
	svc, mockProd, mockAlert := setupTestService(model.ClassificationLogicBug, "Null pointer dereference in order calculation")

	msg := model.DLQMessage{
		ID:         "msg-4",
		Payload:    []byte(`{"order_id": 999}`),
		StackTrace: "goroutine 1 [running]: main.ProcessOrder()",
	}

	res, err := svc.Triage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ActionTaken != "TRIGGERED_ALERT_AND_DROPPED" {
		t.Errorf("expected TRIGGERED_ALERT_AND_DROPPED, got %s", res.ActionTaken)
	}

	if len(mockAlert.alerts) != 1 {
		t.Fatalf("expected 1 alert triggered, got %d", len(mockAlert.alerts))
	}

	if len(mockProd.published) != 0 {
		t.Errorf("expected 0 published messages (message dropped), got %d", len(mockProd.published))
	}
}

func TestTriage_MissingUserData_LogsWarningAndDrops(t *testing.T) {
	svc, mockProd, mockAlert := setupTestService(model.ClassificationMissingUserData, "User 8847 not found in DB")

	msg := model.DLQMessage{
		ID:      "msg-5",
		Payload: []byte(`{"user_id": 8847}`),
	}

	res, err := svc.Triage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ActionTaken != "LOGGED_WARNING_AND_DROPPED" {
		t.Errorf("expected LOGGED_WARNING_AND_DROPPED, got %s", res.ActionTaken)
	}

	if len(mockAlert.alerts) != 0 {
		t.Errorf("expected 0 alerts, got %d", len(mockAlert.alerts))
	}

	if len(mockProd.published) != 0 {
		t.Errorf("expected 0 messages published, got %d", len(mockProd.published))
	}
}
