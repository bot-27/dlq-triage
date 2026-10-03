package service

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"dlq-triage/internal/config"
	"dlq-triage/internal/model"
	"dlq-triage/internal/observability"
	"dlq-triage/internal/port"

	"go.uber.org/zap"
)

// TriageService coordinates the evaluation, Jev AI decision classification, and strategy routing of DLQ messages.
type TriageService struct {
	cfg      config.Config
	jev      port.JevClient
	producer port.KafkaProducer
	alerter  port.Alerter
	metrics  *observability.Metrics
	logger   *zap.Logger
}

// NewTriageService constructs a new Triage service with explicit dependency injection.
func NewTriageService(
	cfg config.Config,
	jev port.JevClient,
	producer port.KafkaProducer,
	alerter port.Alerter,
	metrics *observability.Metrics,
	logger *zap.Logger,
) *TriageService {
	return &TriageService{
		cfg:      cfg,
		jev:      jev,
		producer: producer,
		alerter:  alerter,
		metrics:  metrics,
		logger:   logger.Named("triage_service"),
	}
}

// Triage executes the core business logic workflow for an incoming DLQ message.
func (s *TriageService) Triage(ctx context.Context, msg model.DLQMessage) (*model.TriageResult, error) {
	startTime := time.Now()

	s.logger.Info("Starting DLQ message triage",
		zap.String("message_id", msg.ID),
		zap.String("topic", msg.OriginalTopic),
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
	)

	// Step 1: Query Jev AI Decision Choice primitive
	decision, err := s.jev.Classify(ctx, msg)
	if err != nil {
		s.logger.Error("Jev classification failed; routing to dead-end safety topic",
			zap.String("message_id", msg.ID),
			zap.Error(err),
		)
		fallbackHeaders := copyHeaders(msg.Headers)
		fallbackHeaders["X-Triage-Fallback"] = "jev_api_error"
		_ = s.producer.Publish(ctx, s.cfg.Kafka.DeadEndTopic, msg.OriginalKey, msg.Payload, fallbackHeaders)
		return nil, fmt.Errorf("triage aborted due to classification failure: %w", err)
	}

	// Step 2: Extract and parse current retry count from headers
	retryCount := s.extractRetryCount(msg.Headers)

	result := &model.TriageResult{
		MessageID:     msg.ID,
		Decision:      *decision,
		RetryCount:    retryCount,
		ProcessedAt:   time.Now(),
		ExecutionTime: time.Since(startTime),
	}

	// Step 3: Strategy routing based on probabilistic classification
	switch decision.Classification {

	// Strategy 1: Transient network error -> Requeue to main topic with incremented retry count
	case model.ClassificationTransientNetworkError:
		if retryCount < s.cfg.Triage.MaxRetries {
			newRetryCount := retryCount + 1
			headers := copyHeaders(msg.Headers)
			headers[s.cfg.Triage.RetryHeader] = strconv.Itoa(newRetryCount)
			headers["X-Triage-Decision"] = string(decision.Classification)
			headers["X-Triage-Reasoning"] = decision.Reasoning

			if err := s.producer.Publish(ctx, s.cfg.Kafka.MainTopic, msg.OriginalKey, msg.Payload, headers); err != nil {
				s.logger.Error("Failed to requeue message to main topic",
					zap.String("message_id", msg.ID),
					zap.Int("retry_attempt", newRetryCount),
					zap.Error(err),
				)
				return nil, fmt.Errorf("failed to requeue transient error message: %w", err)
			}

			s.metrics.MessagesRequeuedTotal.WithLabelValues(s.cfg.Kafka.MainTopic, strconv.Itoa(newRetryCount)).Inc()
			s.metrics.TriageDecisionsTotal.WithLabelValues(string(decision.Classification), "requeued_main_topic").Inc()

			result.ActionTaken = "REQUEUED_TO_MAIN_TOPIC"
			result.Destination = s.cfg.Kafka.MainTopic
			result.RetryCount = newRetryCount

			s.logger.Info("Message successfully requeued to main topic",
				zap.String("message_id", msg.ID),
				zap.String("target_topic", s.cfg.Kafka.MainTopic),
				zap.Int("new_retry_count", newRetryCount),
				zap.Float64("confidence", decision.Confidence),
			)
		} else {
			headers := copyHeaders(msg.Headers)
			headers["X-Triage-Status"] = "max_retries_exceeded"
			headers["X-Original-Classification"] = string(decision.Classification)

			if err := s.producer.Publish(ctx, s.cfg.Kafka.DeadEndTopic, msg.OriginalKey, msg.Payload, headers); err != nil {
				return nil, fmt.Errorf("failed to push to dead end topic after retry exhaustion: %w", err)
			}

			s.metrics.TriageDecisionsTotal.WithLabelValues(string(decision.Classification), "dead_end_max_retries").Inc()
			result.ActionTaken = "MAX_RETRIES_EXCEEDED_MOVED_TO_DEAD_END"
			result.Destination = s.cfg.Kafka.DeadEndTopic

			s.logger.Warn("Message exceeded maximum retry count; forwarded to dead-end topic",
				zap.String("message_id", msg.ID),
				zap.Int("retry_count", retryCount),
				zap.Int("max_retries", s.cfg.Triage.MaxRetries),
			)
		}

	// Strategy 2: Malformed JSON -> Publish to dead_end_topic & structured-log an Error
	case model.ClassificationMalformedJSON:
		headers := copyHeaders(msg.Headers)
		headers["X-Triage-Decision"] = string(decision.Classification)
		headers["X-Triage-Reasoning"] = decision.Reasoning

		if err := s.producer.Publish(ctx, s.cfg.Kafka.DeadEndTopic, msg.OriginalKey, msg.Payload, headers); err != nil {
			s.logger.Error("Failed to route malformed JSON to dead_end_topic",
				zap.String("message_id", msg.ID),
				zap.Error(err),
			)
			return nil, fmt.Errorf("failed to route malformed JSON: %w", err)
		}

		s.metrics.TriageDecisionsTotal.WithLabelValues(string(decision.Classification), "dead_end_topic").Inc()
		result.ActionTaken = "ROUTED_TO_DEAD_END_TOPIC"
		result.Destination = s.cfg.Kafka.DeadEndTopic

		s.logger.Error("Malformed JSON detected in DLQ message; routed to dead-end topic",
			zap.String("message_id", msg.ID),
			zap.String("topic", msg.OriginalTopic),
			zap.String("dead_end_topic", s.cfg.Kafka.DeadEndTopic),
			zap.String("reasoning", decision.Reasoning),
			zap.Float64("confidence", decision.Confidence),
		)

	// Strategy 3: Logic bug -> Trigger alert via webhook and drop message
	case model.ClassificationLogicBug:
		alertPayload := model.AlertPayload{
			Severity:    "CRITICAL",
			Title:       "Application Logic Bug Detected in Event Stream",
			Description: fmt.Sprintf("Jev Decision AI detected a deterministic logic bug in payload from topic %s", msg.OriginalTopic),
			MessageID:   msg.ID,
			Topic:       msg.OriginalTopic,
			StackTrace:  msg.StackTrace,
			Reasoning:   decision.Reasoning,
			Timestamp:   time.Now(),
			Metadata: map[string]string{
				"partition":  strconv.Itoa(msg.Partition),
				"offset":     strconv.FormatInt(msg.Offset, 10),
				"confidence": fmt.Sprintf("%.2f", decision.Confidence),
			},
		}

		if err := s.alerter.Alert(ctx, alertPayload); err != nil {
			s.logger.Error("Failed to dispatch alert webhook for logic bug",
				zap.String("message_id", msg.ID),
				zap.Error(err),
			)
		}

		s.metrics.TriageDecisionsTotal.WithLabelValues(string(decision.Classification), "alerted_and_dropped").Inc()
		result.ActionTaken = "TRIGGERED_ALERT_AND_DROPPED"

		s.logger.Error("Logic bug classified; dispatched PagerDuty alert and dropped message",
			zap.String("message_id", msg.ID),
			zap.String("error_message", msg.ErrorMessage),
			zap.String("reasoning", decision.Reasoning),
			zap.Float64("confidence", decision.Confidence),
		)

	// Strategy 4: Missing user data -> Structured-log a Warning and drop message
	case model.ClassificationMissingUserData:
		s.metrics.TriageDecisionsTotal.WithLabelValues(string(decision.Classification), "logged_warning_and_dropped").Inc()
		result.ActionTaken = "LOGGED_WARNING_AND_DROPPED"

		s.logger.Warn("Dropping DLQ message due to missing user data (unrecoverable entity state)",
			zap.String("message_id", msg.ID),
			zap.String("topic", msg.OriginalTopic),
			zap.String("reasoning", decision.Reasoning),
			zap.Float64("confidence", decision.Confidence),
		)

	default:
		s.logger.Error("Unrecognized Jev classification; routing to dead-end safety topic",
			zap.String("message_id", msg.ID),
			zap.String("unknown_classification", string(decision.Classification)),
		)
		_ = s.producer.Publish(ctx, s.cfg.Kafka.DeadEndTopic, msg.OriginalKey, msg.Payload, msg.Headers)
		result.ActionTaken = "UNKNOWN_CLASSIFICATION_DEAD_END"
		result.Destination = s.cfg.Kafka.DeadEndTopic
	}

	result.ExecutionTime = time.Since(startTime)
	return result, nil
}

func (s *TriageService) extractRetryCount(headers map[string]string) int {
	if headers == nil {
		return 0
	}
	val, ok := headers[s.cfg.Triage.RetryHeader]
	if !ok {
		return 0
	}
	count, err := strconv.Atoi(val)
	if err != nil || count < 0 {
		return 0
	}
	return count
}

func copyHeaders(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src)+4)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
