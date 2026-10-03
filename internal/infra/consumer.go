package infra

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"dlq-triage/internal/config"
	"dlq-triage/internal/model"
	"dlq-triage/internal/observability"
	"dlq-triage/internal/port"

	kafkaGo "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Consumer manages the Kafka message ingestion, channel dispatch, worker pool, and offset commits.
type Consumer struct {
	reader        *kafkaGo.Reader
	triageService port.TriageUsecase
	metrics       *observability.Metrics
	logger        *zap.Logger
	workerCount   int
	channelBuffer int
	workChan      chan kafkaGo.Message
	wg            sync.WaitGroup
}

// NewConsumer initializes the Kafka DLQ consumer with a bounded worker pool.
func NewConsumer(
	cfg config.KafkaConfig,
	triageCfg config.TriageConfig,
	triageService port.TriageUsecase,
	metrics *observability.Metrics,
	logger *zap.Logger,
) *Consumer {
	reader := kafkaGo.NewReader(kafkaGo.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.ConsumerGroup,
		Topic:          cfg.DLQTopic,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0,
		StartOffset:    kafkaGo.FirstOffset,
	})

	return &Consumer{
		reader:        reader,
		triageService: triageService,
		metrics:       metrics,
		logger:        logger.Named("kafka_consumer"),
		workerCount:   triageCfg.WorkerPoolSize,
		channelBuffer: triageCfg.ChannelBuffer,
		workChan:      make(chan kafkaGo.Message, triageCfg.ChannelBuffer),
	}
}

// Start begins the ingestion loop and spawns concurrent worker goroutines. Blocks until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("Starting DLQ Kafka consumer worker pool",
		zap.Int("workers", c.workerCount),
		zap.Int("channel_buffer", c.channelBuffer),
	)

	for i := 1; i <= c.workerCount; i++ {
		c.wg.Add(1)
		go c.worker(ctx, i)
	}

	defer func() {
		close(c.workChan)
		c.logger.Info("Waiting for in-flight worker tasks to finish...")
		c.wg.Wait()
		if err := c.reader.Close(); err != nil {
			c.logger.Error("Error closing Kafka reader", zap.Error(err))
		} else {
			c.logger.Info("Kafka reader closed gracefully")
		}
	}()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Consumer context cancelled; stopping ingestion loop")
			return nil
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return nil
			}
			c.logger.Error("Failed to fetch message from Kafka", zap.Error(err))
			time.Sleep(200 * time.Millisecond)
			continue
		}

		c.metrics.MessagesConsumedTotal.Inc()

		select {
		case c.workChan <- msg:
		case <-ctx.Done():
			c.logger.Warn("Shutdown initiated while buffering message; discarding uncommitted fetch",
				zap.Int("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
			)
			return nil
		}
	}
}

func (c *Consumer) worker(ctx context.Context, workerID int) {
	defer c.wg.Done()
	workerLogger := c.logger.With(zap.Int("worker_id", workerID))
	workerLogger.Debug("Worker started")

	for msg := range c.workChan {
		dlqMsg := c.parseDLQMessage(msg)

		triageCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		res, err := c.triageService.Triage(triageCtx, dlqMsg)
		cancel()

		if err != nil {
			workerLogger.Error("Triage processing error for message",
				zap.String("msg_id", dlqMsg.ID),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
		} else {
			workerLogger.Debug("Message triaged successfully",
				zap.String("msg_id", dlqMsg.ID),
				zap.String("action", res.ActionTaken),
				zap.Duration("latency", res.ExecutionTime),
			)
		}

		commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.reader.CommitMessages(commitCtx, msg); err != nil {
			workerLogger.Error("Failed to commit Kafka offset",
				zap.Int("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
		}
		commitCancel()
	}

	workerLogger.Debug("Worker shut down cleanly")
}

func (c *Consumer) parseDLQMessage(msg kafkaGo.Message) model.DLQMessage {
	headers := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}

	var envelope struct {
		Payload      json.RawMessage `json:"payload"`
		ErrorMessage string          `json:"error_message"`
		StackTrace   string          `json:"stack_trace"`
		FailedAt     time.Time       `json:"failed_at"`
	}

	dlqMsg := model.DLQMessage{
		ID:            headers["X-Message-ID"],
		OriginalTopic: headers["X-Original-Topic"],
		OriginalKey:   msg.Key,
		Payload:       msg.Value,
		Headers:       headers,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		FailedAt:      msg.Time,
	}

	if dlqMsg.ID == "" {
		dlqMsg.ID = headers["message_id"]
	}
	if dlqMsg.OriginalTopic == "" {
		dlqMsg.OriginalTopic = msg.Topic
	}

	if err := json.Unmarshal(msg.Value, &envelope); err == nil && (envelope.ErrorMessage != "" || envelope.StackTrace != "") {
		if len(envelope.Payload) > 0 {
			dlqMsg.Payload = envelope.Payload
		}
		dlqMsg.ErrorMessage = envelope.ErrorMessage
		dlqMsg.StackTrace = envelope.StackTrace
		if !envelope.FailedAt.IsZero() {
			dlqMsg.FailedAt = envelope.FailedAt
		}
	} else {
		dlqMsg.ErrorMessage = headers["X-Error-Message"]
		dlqMsg.StackTrace = headers["X-Stack-Trace"]
	}

	return dlqMsg
}
