package infra

import (
	"context"
	"fmt"
	"time"

	"dlq-triage/internal/config"
	"dlq-triage/internal/port"

	kafkaGo "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Producer implements the port.KafkaProducer interface using segmentio/kafka-go.
type Producer struct {
	writer *kafkaGo.Writer
	logger *zap.Logger
}

// NewProducer initializes a resilient Kafka producer.
func NewProducer(cfg config.KafkaConfig, logger *zap.Logger) port.KafkaProducer {
	writer := &kafkaGo.Writer{
		Addr:         kafkaGo.TCP(cfg.Brokers...),
		Balancer:     &kafkaGo.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		RequiredAcks: kafkaGo.RequireOne,
		Compression:  kafkaGo.Snappy,
		Async:        false,
	}

	return &Producer{
		writer: writer,
		logger: logger.Named("kafka_producer"),
	}
}

// Publish sends a message to the target topic with the specified key, value, and headers.
func (p *Producer) Publish(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) error {
	var kafkaHeaders []kafkaGo.Header
	for k, v := range headers {
		kafkaHeaders = append(kafkaHeaders, kafkaGo.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	msg := kafkaGo.Message{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: kafkaHeaders,
		Time:    time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.logger.Error("Failed to write message to Kafka",
			zap.String("topic", topic),
			zap.ByteString("key", key),
			zap.Error(err),
		)
		return fmt.Errorf("kafka write failed for topic %s: %w", topic, err)
	}

	p.logger.Debug("Successfully published message to Kafka",
		zap.String("topic", topic),
		zap.ByteString("key", key),
		zap.Int("payload_bytes", len(value)),
	)

	return nil
}

// Close gracefully closes the underlying Kafka writer.
func (p *Producer) Close() error {
	p.logger.Info("Closing Kafka producer connection...")
	return p.writer.Close()
}
