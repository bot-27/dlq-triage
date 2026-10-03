package model

import "time"

// DLQMessage represents the deserialized Dead Letter Queue message received from Kafka.
type DLQMessage struct {
	ID            string            `json:"id"`
	OriginalTopic string            `json:"original_topic"`
	OriginalKey   []byte            `json:"original_key"`
	Payload       []byte            `json:"payload"`
	ErrorMessage  string            `json:"error_message"`
	StackTrace    string            `json:"stack_trace"`
	FailedAt      time.Time         `json:"failed_at"`
	Headers       map[string]string `json:"headers"`
	Partition     int               `json:"partition"`
	Offset        int64             `json:"offset"`
	ConsumerGroup string            `json:"consumer_group"`
}
