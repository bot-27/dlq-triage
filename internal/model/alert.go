package model

import "time"

// AlertPayload defines the payload dispatched to PagerDuty/Slack for critical issues like logic bugs.
type AlertPayload struct {
	Severity    string            `json:"severity"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	MessageID   string            `json:"message_id"`
	Topic       string            `json:"topic"`
	StackTrace  string            `json:"stack_trace"`
	Reasoning   string            `json:"reasoning"`
	Timestamp   time.Time         `json:"timestamp"`
	Metadata    map[string]string `json:"metadata"`
}
