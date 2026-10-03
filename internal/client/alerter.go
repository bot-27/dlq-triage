package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"dlq-triage/internal/config"
	"dlq-triage/internal/model"

	"go.uber.org/zap"
)

// PagerDutyEvent represents the standard Events API v2 payload.
type PagerDutyEvent struct {
	RoutingKey  string                `json:"routing_key,omitempty"`
	EventAction string                `json:"event_action"`
	Payload     PagerDutyEventPayload `json:"payload"`
	Client      string                `json:"client"`
	ClientURL   string                `json:"client_url,omitempty"`
}

type PagerDutyEventPayload struct {
	Summary       string                 `json:"summary"`
	Source        string                 `json:"source"`
	Severity      string                 `json:"severity"`
	Timestamp     string                 `json:"timestamp"`
	CustomDetails map[string]interface{} `json:"custom_details"`
}

// WebhookAlerter implements port.Alerter via HTTP webhook (PagerDuty / Slack compatible).
type WebhookAlerter struct {
	cfg        config.AlertConfig
	httpClient *http.Client
	logger     *zap.Logger
}

// NewWebhookAlerter constructs an alerting client.
func NewWebhookAlerter(cfg config.AlertConfig, logger *zap.Logger) *WebhookAlerter {
	return &WebhookAlerter{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		logger: logger.Named("alerter"),
	}
}

// Alert dispatches an incident payload to the configured webhook endpoint.
func (a *WebhookAlerter) Alert(ctx context.Context, alert model.AlertPayload) error {
	event := PagerDutyEvent{
		EventAction: "trigger",
		Client:      "DLQ-Auto-Triage-Service",
		Payload: PagerDutyEventPayload{
			Summary:   fmt.Sprintf("[DLQ Logic Bug Alert] %s: %s", alert.Topic, alert.Title),
			Source:    "dlq-triage-microservice",
			Severity:  "critical",
			Timestamp: alert.Timestamp.Format(time.RFC3339),
			CustomDetails: map[string]interface{}{
				"message_id":  alert.MessageID,
				"topic":       alert.Topic,
				"reasoning":   alert.Reasoning,
				"stack_trace": alert.StackTrace,
				"metadata":    alert.Metadata,
			},
		},
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal alert payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create alert HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		a.logger.Error("Failed to trigger webhook alert",
			zap.String("webhook_url", a.cfg.WebhookURL),
			zap.String("message_id", alert.MessageID),
			zap.Error(err),
		)
		return fmt.Errorf("webhook call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		a.logger.Error("Webhook endpoint returned non-2xx status",
			zap.Int("status_code", resp.StatusCode),
			zap.String("response", string(respBody)),
		)
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}

	a.logger.Info("Dispatched critical logic bug alert to incident webhook",
		zap.String("message_id", alert.MessageID),
		zap.String("topic", alert.Topic),
	)

	return nil
}
