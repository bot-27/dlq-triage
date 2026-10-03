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
	"dlq-triage/internal/observability"

	"go.uber.org/zap"
)

// ChoiceRequest represents the schema sent to Jev AI Decision Choice API.
type ChoiceRequest struct {
	Context struct {
		MessageID     string            `json:"message_id"`
		OriginalTopic string            `json:"original_topic"`
		ErrorMessage  string            `json:"error_message"`
		StackTrace    string            `json:"stack_trace"`
		PayloadSample string            `json:"payload_sample"`
		Headers       map[string]string `json:"headers"`
	} `json:"context"`
	Choices           []model.Classification `json:"choices"`
	Temperature       float64                `json:"temperature"`
	ReasoningRequired bool                   `json:"reasoning_required"`
}

// ChoiceResponse represents the schema received from Jev AI Decision Choice API.
type ChoiceResponse struct {
	SelectedChoice      model.Classification   `json:"selected_choice"`
	Confidence          float64                `json:"confidence"`
	Reasoning           string                 `json:"reasoning"`
	ChoiceProbabilities map[string]float64     `json:"choice_probabilities"`
	Metadata            map[string]interface{} `json:"metadata,omitempty"`
}

// JevClient implements the port.JevClient interface.
type JevClient struct {
	httpClient *http.Client
	cfg        config.JevConfig
	metrics    *observability.Metrics
	logger     *zap.Logger
}

// NewJevClient instantiates a production-ready Jev Decision API client.
func NewJevClient(cfg config.JevConfig, metrics *observability.Metrics, logger *zap.Logger) *JevClient {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
	}

	return &JevClient{
		httpClient: &http.Client{
			Timeout:   cfg.Timeout,
			Transport: transport,
		},
		cfg:     cfg,
		metrics: metrics,
		logger:  logger.Named("jev_client"),
	}
}

// Classify queries the Jev AI Decision API using its Choice primitive to classify the failure root cause.
func (c *JevClient) Classify(ctx context.Context, msg model.DLQMessage) (*model.TriageDecision, error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Milliseconds()
		c.metrics.JevAPILatencyMs.Observe(float64(duration))
	}()

	var payloadSample string
	var forwardedHeaders map[string]string

	if c.cfg.MetadataOnly {
		payloadSample = "[OMITTED_METADATA_ONLY_MODE]"
		// In metadata-only mode, only retain structural, non-sensitive routing headers
		forwardedHeaders = make(map[string]string)
		for _, k := range []string{"X-Original-Topic", "X-Message-ID", "X-Retry-Count"} {
			if v, ok := msg.Headers[k]; ok {
				forwardedHeaders[k] = v
			}
		}
	} else {
		payloadStr := string(msg.Payload)
		if len(payloadStr) > 4096 {
			payloadStr = payloadStr[:4096] + "... [TRUNCATED]"
		}
		payloadSample = payloadStr
		forwardedHeaders = msg.Headers
	}

	reqBody := ChoiceRequest{
		Choices: []model.Classification{
			model.ClassificationTransientNetworkError,
			model.ClassificationMalformedJSON,
			model.ClassificationLogicBug,
			model.ClassificationMissingUserData,
		},
		Temperature:       0.1,
		ReasoningRequired: true,
	}
	reqBody.Context.MessageID = msg.ID
	reqBody.Context.OriginalTopic = msg.OriginalTopic
	reqBody.Context.ErrorMessage = msg.ErrorMessage
	reqBody.Context.StackTrace = msg.StackTrace
	reqBody.Context.PayloadSample = payloadSample
	reqBody.Context.Headers = forwardedHeaders

	jsonPayload, err := json.Marshal(reqBody)
	if err != nil {
		c.metrics.JevAPIErrorsTotal.WithLabelValues("marshal_error").Inc()
		return nil, fmt.Errorf("failed to marshal Jev Choice request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL, bytes.NewReader(jsonPayload))
	if err != nil {
		c.metrics.JevAPIErrorsTotal.WithLabelValues("request_creation_error").Inc()
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.cfg.APIKey))
	httpReq.Header.Set("User-Agent", "DLQ-Auto-Triage/1.0")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		c.metrics.JevAPIErrorsTotal.WithLabelValues("network_error").Inc()
		return nil, fmt.Errorf("http request to Jev API failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		c.metrics.JevAPIErrorsTotal.WithLabelValues("read_error").Inc()
		return nil, fmt.Errorf("failed to read Jev response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.metrics.JevAPIErrorsTotal.WithLabelValues(fmt.Sprintf("http_%d", resp.StatusCode)).Inc()
		c.logger.Error("Jev API returned error status",
			zap.Int("status_code", resp.StatusCode),
			zap.String("body", string(bodyBytes)),
		)
		return nil, fmt.Errorf("jev api returned non-200 status: %d", resp.StatusCode)
	}

	var choiceResp ChoiceResponse
	if err := json.Unmarshal(bodyBytes, &choiceResp); err != nil {
		c.metrics.JevAPIErrorsTotal.WithLabelValues("unmarshal_error").Inc()
		return nil, fmt.Errorf("failed to unmarshal Jev response: %w", err)
	}

	latency := time.Since(start).Milliseconds()

	decision := &model.TriageDecision{
		Classification: choiceResp.SelectedChoice,
		Confidence:     choiceResp.Confidence,
		Reasoning:      choiceResp.Reasoning,
		Probabilities:  choiceResp.ChoiceProbabilities,
		LatencyMs:      latency,
	}

	c.logger.Debug("Jev classification completed",
		zap.String("message_id", msg.ID),
		zap.String("classification", string(decision.Classification)),
		zap.Float64("confidence", decision.Confidence),
		zap.Int64("latency_ms", latency),
	)

	return decision, nil
}
