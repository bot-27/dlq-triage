package client_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dlq-triage/internal/client"
	"dlq-triage/internal/config"
	"dlq-triage/internal/model"
	"dlq-triage/internal/observability"

	"go.uber.org/zap"
)

func TestJevClient_MetadataOnly_StripsSensitivePayload(t *testing.T) {
	var capturedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		capturedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read request body: %v", err)
		}

		resp := client.ChoiceResponse{
			SelectedChoice: model.ClassificationTransientNetworkError,
			Confidence:     0.95,
			Reasoning:      "Connection timeout to upstream cluster",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := config.JevConfig{
		APIKey:       "test-key",
		BaseURL:      server.URL,
		Timeout:      2 * time.Second,
		MetadataOnly: true,
	}

	metrics := observability.NewTestMetrics()
	logger := zap.NewNop()
	jevClient := client.NewJevClient(cfg, metrics, logger)

	dlqMsg := model.DLQMessage{
		ID:            "msg-secret-123",
		OriginalTopic: "payments_topic",
		Payload:       []byte(`{"credit_card":"4111-2222-3333-4444","cvv":"123","user_ssn":"999-00-1111"}`),
		ErrorMessage:  "dial tcp 10.0.0.1:5432: i/o timeout",
		StackTrace:    "main.go:50",
		Headers: map[string]string{
			"Authorization":   "Bearer secret-token-xyz",
			"X-User-Password": "plainTextPassword123",
			"X-Original-Topic": "payments_topic",
			"X-Message-ID":    "msg-secret-123",
			"X-Retry-Count":   "1",
		},
	}

	decision, err := jevClient.Classify(context.Background(), dlqMsg)
	if err != nil {
		t.Fatalf("unexpected classification error: %v", err)
	}

	if decision.Classification != model.ClassificationTransientNetworkError {
		t.Errorf("expected classification %s, got %s", model.ClassificationTransientNetworkError, decision.Classification)
	}

	capturedStr := string(capturedBody)

	// Verify sensitive customer payload is completely absent
	if strings.Contains(capturedStr, "4111-2222-3333-4444") {
		t.Errorf("SECURITY LEAK: credit card number found in outbound request body: %s", capturedStr)
	}
	if strings.Contains(capturedStr, "user_ssn") {
		t.Errorf("SECURITY LEAK: user_ssn found in outbound request body: %s", capturedStr)
	}

	// Verify sensitive headers are stripped
	if strings.Contains(capturedStr, "secret-token-xyz") {
		t.Errorf("SECURITY LEAK: bearer token found in outbound request headers: %s", capturedStr)
	}
	if strings.Contains(capturedStr, "plainTextPassword123") {
		t.Errorf("SECURITY LEAK: password found in outbound request headers: %s", capturedStr)
	}

	// Verify error metadata is preserved for accurate AI classification
	if !strings.Contains(capturedStr, "dial tcp 10.0.0.1:5432: i/o timeout") {
		t.Errorf("expected error message to be present for classification: %s", capturedStr)
	}
	if !strings.Contains(capturedStr, "[OMITTED_METADATA_ONLY_MODE]") {
		t.Errorf("expected payload placeholder in metadata-only mode: %s", capturedStr)
	}
}

func TestJevClient_MetadataDisabled_IncludesPayloadSample(t *testing.T) {
	var capturedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		capturedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read request body: %v", err)
		}

		resp := client.ChoiceResponse{
			SelectedChoice: model.ClassificationMalformedJSON,
			Confidence:     0.99,
			Reasoning:      "Unexpected token at position 0",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := config.JevConfig{
		APIKey:       "test-key",
		BaseURL:      server.URL,
		Timeout:      2 * time.Second,
		MetadataOnly: false, // Disabled
	}

	metrics := observability.NewTestMetrics()
	logger := zap.NewNop()
	jevClient := client.NewJevClient(cfg, metrics, logger)

	dlqMsg := model.DLQMessage{
		ID:           "msg-debug-456",
		Payload:      []byte(`{"test_key":"test_value"}`),
		ErrorMessage: "invalid character 'x'",
	}

	_, err := jevClient.Classify(context.Background(), dlqMsg)
	if err != nil {
		t.Fatalf("unexpected classification error: %v", err)
	}

	if !strings.Contains(string(capturedBody), `\"test_key\":\"test_value\"`) {
		t.Errorf("expected payload sample to be present when MetadataOnly is false: %s", string(capturedBody))
	}
}
