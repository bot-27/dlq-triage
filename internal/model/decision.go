package model

import "time"

// Classification represents the deterministic decision categories returned by the Jev Choice API.
type Classification string

const (
	ClassificationTransientNetworkError Classification = "transient_network_error"
	ClassificationMalformedJSON         Classification = "malformed_json"
	ClassificationLogicBug              Classification = "logic_bug"
	ClassificationMissingUserData       Classification = "missing_user_data"
)

// TriageDecision contains the probabilistic classification result from Jev AI Decision API.
type TriageDecision struct {
	Classification Classification     `json:"selected_choice"`
	Confidence     float64            `json:"confidence"`
	Reasoning      string             `json:"reasoning"`
	Probabilities  map[string]float64 `json:"choice_probabilities"`
	LatencyMs      int64              `json:"latency_ms"`
}

// TriageResult describes the final routing decision executed by the Triage Service.
type TriageResult struct {
	MessageID     string         `json:"message_id"`
	Decision      TriageDecision `json:"decision"`
	ActionTaken   string         `json:"action_taken"`
	Destination   string         `json:"destination,omitempty"`
	RetryCount    int            `json:"retry_count"`
	ExecutionTime time.Duration  `json:"execution_time"`
	ProcessedAt   time.Time      `json:"processed_at"`
}
