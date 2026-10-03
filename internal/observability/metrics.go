package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics encapsulates all Prometheus metrics used for operational monitoring of the DLQ Triage service.
type Metrics struct {
	MessagesConsumedTotal prometheus.Counter
	MessagesRequeuedTotal *prometheus.CounterVec
	JevAPIErrorsTotal     *prometheus.CounterVec
	JevAPILatencyMs       prometheus.Histogram
	TriageDecisionsTotal  *prometheus.CounterVec
}

// NewMetrics initializes and registers the required Prometheus metrics with the default global registry.
func NewMetrics() *Metrics {
	return newMetrics(prometheus.DefaultRegisterer)
}

// NewTestMetrics creates metrics registered against a throwaway registry, safe to call multiple times in tests.
func NewTestMetrics() *Metrics {
	return newMetrics(prometheus.NewRegistry())
}

func newMetrics(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)
	return &Metrics{
		MessagesConsumedTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name: "messages_consumed_total",
				Help: "Total count of DLQ messages consumed from Kafka.",
			},
		),
		MessagesRequeuedTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "messages_requeued_total",
				Help: "Total count of messages requeued back into the main pipeline.",
			},
			[]string{"topic", "retry_attempt"},
		),
		JevAPIErrorsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "jev_api_errors_total",
				Help: "Total count of errors encountered when calling the Jev Decision API.",
			},
			[]string{"error_type"},
		),
		JevAPILatencyMs: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "jev_api_latency_milliseconds",
				Help:    "Latency distribution of Jev AI Decision Choice API calls in milliseconds.",
				Buckets: []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
			},
		),
		TriageDecisionsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "triage_decisions_total",
				Help: "Total count of triage decisions partitioned by classification outcome.",
			},
			[]string{"classification", "action"},
		),
	}
}
