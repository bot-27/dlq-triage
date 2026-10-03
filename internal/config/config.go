package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config encapsulates all configuration parameters required by the DLQ Triage service.
type Config struct {
	Kafka   KafkaConfig
	Jev     JevConfig
	Alert   AlertConfig
	Metrics MetricsConfig
	Triage  TriageConfig
	Logging LoggingConfig
}

type KafkaConfig struct {
	Brokers       []string
	DLQTopic      string
	MainTopic     string
	DeadEndTopic  string
	ConsumerGroup string
}

type JevConfig struct {
	APIKey       string
	BaseURL      string
	Timeout      time.Duration
	MaxRetries   int
	MetadataOnly bool // When true, payloads and sensitive headers are stripped before external API calls
}

type AlertConfig struct {
	WebhookURL string
	Channel    string
	Timeout    time.Duration
}

type MetricsConfig struct {
	Port int
	Path string
}

type TriageConfig struct {
	WorkerPoolSize int
	ChannelBuffer  int
	MaxRetries     int
	RetryHeader    string
}

type LoggingConfig struct {
	Level       string
	Environment string
}

// LoadConfig loads the .env file (if present) then parses environment variables with secure defaults.
func LoadConfig() (*Config, error) {
	// Load .env file if it exists; silently ignored in production where env vars are injected directly.
	_ = godotenv.Load()

	brokersRaw := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokersRaw, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	workerPoolSize, err := strconv.Atoi(getEnv("WORKER_POOL_SIZE", "10"))
	if err != nil || workerPoolSize <= 0 {
		workerPoolSize = 10
	}

	channelBuffer, err := strconv.Atoi(getEnv("CHANNEL_BUFFER_SIZE", "100"))
	if err != nil || channelBuffer <= 0 {
		channelBuffer = 100
	}

	maxRetries, err := strconv.Atoi(getEnv("MAX_RETRY_COUNT", "3"))
	if err != nil || maxRetries < 0 {
		maxRetries = 3
	}

	metricsPort, err := strconv.Atoi(getEnv("METRICS_PORT", "9090"))
	if err != nil || metricsPort <= 0 {
		metricsPort = 9090
	}

	jevTimeoutSec, err := strconv.Atoi(getEnv("JEV_TIMEOUT_SECONDS", "5"))
	if err != nil || jevTimeoutSec <= 0 {
		jevTimeoutSec = 5
	}

	cfg := &Config{
		Kafka: KafkaConfig{
			Brokers:       brokers,
			DLQTopic:      getEnv("KAFKA_DLQ_TOPIC", "dead_letter_queue"),
			MainTopic:     getEnv("KAFKA_MAIN_TOPIC", "orders_main_stream"),
			DeadEndTopic:  getEnv("KAFKA_DEAD_END_TOPIC", "dead_end_unrecoverable"),
			ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "dlq-auto-triage-workers"),
		},
		Jev: JevConfig{
			APIKey:       getEnv("JEV_API_KEY", "jev_live_sample_key"),
			BaseURL:      getEnv("JEV_BASE_URL", "https://api.jev.ai/v1/decide/choice"),
			Timeout:      time.Duration(jevTimeoutSec) * time.Second,
			MaxRetries:   2,
			MetadataOnly: getEnv("METADATA_ONLY_TRIAGE", "true") == "true",
		},
		Alert: AlertConfig{
			WebhookURL: getEnv("ALERT_WEBHOOK_URL", "https://events.pagerduty.com/v2/enqueue"),
			Channel:    getEnv("ALERT_CHANNEL", "#dlq-critical-alerts"),
			Timeout:    3 * time.Second,
		},
		Metrics: MetricsConfig{
			Port: metricsPort,
			Path: getEnv("METRICS_PATH", "/metrics"),
		},
		Triage: TriageConfig{
			WorkerPoolSize: workerPoolSize,
			ChannelBuffer:  channelBuffer,
			MaxRetries:     maxRetries,
			RetryHeader:    getEnv("RETRY_COUNT_HEADER", "X-Retry-Count"),
		},
		Logging: LoggingConfig{
			Level:       getEnv("LOG_LEVEL", "info"),
			Environment: getEnv("APP_ENV", "production"),
		},
	}

	if len(cfg.Kafka.Brokers) == 0 || cfg.Kafka.Brokers[0] == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS must contain at least one valid broker address")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		return strings.TrimSpace(val)
	}
	return fallback
}
