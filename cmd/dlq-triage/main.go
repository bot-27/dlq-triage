package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dlq-triage/internal/client"
	"dlq-triage/internal/config"
	"dlq-triage/internal/infra"
	"dlq-triage/internal/observability"
	"dlq-triage/internal/service"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	logger := initLogger()
	defer func() { _ = logger.Sync() }()

	logger.Info("Starting Self-Healing DLQ Auto-Triage Microservice",
		zap.String("version", "1.0.0"),
		zap.String("go_version", "1.21+"),
	)

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatal("Failed to load environment configuration", zap.Error(err))
	}
	logger.Info("Configuration loaded successfully",
		zap.Strings("kafka_brokers", cfg.Kafka.Brokers),
		zap.String("dlq_topic", cfg.Kafka.DLQTopic),
		zap.String("main_topic", cfg.Kafka.MainTopic),
		zap.String("dead_end_topic", cfg.Kafka.DeadEndTopic),
		zap.Int("worker_pool_size", cfg.Triage.WorkerPoolSize),
	)

	metrics := observability.NewMetrics()

	// Dependency Injection Wiring
	jevClient := client.NewJevClient(cfg.Jev, metrics, logger)
	producer := infra.NewProducer(cfg.Kafka, logger)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("Failed to close Kafka producer cleanly", zap.Error(err))
		}
	}()

	alerter := client.NewWebhookAlerter(cfg.Alert, logger)
	triageService := service.NewTriageService(*cfg, jevClient, producer, alerter, metrics, logger)
	consumer := infra.NewConsumer(cfg.Kafka, cfg.Triage, triageService, metrics, logger)

	// Metrics & Health HTTP Server
	metricsMux := http.NewServeMux()
	metricsMux.Handle(cfg.Metrics.Path, promhttp.Handler())
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP","service":"dlq-auto-triage"}`))
	})

	metricsServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Metrics.Port),
		Handler:      metricsMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("Prometheus metrics server listening",
			zap.Int("port", cfg.Metrics.Port),
			zap.String("path", cfg.Metrics.Path),
		)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Metrics server terminated unexpectedly", zap.Error(err))
		}
	}()

	// Graceful Shutdown on SIGINT/SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	consumerDone := make(chan error, 1)
	go func() {
		logger.Info("Ingestion pipeline active. Consuming messages from DLQ...")
		consumerDone <- consumer.Start(ctx)
	}()

	sig := <-sigChan
	logger.Warn("Shutdown signal received; initiating graceful termination sequence",
		zap.String("signal", sig.String()),
	)

	cancel()

	select {
	case err := <-consumerDone:
		if err != nil {
			logger.Error("Consumer exited with error during drain", zap.Error(err))
		} else {
			logger.Info("All worker goroutines drained and Kafka offsets committed successfully")
		}
	case <-time.After(30 * time.Second):
		logger.Error("Drain timeout reached (30s); forcing exit")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("Error shutting down metrics server", zap.Error(err))
	} else {
		logger.Info("Metrics server stopped")
	}

	logger.Info("Self-Healing DLQ Auto-Triage Service stopped cleanly. Goodbye.")
}

func initLogger() *zap.Logger {
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	if os.Getenv("APP_ENV") == "development" {
		config = zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	logger, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize zap logger: %v", err))
	}
	return logger
}
