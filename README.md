# Self-Healing DLQ Auto-Triage Service

[![Go Version](https://img.shields.io/badge/Go-1.21%2B-00ADD8?style=flat&logo=go)](https://golang.org/)
[![Apache Kafka](https://img.shields.io/badge/Kafka-7.5.0-231F20?style=flat&logo=apachekafka)](https://kafka.apache.org/)
[![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C?style=flat&logo=prometheus)](https://prometheus.io/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Architecture](https://img.shields.io/badge/Architecture-Clean%20Layered-brightgreen.svg)]()

A production-grade, highly concurrent Go microservice that consumes failed messages from an Apache Kafka Dead Letter Queue (DLQ), applies probabilistic AI root-cause classification using the **Jev AI Decision API** (`Choice` primitive), and automatically executes self-healing triage strategies.

---

## Table of Contents

- [The Problem Solved](#the-problem-solved)
- [How It Works (Triage Strategies)](#how-it-works-triage-strategies)
- [Architecture & Design](#architecture--design)
- [Prerequisites](#prerequisites)
- [Installation & Quickstart](#installation--quickstart)
  - [Option 1: Complete Setup with Docker Compose (Recommended)](#option-1-complete-setup-with-docker-compose-recommended)
  - [Option 2: Native Local Setup](#option-2-native-local-setup)
- [Configuration Reference (.env)](#configuration-reference-env)
- [Simulating & Testing DLQ Events](#simulating--testing-dlq-events)
  - [1. Transient Network Error (Auto-Requeued)](#1-transient-network-error-auto-requeued)
  - [2. Malformed JSON (Quarantined to Dead-End)](#2-malformed-json-quarantined-to-dead-end)
  - [3. Application Logic Bug (PagerDuty Alert + Dropped)](#3-application-logic-bug-pagerduty-alert--dropped)
- [Observability & Health Checks](#observability--health-checks)
- [Testing & Quality Assurance](#testing--quality-assurance)
- [Project Layout](#project-layout)
- [Graceful Shutdown](#graceful-shutdown)

---

## The Problem Solved

In high-throughput event-driven microservices, unprocessable messages land in a Dead Letter Queue (DLQ). Usually:
1. **Transient glitches** (network timeouts, rate limits) rot in the DLQ instead of safely retrying after recovery.
2. **Poison pills** (malformed JSON or corrupted payloads) repeatedly crash downstream consumers.
3. **Application logic bugs** create silent failure backlogs until engineers manually inspect logs days later.
4. **Orphaned entity events** (e.g. missing user IDs) waste storage and engineering triage hours.

**Self-Healing DLQ Auto-Triage** eliminates manual triage toil. It continuously ingests DLQ messages, queries Jev AI for probabilistic root-cause classification, and routes each message into an automated healing lifecycle without human intervention.

---

## How It Works (Triage Strategies)

Each DLQ message is evaluated through the Jev AI Decision `Choice` primitive. Based on the classification, the service triggers one of four deterministic routing strategies:

| Classification | Action Taken | Target Destination / Handler | Retry Handling |
| :--- | :--- | :--- | :--- |
| `transient_network_error` | **Auto-Requeue** | `KAFKA_MAIN_TOPIC` (e.g. `orders_main_stream`) | Increments `X-Retry-Count`. If retry count reaches `MAX_RETRY_COUNT` (default `3`), routes to `dead_end_unrecoverable`. |
| `malformed_json` | **Quarantine** | `KAFKA_DEAD_END_TOPIC` (e.g. `dead_end_unrecoverable`) | Headers tagged with `X-Triage-Decision` and AI reasoning. Structured error logged. |
| `logic_bug` | **Alert & Drop** | PagerDuty / Webhook (`ALERT_WEBHOOK_URL`) | Sends critical JSON incident payload with stack trace & context; drops message to prevent crash loops. |
| `missing_user_data` | **Log & Drop** | Dropped / Acknowledged | Logs structured warning with AI reasoning; commits offset to safely unblock pipeline. |
| *API Failure / Fallback* | **Safe Quarantine** | `KAFKA_DEAD_END_TOPIC` | Failsafe mechanism: never drops messages without explicit classification. |

---

## Architecture & Design

The service follows **Clean / Hexagonal Layered Architecture** with strict dependency inversion:

```
                      ┌──────────────────────────────────────────────┐
                      │             Kafka DLQ Topic                  │
                      └──────────────────────┬───────────────────────┘
                                             │
                                             ▼
                      ┌──────────────────────────────────────────────┐
                      │     Kafka Consumer Ingestion Loop            │
                      └──────────────────────┬───────────────────────┘
                                             │  (Bounded Buffer Channel)
                                             ▼
                      ┌──────────────────────────────────────────────┐
                      │    Worker Pool (10 Concurrent Goroutines)    │
                      └──────────────────────┬───────────────────────┘
                                             │
                       ┌─────────────────────┴─────────────────────┐
                       │                                           │
                       ▼                                           ▼
         ┌───────────────────────────┐               ┌───────────────────────────┐
         │    Jev AI Decision API    │               │    Prometheus Metrics     │
         │   (Probabilistic Choice)  │               │   (:9090 /metrics &       │
         └─────────────┬─────────────┘               │    /healthz endpoint)     │
                       │                             └───────────────────────────┘
                       ▼
        ┌─────────────────────────────────────────────────────────┐
        │                 Triage Strategy Router                  │
        └───────┬────────────────────┬────────────────────┬───────┘
                │                    │                    │
                ▼                    ▼                    ▼
     ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
     │   Main Stream    │  │     Dead-End     │  │  PagerDuty /     │
     │  (Requeued with  │  │   Quarantine     │  │  Slack Webhook   │
     │  X-Retry-Count)  │  │   (Poison Pill)  │  │   (Logic Bug)    │
     └──────────────────┘  └──────────────────┘  └──────────────────┘
```

### Architectural Highlights
- **Layered Decoupling**:
  - `internal/model`: Pure Data Transfer Objects (DTOs), zero external dependencies.
  - `internal/port`: Port interfaces (`JevClient`, `KafkaProducer`, `Alerter`, `TriageUsecase`).
  - `internal/client`: Raw outbound HTTP clients (Jev AI Decision API, Webhook alerter).
  - `internal/infra`: Concrete Kafka infrastructure (Sarama/segmentio reader, writer, bounded worker pool).
  - `internal/service`: Pure domain triage orchestration and strategy routing.
  - `internal/config`: Strongly typed configuration loaded via environment variables and `.env`.
  - `internal/observability`: Prometheus instrumentation and thread-safe registry.
- **Backpressure & Bounded Concurrency**: Ingestion loop feeds a bounded Go channel consumed by a pool of worker goroutines. Kafka offsets are committed only after successful triage completion.
- **Graceful Shutdown**: Intercepts `SIGINT` / `SIGTERM`, halts ingestion, drains active workers, flushes Kafka producers, and safely closes HTTP listeners within a 30-second window.

---

## Prerequisites

- **Go**: Version `1.21` or higher (for native compilation)
- **Docker & Docker Compose**: Version `20.10+` / Compose `v2+` (for containerized execution)
- **Jev AI API Key**: Required for live root-cause classification (a placeholder dev key can be used for mock/local testing)

---

## Installation & Quickstart

### Option 1: Complete Setup with Docker Compose (Recommended)

Docker Compose automatically spins up:
1. **Zookeeper** (`:2181`)
2. **Apache Kafka Broker** (`:9092` external, `:29092` internal)
3. **Self-Healing DLQ Auto-Triage Microservice** (`:9090`)

#### Step 1: Clone the repository
```bash
git clone https://github.com/your-org/Self-Healing-DLQ.git
cd Self-Healing-DLQ
```

#### Step 2: Configure your environment
Copy the provided environment template to `.env`:
```bash
cp .env.example .env
```
Open `.env` and set your `JEV_API_KEY` (and optionally `ALERT_WEBHOOK_URL`):
```ini
JEV_API_KEY=your_actual_jev_api_key_here
```

#### Step 3: Start the stack
```bash
docker compose up --build -d
```

#### Step 4: Verify services are healthy
```bash
# Check running containers
docker compose ps

# Check auto-triage service logs
docker compose logs -f dlq-triage

# Check service health endpoint
curl http://localhost:9090/healthz
```

---

### Option 2: Native Local Setup

If you already have a Kafka cluster running locally on `localhost:9092`:

#### Step 1: Clone and install Go dependencies
```bash
git clone https://github.com/your-org/Self-Healing-DLQ.git
cd Self-Healing-DLQ
go mod download
```

#### Step 2: Configure `.env`
Ensure `.env` exists in the project root:
```bash
cp .env.example .env
```
Edit `.env` to point to your local broker and configure your API keys:
```ini
KAFKA_BROKERS=localhost:9092
KAFKA_DLQ_TOPIC=dead_letter_queue
KAFKA_MAIN_TOPIC=orders_main_stream
KAFKA_DEAD_END_TOPIC=dead_end_unrecoverable
JEV_API_KEY=your_jev_api_key
```

#### Step 3: Run the service
```bash
go run ./cmd/dlq-triage/main.go
```

Or build a compiled binary:
```bash
# Build
go build -o bin/dlq-triage ./cmd/dlq-triage/main.go

# Run
./bin/dlq-triage
```

---

## Configuration Reference (.env)

The service automatically loads settings from `.env` in local development, or reads injected system environment variables in production environments (Kubernetes, AWS ECS, Docker).

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-delimited list of Kafka broker addresses. |
| `KAFKA_DLQ_TOPIC` | `dead_letter_queue` | Target DLQ topic to consume failed messages from. |
| `KAFKA_MAIN_TOPIC` | `orders_main_stream` | Main topic to requeue recoverable transient errors into. |
| `KAFKA_DEAD_END_TOPIC` | `dead_end_unrecoverable` | Quarantine topic for unrecoverable messages / poison pills. |
| `KAFKA_CONSUMER_GROUP` | `dlq-auto-triage-workers` | Consumer group ID managing partition assignments and offsets. |
| `JEV_API_KEY` | *(Required in prod)* | Bearer authorization key for Jev AI Decision Choice API. |
| `JEV_BASE_URL` | `https://api.jev.ai/v1/decide/choice` | Endpoint URL for the Jev AI Decision Choice primitive. |
| `JEV_TIMEOUT_SECONDS` | `5` | HTTP client timeout in seconds for Jev AI requests. |
| `ALERT_WEBHOOK_URL` | `https://events.pagerduty.com/v2/enqueue` | Webhook URL for alerting on classified logic bugs. |
| `ALERT_CHANNEL` | `#dlq-critical-alerts` | Target notification channel (Slack/Teams/PagerDuty). |
| `WORKER_POOL_SIZE` | `10` | Number of concurrent worker goroutines triaging messages. |
| `CHANNEL_BUFFER_SIZE` | `100` | In-memory bounded channel buffer size between reader and pool. |
| `MAX_RETRY_COUNT` | `3` | Maximum retry attempts before a transient message is quarantined. |
| `RETRY_COUNT_HEADER` | `X-Retry-Count` | Kafka header key tracking current message retry count. |
| `METRICS_PORT` | `9090` | HTTP port serving Prometheus `/metrics` and `/healthz`. |
| `METRICS_PATH` | `/metrics` | HTTP path where Prometheus metrics are exposed. |
| `LOG_LEVEL` | `info` | Logging verbosity (`debug`, `info`, `warn`, `error`). |
| `APP_ENV` | `production` | Environment mode (`development` enables colorized console output). |

---

## Simulating & Testing DLQ Events

You can test the self-healing behavior by publishing sample error payloads to the `dead_letter_queue` topic using Kafka's CLI tools or Docker.

### 1. Transient Network Error (Auto-Requeued)

A temporary gateway timeout or network hiccup.

```bash
docker compose exec -T kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic dead_letter_queue \
  --property "parse.headers=true" \
  --property "headers.delimiter=|" \
  --property "headers=X-Message-ID:order-101|X-Original-Topic:orders_main_stream" << 'EOF'
{
  "payload": {"order_id": "ORD-101", "amount": 99.50},
  "error_message": "dial tcp 10.0.1.45:5432: i/o timeout while acquiring database connection",
  "stack_trace": "database/sql.(*DB).conn(DB.go:142)\ngithub.com/shop/orders.(*Repo).SaveOrder(repo.go:88)"
}
EOF
```
**Expected Outcome**: 
- Jev classifies as `transient_network_error`.
- Message is republished to `orders_main_stream` with header `X-Retry-Count: 1`.
- Metric `messages_requeued_total{topic="orders_main_stream",retry_attempt="1"}` increments.

---

### 2. Malformed JSON (Quarantined to Dead-End)

A corrupted message or poison pill that would crash consumers.

```bash
docker compose exec -T kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic dead_letter_queue \
  --property "parse.headers=true" \
  --property "headers.delimiter=|" \
  --property "headers=X-Message-ID:poison-202|X-Original-Topic:orders_main_stream" << 'EOF'
{
  "payload": "UNPARSED_RAW_INVALID_XML_OR_CORRUPT_BYTES",
  "error_message": "invalid character 'U' looking for beginning of value",
  "stack_trace": "encoding/json.Unmarshal(decode.go:96)"
}
EOF
```
**Expected Outcome**:
- Jev classifies as `malformed_json`.
- Message is immediately published to `dead_end_unrecoverable`.
- Message offset is committed from the DLQ so processing is never blocked.

---

### 3. Application Logic Bug (PagerDuty Alert + Dropped)

A deterministic `NullPointerException` or nil pointer dereference.

```bash
docker compose exec -T kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic dead_letter_queue \
  --property "parse.headers=true" \
  --property "headers.delimiter=|" \
  --property "headers=X-Message-ID:bug-303|X-Original-Topic:orders_main_stream" << 'EOF'
{
  "payload": {"order_id": "ORD-303", "items": null},
  "error_message": "panic: runtime error: invalid memory address or nil pointer dereference",
  "stack_trace": "github.com/shop/orders.(*Service).CalculateTotal(service.go:210)\ngithub.com/shop/orders.Handle(handler.go:45)"
}
EOF
```
**Expected Outcome**:
- Jev classifies as `logic_bug`.
- Dispatches emergency alert payload to `ALERT_WEBHOOK_URL` containing error context, stack trace, and confidence rating.
- Message is safely dropped from the pipeline to avoid re-triggering application panics.

---

## Observability & Health Checks

The service exposes operational metrics and health status on HTTP port `:9090`.

### Health Check Endpoint
```bash
curl -i http://localhost:9090/healthz
```
Response:
```json
HTTP/1.1 200 OK
Content-Type: application/json

{"status":"UP","service":"dlq-auto-triage"}
```

### Prometheus Metrics
```bash
curl http://localhost:9090/metrics
```

Key exported metrics:

| Metric Name | Type | Description |
| :--- | :--- | :--- |
| `messages_consumed_total` | Counter | Total number of failed messages fetched from the DLQ. |
| `messages_requeued_total` | CounterVec | Messages requeued to main stream labeled by `topic` and `retry_attempt`. |
| `triage_decisions_total` | CounterVec | Decisions partitioned by `classification` outcome and `action`. |
| `jev_api_latency_milliseconds`| Histogram | Latency distribution of Jev AI Decision Choice API calls. |
| `jev_api_errors_total` | CounterVec | Network, timeout, and HTTP errors calling Jev AI API. |

---

## Testing & Quality Assurance

The codebase includes comprehensive unit tests with full mock implementations of Kafka producers, Jev AI clients, and webhook alerters.

### Run Unit Tests
```bash
go test -v ./internal/service/
```

### Run Race Detection & Build Check
```bash
# Run tests with Go race detector
go test -race -v ./...

# Verify clean compilation
go build ./...
```

---

## Project Layout

```
Self-Healing-DLQ/
├── cmd/
│   └── dlq-triage/
│       └── main.go                  # Microservice entry point & dependency wiring
├── internal/
│   ├── model/                       # Core DTOs (DLQMessage, TriageDecision, AlertPayload)
│   ├── port/                        # Clean port interfaces (JevClient, KafkaProducer, Alerter)
│   ├── client/                      # External clients (Jev AI Decision HTTP client, Webhook Alerter)
│   ├── infra/                       # Infrastructure (Kafka Reader, Worker Pool, Kafka Producer)
│   ├── service/                     # Core business logic (TriageService & Strategy routing)
│   ├── config/                      # Strongly typed config with godotenv auto-loader
│   └── observability/               # Prometheus instrumentation & metrics registry
├── .env.example                     # Environment template (safe for version control)
├── Dockerfile                       # Multi-stage minimal production container build
├── docker-compose.yml               # Local stack (Zookeeper + Kafka + dlq-triage)
├── go.mod                           # Go module definition (Go 1.21+)
└── README.md                        # Documentation
```

---

## Graceful Shutdown

The service is engineered for zero-loss teardown in Kubernetes or container runtimes:

1. Listens for `SIGINT` (Ctrl+C) and `SIGTERM` signals.
2. Immediately cancels context on the Kafka consumer ingestion loop.
3. Closes internal work channels and drains active in-flight worker goroutines (up to 30s timeout).
4. Commits processed Kafka offsets to guarantee at-least-once delivery semantics without double-processing.
5. Closes Kafka producers and shuts down the Prometheus HTTP server.
6. Exits cleanly with status code `0`.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
