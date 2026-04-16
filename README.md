# Notification Service

High-throughput notification platform targeting **1 billion deliveries/day** across Email, Slack, and In-App channels.

---

## Services

| Service | Port | Responsibility |
|---|---|---|
| Ingestion | 8080 | Accepts API requests, validates, routes to Kafka |
| Template | 8081 | CRUD for templates; renders body from S3 |
| Scheduler | — | Defers future notifications via Redis sorted set |
| Worker | — | Consumes Kafka, renders, delivers, audits |

---

## Tech Stack

| Concern | Choice |
|---|---|
| Language | Go 1.22 |
| Messaging | Kafka (MSK) — `segmentio/kafka-go` |
| Cache / Schedule queue | Redis — `go-redis/v9` |
| Database | PostgreSQL — `pgx/v5` |
| Object storage | AWS S3 — `aws-sdk-go-v2` |
| Observability | Prometheus + zap |

---

## Getting Started

**Prerequisites:** Go 1.22+, Docker (for local infra)

```bash
# Resolve dependencies
go mod tidy

# Run all tests
go test ./...

# Build all binaries
go build ./cmd/...
```

**Environment variables** — each service reads its config from env. See the `internal/platform/*/config.go` files for the full list. Minimal local set:

```bash
REDIS_ADDR=localhost:6379
POSTGRES_HOST=localhost
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=notifications
KAFKA_BROKERS=localhost:9092
```

---

## Repository Layout

```
cmd/
  ingestion/     # Ingestion Service entrypoint
  template/      # Template Service entrypoint
  scheduler/     # Scheduler Service entrypoint
  worker/        # Worker Pipeline entrypoint

internal/
  ingestion/     # Handler, validator, idempotency, publisher
  template/      # Repository, cache, S3 storage, service, handler
  scheduler/     # Kafka consumer, Redis store, poller
  worker/        # Chain of Responsibility pipeline, steps, audit log
  platform/
    kafka/       # kafka-go writer adapter
    redis/       # go-redis universal client factory
    postgres/    # pgxpool factory

pkg/
  transport/     # Shared Envelope + MessageWriter (avoids import cycles)
  retry/         # Exponential backoff with jitter
  telemetry/     # Prometheus registry
```

---

## Key Design Decisions

- **Rendering in the worker** — the Template Service returns raw HTML; variable substitution (`html/template`) happens in the Worker so the Template Service stays stateless.
- **Idempotency at two layers** — ingestion (`idempotency:` Redis prefix) prevents duplicate API submissions; worker (`worker:done:` prefix) prevents re-delivery after a Kafka retry.
- **Startup gate** — the Template Service warms Redis cache before the HTTP listener opens so the Ingestion Service never reads a missing entry.
