// Package telemetry exposes Prometheus metrics that are shared across all
// ingestion service components. Each metric is registered once via a package-
// level var so callers import and record without any initialisation ceremony.
package telemetry

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace and subsystem constants keep metric names consistent.
const (
	namespace = "notif"
	subsystem = "ingestion"
)

// Ingestion request metrics.
var (
	// RequestsTotal counts every inbound API call, labelled by outcome.
	// outcome: "accepted" | "duplicate" | "validation_error" | "server_error"
	RequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "requests_total",
			Help:      "Total number of POST /v1/notifications requests by outcome.",
		},
		[]string{"outcome"},
	)

	// RequestDuration observes end-to-end handler latency in seconds.
	RequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "request_duration_seconds",
			Help:      "End-to-end handler latency for POST /v1/notifications.",
			Buckets:   []float64{.005, .01, .025, .05, .075, .1, .25, .5, 1},
		},
		[]string{"status_code"},
	)
)

// Idempotency metrics.
var (
	// IdempotencyHits counts Redis idempotency-key lookups, labelled by result.
	// result: "hit" | "miss" | "error"
	IdempotencyHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "idempotency_lookups_total",
			Help:      "Idempotency key lookup results.",
		},
		[]string{"result"},
	)
)

// Validation metrics.
var (
	// ValidationErrors counts per-channel template validation failures.
	ValidationErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "validation_errors_total",
			Help:      "Per-channel template validation failures.",
		},
		[]string{"channel", "reason"},
	)
)

// Publish metrics.
var (
	// PublishTotal counts Kafka publish operations by topic and outcome.
	PublishTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "publish_total",
			Help:      "Kafka publish operations by topic and outcome.",
		},
		[]string{"topic", "outcome"},
	)

	// PublishDuration observes Kafka WriteMessages latency by topic.
	PublishDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "publish_duration_seconds",
			Help:      "Kafka publish latency by topic.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"topic"},
	)
)

// Registry holds all ingestion metrics so main.go can register them with a
// single call and avoid using the global default registry.
var Registry = prometheus.NewRegistry()

func init() {
	Registry.MustRegister(
		RequestsTotal,
		RequestDuration,
		IdempotencyHits,
		ValidationErrors,
		PublishTotal,
		PublishDuration,
		// Standard Go runtime and process collectors.
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
}

// Handler returns an http.Handler that serves the Prometheus metrics
// exposition format from Registry.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}
