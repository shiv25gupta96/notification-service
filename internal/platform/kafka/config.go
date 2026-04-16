package kafka

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// ProducerConfig holds all parameters for constructing a Kafka producer.
type ProducerConfig struct {
	// Brokers is the initial bootstrap broker list (host:port).
	Brokers []string

	// BatchSize is the maximum number of messages batched in one Produce call.
	BatchSize int
	// BatchTimeout is the maximum time a partial batch waits before being sent.
	BatchTimeout time.Duration

	// RequiredAcks controls durability:
	//   0 = fire-and-forget
	//   1 = leader ACK only
	//  -1 = all in-sync replica ACKs (production default)
	RequiredAcks int

	// Compression codec name: "none" | "gzip" | "snappy" | "lz4" | "zstd".
	Compression string

	// WriteTimeout is the per-write-call deadline.
	WriteTimeout time.Duration

	// TLSEnabled controls whether the connection uses TLS.
	TLSEnabled bool

	// SASL parameters. Mechanism must be "PLAIN", "SCRAM-SHA-256", or
	// "SCRAM-SHA-512". Leave empty to disable SASL.
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
}

// ProducerFromEnv constructs a ProducerConfig from environment variables:
//
//	KAFKA_BROKERS           comma-separated host:port list (required)
//	KAFKA_BATCH_SIZE        integer, default 100
//	KAFKA_BATCH_TIMEOUT     duration string, default "10ms"
//	KAFKA_REQUIRED_ACKS     integer, default -1
//	KAFKA_COMPRESSION       string, default "snappy"
//	KAFKA_WRITE_TIMEOUT     duration string, default "10s"
//	KAFKA_TLS_ENABLED       "true" | "false", default "false"
//	KAFKA_SASL_MECHANISM    optional
//	KAFKA_SASL_USERNAME     optional
//	KAFKA_SASL_PASSWORD     optional
func ProducerFromEnv() (ProducerConfig, error) {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		return ProducerConfig{}, fmt.Errorf("kafka config: KAFKA_BROKERS must not be empty")
	}

	cfg := ProducerConfig{
		Brokers:       strings.Split(raw, ","),
		BatchSize:     parseIntEnv("KAFKA_BATCH_SIZE", 100),
		BatchTimeout:  parseDurationEnv("KAFKA_BATCH_TIMEOUT", 10*time.Millisecond),
		RequiredAcks:  parseIntEnv("KAFKA_REQUIRED_ACKS", -1),
		Compression:   envOr("KAFKA_COMPRESSION", "snappy"),
		WriteTimeout:  parseDurationEnv("KAFKA_WRITE_TIMEOUT", 10*time.Second),
		TLSEnabled:    os.Getenv("KAFKA_TLS_ENABLED") == "true",
		SASLMechanism: os.Getenv("KAFKA_SASL_MECHANISM"),
		SASLUsername:  os.Getenv("KAFKA_SASL_USERNAME"),
		SASLPassword:  os.Getenv("KAFKA_SASL_PASSWORD"),
	}
	return cfg, nil
}

func parseDurationEnv(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func parseIntEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
