// Package retry provides a generic, context-aware exponential-backoff retry
// loop with jitter. It is intentionally dependency-free so every service in
// the monorepo can import it without pulling in infrastructure packages.
package retry

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"
)

// Policy describes when and how to retry a failing operation.
type Policy struct {
	// MaxAttempts is the total number of attempts (initial call + retries).
	// A value of 1 means no retries.
	MaxAttempts int

	// InitialInterval is the wait time before the first retry.
	InitialInterval time.Duration

	// MaxInterval caps the computed backoff so it never exceeds this value.
	MaxInterval time.Duration

	// Multiplier is the factor by which the interval grows after each attempt.
	// A value of 2.0 doubles the interval on each retry.
	Multiplier float64

	// JitterFraction adds up to this fraction of the current interval as
	// random jitter to prevent thundering-herd collisions.
	// 0.0 = no jitter; 0.1 = ±10 % of the current interval.
	JitterFraction float64

	// Retryable, when non-nil, is called with each error to decide whether to
	// retry. Return false to abort immediately (e.g. validation errors are not
	// retryable). When nil, all errors are retried.
	Retryable func(err error) bool
}

// DefaultPolicy returns a Policy suited to network I/O (Redis, Kafka):
// up to 3 attempts, 100 ms initial interval doubling to 1 s, 10 % jitter.
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts:     3,
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     1 * time.Second,
		Multiplier:      2.0,
		JitterFraction:  0.1,
	}
}

// Do executes fn according to p, retrying on error until MaxAttempts is
// exhausted, ctx is cancelled, or Retryable returns false.
//
// It returns the last error returned by fn, or ctx.Err() if the context was
// cancelled while waiting between attempts.
func Do(ctx context.Context, p Policy, fn func(ctx context.Context) error) error {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}

	var (
		lastErr  error
		interval = p.InitialInterval
	)

	for attempt := 0; attempt < p.MaxAttempts; attempt++ {
		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}

		if p.Retryable != nil && !p.Retryable(lastErr) {
			return lastErr
		}

		if attempt == p.MaxAttempts-1 {
			break
		}

		wait := jittered(interval, p.JitterFraction)
		select {
		case <-ctx.Done():
			return errors.Join(lastErr, ctx.Err())
		case <-time.After(wait):
		}

		interval = nextInterval(interval, p.Multiplier, p.MaxInterval)
	}

	return lastErr
}

// jittered adds uniform random jitter in the range [−frac·d, +frac·d] to d,
// ensuring the result is always positive.
func jittered(d time.Duration, frac float64) time.Duration {
	if frac == 0 {
		return d
	}
	delta := float64(d) * frac
	jitter := (rand.Float64()*2 - 1) * delta //nolint:gosec // non-crypto jitter
	result := float64(d) + jitter
	if result < 0 {
		return 0
	}
	return time.Duration(result)
}

// nextInterval returns min(current * multiplier, max).
func nextInterval(current time.Duration, multiplier float64, max time.Duration) time.Duration {
	next := time.Duration(math.Round(float64(current) * multiplier))
	if next > max {
		return max
	}
	return next
}
