package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

const templateCacheKeyPrefix = "template:"

// ErrTemplateNotFound is returned by TemplateResolver when the requested
// template ID is absent from the cache. Callers use errors.Is to distinguish
// this from infrastructure failures.
var ErrTemplateNotFound = errors.New("template not found")

// TemplateResolver fetches cached template metadata.
// The interface is declared here (at point of use) so that the ingestion
// package owns the abstraction it needs, not the package that implements it.
type TemplateResolver interface {
	Resolve(ctx context.Context, templateID string) (*TemplateMetadata, error)
}

// RedisTemplateResolver implements TemplateResolver using the Redis cache
// maintained exclusively by the Template Service. The ingestion service is a
// read-only consumer of these keys — it never writes them.
type RedisTemplateResolver struct {
	client redis.UniversalClient
}

// NewRedisTemplateResolver creates a resolver backed by the given client.
func NewRedisTemplateResolver(client redis.UniversalClient) *RedisTemplateResolver {
	return &RedisTemplateResolver{client: client}
}

// Resolve returns the TemplateMetadata for templateID or ErrTemplateNotFound
// when the key is absent. Redis communication errors are returned as-is so
// the caller can distinguish cache miss from infrastructure failure and return
// an appropriate HTTP status code.
func (r *RedisTemplateResolver) Resolve(ctx context.Context, templateID string) (*TemplateMetadata, error) {
	raw, err := r.client.Get(ctx, templateCacheKeyPrefix+templateID).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("%w: %s", ErrTemplateNotFound, templateID)
	}
	if err != nil {
		return nil, fmt.Errorf("template resolver: redis GET %q: %w", templateID, err)
	}

	var meta TemplateMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, fmt.Errorf("template resolver: unmarshal %q: %w", templateID, err)
	}
	return &meta, nil
}

// Validator validates every ChannelRequest in a NotificationRequest against
// Redis-cached template metadata. Each channel is resolved concurrently;
// all failures are collected before returning so callers receive the complete
// error picture in a single response.
type Validator struct {
	resolver TemplateResolver
}

// NewValidator creates a Validator backed by resolver.
func NewValidator(resolver TemplateResolver) *Validator {
	return &Validator{resolver: resolver}
}

// channelResult holds the outcome of validating one ChannelRequest.
// Writing into a pre-allocated slice by index is race-free because each
// goroutine owns a distinct position.
type channelResult struct {
	templateVersion int
	detail          *ValidationDetail // non-nil on any failure
}

// Validate runs parallel validation over every channel in req.
//
// On full success it returns ([]enrichedChannel, nil) where each element
// carries the resolved template version alongside the original request data.
//
// On any failure it returns (nil, []ValidationDetail) covering every channel
// that failed — never a partial success list — enforcing the atomic
// all-or-nothing contract specified in the API design.
func (v *Validator) Validate(ctx context.Context, req *NotificationRequest) ([]enrichedChannel, []ValidationDetail) {
	results := make([]channelResult, len(req.Notifications))

	var wg sync.WaitGroup
	for i, ch := range req.Notifications {
		wg.Add(1)
		i, ch := i, ch
		go func() {
			defer wg.Done()
			results[i] = v.validateOne(ctx, ch)
		}()
	}
	wg.Wait()

	// Collect failures. Iterate in order so the error list mirrors the
	// request array, making it easy for callers to correlate.
	var failures []ValidationDetail
	for _, r := range results {
		if r.detail != nil {
			failures = append(failures, *r.detail)
		}
	}
	if len(failures) > 0 {
		return nil, failures
	}

	enriched := make([]enrichedChannel, len(req.Notifications))
	for i, ch := range req.Notifications {
		enriched[i] = enrichedChannel{
			ChannelRequest:  ch,
			templateVersion: results[i].templateVersion,
			// notificationID is assigned by the handler after this call.
		}
	}
	return enriched, nil
}

// validateOne validates a single ChannelRequest. It always returns a
// channelResult — never panics or propagates errors upward. Structural checks
// run first so that Redis is only consulted when the request is well-formed.
func (v *Validator) validateOne(ctx context.Context, ch ChannelRequest) channelResult {
	if detail := checkStructure(ch); detail != nil {
		return channelResult{detail: detail}
	}

	meta, err := v.resolver.Resolve(ctx, ch.TemplateID)
	if err != nil {
		return channelResult{detail: &ValidationDetail{
			Channel:    ch.Channel,
			TemplateID: ch.TemplateID,
			Error:      resolveErrMessage(err, ch.TemplateID),
		}}
	}

	if !meta.IsActive {
		return channelResult{detail: &ValidationDetail{
			Channel:    ch.Channel,
			TemplateID: ch.TemplateID,
			Error:      "template is inactive",
		}}
	}

	if meta.Channel != ch.Channel {
		return channelResult{detail: &ValidationDetail{
			Channel:    ch.Channel,
			TemplateID: ch.TemplateID,
			Error: fmt.Sprintf(
				"template is for channel %q, cannot be used on channel %q",
				meta.Channel, ch.Channel,
			),
		}}
	}

	if detail := checkRequiredVariables(ch, meta); detail != nil {
		return channelResult{detail: detail}
	}

	return channelResult{templateVersion: meta.Version}
}

// checkStructure validates fields that require no external calls.
func checkStructure(ch ChannelRequest) *ValidationDetail {
	if !ch.Channel.IsValid() {
		return &ValidationDetail{
			Channel: ch.Channel,
			Error:   fmt.Sprintf("unknown channel %q", ch.Channel),
		}
	}
	if strings.TrimSpace(ch.TemplateID) == "" {
		return &ValidationDetail{
			Channel: ch.Channel,
			Error:   "template_id must not be empty",
		}
	}
	return nil
}

// checkRequiredVariables ensures that every variable declared as required
// in the template metadata is present in the request payload.
func checkRequiredVariables(ch ChannelRequest, meta *TemplateMetadata) *ValidationDetail {
	var missing []string
	for _, name := range meta.RequiredVariables {
		if _, ok := ch.Variables[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return &ValidationDetail{
			Channel:    ch.Channel,
			TemplateID: ch.TemplateID,
			Error:      "missing required variable(s): " + strings.Join(missing, ", "),
		}
	}
	return nil
}

// resolveErrMessage returns a user-facing message for a Resolve error,
// distinguishing cache misses from infrastructure failures so callers can
// respond with the correct detail text.
func resolveErrMessage(err error, templateID string) string {
	if errors.Is(err, ErrTemplateNotFound) {
		return fmt.Sprintf("template %q not found", templateID)
	}
	return "template service unavailable"
}
