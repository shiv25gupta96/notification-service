package ingestion

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Handler handles POST /v1/notifications.
// It is responsible solely for the HTTP boundary: decoding, orchestrating the
// pipeline, and encoding responses. All domain logic lives in the collaborators
// it receives via constructor injection.
type Handler struct {
	idempotency IdempotencyStore
	validator   *Validator
	publisher   *Publisher
	logger      *zap.Logger
}

// NewHandler creates a Handler with all dependencies injected.
// No concrete types are used — every collaborator is an interface or a struct
// that depends only on interfaces internally.
func NewHandler(
	idempotency IdempotencyStore,
	validator *Validator,
	publisher *Publisher,
	logger *zap.Logger,
) *Handler {
	return &Handler{
		idempotency: idempotency,
		validator:   validator,
		publisher:   publisher,
		logger:      logger,
	}
}

// ServeHTTP implements http.Handler for POST /v1/notifications.
//
// Pipeline (each step is a hard gate — failure aborts and returns immediately):
//  1. JSON decode
//  2. Structural validation (no I/O)
//  3. Idempotency check  → cached 202 if duplicate
//  4. Template validation (parallel Redis reads)
//  5. ID assignment
//  6. Kafka publish (batched)
//  7. Idempotency commit (best-effort)
//  8. 202 Accepted
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log := h.logger.With(zap.String("request_id", requestIDFromContext(ctx)))

	// 1. Decode ---------------------------------------------------------------
	var req NotificationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ValidationErrorResponse{
			Error:   "INVALID_REQUEST",
			Details: []ValidationDetail{{Error: "request body is not valid JSON: " + err.Error()}},
		})
		return
	}

	// 2. Structural validation (pure, no I/O) ---------------------------------
	if details := validateTopLevel(&req); len(details) > 0 {
		writeJSON(w, http.StatusBadRequest, ValidationErrorResponse{
			Error:   "VALIDATION_FAILED",
			Details: details,
		})
		return
	}

	// 3. Idempotency check ----------------------------------------------------
	cached, err := h.idempotency.Check(ctx, req.IdempotencyKey)
	if err != nil {
		log.Error("idempotency check failed",
			zap.String("idempotency_key", req.IdempotencyKey),
			zap.Error(err),
		)
		writeJSON(w, http.StatusInternalServerError, errBody("INTERNAL_ERROR"))
		return
	}
	if cached != nil {
		log.Debug("idempotency hit — returning cached response",
			zap.String("idempotency_key", req.IdempotencyKey),
			zap.String("parent_notification_id", cached.ParentNotificationID),
		)
		writeJSON(w, http.StatusAccepted, cached)
		return
	}

	// 4. Template validation --------------------------------------------------
	enriched, failures := h.validator.Validate(ctx, &req)
	if len(failures) > 0 {
		writeJSON(w, http.StatusBadRequest, ValidationErrorResponse{
			Error:   "VALIDATION_FAILED",
			Details: failures,
		})
		return
	}

	// 5. ID assignment --------------------------------------------------------
	// IDs are assigned after validation so we never create artefacts for
	// requests that would have been rejected.
	parentID := uuid.New().String()
	status := determineStatus(req.ScheduleAt)

	channelResponses := make([]ChannelResponse, len(enriched))
	for i := range enriched {
		notifID := uuid.New().String()
		enriched[i].notificationID = notifID
		channelResponses[i] = ChannelResponse{
			Channel:        enriched[i].Channel,
			NotificationID: notifID,
			Status:         status,
		}
	}

	resp := &NotificationResponse{
		ParentNotificationID: parentID,
		Channels:             channelResponses,
	}

	// 6. Kafka publish --------------------------------------------------------
	if err := h.publisher.Publish(ctx, publishInput{
		parentNotificationID: parentID,
		userID:               req.UserID,
		category:             req.Category,
		idempotencyKey:       req.IdempotencyKey,
		scheduleAt:           req.ScheduleAt,
		channels:             enriched,
	}); err != nil {
		log.Error("kafka publish failed",
			zap.String("parent_notification_id", parentID),
			zap.String("user_id", req.UserID),
			zap.Error(err),
		)
		writeJSON(w, http.StatusInternalServerError, errBody("INTERNAL_ERROR"))
		return
	}

	// 7. Idempotency commit (best-effort) -------------------------------------
	// Failure here is non-fatal: the worst outcome is a duplicate publish on a
	// subsequent retry with the same key. Downstream workers deduplicate by
	// notification_id, so end-to-end exactly-once is preserved.
	if err := h.idempotency.Commit(ctx, req.IdempotencyKey, resp); err != nil {
		log.Warn("idempotency commit failed — duplicate publish possible on retry",
			zap.String("idempotency_key", req.IdempotencyKey),
			zap.String("parent_notification_id", parentID),
			zap.Error(err),
		)
	}

	log.Info("notification accepted",
		zap.String("parent_notification_id", parentID),
		zap.String("user_id", req.UserID),
		zap.String("category", string(req.Category)),
		zap.Int("channel_count", len(enriched)),
		zap.Stringer("status", status),
	)

	// 8. 202 Accepted ---------------------------------------------------------
	writeJSON(w, http.StatusAccepted, resp)
}

// validateTopLevel checks top-level fields that can be rejected without any
// I/O, keeping Redis calls off the path for obviously malformed requests.
func validateTopLevel(req *NotificationRequest) []ValidationDetail {
	var details []ValidationDetail

	if req.UserID == "" {
		details = append(details, ValidationDetail{Error: "user_id must not be empty"})
	}
	if req.IdempotencyKey == "" {
		details = append(details, ValidationDetail{Error: "idempotency_key must not be empty"})
	}
	if !req.Category.IsValid() {
		details = append(details, ValidationDetail{
			Error: fmt.Sprintf("unknown category %q", req.Category),
		})
	}
	if len(req.Notifications) == 0 {
		details = append(details, ValidationDetail{Error: "notifications must not be empty"})
	}
	if req.ScheduleAt != nil && !req.ScheduleAt.After(time.Now().UTC()) {
		details = append(details, ValidationDetail{Error: "schedule_at must be a future timestamp"})
	}

	return details
}

// determineStatus returns SCHEDULED when the notification has a future
// send time, QUEUED otherwise.
func determineStatus(scheduleAt *time.Time) DeliveryStatus {
	if scheduleAt != nil && scheduleAt.After(time.Now().UTC()) {
		return StatusScheduled
	}
	return StatusQueued
}

// writeJSON sets the Content-Type header, writes status, and encodes body
// as JSON. Encoding errors are silently dropped because the status line has
// already been flushed.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// errBody returns a minimal error envelope used for 500 responses where
// internal detail must not be leaked to callers.
func errBody(code string) map[string]string {
	return map[string]string{"error": code}
}

// Stringer implementation on DeliveryStatus so zap.Stringer works.
func (d DeliveryStatus) String() string { return string(d) }
