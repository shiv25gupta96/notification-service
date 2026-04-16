package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"

	"notification-service/pkg/transport"
)

// ── Stub MessageWriter ────────────────────────────────────────────────────────

// captureWriter records every batch of transport.Envelopes passed to Write.
type captureWriter struct {
	calls     [][]transport.Envelope
	returnErr error
}

func (c *captureWriter) Write(_ context.Context, envelopes []transport.Envelope) error {
	if c.returnErr != nil {
		return c.returnErr
	}
	cp := make([]transport.Envelope, len(envelopes))
	copy(cp, envelopes)
	c.calls = append(c.calls, cp)
	return nil
}

// ── Test wiring helpers ───────────────────────────────────────────────────────

func newTestHandler(t *testing.T) (*Handler, *memStore, *captureWriter) {
	t.Helper()

	store := newMemStore()
	resolver := &stubResolver{responses: map[string]resolverResponse{
		"order_confirmation_email_v2": {meta: activeMeta(ChannelEmail, "order_id", "amount", "customer_name")},
		"order_confirmation_inapp_v1": {meta: activeMeta(ChannelInApp, "order_id", "amount")},
		"slack_alert_v1":              {meta: activeMeta(ChannelSlack)},
	}}
	writer := &captureWriter{}

	h := NewHandler(
		store,
		NewValidator(resolver),
		NewPublisher(writer),
		zap.NewNop(),
	)
	return h, store, writer
}

func postNotification(h http.Handler, body any) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/notifications", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)
	return rw
}

func decodeResponse(t *testing.T, rw *httptest.ResponseRecorder) NotificationResponse {
	t.Helper()
	var resp NotificationResponse
	if err := json.NewDecoder(rw.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

func decodeErrResponse(t *testing.T, rw *httptest.ResponseRecorder) ValidationErrorResponse {
	t.Helper()
	var resp ValidationErrorResponse
	if err := json.NewDecoder(rw.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	return resp
}

// ── Handler tests ─────────────────────────────────────────────────────────────

func TestHandler_AcceptsValidRequest(t *testing.T) {
	t.Parallel()
	h, _, writer := newTestHandler(t)

	rw := postNotification(h, NotificationRequest{
		UserID:         "u_456",
		IdempotencyKey: "order-123-confirm",
		Category:       CategoryTransactional,
		Notifications: []ChannelRequest{
			{
				Channel:    ChannelEmail,
				TemplateID: "order_confirmation_email_v2",
				Variables:  map[string]string{"order_id": "ORD-123", "amount": "$49.99", "customer_name": "Aditya"},
			},
			{
				Channel:    ChannelInApp,
				TemplateID: "order_confirmation_inapp_v1",
				Variables:  map[string]string{"order_id": "ORD-123", "amount": "$49.99"},
			},
		},
	})

	if rw.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rw.Code, rw.Body.String())
	}

	resp := decodeResponse(t, rw)
	if resp.ParentNotificationID == "" {
		t.Error("parent_notification_id must not be empty")
	}
	if len(resp.Channels) != 2 {
		t.Fatalf("want 2 channel responses, got %d", len(resp.Channels))
	}
	for _, ch := range resp.Channels {
		if ch.NotificationID == "" {
			t.Errorf("channel %s: notification_id must not be empty", ch.Channel)
		}
		if ch.Status != StatusQueued {
			t.Errorf("channel %s: want status QUEUED, got %s", ch.Channel, ch.Status)
		}
	}

	// Exactly one batched write containing two envelopes.
	if len(writer.calls) != 1 {
		t.Fatalf("want 1 Write call, got %d", len(writer.calls))
	}
	if len(writer.calls[0]) != 2 {
		t.Fatalf("want 2 envelopes in batch, got %d", len(writer.calls[0]))
	}
}

func TestHandler_ScheduledNotification(t *testing.T) {
	t.Parallel()
	h, _, writer := newTestHandler(t)

	future := time.Now().Add(2 * time.Hour).UTC()
	rw := postNotification(h, NotificationRequest{
		UserID:         "u_789",
		IdempotencyKey: "sched-001",
		Category:       CategoryMarketing,
		ScheduleAt:     &future,
		Notifications: []ChannelRequest{{
			Channel:    ChannelEmail,
			TemplateID: "order_confirmation_email_v2",
			Variables:  map[string]string{"order_id": "X", "amount": "$0", "customer_name": "Q"},
		}},
	})

	if rw.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d", rw.Code)
	}

	resp := decodeResponse(t, rw)
	if resp.Channels[0].Status != StatusScheduled {
		t.Errorf("want SCHEDULED, got %s", resp.Channels[0].Status)
	}

	if len(writer.calls) == 0 {
		t.Fatal("no Write calls recorded")
	}
	for _, env := range writer.calls[0] {
		if env.Topic != topicScheduled {
			t.Errorf("want topic %q for scheduled notification, got %q", topicScheduled, env.Topic)
		}
	}
}

func TestHandler_IdempotencyDeduplicatesRequest(t *testing.T) {
	t.Parallel()
	h, _, writer := newTestHandler(t)

	req := NotificationRequest{
		UserID:         "u_123",
		IdempotencyKey: "dedup-key",
		Category:       CategoryTransactional,
		Notifications: []ChannelRequest{{
			Channel:    ChannelEmail,
			TemplateID: "order_confirmation_email_v2",
			Variables:  map[string]string{"order_id": "1", "amount": "$1", "customer_name": "X"},
		}},
	}

	rw1 := postNotification(h, req)
	if rw1.Code != http.StatusAccepted {
		t.Fatalf("first request: want 202, got %d", rw1.Code)
	}
	resp1 := decodeResponse(t, rw1)

	rw2 := postNotification(h, req)
	if rw2.Code != http.StatusAccepted {
		t.Fatalf("second request: want 202, got %d", rw2.Code)
	}
	resp2 := decodeResponse(t, rw2)

	if resp1.ParentNotificationID != resp2.ParentNotificationID {
		t.Errorf("idempotency broken: parent IDs differ (%s vs %s)",
			resp1.ParentNotificationID, resp2.ParentNotificationID)
	}

	// The second request must not reach Kafka.
	if len(writer.calls) != 1 {
		t.Errorf("want exactly 1 Kafka write call, got %d", len(writer.calls))
	}
}

func TestHandler_ValidationError_MissingVariable(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)

	rw := postNotification(h, NotificationRequest{
		UserID:         "u_x",
		IdempotencyKey: "val-err-1",
		Category:       CategoryTransactional,
		Notifications: []ChannelRequest{{
			Channel:    ChannelEmail,
			TemplateID: "order_confirmation_email_v2",
			Variables:  map[string]string{"order_id": "1"}, // amount and customer_name missing
		}},
	})

	if rw.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rw.Code, rw.Body.String())
	}
	errResp := decodeErrResponse(t, rw)
	if errResp.Error != "VALIDATION_FAILED" {
		t.Errorf("want VALIDATION_FAILED, got %q", errResp.Error)
	}
	if len(errResp.Details) == 0 {
		t.Error("want at least one detail in error response")
	}
}

func TestHandler_ValidationError_EmptyNotifications(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)

	rw := postNotification(h, NotificationRequest{
		UserID:         "u_x",
		IdempotencyKey: "val-err-2",
		Category:       CategoryTransactional,
		Notifications:  []ChannelRequest{},
	})

	if rw.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rw.Code)
	}
}

func TestHandler_ValidationError_MissingUserID(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)

	rw := postNotification(h, NotificationRequest{
		IdempotencyKey: "val-err-3",
		Category:       CategoryTransactional,
		Notifications: []ChannelRequest{{
			Channel:    ChannelEmail,
			TemplateID: "order_confirmation_email_v2",
		}},
	})

	if rw.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rw.Code)
	}
}

func TestHandler_ValidationError_PastScheduleAt(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)

	past := time.Now().Add(-1 * time.Hour)
	rw := postNotification(h, NotificationRequest{
		UserID:         "u_past",
		IdempotencyKey: "past-sched",
		Category:       CategoryTransactional,
		ScheduleAt:     &past,
		Notifications: []ChannelRequest{{
			Channel:    ChannelEmail,
			TemplateID: "order_confirmation_email_v2",
			Variables:  map[string]string{"order_id": "1", "amount": "$1", "customer_name": "X"},
		}},
	})

	if rw.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for past schedule_at, got %d", rw.Code)
	}
}

func TestHandler_KafkaFailureReturns500(t *testing.T) {
	t.Parallel()
	h, _, writer := newTestHandler(t)
	writer.returnErr = errors.New("kafka: broker unavailable")

	rw := postNotification(h, NotificationRequest{
		UserID:         "u_kafka",
		IdempotencyKey: "kafka-fail-1",
		Category:       CategoryTransactional,
		Notifications: []ChannelRequest{{
			Channel:    ChannelEmail,
			TemplateID: "order_confirmation_email_v2",
			Variables:  map[string]string{"order_id": "1", "amount": "$1", "customer_name": "X"},
		}},
	})

	if rw.Code != http.StatusInternalServerError {
		t.Fatalf("want 500 on Kafka failure, got %d", rw.Code)
	}
}

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/notifications", bytes.NewBufferString(`{invalid`))
	req.Header.Set("Content-Type", "application/json")
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	if rw.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for invalid JSON, got %d", rw.Code)
	}
}

func TestHandler_UnknownCategory(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)

	rw := postNotification(h, NotificationRequest{
		UserID:         "u_x",
		IdempotencyKey: "bad-cat",
		Category:       "URGENT", // not a known category
		Notifications: []ChannelRequest{{
			Channel: ChannelEmail, TemplateID: "order_confirmation_email_v2",
		}},
	})

	if rw.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for unknown category, got %d", rw.Code)
	}
}

func TestHandler_ResponseChannelsMatchRequestOrder(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)

	rw := postNotification(h, NotificationRequest{
		UserID:         "u_order",
		IdempotencyKey: "order-x",
		Category:       CategoryTransactional,
		Notifications: []ChannelRequest{
			{
				Channel:    ChannelEmail,
				TemplateID: "order_confirmation_email_v2",
				Variables:  map[string]string{"order_id": "1", "amount": "$1", "customer_name": "X"},
			},
			{
				Channel:    ChannelInApp,
				TemplateID: "order_confirmation_inapp_v1",
				Variables:  map[string]string{"order_id": "1", "amount": "$1"},
			},
		},
	})

	if rw.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d", rw.Code)
	}

	resp := decodeResponse(t, rw)
	if resp.Channels[0].Channel != ChannelEmail {
		t.Errorf("want first channel email, got %s", resp.Channels[0].Channel)
	}
	if resp.Channels[1].Channel != ChannelInApp {
		t.Errorf("want second channel inapp, got %s", resp.Channels[1].Channel)
	}
}
