package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
)

// errKafkaDown simulates a Kafka write failure in poller tests.
// Intentionally distinct from errStoreFailure.
var errKafkaDown = errors.New("kafka: broker unavailable")

// newTestPoller returns a Poller with a very short poll interval for tests.
func newTestPoller(store ScheduleStore, writer *captureWriter) *Poller {
	p := NewPoller(store, writer, zap.NewNop())
	p.interval = 10 * time.Millisecond
	return p
}

func TestPoller_DispatchesDueMessages(t *testing.T) {
	t.Parallel()

	store := newMemScheduleStore()
	writer := &captureWriter{}
	poller := newTestPoller(store, writer)

	past := time.Now().Add(-time.Minute)
	msgs := []ScheduledMessage{
		{NotificationID: "n-1", UserID: "u-1", Channel: ChannelEmail, Category: CategoryTransactional, ScheduleAt: &past},
		{NotificationID: "n-2", UserID: "u-1", Channel: ChannelSlack, Category: CategoryAlert, ScheduleAt: &past},
		{NotificationID: "n-3", UserID: "u-2", Channel: ChannelInApp, Category: CategoryProduct, ScheduleAt: &past},
	}
	ctx := context.Background()
	for _, m := range msgs {
		if err := store.Enqueue(ctx, m, past); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- poller.Run(runCtx) }()

	for i := 0; i < 100; i++ {
		if store.size() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if store.size() != 0 {
		t.Errorf("want store empty after dispatch, got %d entries", store.size())
	}
	if writer.callCount() == 0 {
		t.Fatal("want at least one Write call, got 0")
	}
	total := 0
	for _, batch := range writer.calls {
		total += len(batch)
	}
	if total != 3 {
		t.Errorf("want 3 total envelopes written, got %d", total)
	}
}

func TestPoller_RoutesEnvelopesToCorrectTopics(t *testing.T) {
	t.Parallel()

	store := newMemScheduleStore()
	writer := &captureWriter{}
	poller := newTestPoller(store, writer)

	past := time.Now().Add(-time.Second)
	msgs := []ScheduledMessage{
		{NotificationID: "e-1", UserID: "u-1", Channel: ChannelEmail, ScheduleAt: &past},
		{NotificationID: "s-1", UserID: "u-2", Channel: ChannelSlack, ScheduleAt: &past},
		{NotificationID: "i-1", UserID: "u-3", Channel: ChannelInApp, ScheduleAt: &past},
	}
	ctx := context.Background()
	for _, m := range msgs {
		_ = store.Enqueue(ctx, m, past)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- poller.Run(runCtx) }()

	for i := 0; i < 100; i++ {
		if store.size() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	topicsSeen := make(map[string]struct{})
	for _, batch := range writer.calls {
		for _, env := range batch {
			topicsSeen[env.Topic] = struct{}{}
		}
	}
	for _, want := range []string{"notif.email", "notif.slack", "notif.inapp"} {
		if _, ok := topicsSeen[want]; !ok {
			t.Errorf("expected topic %q not seen in published envelopes", want)
		}
	}
}

func TestPoller_EnvelopeKeyIsUserID(t *testing.T) {
	t.Parallel()

	store := newMemScheduleStore()
	writer := &captureWriter{}
	poller := newTestPoller(store, writer)

	past := time.Now().Add(-time.Second)
	msg := ScheduledMessage{
		NotificationID: "n-key",
		UserID:         "u-partition-key",
		Channel:        ChannelEmail,
		ScheduleAt:     &past,
	}
	ctx := context.Background()
	_ = store.Enqueue(ctx, msg, past)

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- poller.Run(runCtx) }()

	for i := 0; i < 100; i++ {
		if store.size() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	batch := writer.firstBatch()
	if len(batch) == 0 {
		t.Fatal("want at least one envelope")
	}
	if string(batch[0].Key) != "u-partition-key" {
		t.Errorf("want partition key %q, got %q", "u-partition-key", string(batch[0].Key))
	}
}

func TestPoller_KafkaFailureKeepsMessagesInStore(t *testing.T) {
	t.Parallel()

	store := newMemScheduleStore()
	writer := &captureWriter{returnErr: errKafkaDown}
	poller := newTestPoller(store, writer)

	past := time.Now().Add(-time.Second)
	msg := ScheduledMessage{
		NotificationID: "n-retry",
		UserID:         "u-1",
		Channel:        ChannelEmail,
		ScheduleAt:     &past,
	}
	ctx := context.Background()
	_ = store.Enqueue(ctx, msg, past)

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- poller.Run(runCtx) }()

	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done

	if store.size() != 1 {
		t.Errorf("want 1 message retained in store after write failure, got %d", store.size())
	}
}

func TestPoller_FutureMessagesNotDispatched(t *testing.T) {
	t.Parallel()

	store := newMemScheduleStore()
	writer := &captureWriter{}
	poller := newTestPoller(store, writer)

	future := time.Now().Add(time.Hour)
	msg := ScheduledMessage{
		NotificationID: "n-future",
		UserID:         "u-1",
		Channel:        ChannelEmail,
		ScheduleAt:     &future,
	}
	ctx := context.Background()
	_ = store.Enqueue(ctx, msg, future)

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- poller.Run(runCtx) }()

	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done

	if writer.callCount() != 0 {
		t.Errorf("want 0 Write calls for future message, got %d", writer.callCount())
	}
	if store.size() != 1 {
		t.Errorf("want future message still in store, got size %d", store.size())
	}
}

func TestPoller_EnvelopeValueContainsNotificationID(t *testing.T) {
	t.Parallel()

	store := newMemScheduleStore()
	writer := &captureWriter{}
	poller := newTestPoller(store, writer)

	past := time.Now().Add(-time.Second)
	msg := ScheduledMessage{
		NotificationID: "n-payload-check",
		UserID:         "u-1",
		Channel:        ChannelEmail,
		ScheduleAt:     &past,
	}
	ctx := context.Background()
	_ = store.Enqueue(ctx, msg, past)

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- poller.Run(runCtx) }()

	for i := 0; i < 100; i++ {
		if store.size() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	batch := writer.firstBatch()
	if len(batch) == 0 {
		t.Fatal("want at least one envelope")
	}

	var decoded ScheduledMessage
	if err := json.Unmarshal(batch[0].Value, &decoded); err != nil {
		t.Fatalf("unmarshal envelope value: %v", err)
	}
	if decoded.NotificationID != "n-payload-check" {
		t.Errorf("want notification_id %q in payload, got %q", "n-payload-check", decoded.NotificationID)
	}
}
