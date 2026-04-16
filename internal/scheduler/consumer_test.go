package scheduler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func marshalMessage(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestConsumer_EnqueuesValidMessage(t *testing.T) {
	t.Parallel()

	future := time.Now().Add(time.Hour).UTC()
	msg := ScheduledMessage{
		NotificationID:       "n-001",
		ParentNotificationID: "p-001",
		UserID:               "u-1",
		Channel:              ChannelEmail,
		Category:             CategoryTransactional,
		TemplateID:           "tmpl-1",
		ScheduleAt:           &future,
		IdempotencyKey:       "idem-1",
		EnqueuedAt:           time.Now().UTC(),
	}

	store := newMemScheduleStore()
	reader := &stubReader{msgs: []kafka.Message{
		{Value: marshalMessage(t, msg)},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewConsumer(reader, store, zap.NewNop())

	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	// Wait for the message to be processed then cancel.
	for i := 0; i < 50; i++ {
		if store.size() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if store.size() != 1 {
		t.Fatalf("want 1 enqueued message, got %d", store.size())
	}
	if len(reader.commits) != 1 {
		t.Errorf("want 1 committed offset, got %d", len(reader.commits))
	}
}

func TestConsumer_SkipsMessageWithNilScheduleAt(t *testing.T) {
	t.Parallel()

	// ScheduleAt omitted (nil pointer after unmarshal).
	bad := map[string]any{
		"notification_id": "n-bad",
		"channel":         "email",
	}

	store := newMemScheduleStore()
	reader := &stubReader{msgs: []kafka.Message{
		{Value: marshalMessage(t, bad)},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewConsumer(reader, store, zap.NewNop())

	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	// Give the consumer enough time to read and skip the message.
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	// Nothing should have been enqueued for the malformed message.
	if store.size() != 0 {
		t.Errorf("want 0 enqueued, got %d", store.size())
	}
}

func TestConsumer_SkipsInvalidJSON(t *testing.T) {
	t.Parallel()

	store := newMemScheduleStore()
	reader := &stubReader{msgs: []kafka.Message{
		{Value: []byte(`{not valid json`)},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewConsumer(reader, store, zap.NewNop())

	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if store.size() != 0 {
		t.Errorf("want 0 enqueued, got %d", store.size())
	}
}

func TestConsumer_StoreFailureDoesNotCommit(t *testing.T) {
	t.Parallel()

	future := time.Now().Add(time.Hour).UTC()
	msg := ScheduledMessage{
		NotificationID: "n-storeerr",
		Channel:        ChannelSlack,
		Category:       CategoryAlert,
		ScheduleAt:     &future,
	}

	reader := &stubReader{msgs: []kafka.Message{
		{Value: marshalMessage(t, msg)},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewConsumer(reader, failingStore{}, zap.NewNop())

	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	// The offset must not have been committed because the Enqueue failed.
	if len(reader.commits) != 0 {
		t.Errorf("want 0 committed offsets after store failure, got %d", len(reader.commits))
	}
}

func TestConsumer_MultipleMessagesAllEnqueued(t *testing.T) {
	t.Parallel()

	future := time.Now().Add(time.Hour).UTC()
	var kafkaMsgs []kafka.Message
	for i := range 5 {
		msg := ScheduledMessage{
			NotificationID: "n-multi-" + string(rune('0'+i)),
			Channel:        ChannelEmail,
			Category:       CategoryMarketing,
			ScheduleAt:     &future,
			IdempotencyKey: "key-" + string(rune('0'+i)),
		}
		kafkaMsgs = append(kafkaMsgs, kafka.Message{Value: marshalMessage(t, msg)})
	}

	store := newMemScheduleStore()
	reader := &stubReader{msgs: kafkaMsgs}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewConsumer(reader, store, zap.NewNop())

	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	for i := 0; i < 100; i++ {
		if store.size() == 5 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if store.size() != 5 {
		t.Fatalf("want 5 enqueued, got %d", store.size())
	}
}
