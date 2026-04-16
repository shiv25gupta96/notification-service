package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

// ── in-memory stubs ───────────────────────────────────────────────────────────

type stubTemplateEngine struct {
	fetches map[string]TemplateFetch
	err     error
}

func (s *stubTemplateEngine) Fetch(_ context.Context, id string) (TemplateFetch, error) {
	if s.err != nil {
		return TemplateFetch{}, s.err
	}
	f, ok := s.fetches[id]
	if !ok {
		return TemplateFetch{}, ErrTemplateNotFound
	}
	return f, nil
}

type captureEmailProvider struct {
	mu   sync.Mutex
	sent []EmailMessage
	err  error
}

func (p *captureEmailProvider) Send(_ context.Context, msg EmailMessage) error {
	if p.err != nil {
		return p.err
	}
	p.mu.Lock()
	p.sent = append(p.sent, msg)
	p.mu.Unlock()
	return nil
}

type memAuditLog struct {
	mu       sync.Mutex
	attempts []DeliveryAttempt
	err      error
}

func (a *memAuditLog) Record(_ context.Context, attempt DeliveryAttempt) error {
	if a.err != nil {
		return a.err
	}
	a.mu.Lock()
	a.attempts = append(a.attempts, attempt)
	a.mu.Unlock()
	return nil
}

type alwaysStep struct {
	name string
	err  error
}

func (s *alwaysStep) Name() string                                 { return s.name }
func (s *alwaysStep) Execute(_ context.Context, _ *WorkItem) error { return s.err }

// workItem builds a minimal WorkItem for tests.
func workItem(id string) *WorkItem {
	return &WorkItem{
		NotificationID:       id,
		ParentNotificationID: "parent-" + id,
		UserID:               "user-1",
		TemplateID:           "tmpl-welcome",
		Variables:            map[string]string{"Name": "Alice"},
		IdempotencyKey:       "idem-" + id,
		EnqueuedAt:           time.Now(),
	}
}

// ── Chain tests ───────────────────────────────────────────────────────────────

func TestChain_AllStepsSucceed_OutcomeDelivered(t *testing.T) {
	t.Parallel()

	audit := &memAuditLog{}
	chain := NewChain([]Step{
		&alwaysStep{name: "a"},
		&alwaysStep{name: "b"},
	}, audit, zap.NewNop())

	item := workItem("n-001")
	chain.Process(context.Background(), item)

	if item.Outcome != OutcomeDelivered {
		t.Errorf("want OutcomeDelivered, got %q", item.Outcome)
	}
	if len(audit.attempts) != 1 {
		t.Fatalf("want 1 audit record, got %d", len(audit.attempts))
	}
	if audit.attempts[0].Outcome != OutcomeDelivered {
		t.Errorf("audit outcome: want DELIVERED, got %q", audit.attempts[0].Outcome)
	}
}

func TestChain_StepReturnsError_OutcomeFailed(t *testing.T) {
	t.Parallel()

	audit := &memAuditLog{}
	chain := NewChain([]Step{
		&alwaysStep{name: "a"},
		&alwaysStep{name: "fail", err: errors.New("downstream unavailable")},
		&alwaysStep{name: "unreachable"},
	}, audit, zap.NewNop())

	item := workItem("n-002")
	chain.Process(context.Background(), item)

	if item.Outcome != OutcomeFailed {
		t.Errorf("want OutcomeFailed, got %q", item.Outcome)
	}
	if len(audit.attempts) != 1 {
		t.Fatalf("want 1 audit record, got %d", len(audit.attempts))
	}
	if audit.attempts[0].ErrorMessage == "" {
		t.Error("want non-empty error_message in audit record for failed outcome")
	}
}

func TestChain_StepReturnsSuppressed_OutcomeSuppressed(t *testing.T) {
	t.Parallel()

	audit := &memAuditLog{}
	chain := NewChain([]Step{
		&alwaysStep{name: "suppress", err: ErrSuppressed},
		&alwaysStep{name: "unreachable"},
	}, audit, zap.NewNop())

	item := workItem("n-003")
	chain.Process(context.Background(), item)

	if item.Outcome != OutcomeSuppressed {
		t.Errorf("want OutcomeSuppressed, got %q", item.Outcome)
	}
	if audit.attempts[0].ErrorMessage != "" {
		t.Error("suppressed outcome must not write an error_message")
	}
}

func TestChain_AuditAlwaysWritten_EvenOnFailure(t *testing.T) {
	t.Parallel()

	audit := &memAuditLog{}
	chain := NewChain([]Step{
		&alwaysStep{name: "fail", err: errors.New("boom")},
	}, audit, zap.NewNop())

	chain.Process(context.Background(), workItem("n-audit"))

	if len(audit.attempts) != 1 {
		t.Errorf("want 1 audit attempt after step failure, got %d", len(audit.attempts))
	}
}

// ── TemplateStep tests ────────────────────────────────────────────────────────

func TestTemplateStep_RendersSubjectAndBody(t *testing.T) {
	t.Parallel()

	engine := &stubTemplateEngine{
		fetches: map[string]TemplateFetch{
			"tmpl-welcome": {
				Subject:  "Hello {{.Name}}",
				BodyHTML: "<p>Welcome {{.Name}}!</p>",
				Channel:  "email",
				Version:  1,
			},
		},
	}
	step := NewTemplateStep(engine)
	item := workItem("n-tmpl")

	if err := step.Execute(context.Background(), item); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if item.RenderedSubject != "Hello Alice" {
		t.Errorf("subject: want %q, got %q", "Hello Alice", item.RenderedSubject)
	}
	if item.RenderedBody != "<p>Welcome Alice!</p>" {
		t.Errorf("body: want %q, got %q", "<p>Welcome Alice!</p>", item.RenderedBody)
	}
}

func TestTemplateStep_MissingVariable_ReturnsError(t *testing.T) {
	t.Parallel()

	engine := &stubTemplateEngine{
		fetches: map[string]TemplateFetch{
			"tmpl-welcome": {
				Subject:  "Hello {{.MissingVar}}",
				BodyHTML: "<p>ok</p>",
			},
		},
	}
	step := NewTemplateStep(engine)
	item := workItem("n-missing")

	if err := step.Execute(context.Background(), item); err == nil {
		t.Error("want error for missing template variable, got nil")
	}
}

func TestTemplateStep_EngineError_Propagated(t *testing.T) {
	t.Parallel()

	engine := &stubTemplateEngine{err: errors.New("template service unavailable")}
	step := NewTemplateStep(engine)

	if err := step.Execute(context.Background(), workItem("n-engfail")); err == nil {
		t.Error("want error when engine returns error")
	}
}

func TestTemplateStep_NotFound_Propagated(t *testing.T) {
	t.Parallel()

	engine := &stubTemplateEngine{fetches: map[string]TemplateFetch{}} // empty — will return ErrTemplateNotFound
	step := NewTemplateStep(engine)
	item := workItem("n-notfound")

	err := step.Execute(context.Background(), item)
	if err == nil {
		t.Fatal("want error for unknown template_id")
	}
}

// ── DeliveryStep tests ────────────────────────────────────────────────────────

func TestDeliveryStep_SendsEmail(t *testing.T) {
	t.Parallel()

	provider := &captureEmailProvider{}
	redis := newMemRedis()
	step := NewDeliveryStep(provider, redis)

	item := workItem("n-send")
	item.RenderedSubject = "Test Subject"
	item.RenderedBody = "<p>body</p>"

	if err := step.Execute(context.Background(), item); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(provider.sent) != 1 {
		t.Fatalf("want 1 sent email, got %d", len(provider.sent))
	}
	if provider.sent[0].Subject != "Test Subject" {
		t.Errorf("subject: want %q, got %q", "Test Subject", provider.sent[0].Subject)
	}
}

func TestDeliveryStep_ProviderError_Propagated(t *testing.T) {
	t.Parallel()

	provider := &captureEmailProvider{err: errors.New("SMTP connection refused")}
	step := NewDeliveryStep(provider, newMemRedis())

	if err := step.Execute(context.Background(), workItem("n-smtp")); err == nil {
		t.Error("want error when provider fails")
	}
}

// ── Renderer tests ────────────────────────────────────────────────────────────

func TestRenderSubject_SubstitutesVariables(t *testing.T) {
	t.Parallel()

	got, err := renderSubject("Order {{.OrderID}} confirmed", map[string]string{"OrderID": "ORD-42"})
	if err != nil {
		t.Fatalf("renderSubject: %v", err)
	}
	if got != "Order ORD-42 confirmed" {
		t.Errorf("want %q, got %q", "Order ORD-42 confirmed", got)
	}
}

func TestRenderBody_EscapesHTMLInVariables(t *testing.T) {
	t.Parallel()

	vars := map[string]string{"Input": "<script>alert(1)</script>"}
	got, err := renderBody("<p>{{.Input}}</p>", vars)
	if err != nil {
		t.Fatalf("renderBody: %v", err)
	}
	// html/template must escape the injected script tag.
	const want = "<p>&lt;script&gt;alert(1)&lt;/script&gt;</p>"
	if got != want {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestRenderSubject_MissingKey_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := renderSubject("Hello {{.Missing}}", map[string]string{})
	if err == nil {
		t.Error("want error for missing key with missingkey=error option")
	}
}

// ── Idempotency tests ─────────────────────────────────────────────────────────

func TestIdempotencyStep_NewMessage_Passes(t *testing.T) {
	t.Parallel()

	step := NewIdempotencyStep(newMemRedis())
	if err := step.Execute(context.Background(), workItem("n-new")); err != nil {
		t.Errorf("want nil for unseen notification, got %v", err)
	}
}

func TestIdempotencyStep_AlreadyDelivered_Suppressed(t *testing.T) {
	t.Parallel()

	r := newMemRedis()
	// Pre-seed the done key as if a prior delivery cycle completed.
	_ = r.Set(context.Background(), workerIdempotencyPrefix+"n-dup", "1", workerIdempotencyTTL).Error()

	step := NewIdempotencyStep(r)
	item := workItem("n-dup")

	err := step.Execute(context.Background(), item)
	if !errors.Is(err, ErrSuppressed) {
		t.Errorf("want ErrSuppressed for already-delivered notification, got %v", err)
	}
}

// ── PreferenceStep tests ──────────────────────────────────────────────────────

func TestPreferenceStep_AlwaysPasses(t *testing.T) {
	t.Parallel()

	step := NewPreferenceStep(zap.NewNop())
	if err := step.Execute(context.Background(), workItem("n-pref")); err != nil {
		t.Errorf("preference stub must always pass, got %v", err)
	}
}
