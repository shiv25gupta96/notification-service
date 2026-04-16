package worker

import (
	"context"

	"go.uber.org/zap"
)

// EmailMessage is the fully-rendered payload passed to the email provider.
type EmailMessage struct {
	To      string // resolved from user_id by the delivery step
	Subject string // rendered from template subject
	Body    string // rendered HTML body
}

// EmailProvider is the delivery abstraction for the email channel.
// Real implementations (SES, SendGrid, SMTP) satisfy this interface;
// only the DummyEmailProvider is shipped for now.
type EmailProvider interface {
	Send(ctx context.Context, msg EmailMessage) error
}

// DummyEmailProvider logs the delivery intent and returns nil.
// It is the sole implementation until a real email provider is wired in.
type DummyEmailProvider struct {
	logger *zap.Logger
}

// NewDummyEmailProvider creates a DummyEmailProvider.
func NewDummyEmailProvider(logger *zap.Logger) *DummyEmailProvider {
	return &DummyEmailProvider{logger: logger}
}

// Send logs the email details and always reports success.
func (d *DummyEmailProvider) Send(_ context.Context, msg EmailMessage) error {
	d.logger.Info("dummy email provider: send",
		zap.String("to", msg.To),
		zap.String("subject", msg.Subject),
		zap.Int("body_bytes", len(msg.Body)),
	)
	return nil
}
