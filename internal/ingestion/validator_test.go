package ingestion

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// stubResolver is a controllable TemplateResolver for tests. Each entry in
// the responses map is keyed by template_id; if the key is absent the resolver
// returns ErrTemplateNotFound.
type stubResolver struct {
	responses map[string]resolverResponse
}

type resolverResponse struct {
	meta *TemplateMetadata
	err  error
}

func (s *stubResolver) Resolve(_ context.Context, templateID string) (*TemplateMetadata, error) {
	if r, ok := s.responses[templateID]; ok {
		return r.meta, r.err
	}
	return nil, fmt.Errorf("%w: %s", ErrTemplateNotFound, templateID)
}

// activeMeta returns a minimal active TemplateMetadata for the given channel
// with the supplied required variables.
func activeMeta(ch Channel, requiredVars ...string) *TemplateMetadata {
	return &TemplateMetadata{
		ID:                "tmpl-1",
		Channel:           ch,
		Version:           3,
		RequiredVariables: requiredVars,
		IsActive:          true,
	}
}

func TestValidator_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resolver     *stubResolver
		request      *NotificationRequest
		wantErr      bool
		wantErrCount int
		wantVersions []int // expected templateVersion per enriched channel (success only)
	}{
		{
			name: "single email channel — all variables present",
			resolver: &stubResolver{responses: map[string]resolverResponse{
				"tmpl-order-email": {meta: activeMeta(ChannelEmail, "order_id", "amount")},
			}},
			request: &NotificationRequest{
				Notifications: []ChannelRequest{{
					Channel:    ChannelEmail,
					TemplateID: "tmpl-order-email",
					Variables:  map[string]string{"order_id": "123", "amount": "$9.99"},
				}},
			},
			wantVersions: []int{3},
		},
		{
			name: "two channels — both valid",
			resolver: &stubResolver{responses: map[string]resolverResponse{
				"tmpl-email": {meta: activeMeta(ChannelEmail, "name")},
				"tmpl-inapp": {meta: activeMeta(ChannelInApp)},
			}},
			request: &NotificationRequest{
				Notifications: []ChannelRequest{
					{Channel: ChannelEmail, TemplateID: "tmpl-email", Variables: map[string]string{"name": "Ada"}},
					{Channel: ChannelInApp, TemplateID: "tmpl-inapp"},
				},
			},
			wantVersions: []int{3, 3},
		},
		{
			name: "missing required variable",
			resolver: &stubResolver{responses: map[string]resolverResponse{
				"tmpl-email": {meta: activeMeta(ChannelEmail, "order_id", "customer_name")},
			}},
			request: &NotificationRequest{
				Notifications: []ChannelRequest{{
					Channel:    ChannelEmail,
					TemplateID: "tmpl-email",
					Variables:  map[string]string{"order_id": "123"}, // customer_name absent
				}},
			},
			wantErr:      true,
			wantErrCount: 1,
		},
		{
			name: "template not found in cache",
			resolver: &stubResolver{responses: map[string]resolverResponse{}},
			request: &NotificationRequest{
				Notifications: []ChannelRequest{{
					Channel:    ChannelEmail,
					TemplateID: "ghost-template",
				}},
			},
			wantErr:      true,
			wantErrCount: 1,
		},
		{
			name: "inactive template",
			resolver: &stubResolver{responses: map[string]resolverResponse{
				"tmpl-old": {meta: &TemplateMetadata{
					ID: "tmpl-old", Channel: ChannelEmail, Version: 1,
					IsActive: false,
				}},
			}},
			request: &NotificationRequest{
				Notifications: []ChannelRequest{{
					Channel: ChannelEmail, TemplateID: "tmpl-old",
				}},
			},
			wantErr:      true,
			wantErrCount: 1,
		},
		{
			name: "channel mismatch — template is for email but request says inapp",
			resolver: &stubResolver{responses: map[string]resolverResponse{
				"tmpl-email": {meta: activeMeta(ChannelEmail)},
			}},
			request: &NotificationRequest{
				Notifications: []ChannelRequest{{
					Channel: ChannelInApp, TemplateID: "tmpl-email",
				}},
			},
			wantErr:      true,
			wantErrCount: 1,
		},
		{
			name: "unknown channel value",
			resolver: &stubResolver{},
			request: &NotificationRequest{
				Notifications: []ChannelRequest{{
					Channel:    "sms",
					TemplateID: "tmpl-sms",
				}},
			},
			wantErr:      true,
			wantErrCount: 1,
		},
		{
			name: "empty template_id",
			resolver: &stubResolver{},
			request: &NotificationRequest{
				Notifications: []ChannelRequest{{
					Channel: ChannelEmail,
					// TemplateID intentionally blank
				}},
			},
			wantErr:      true,
			wantErrCount: 1,
		},
		{
			name: "resolver infrastructure error — all channels fail",
			resolver: &stubResolver{responses: map[string]resolverResponse{
				"tmpl-a": {err: errors.New("redis: connection refused")},
				"tmpl-b": {err: errors.New("redis: connection refused")},
			}},
			request: &NotificationRequest{
				Notifications: []ChannelRequest{
					{Channel: ChannelEmail, TemplateID: "tmpl-a"},
					{Channel: ChannelInApp, TemplateID: "tmpl-b"},
				},
			},
			wantErr:      true,
			wantErrCount: 2,
		},
		{
			name: "atomic failure — one bad channel poisons the whole request",
			resolver: &stubResolver{responses: map[string]resolverResponse{
				"tmpl-good": {meta: activeMeta(ChannelEmail)},
				// tmpl-bad is absent → ErrTemplateNotFound
			}},
			request: &NotificationRequest{
				Notifications: []ChannelRequest{
					{Channel: ChannelEmail, TemplateID: "tmpl-good"},
					{Channel: ChannelInApp, TemplateID: "tmpl-bad"},
				},
			},
			wantErr:      true,
			wantErrCount: 1,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v := NewValidator(tc.resolver)
			enriched, failures := v.Validate(context.Background(), tc.request)

			if tc.wantErr {
				if len(failures) == 0 {
					t.Fatal("expected validation failures, got none")
				}
				if tc.wantErrCount > 0 && len(failures) != tc.wantErrCount {
					t.Errorf("want %d failures, got %d: %v", tc.wantErrCount, len(failures), failures)
				}
				if enriched != nil {
					t.Errorf("expected nil enriched slice on failure, got %v", enriched)
				}
				return
			}

			if len(failures) > 0 {
				t.Fatalf("unexpected validation failures: %v", failures)
			}
			if len(enriched) != len(tc.request.Notifications) {
				t.Fatalf("want %d enriched channels, got %d", len(tc.request.Notifications), len(enriched))
			}
			for i, want := range tc.wantVersions {
				if enriched[i].templateVersion != want {
					t.Errorf("channel[%d]: want templateVersion %d, got %d", i, want, enriched[i].templateVersion)
				}
			}
		})
	}
}

func TestCheckStructure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ch      ChannelRequest
		wantNil bool
	}{
		{"valid email channel", ChannelRequest{Channel: ChannelEmail, TemplateID: "t"}, true},
		{"valid slack channel", ChannelRequest{Channel: ChannelSlack, TemplateID: "t"}, true},
		{"valid inapp channel", ChannelRequest{Channel: ChannelInApp, TemplateID: "t"}, true},
		{"unknown channel", ChannelRequest{Channel: "push", TemplateID: "t"}, false},
		{"empty template_id", ChannelRequest{Channel: ChannelEmail, TemplateID: ""}, false},
		{"whitespace template_id", ChannelRequest{Channel: ChannelEmail, TemplateID: "   "}, false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := checkStructure(tc.ch)
			if tc.wantNil && got != nil {
				t.Errorf("want nil, got %+v", got)
			}
			if !tc.wantNil && got == nil {
				t.Error("want non-nil ValidationDetail, got nil")
			}
		})
	}
}

func TestCheckRequiredVariables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ch        ChannelRequest
		meta      *TemplateMetadata
		wantNil   bool
		wantInErr string
	}{
		{
			name:    "all required variables present",
			ch:      ChannelRequest{Variables: map[string]string{"a": "1", "b": "2"}},
			meta:    &TemplateMetadata{RequiredVariables: []string{"a", "b"}},
			wantNil: true,
		},
		{
			name:    "no required variables declared",
			ch:      ChannelRequest{Variables: nil},
			meta:    &TemplateMetadata{RequiredVariables: nil},
			wantNil: true,
		},
		{
			name:      "one variable missing",
			ch:        ChannelRequest{Variables: map[string]string{"a": "1"}},
			meta:      &TemplateMetadata{RequiredVariables: []string{"a", "b"}},
			wantNil:   false,
			wantInErr: "b",
		},
		{
			name:      "all variables missing",
			ch:        ChannelRequest{Variables: map[string]string{}},
			meta:      &TemplateMetadata{RequiredVariables: []string{"x", "y", "z"}},
			wantNil:   false,
			wantInErr: "x",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := checkRequiredVariables(tc.ch, tc.meta)
			if tc.wantNil && got != nil {
				t.Errorf("want nil, got %+v", got)
			}
			if !tc.wantNil {
				if got == nil {
					t.Fatal("want non-nil ValidationDetail, got nil")
				}
				if tc.wantInErr != "" && !contains(got.Error, tc.wantInErr) {
					t.Errorf("error %q does not contain %q", got.Error, tc.wantInErr)
				}
			}
		})
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsRune(s, sub))
}

func containsRune(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
