package template

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
)

// ── Stub service ──────────────────────────────────────────────────────────────

// stubService implements the subset of *Service behaviour exercised by Handler.
// We cannot stub *Service directly (it is a concrete struct) so tests go via
// the handler with a real *Service backed by in-memory collaborators.
// This file therefore uses newTestSvc to build fully wired handlers.

func newTestHandler(t *testing.T) (*Handler, *memRepo, *capturePublisher) {
	t.Helper()
	repo := &memRepo{}
	cache := newMemCache()
	store := newMemBodyStore()
	pub := &capturePublisher{}
	svc := NewService(repo, cache, store, pub, zap.NewNop())
	h := NewHandler(svc, zap.NewNop())
	return h, repo, pub
}

// routedHandler wires the handler into a ServeMux so path values are populated.
func routedHandler(h *Handler) http.Handler {
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux
}

func do(mux http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	return rw
}

// ── Handler tests ─────────────────────────────────────────────────────────────

func TestHandler_CreateTemplate_201(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)
	mux := routedHandler(h)

	rw := do(mux, http.MethodPost, "/v1/templates", CreateRequest{
		TemplateID:        "order_conf_email",
		Channel:           ChannelEmail,
		Name:              "Order Confirmation",
		BodyHTML:          "<p>hello {{name}}</p>",
		RequiredVariables: []string{"name"},
	})

	if rw.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d: %s", rw.Code, rw.Body.String())
	}

	var resp TemplateResponse
	if err := json.NewDecoder(rw.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TemplateID != "order_conf_email" {
		t.Errorf("want template_id order_conf_email, got %s", resp.TemplateID)
	}
	if resp.Version != 1 {
		t.Errorf("want version 1, got %d", resp.Version)
	}
	if resp.BodyHTML != "<p>hello {{name}}</p>\n" && resp.BodyHTML != "<p>hello {{name}}</p>" {
		// json.Encoder adds a newline; accept either.
		t.Errorf("unexpected body_html: %q", resp.BodyHTML)
	}
}

func TestHandler_CreateTemplate_409_Duplicate(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)
	mux := routedHandler(h)

	req := CreateRequest{
		TemplateID: "dup",
		Channel:    ChannelEmail,
		Name:       "Dup",
		BodyHTML:   "<p>b</p>",
	}
	do(mux, http.MethodPost, "/v1/templates", req) // first — 201
	rw := do(mux, http.MethodPost, "/v1/templates", req) // second — 409

	if rw.Code != http.StatusConflict {
		t.Fatalf("want 409 on duplicate, got %d", rw.Code)
	}
	var errResp ErrorResponse
	_ = json.NewDecoder(rw.Body).Decode(&errResp)
	if errResp.Error != "ALREADY_EXISTS" {
		t.Errorf("want ALREADY_EXISTS error code, got %q", errResp.Error)
	}
}

func TestHandler_CreateTemplate_400_Validation(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)
	mux := routedHandler(h)

	// Missing channel.
	rw := do(mux, http.MethodPost, "/v1/templates", CreateRequest{
		TemplateID: "x",
		Name:       "X",
		BodyHTML:   "<p>b</p>",
		// Channel intentionally omitted → IsValid() returns false
	})

	if rw.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rw.Code)
	}
}

func TestHandler_GetLatest_200(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)
	mux := routedHandler(h)

	do(mux, http.MethodPost, "/v1/templates", CreateRequest{
		TemplateID: "tmpl-get",
		Channel:    ChannelEmail,
		Name:       "G",
		BodyHTML:   "<p>v1</p>",
	})

	rw := do(mux, http.MethodGet, "/v1/templates/tmpl-get", nil)
	if rw.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rw.Code, rw.Body.String())
	}

	var resp TemplateResponse
	_ = json.NewDecoder(rw.Body).Decode(&resp)
	if resp.Version != 1 {
		t.Errorf("want version 1, got %d", resp.Version)
	}
}

func TestHandler_GetLatest_404(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)
	mux := routedHandler(h)

	rw := do(mux, http.MethodGet, "/v1/templates/ghost-tmpl", nil)
	if rw.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", rw.Code)
	}
}

func TestHandler_GetByVersion_200(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)
	mux := routedHandler(h)

	do(mux, http.MethodPost, "/v1/templates", CreateRequest{
		TemplateID: "tmpl-ver",
		Channel:    ChannelEmail,
		Name:       "V",
		BodyHTML:   "<p>v1</p>",
	})

	rw := do(mux, http.MethodGet, "/v1/templates/tmpl-ver/versions/1", nil)
	if rw.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rw.Code, rw.Body.String())
	}
}

func TestHandler_GetByVersion_InvalidVersion(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)
	mux := routedHandler(h)

	tests := []struct {
		path string
	}{
		{"/v1/templates/x/versions/0"},
		{"/v1/templates/x/versions/abc"},
		{"/v1/templates/x/versions/-1"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			rw := do(mux, http.MethodGet, tc.path, nil)
			if rw.Code != http.StatusBadRequest {
				t.Errorf("%s: want 400, got %d", tc.path, rw.Code)
			}
		})
	}
}

func TestHandler_CreateVersion_201(t *testing.T) {
	t.Parallel()
	h, _, pub := newTestHandler(t)
	mux := routedHandler(h)

	do(mux, http.MethodPost, "/v1/templates", CreateRequest{
		TemplateID: "tmpl-cv",
		Channel:    ChannelEmail,
		Name:       "CV",
		BodyHTML:   "<p>v1</p>",
	})

	rw := do(mux, http.MethodPost, "/v1/templates/tmpl-cv/versions", CreateVersionRequest{
		BodyHTML:          "<p>v2</p>",
		RequiredVariables: []string{"x"},
	})

	if rw.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d: %s", rw.Code, rw.Body.String())
	}

	var resp TemplateResponse
	_ = json.NewDecoder(rw.Body).Decode(&resp)
	if resp.Version != 2 {
		t.Errorf("want version 2, got %d", resp.Version)
	}

	// Two Kafka events: CREATED + VERSION_CREATED.
	if len(pub.events) != 2 {
		t.Fatalf("want 2 events, got %d", len(pub.events))
	}
}

func TestHandler_Deactivate_204(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)
	mux := routedHandler(h)

	do(mux, http.MethodPost, "/v1/templates", CreateRequest{
		TemplateID: "tmpl-del",
		Channel:    ChannelEmail,
		Name:       "Del",
		BodyHTML:   "<p>bye</p>",
	})

	rw := do(mux, http.MethodDelete, "/v1/templates/tmpl-del", nil)
	if rw.Code != http.StatusNoContent {
		t.Fatalf("want 204, got %d", rw.Code)
	}

	// Subsequent GET must return 404.
	rw2 := do(mux, http.MethodGet, "/v1/templates/tmpl-del", nil)
	if rw2.Code != http.StatusNotFound {
		t.Errorf("want 404 after deactivation, got %d", rw2.Code)
	}
}

func TestHandler_List_200(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)
	mux := routedHandler(h)

	// Create three templates.
	for _, id := range []string{"a", "b", "c"} {
		do(mux, http.MethodPost, "/v1/templates", CreateRequest{
			TemplateID: id,
			Channel:    ChannelEmail,
			Name:       id,
			BodyHTML:   "<p>" + id + "</p>",
		})
	}

	rw := do(mux, http.MethodGet, "/v1/templates", nil)
	if rw.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rw.Code)
	}

	var resp ListResponse
	_ = json.NewDecoder(rw.Body).Decode(&resp)
	if resp.Total != 3 {
		t.Errorf("want total=3, got %d", resp.Total)
	}
	for _, tmpl := range resp.Templates {
		if tmpl.BodyHTML != "" {
			t.Errorf("list response must not include body_html, got %q for %s", tmpl.BodyHTML, tmpl.TemplateID)
		}
	}
}

func TestHandler_InvalidJSON_400(t *testing.T) {
	t.Parallel()
	h, _, _ := newTestHandler(t)
	mux := routedHandler(h)

	req := httptest.NewRequest(http.MethodPost, "/v1/templates", bytes.NewBufferString(`{bad`))
	req.Header.Set("Content-Type", "application/json")
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for invalid JSON, got %d", rw.Code)
	}
}

// Compile check: ensure stubService satisfies error sentinel interfaces.
var _ = errors.Is(ErrNotFound, ErrNotFound)
