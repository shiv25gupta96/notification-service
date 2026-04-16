package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// ErrTemplateNotFound is returned when the Template Service reports 404.
var ErrTemplateNotFound = errors.New("template not found")

// TemplateFetch holds the raw, unrendered content returned by the Template
// Service. Variable substitution is the worker's responsibility — the Template
// Service is kept free of rendering concerns.
type TemplateFetch struct {
	Subject  string // may contain {{.VarName}} placeholders
	BodyHTML string // may contain {{.VarName}} placeholders
	Channel  string
	Version  int
}

// TemplateEngine is the abstraction over the Template Service used by the
// worker. Defined at point of use; the HTTP implementation is injected in main.
type TemplateEngine interface {
	// Fetch returns the raw template content for templateID.
	// The Template Service resolves the latest active version and fetches the
	// HTML body from S3 internally; the worker never accesses S3 directly.
	Fetch(ctx context.Context, templateID string) (TemplateFetch, error)
}

// templateServiceResponse mirrors the JSON shape of the Template Service's
// GET /v1/templates/{template_id} response.
type templateServiceResponse struct {
	TemplateID string `json:"template_id"`
	Version    int    `json:"version"`
	Channel    string `json:"channel"`
	Subject    string `json:"subject"`
	BodyHTML   string `json:"body_html"`
}

// HTTPTemplateEngine implements TemplateEngine by calling the Template Service
// REST API. It uses the existing GET /v1/templates/{id} endpoint which already
// returns the full HTML body (the Template Service fetches it from S3 and
// includes it in the response).
type HTTPTemplateEngine struct {
	baseURL string
	client  *http.Client
}

// NewHTTPTemplateEngine creates an engine pointing at the given Template
// Service base URL (e.g. "http://template-service:8081").
func NewHTTPTemplateEngine(baseURL string) *HTTPTemplateEngine {
	return &HTTPTemplateEngine{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

// Fetch calls GET /v1/templates/{templateID} and returns the raw body.
func (e *HTTPTemplateEngine) Fetch(ctx context.Context, templateID string) (TemplateFetch, error) {
	url := fmt.Sprintf("%s/v1/templates/%s", e.baseURL, templateID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return TemplateFetch{}, fmt.Errorf("template engine: build request: %w", err)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return TemplateFetch{}, fmt.Errorf("template engine: get %s: %w", templateID, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// handled below
	case http.StatusNotFound:
		return TemplateFetch{}, fmt.Errorf("%w: %s", ErrTemplateNotFound, templateID)
	default:
		return TemplateFetch{}, fmt.Errorf("template engine: unexpected status %d for %s", resp.StatusCode, templateID)
	}

	var body templateServiceResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return TemplateFetch{}, fmt.Errorf("template engine: decode response: %w", err)
	}

	return TemplateFetch{
		Subject:  body.Subject,
		BodyHTML: body.BodyHTML,
		Channel:  body.Channel,
		Version:  body.Version,
	}, nil
}
