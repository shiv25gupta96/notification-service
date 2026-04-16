package template

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"go.uber.org/zap"
)

// Handler holds HTTP handlers for the Template Service REST API.
// All route registration is done by the caller (main.go) using Go 1.22's
// method+path syntax, keeping the handler itself free of routing concerns.
type Handler struct {
	svc    *Service
	logger *zap.Logger
}

// NewHandler creates a Handler backed by svc.
func NewHandler(svc *Service, logger *zap.Logger) *Handler {
	return &Handler{svc: svc, logger: logger}
}

// RegisterRoutes wires all template routes onto mux using the Go 1.22
// enhanced ServeMux pattern syntax.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("POST /v1/templates", h.create())
	mux.Handle("GET /v1/templates", h.list())
	mux.Handle("GET /v1/templates/{template_id}", h.getLatest())
	mux.Handle("GET /v1/templates/{template_id}/versions/{version}", h.getByVersion())
	mux.Handle("POST /v1/templates/{template_id}/versions", h.createVersion())
	mux.Handle("DELETE /v1/templates/{template_id}", h.deactivate())
}

// create handles POST /v1/templates.
func (h *Handler) create() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req CreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
			return
		}

		resp, err := h.svc.Create(r.Context(), req)
		if err != nil {
			h.handleServiceError(w, err, "create template")
			return
		}
		writeJSON(w, http.StatusCreated, resp)
	})
}

// list handles GET /v1/templates.
func (h *Handler) list() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp, err := h.svc.ListActive(r.Context())
		if err != nil {
			h.handleServiceError(w, err, "list templates")
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

// getLatest handles GET /v1/templates/{template_id}.
func (h *Handler) getLatest() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		templateID := r.PathValue("template_id")
		resp, err := h.svc.GetLatest(r.Context(), templateID)
		if err != nil {
			h.handleServiceError(w, err, "get latest template")
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

// getByVersion handles GET /v1/templates/{template_id}/versions/{version}.
func (h *Handler) getByVersion() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		templateID := r.PathValue("template_id")
		versionStr := r.PathValue("version")

		version, err := strconv.Atoi(versionStr)
		if err != nil || version < 1 {
			writeErr(w, http.StatusBadRequest, "INVALID_VERSION", "version must be a positive integer")
			return
		}

		resp, err := h.svc.GetByVersion(r.Context(), templateID, version)
		if err != nil {
			h.handleServiceError(w, err, "get template version")
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

// createVersion handles POST /v1/templates/{template_id}/versions.
func (h *Handler) createVersion() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		templateID := r.PathValue("template_id")

		var req CreateVersionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
			return
		}

		resp, err := h.svc.CreateVersion(r.Context(), templateID, req)
		if err != nil {
			h.handleServiceError(w, err, "create template version")
			return
		}
		writeJSON(w, http.StatusCreated, resp)
	})
}

// deactivate handles DELETE /v1/templates/{template_id}.
func (h *Handler) deactivate() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		templateID := r.PathValue("template_id")

		if err := h.svc.Deactivate(r.Context(), templateID); err != nil {
			h.handleServiceError(w, err, "deactivate template")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

// handleServiceError maps domain errors to HTTP status codes. Internal errors
// are logged with full detail; callers only see a generic error code.
func (h *Handler) handleServiceError(w http.ResponseWriter, err error, op string) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeErr(w, http.StatusNotFound, "NOT_FOUND", err.Error())
	case errors.Is(err, ErrAlreadyExists):
		writeErr(w, http.StatusConflict, "ALREADY_EXISTS", err.Error())
	case errors.Is(err, ErrInactiveTemplate):
		writeErr(w, http.StatusUnprocessableEntity, "INACTIVE_TEMPLATE", err.Error())
	default:
		// Validation errors from the service layer start with "validation:".
		// Surface them as 400 without leaking internal paths.
		if isValidationError(err) {
			writeErr(w, http.StatusBadRequest, "VALIDATION_FAILED", err.Error())
			return
		}
		h.logger.Error("internal error", zap.String("op", op), zap.Error(err))
		writeErr(w, http.StatusInternalServerError, "INTERNAL_ERROR", "")
	}
}

// isValidationError reports whether err originated from validateCreateRequest
// or validateCreateVersionRequest (they use a "validation:" prefix convention).
func isValidationError(err error) bool {
	if err == nil {
		return false
	}
	const prefix = "validation:"
	msg := err.Error()
	return len(msg) >= len(prefix) && msg[:len(prefix)] == prefix
}

// writeJSON sets Content-Type, writes status, and encodes body as JSON.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// writeErr writes a structured error JSON body.
func writeErr(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, ErrorResponse{Error: code, Message: message})
}
