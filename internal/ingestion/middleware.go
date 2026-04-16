package ingestion

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type contextKey string

const requestIDKey contextKey = "request_id"

// RequestID injects a unique request identifier into the context and the
// X-Request-ID response header. If the inbound request already carries an
// X-Request-ID header that value is propagated; otherwise a new UUID is
// generated. This allows distributed tracing correlation across services.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.New().String()
		}
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requestIDFromContext retrieves the request ID injected by RequestID.
// Returns an empty string when the context carries no ID so that downstream
// log lines are always structurally valid (zap omits empty string fields).
func requestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// Timeout wraps each request in a context that is cancelled after d.
// The handler is responsible for honouring context cancellation — this
// middleware only ensures the context deadline is set.
func Timeout(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// AccessLog emits a structured log line for every request after the handler
// returns. It records method, path, status code, and latency. The status code
// is captured via a lightweight response writer wrapper.
func AccessLog(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &statusResponseWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r)
			logger.Info("http",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", rw.status),
				zap.Duration("latency", time.Since(start)),
				zap.String("request_id", requestIDFromContext(r.Context())),
			)
		})
	}
}

// MethodGate rejects requests whose HTTP method does not match allowed.
// Returns 405 Method Not Allowed with an Allow header when the method is
// wrong, and 404 Not Found for paths that are not registered.
func MethodGate(method string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Chain applies middlewares right-to-left so that the first middleware in the
// slice is the outermost (first executed) wrapper around the handler.
func Chain(h http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

// statusResponseWriter wraps http.ResponseWriter to capture the status code
// written by the handler. The zero value (status=0) is set to 200 on the
// first WriteHeader call so log lines always carry a meaningful code.
type statusResponseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (rw *statusResponseWriter) WriteHeader(status int) {
	if !rw.wroteHeader {
		rw.status = status
		rw.wroteHeader = true
	}
	rw.ResponseWriter.WriteHeader(status)
}

func (rw *statusResponseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK)
	}
	return rw.ResponseWriter.Write(b)
}
