package telemetry

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
)

type captureWriter struct {
	http.ResponseWriter
	status int
}

func (w *captureWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *captureWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(body)
}
func (w *captureWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func (w *captureWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type observation struct {
	count   uint64
	sum     float64
	buckets [8]uint64
}

var bounds = [8]float64{.005, .01, .025, .05, .1, .25, 1, 10}

type HTTP struct {
	mu           sync.Mutex
	observations map[string]*observation
	next         http.Handler
	traced       http.Handler
	gatewayStats func() map[string]int64
}

func NewHTTP(next http.Handler, gatewayStats func() map[string]int64) *HTTP {
	h := &HTTP{observations: map[string]*observation{}, next: next, gatewayStats: gatewayStats}
	// Observe inside the tracing handler so logs can carry the request's trace ID.
	h.traced = otelhttp.NewHandler(http.HandlerFunc(h.observe), "kaflux.http", otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method + " " + route(r.URL.Path) }))
	return h
}
func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/metrics" && r.Method == "GET" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		h.next.ServeHTTP(w, r)
		h.expose(w)
		return
	}
	h.traced.ServeHTTP(w, r)
}
func (h *HTTP) observe(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	writer := &captureWriter{ResponseWriter: w}
	method := r.Method
	switch method {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD":
	default:
		method = "OTHER"
	}
	path := route(r.URL.Path)
	defer func() {
		recovered := recover()
		if recovered == http.ErrAbortHandler {
			panic(recovered)
		}
		if recovered != nil {
			slog.Error("request panicked", append(traceAttrs(r), "route", path, "method", method, "panic", fmt.Sprint(recovered), "stack", string(debug.Stack()))...)
			if writer.status == 0 {
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(http.StatusInternalServerError)
				_, _ = writer.Write([]byte(`{"error":{"code":"internal_error","message":"Internal server error"}}` + "\n"))
			}
		}
		h.record(r, writer.status, path, method, time.Since(start).Seconds())
	}()
	h.next.ServeHTTP(writer, r)
}
func (h *HTTP) record(r *http.Request, status int, path, method string, elapsed float64) {
	if status == 0 {
		status = 200
	}
	key := fmt.Sprintf("route=%q,method=%q,status=%q", path, method, strconv.Itoa(status))
	h.mu.Lock()
	entry := h.observations[key]
	if entry == nil {
		entry = &observation{}
		h.observations[key] = entry
	}
	entry.count++
	entry.sum += elapsed
	for i, bound := range bounds {
		if elapsed <= bound {
			entry.buckets[i]++
		}
	}
	h.mu.Unlock()
	if status >= 500 {
		slog.Warn("server error response", append(traceAttrs(r), "route", path, "method", method, "status", status, "durationSeconds", elapsed)...)
	}
}
func traceAttrs(r *http.Request) []any {
	span := trace.SpanContextFromContext(r.Context())
	if !span.IsValid() {
		return nil
	}
	return []any{"trace_id", span.TraceID().String(), "span_id", span.SpanID().String()}
}
func route(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 1 {
		switch parts[0] {
		case "health", "ready", "metrics":
			return "/" + parts[0]
		}
	}
	if len(parts) >= 3 && parts[0] == "api" && parts[1] == "v1" {
		if parts[2] == "auth" {
			return "/api/v1/auth"
		}
		if parts[2] == "clusters" {
			if len(parts) >= 5 {
				switch parts[4] {
				case "topics", "brokers", "consumer-groups", "messages", "balance", "rebalances", "audit", "metrics", "capabilities", "configuration":
					return "/api/v1/clusters/:id/" + parts[4]
				}
			}
			return "/api/v1/clusters"
		}
	}
	return "other"
}

// expose writes each metric family as one contiguous, sorted group as the
// Prometheus text format requires.
func (h *HTTP) expose(w http.ResponseWriter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	keys := make([]string, 0, len(h.observations))
	for labels := range h.observations {
		keys = append(keys, labels)
	}
	sort.Strings(keys)
	fmt.Fprintln(w, "# HELP kaflux_http_requests_total Completed HTTP requests.")
	fmt.Fprintln(w, "# TYPE kaflux_http_requests_total counter")
	for _, labels := range keys {
		fmt.Fprintf(w, "kaflux_http_requests_total{%s} %d\n", labels, h.observations[labels].count)
	}
	fmt.Fprintln(w, "# HELP kaflux_http_request_duration_seconds HTTP request latency.")
	fmt.Fprintln(w, "# TYPE kaflux_http_request_duration_seconds histogram")
	for _, labels := range keys {
		e := h.observations[labels]
		for i, bound := range bounds {
			fmt.Fprintf(w, "kaflux_http_request_duration_seconds_bucket{%s,le=%q} %d\n", labels, strconv.FormatFloat(bound, 'f', -1, 64), e.buckets[i])
		}
		fmt.Fprintf(w, "kaflux_http_request_duration_seconds_bucket{%s,le=\"+Inf\"} %d\n", labels, e.count)
		fmt.Fprintf(w, "kaflux_http_request_duration_seconds_sum{%s} %g\n", labels, e.sum)
		fmt.Fprintf(w, "kaflux_http_request_duration_seconds_count{%s} %d\n", labels, e.count)
	}
	if h.gatewayStats != nil {
		stats := h.gatewayStats()
		for _, metric := range []struct{ name, key, help string }{
			{"kaflux_prometheus_queries_total", "queries", "Prometheus queries issued."},
			{"kaflux_prometheus_cache_hits_total", "cacheHits", "Prometheus queries served from cache."},
			{"kaflux_prometheus_query_errors_total", "errors", "Failed Prometheus queries."},
		} {
			fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", metric.name, metric.help, metric.name, metric.name, stats[metric.key])
		}
	}
}
