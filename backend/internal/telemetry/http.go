package telemetry

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
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
	count, errors uint64
	sum           float64
	buckets       [8]uint64
}

var bounds = [8]float64{.005, .01, .025, .05, .1, .25, 1, 10}

type HTTP struct {
	mu           sync.Mutex
	observations map[string]*observation
	next         http.Handler
	gatewayStats func() map[string]int64
}

func NewHTTP(next http.Handler, gatewayStats func() map[string]int64) *HTTP {
	traced := otelhttp.NewHandler(next, "kaflux.http", otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method + " " + route(r.URL.Path) }))
	return &HTTP{observations: map[string]*observation{}, next: traced, gatewayStats: gatewayStats}
}
func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/metrics" && r.Method == "GET" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		h.next.ServeHTTP(w, r)
		h.expose(w)
		return
	}
	start := time.Now()
	writer := &captureWriter{ResponseWriter: w}
	h.next.ServeHTTP(writer, r)
	elapsed := time.Since(start).Seconds()
	status := writer.status
	if status == 0 {
		status = 200
	}
	method := r.Method
	switch method {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD":
	default:
		method = "OTHER"
	}
	key := fmt.Sprintf("route=%q,method=%q,status=%q", route(r.URL.Path), method, strconv.Itoa(status))
	h.mu.Lock()
	entry := h.observations[key]
	if entry == nil {
		entry = &observation{}
		h.observations[key] = entry
	}
	entry.count++
	entry.sum += elapsed
	if status >= 500 {
		entry.errors++
	}
	for i, bound := range bounds {
		if elapsed <= bound {
			entry.buckets[i]++
		}
	}
	h.mu.Unlock()
	if status >= 500 {
		slog.Warn("request failed", "route", route(r.URL.Path), "method", method, "status", status, "durationSeconds", elapsed)
	}
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
func (h *HTTP) expose(w http.ResponseWriter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Fprintln(w, "# HELP kaflux_http_requests_total Completed HTTP requests.")
	fmt.Fprintln(w, "# TYPE kaflux_http_requests_total counter")
	fmt.Fprintln(w, "# TYPE kaflux_http_request_duration_seconds histogram")
	for labels, e := range h.observations {
		fmt.Fprintf(w, "kaflux_http_requests_total{%s} %d\n", labels, e.count)
		fmt.Fprintf(w, "kaflux_http_errors_total{%s} %d\n", labels, e.errors)
		for i, bound := range bounds {
			fmt.Fprintf(w, "kaflux_http_request_duration_seconds_bucket{%s,le=%q} %d\n", labels, strconv.FormatFloat(bound, 'f', -1, 64), e.buckets[i])
		}
		fmt.Fprintf(w, "kaflux_http_request_duration_seconds_bucket{%s,le=\"+Inf\"} %d\n", labels, e.count)
		fmt.Fprintf(w, "kaflux_http_request_duration_seconds_sum{%s} %g\n", labels, e.sum)
		fmt.Fprintf(w, "kaflux_http_request_duration_seconds_count{%s} %d\n", labels, e.count)
	}
	if h.gatewayStats != nil {
		stats := h.gatewayStats()
		fmt.Fprintf(w, "kaflux_prometheus_queries_total %d\nkaflux_prometheus_cache_hits_total %d\nkaflux_prometheus_query_errors_total %d\n", stats["queries"], stats["cacheHits"], stats["errors"])
	}
}
