package telemetry

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsExposeMeasuredHTTPRequestsWithoutResourceCardinality(t *testing.T) {
	handler := NewHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "failed") {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(200)
		}
	}), nil)
	server := httptest.NewServer(handler)
	defer server.Close()
	for _, path := range []string{"/api/v1/clusters/a/topics/private-topic", "/api/v1/clusters/b/topics/failed-topic"} {
		response, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	}
	response, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	text := string(body)
	if !strings.Contains(text, "kaflux_http_request_duration_seconds_bucket") || !strings.Contains(text, "kaflux_http_requests_total") {
		t.Fatal("measured counters missing")
	}
	if strings.Contains(text, "private-topic") || strings.Contains(text, "failed-topic") {
		t.Fatal("resource names leaked into metric labels")
	}
}

func TestSSEFlusherAndUnwrapPreserved(t *testing.T) {
	wrapped := &captureWriter{ResponseWriter: httptest.NewRecorder()}
	if _, ok := any(wrapped).(http.Flusher); !ok {
		t.Fatal("stream flush lost")
	}
	if wrapped.Unwrap() == nil {
		t.Fatal("response controller cannot unwrap")
	}
}

func TestMetricsGroupEachFamilyContiguously(t *testing.T) {
	handler := NewHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), func() map[string]int64 { return map[string]int64{"queries": 2} })
	for _, path := range []string{"/health", "/api/v1/clusters", "/ready"} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	seen := map[string]bool{}
	current := ""
	for _, line := range strings.Split(strings.TrimSpace(w.Body.String()), "\n") {
		if strings.HasPrefix(line, "# HELP ") {
			continue
		}
		var family string
		if strings.HasPrefix(line, "# TYPE ") {
			family = strings.Fields(line)[2]
			if seen[family] {
				t.Fatalf("family %s declared twice", family)
			}
			seen[family] = true
			current = family
			continue
		}
		name := strings.FieldsFunc(line, func(r rune) bool { return r == '{' || r == ' ' })[0]
		if !strings.HasPrefix(name, current) {
			t.Fatalf("sample %s outside its family group (current %s)", name, current)
		}
	}
	if !seen["kaflux_prometheus_queries_total"] || !seen["kaflux_http_request_duration_seconds"] {
		t.Fatalf("missing families: %v", seen)
	}
}

func TestPanicsReturnInternalErrorAndAreCounted(t *testing.T) {
	handler := NewHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			panic("boom")
		}
	}), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/clusters", nil))
	if w.Code != http.StatusInternalServerError || !strings.Contains(w.Body.String(), "internal_error") {
		t.Fatalf("panic response = %d %q", w.Code, w.Body.String())
	}
	metrics := httptest.NewRecorder()
	handler.ServeHTTP(metrics, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(metrics.Body.String(), `kaflux_http_requests_total{route="/api/v1/clusters",method="GET",status="500"} 1`) {
		t.Fatal("panicked request not counted as 500")
	}
}
