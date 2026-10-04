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
