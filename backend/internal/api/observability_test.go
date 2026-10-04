package api

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestIDAcceptsOnlyBoundedPlainClientValues(t *testing.T) {
	for value, expected := range map[string]string{
		"req-123:abc":            "req-123:abc",
		strings.Repeat("a", 129): "",
		"bad id\nforged":         "",
		"":                       "",
	} {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("X-Request-ID", value)
		if got := requestID(r); got != expected {
			t.Errorf("requestID(%q) = %q, want %q", value, got, expected)
		}
	}
}

func TestFailCauseLogsCauseButReturnsSanitizedMessage(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(previous)
	w := httptest.NewRecorder()
	failCause(w, httptest.NewRequest("GET", "/api/v1/clusters/a/messages", nil), 503, "consume_failed", "Unable to read messages", errors.New("broker 3 unreachable"))
	if w.Code != 503 || strings.Contains(w.Body.String(), "broker 3") {
		t.Fatalf("client response leaked cause: %d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(logs.String(), "broker 3 unreachable") || !strings.Contains(logs.String(), "consume_failed") {
		t.Fatalf("cause not logged: %s", logs.String())
	}
}
