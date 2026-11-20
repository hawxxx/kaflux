package mcp

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

func TestDoRefusesPlaceholderInjection(t *testing.T) {
	var hits atomic.Int32
	d := NewDispatcher(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); jsonHandler(200, `{}`)(w, r) }), Limits{})
	for _, c := range []Call{
		{Cluster: "{topic}", Args: map[string]any{"topic": "prod"}},
		{Args: map[string]any{"cluster": "{topic}", "topic": "prod"}},
		{Cluster: "prod", Args: map[string]any{"topic": "a}b"}},
	} {
		if _, e := d.Do(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_get"), c); e == nil || e.Code != "invalid_argument" {
			t.Fatalf("%+v: want invalid_argument, got %+v", c, e)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("handler reached %d times", hits.Load())
	}
}

func TestWriteFailureHintsWarnOfApplication(t *testing.T) {
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	read, write := mustTool(t, "kaflux_topic_get"), mustTool(t, "kaflux_topic_delete")
	readCall := Call{Cluster: "local", Args: map[string]any{"topic": "orders"}}
	writeCall := Call{Cluster: "local", Args: map[string]any{"topic": "orders", "body": map[string]any{"confirmation": "orders"}}}
	for _, h := range []http.Handler{slow, jsonHandler(503, `{"error":{"code":"broker_unavailable","message":"Unavailable"}}`)} {
		d := NewDispatcher(h, Limits{Timeout: 50 * time.Millisecond})
		_, e := d.Do(context.Background(), testPrincipal(), read, readCall)
		if e == nil || !strings.Contains(e.Hint, "retry") || strings.Contains(e.Hint, "may have been applied") {
			t.Fatalf("read hint %+v", e)
		}
		_, e = d.Do(context.Background(), testPrincipal(), write, writeCall)
		if e == nil || !strings.Contains(e.Hint, "may have been applied") || !strings.Contains(e.Hint, "before retrying") {
			t.Fatalf("write hint %+v", e)
		}
		if e.Code == "timeout" && !strings.Contains(e.Message, "may have been applied") {
			t.Fatalf("write timeout message %q", e.Message)
		}
	}
}

func TestDoRejectsMissingTokenID(t *testing.T) {
	var hits atomic.Int32
	d := NewDispatcher(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); jsonHandler(200, `{}`)(w, r) }), Limits{})
	p := testPrincipal()
	p.TokenID = ""
	if _, e := d.Do(context.Background(), p, mustTool(t, "kaflux_topic_get"), Call{Cluster: "local", Args: map[string]any{"topic": "orders"}}); e == nil || e.Code != "internal" || e.Status != 500 {
		t.Fatalf("want internal, got %+v", e)
	}
	if hits.Load() != 0 {
		t.Fatal("handler reached")
	}
}

func TestConcurrencyRefusalKeepsRateToken(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var block atomic.Bool
	block.Store(true)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if block.Load() {
			entered <- struct{}{}
			<-release
		}
		jsonHandler(200, `{"data":{}}`)(w, r)
	})
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	d := NewDispatcher(h, Limits{PerMinute: 2, Concurrency: 1})
	d.now = func() time.Time { return now }
	tool, call, p := mustTool(t, "kaflux_topic_get"), Call{Cluster: "local", Args: map[string]any{"topic": "orders"}}, testPrincipal()
	done := make(chan struct{})
	go func() { _, _ = d.Do(context.Background(), p, tool, call); close(done) }()
	<-entered
	for i := 0; i < 3; i++ {
		if _, e := d.Do(context.Background(), p, tool, call); e == nil || !strings.Contains(e.Hint, "concurrent") {
			t.Fatalf("want concurrency refusal, got %+v", e)
		}
	}
	block.Store(false)
	close(release)
	<-done
	// The refused calls spent nothing: one of the two tokens is still left.
	if _, e := d.Do(context.Background(), p, tool, call); e != nil {
		t.Fatalf("rate token spent by refused calls: %+v", e)
	}
	if _, e := d.Do(context.Background(), p, tool, call); e == nil || e.Code != "rate_limited" {
		t.Fatalf("want rate_limited, got %+v", e)
	}
}

func TestRetryHintAtLeastOneSecond(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	d := NewDispatcher(jsonHandler(200, `{"data":{}}`), Limits{PerMinute: 6000})
	d.now = func() time.Time { return now }
	tool, call, p := mustTool(t, "kaflux_topic_get"), Call{Cluster: "local", Args: map[string]any{"topic": "orders"}}, testPrincipal()
	var e *ToolError
	for i := 0; i < 6001 && e == nil; i++ {
		_, e = d.Do(context.Background(), p, tool, call)
	}
	if e == nil || !strings.Contains(e.Hint, "retry after 1s") {
		t.Fatalf("hint %+v", e)
	}
}

func TestErrorMessageCutOnRuneBoundary(t *testing.T) {
	msg := strings.Repeat("a", 511) + strings.Repeat("é", 10)
	d := NewDispatcher(jsonHandler(400, `{"error":{"code":"invalid_request","message":"`+msg+`"}}`), Limits{})
	_, e := d.Do(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_get"), Call{Cluster: "local", Args: map[string]any{"topic": "orders"}})
	if e == nil || len(e.Message) > 512 || !utf8.ValidString(e.Message) {
		t.Fatalf("message %d bytes valid %v", len(e.Message), e != nil && utf8.ValidString(e.Message))
	}
}
