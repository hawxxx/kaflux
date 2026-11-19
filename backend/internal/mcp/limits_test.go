package mcp

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRateLimit429(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	d := NewDispatcher(jsonHandler(200, `{"data":{}}`), Limits{PerMinute: 2})
	d.now = func() time.Time { return now }
	tool := mustTool(t, "kaflux_topic_get")
	call := Call{Cluster: "local", Args: map[string]any{"topic": "orders"}}
	p := testPrincipal()
	for i := 0; i < 2; i++ {
		if _, e := d.Do(context.Background(), p, tool, call); e != nil {
			t.Fatalf("call %d: %+v", i, e)
		}
	}
	_, e := d.Do(context.Background(), p, tool, call)
	if e == nil || e.Code != "rate_limited" || e.Status != 429 || !strings.Contains(e.Hint, "retry after 30s") {
		t.Fatalf("want rate_limited, got %+v", e)
	}
	other := p
	other.TokenID, other.TokenName = "tok-2", "other"
	if _, e := d.Do(context.Background(), other, tool, call); e != nil {
		t.Fatalf("other token limited: %+v", e)
	}
	now = now.Add(30 * time.Second)
	if _, e := d.Do(context.Background(), p, tool, call); e != nil {
		t.Fatalf("after refill: %+v", e)
	}
	if _, e := d.Do(context.Background(), p, tool, call); e == nil || e.Code != "rate_limited" {
		t.Fatalf("bucket refilled too fast: %+v", e)
	}
}

func TestConcurrencyCap(t *testing.T) {
	entered, release := make(chan struct{}, 4), make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		jsonHandler(200, `{"data":{}}`)(w, r)
	})
	d := NewDispatcher(h, Limits{Concurrency: 2})
	tool := mustTool(t, "kaflux_topic_get")
	call := Call{Cluster: "local", Args: map[string]any{"topic": "orders"}}
	p := testPrincipal()
	done := make(chan *ToolError, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, e := d.Do(context.Background(), p, tool, call)
			done <- e
		}()
	}
	<-entered
	<-entered
	_, e := d.Do(context.Background(), p, tool, call)
	if e == nil || e.Code != "rate_limited" || e.Status != 429 || !strings.Contains(e.Hint, "concurrent") {
		t.Fatalf("want concurrency refusal, got %+v", e)
	}
	other := p
	other.TokenID = "tok-2"
	go func() { _, _ = d.Do(context.Background(), other, tool, call) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("other token blocked by first token's cap")
	}
	close(release)
	for i := 0; i < 2; i++ {
		if e := <-done; e != nil {
			t.Fatalf("held call: %+v", e)
		}
	}
	if _, e := d.Do(context.Background(), p, tool, call); e != nil {
		t.Fatalf("slot not released: %+v", e)
	}
}
