package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGatewayCoalescesAndDoesNotBlockSnapshots(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"b-1"},"values":[[100,"3"]]}]}}`)
	}))
	defer server.Close()
	g, err := NewGateway([]SourceConfig{{ID: "prometheus", URL: server.URL, Kind: "prometheus"}}, Options{TTL: time.Minute, Timeout: time.Second, Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	q := Query{Source: "prometheus", Expression: "up", Start: 100, End: 200, Step: 10}
	start := time.Now()
	for i := 0; i < 100; i++ {
		if r := g.Snapshot(q); r.Status != "pending" {
			t.Fatalf("expected pending, got %s", r.Status)
		}
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("snapshot blocked on remote metrics")
	}
	close(release)
	r, err := g.Wait(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("coalescing failed: %d requests", calls.Load())
	}
	if len(r.Series) != 1 || r.Series[0].Points[0].Value != 3 {
		t.Fatalf("bad values: %+v", r)
	}
	if !g.Snapshot(q).Cached {
		t.Fatal("fresh snapshot should be cached")
	}
}

func TestUnavailableSourceAndResponseLimits(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, strings.Repeat("x", 4096))
	}))
	defer server.Close()
	g, err := NewGateway([]SourceConfig{{ID: "p", URL: server.URL, Kind: "prometheus"}}, Options{MaxResponseBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if got := g.Snapshot(Query{Source: "missing", Expression: "up"}); got.Status != "unavailable" {
		t.Fatalf("missing source %+v", got)
	}
	_, err = g.Wait(context.Background(), Query{Source: "p", Expression: "up", Start: 100, End: 200, Step: 10})
	if err == nil {
		t.Fatal("oversized response accepted")
	}
}

func TestImportDashboardSanitizesCloudWatchAndKeepsPromQL(t *testing.T) {
	dashboard := `{"dashboard":{"title":"Kafka","panels":[{"id":1,"title":"Size","type":"stat","fieldConfig":{"defaults":{"unit":"decbytes"}},"datasource":{"type":"prometheus","uid":"private-id"},"targets":[{"expr":"sum(kafka_log_Log_Value{instance=~\"$instance\",name=\"Size\"})","refId":"A"}]},{"id":2,"title":"AWS","datasource":{"type":"cloudwatch"},"targets":[{"namespace":"AWS/Kafka","metricName":"UnderProvisioned","dimensions":{"Cluster Name":["private-cluster"]}}]}]}}`
	catalog, err := ImportDashboard(strings.NewReader(dashboard))
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog) != 2 || catalog[0].Unit != "decbytes" {
		t.Fatalf("invalid catalog %+v", catalog)
	}
	if !strings.Contains(catalog[0].Expression, "$instance") {
		t.Fatal("PromQL lost")
	}
	if catalog[1].CloudWatch.Dimensions["Cluster Name"] != "$cluster" {
		t.Fatal("private cluster retained")
	}
}

func TestTemplateEscapesPromQLAndBoundsRanges(t *testing.T) {
	expression, err := Expand(`rate(metric{instance=~"$instance"}[$__range])`, `b-1"} or up{foo="`, "15m")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(expression, `\"`) {
		t.Fatal("instance not quoted safely")
	}
	if _, err := Expand("up", "x", "999h"); err == nil {
		t.Fatal("unbounded range accepted")
	}
	if _, err := NewGateway([]SourceConfig{{ID: "p", URL: "file:///etc/passwd"}}, Options{}); err == nil {
		t.Fatal("invalid scheme accepted")
	}
}

func TestCatalogCoversImportedTargetsWithoutPrivateBindings(t *testing.T) {
	defs := DefaultCatalog()
	if len(defs) != 173 {
		t.Fatalf("expected every imported target, got %d", len(defs))
	}
	unique := map[string]bool{}
	for _, def := range defs {
		if unique[def.ID] {
			t.Fatalf("duplicate ID %s", def.ID)
		}
		unique[def.ID] = true
	}
	encoded, _ := json.Marshal(defs)
	if strings.Contains(string(encoded), "tau-prod") || strings.Contains(string(encoded), "DS_MSK-MIGRATION") {
		t.Fatal("private binding retained")
	}
}

func TestDeadlineAndBoundedCache(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(time.Second):
			fmt.Fprint(w, `{"status":"success","data":{"result":[]}}`)
		}
	}))
	defer server.Close()
	g, err := NewGateway([]SourceConfig{{ID: "p", URL: server.URL}}, Options{MaxEntries: 2, Timeout: 20 * time.Millisecond, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = g.Wait(ctx, Query{Source: "p", Expression: fmt.Sprintf("metric_%d", i), Start: 100, End: 200, Step: 10})
		cancel()
		if err == nil {
			t.Fatal("deadline not enforced")
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.entries) > 2 {
		t.Fatalf("cache unbounded: %d", len(g.entries))
	}
}

func TestNextWindowServesPreviousSamplesWhileRefreshing(t *testing.T) {
	release := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"b-1"},"values":[[100,"3"]]}]}}`)
	}))
	defer server.Close()
	g, err := NewGateway([]SourceConfig{{ID: "prometheus", URL: server.URL, Kind: "prometheus"}}, Options{TTL: time.Minute, Timeout: time.Second, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	first := Query{Source: "prometheus", Expression: "up", Start: 100, End: 200, Step: 10}
	release <- struct{}{}
	if _, err := g.Wait(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	next := first
	next.Start, next.End = 115, 215
	r := g.Snapshot(next)
	if r.Status != "available" || !r.Stale || len(r.Series) != 1 {
		t.Fatalf("next window should serve the previous samples as stale, got %+v", r)
	}
	if other := g.Snapshot(Query{Source: "prometheus", Expression: "down", Start: 115, End: 215, Step: 10}); other.Status != "pending" {
		t.Fatalf("a different expression must not reuse samples, got %s", other.Status)
	}
	release <- struct{}{}
	release <- struct{}{}
	if r, err := g.Wait(context.Background(), next); err != nil || r.Stale {
		t.Fatalf("refreshed window should be fresh, got %+v %v", r, err)
	}
}
