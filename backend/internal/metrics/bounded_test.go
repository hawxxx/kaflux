package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Per-topic panels on clusters with thousands of topics must be bounded in PromQL, not
// rejected after the datasource has already returned every series.
func TestHandlerBoundsMultiSeriesQueries(t *testing.T) {
	var mu sync.Mutex
	var seen string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = r.URL.Query().Get("query")
		mu.Unlock()
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"topic":"a"},"values":[[100,"5"]]}]}}`)
	}))
	defer server.Close()
	catalog := []Definition{
		{ID: "per-topic", Kind: "prometheus", Legend: "{{topic}}", Expression: `sum by (topic) (kafka_server_BrokerTopicMetrics_OneMinuteRate{instance=~"$instance",name="MessagesInPerSec",topic!=""})`},
		{ID: "single", Kind: "prometheus", Expression: `sum(kafka_server_BrokerTopicMetrics_OneMinuteRate{instance=~"$instance",name="MessagesInPerSec",topic=""})`},
	}
	g, err := NewGateway([]SourceConfig{{ID: "prometheus", ClusterID: "c1", URL: server.URL}}, Options{TTL: time.Minute, Catalog: catalog})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	get := func(metric string) Result {
		t.Helper()
		var body struct{ Data Result }
		for i := 0; i < 50; i++ {
			rec := httptest.NewRecorder()
			g.HandlerForCluster("c1").ServeHTTP(rec, httptest.NewRequest("GET", "/query?metric="+metric+"&range=1h", nil))
			if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Data.Status != "pending" {
				return body.Data
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("%s never resolved", metric)
		return Result{}
	}

	r := get("per-topic")
	mu.Lock()
	q := seen
	mu.Unlock()
	if r.Status != "available" || r.SeriesLimit != 50 {
		t.Fatalf("bounded result %+v", r)
	}
	if !strings.Contains(q, "topk(50, avg_over_time((") || !strings.Contains(q, "[1h:15s] @ end())") {
		t.Fatalf("per-topic query not bounded: %s", q)
	}

	r = get("single")
	mu.Lock()
	q = seen
	mu.Unlock()
	if r.SeriesLimit != 0 || strings.Contains(q, "topk(") {
		t.Fatalf("single-series query should not be rewritten: %s", q)
	}
}

// A query that returns too many series is a query-shape problem. It must report why and
// must not open the circuit breaker for every other panel on the same datasource.
func TestSeriesLimitDoesNotOpenCircuit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("query") == "wide" {
			var b strings.Builder
			b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
			for i := 0; i < 5; i++ {
				if i > 0 {
					b.WriteString(",")
				}
				fmt.Fprintf(&b, `{"metric":{"topic":"t%d"},"values":[[100,"1"]]}`, i)
			}
			b.WriteString(`]}}`)
			fmt.Fprint(w, b.String())
			return
		}
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[100,"1"]]}]}}`)
	}))
	defer server.Close()
	g, err := NewGateway([]SourceConfig{{ID: "p", URL: server.URL}}, Options{MaxSeries: 2, TTL: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	for i := 0; i < 5; i++ {
		r, err := g.Wait(context.Background(), Query{Source: "p", Expression: "wide", Start: 100, End: 200 + int64(i), Step: 10})
		if err == nil || !strings.Contains(r.Error, "5 series, limit is 2") {
			t.Fatalf("expected series limit error, got %+v %v", r, err)
		}
	}
	if r, err := g.Wait(context.Background(), Query{Source: "p", Expression: "narrow", Start: 100, End: 200, Step: 10}); err != nil || r.Status != "available" {
		t.Fatalf("healthy query blocked after series limit errors: %+v %v", r, err)
	}
}

func TestRejectedQuerySurfacesDatasourceError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"status":"error","errorType":"bad_data","error":"parse error: unexpected end of input"}`)
	}))
	defer server.Close()
	g, err := NewGateway([]SourceConfig{{ID: "p", URL: server.URL}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	r, err := g.Wait(context.Background(), Query{Source: "p", Expression: "sum(", Start: 100, End: 200, Step: 10})
	if err == nil || !strings.Contains(r.Error, "HTTP 400") || !strings.Contains(r.Error, "parse error") {
		t.Fatalf("datasource error not surfaced: %+v", r)
	}
}

func TestSplitsByEntity(t *testing.T) {
	for legend, want := range map[string]bool{
		"":                             false,
		"BytesInPerSec":                false,
		"{{jmx_version}}":              false, // single-value tiles such as topic or broker count
		"{{instance}}":                 false, // one series per broker
		"{{ instance }}":               false,
		"{{instance}} {{jmx_version}}": false,
		"{{topic}}":                    true,
		"{{groupId}} / {{topic}}":      true,
		"{{listener}} {{instance}}":    true, // a label that is not per broker
		"{{instance}} {{device}}":      true,
		"{{request}}":                  true,
		"topic: {{ topic }}":           true,
	} {
		if got := splitsByEntity(legend); got != want {
			t.Errorf("splitsByEntity(%q) = %v, want %v", legend, got, want)
		}
	}
}

// Single-value tiles and per-broker charts return few series, so they must reach the datasource
// unchanged. Wrapping a cheap count in a subquery made an expensive query out of it.
func TestOnlyEntityLegendsAreRewrittenByTheHandler(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		mu.Lock()
		seen[q] = q
		mu.Unlock()
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[100,"5"]]}]}}`)
	}))
	defer server.Close()
	expr := func(name string) string {
		return `sum(kafka_server_BrokerTopicMetrics_OneMinuteRate{instance=~"$instance",name="` + name + `"})`
	}
	catalog := []Definition{
		{ID: "tile", Kind: "prometheus", Legend: "{{jmx_version}}", Expression: expr("MarkerTile")},
		{ID: "per-broker", Kind: "prometheus", Legend: "{{instance}}", Expression: expr("MarkerBroker")},
		{ID: "per-topic", Kind: "prometheus", Legend: "{{topic}}", Expression: expr("MarkerTopic")},
	}
	g, err := NewGateway([]SourceConfig{{ID: "prometheus", ClusterID: "c1", URL: server.URL}}, Options{TTL: time.Minute, Catalog: catalog})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	for _, id := range []string{"tile", "per-broker", "per-topic"} {
		for i := 0; i < 50; i++ {
			rec := httptest.NewRecorder()
			g.HandlerForCluster("c1").ServeHTTP(rec, httptest.NewRequest("GET", "/query?metric="+id+"&range=1h", nil))
			var body struct{ Data Result }
			if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Data.Status != "pending" {
				wantLimit := 0
				if id == "per-topic" {
					wantLimit = 50
				}
				if body.Data.SeriesLimit != wantLimit {
					t.Errorf("%s: seriesLimit = %d, want %d", id, body.Data.SeriesLimit, wantLimit)
				}
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for q := range seen {
		bounded := strings.Contains(q, "topk(")
		isTopic := strings.Contains(q, "MarkerTopic")
		if bounded != isTopic {
			t.Errorf("bounded=%v for query %q", bounded, q)
		}
	}
	if len(seen) != 3 {
		t.Errorf("expected three distinct queries, saw %d", len(seen))
	}
}
