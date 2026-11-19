package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClusterMetricsCannotSelectAnotherClusterSource(t *testing.T) {
	g, err := NewGateway([]SourceConfig{{ID: "secret-prod", URL: "https://metrics.example.com", Kind: "prometheus", ClusterID: "prod"}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	request := httptest.NewRequest("GET", "/query?metric=brokers&source=secret-prod", nil)
	response := httptest.NewRecorder()
	g.HandlerForCluster("dev").ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatalf("unexpected %d", response.Code)
	}
	if !strings.Contains(response.Body.String(), "unavailable") {
		t.Fatal("cross-cluster metrics were allowed")
	}
	var data map[string]any
	if err = json.Unmarshal(response.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	if g.Stats()["queries"] != 0 {
		t.Fatal("unauthorized source queried")
	}
}

func TestClusterMetricsPreferClusterPrometheus(t *testing.T) {
	called := make(chan string, 2)
	server := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called <- name
			w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		}))
	}
	global := server("global")
	defer global.Close()
	cluster := server("cluster")
	defer cluster.Close()

	g, err := NewGateway([]SourceConfig{
		{ID: "prometheus", ClusterID: "dev", URL: global.URL, Kind: "prometheus"},
		{ID: "prometheus-dev", ClusterID: "dev", URL: cluster.URL, Kind: "prometheus", PreferForCluster: true},
	}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	request := httptest.NewRequest("GET", "/query?metric=brokers", nil)
	response := httptest.NewRecorder()
	g.HandlerForCluster("dev").ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected response %d: %s", response.Code, response.Body)
	}
	select {
	case source := <-called:
		if source != "cluster" {
			t.Fatalf("queried %s Prometheus, want cluster Prometheus", source)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no Prometheus request received")
	}
}
