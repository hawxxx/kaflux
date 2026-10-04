package metrics

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
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
