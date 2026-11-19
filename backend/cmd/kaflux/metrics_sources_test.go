package main

import (
	"encoding/json"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/config"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
)

func TestClusterMetricSource(t *testing.T) {
	// Same path as startup: config cluster to JSON to the provider config.
	b, err := json.Marshal(config.Cluster{ID: "perf", PrometheusURL: "http://prometheus.perf:9090"})
	if err != nil {
		t.Fatal(err)
	}
	var c kafka.Config
	if err = json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	m, ok := clusterMetricSource(c)
	if !ok || m.ID != "prometheus-perf" || m.ClusterID != "perf" || m.URL != "http://prometheus.perf:9090" || m.Kind != "prometheus" || !m.PreferForCluster {
		t.Fatalf("unexpected source %+v ok=%v", m, ok)
	}
	if _, ok = clusterMetricSource(kafka.Config{ID: "int"}); ok {
		t.Fatal("a cluster without a URL got a source")
	}
}
