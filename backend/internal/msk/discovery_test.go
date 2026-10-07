package msk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/awsutil"
)

// noopSign is a fake awsutil.RequestSigner for tests that exercise matching
// logic and should never need real AWS credentials.
func noopSign(context.Context, *http.Request, []byte) error { return nil }

func TestParseBootstrapHostReadsNameAndGeneration(t *testing.T) {
	for _, test := range []struct {
		seed string
		ok   bool
		name string
		gen  int
	}{
		{"b-1.examplecluster.abc123.c2.kafka.us-west-2.amazonaws.com:9094", true, "examplecluster", 2},
		{"b-1.examplecluster.abc123.c2.kafka.us-west-2.amazonaws.com", true, "examplecluster", 2},
		{"b-2.examplecluster.abc123.c26.kafka.us-west-2.amazonaws.com.cn:9094", true, "examplecluster", 26},
		{"kafka-1.example.internal:9092", false, "", 0},
		{"b-1.examplecluster.abc123.kafka.us-west-2.amazonaws.com", false, "", 0},    // missing cN label
		{"b-1.examplecluster.abc123.x2.kafka.us-west-2.amazonaws.com", false, "", 0}, // not a cN label
		{"b-1.example-serverless.abc123.kafka-serverless.us-west-2.amazonaws.com", false, "", 0},
	} {
		labels, ok := parseBootstrapHost(test.seed)
		if ok != test.ok {
			t.Errorf("%s: ok = %v, want %v", test.seed, ok, test.ok)
			continue
		}
		if !ok {
			continue
		}
		if labels.NameNoDashes != test.name || labels.Generation != test.gen {
			t.Errorf("%s: got name=%q generation=%d, want name=%q generation=%d", test.seed, labels.NameNoDashes, labels.Generation, test.name, test.gen)
		}
	}
}

// discoveryServer builds an httptest server that serves ListClustersV2 pages
// from the given clusters and otherwise fails the test, plus a Discoverer
// wired to it with a no-op signer so tests exercise only the matching logic.
func discoveryServer(t *testing.T, seed string, clusters []struct{ name, arn string }, status int) (*Discoverer, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		type info struct {
			ClusterArn  string `json:"clusterArn"`
			ClusterName string `json:"clusterName"`
		}
		list := make([]info, len(clusters))
		for i, c := range clusters {
			list[i] = info{ClusterArn: c.arn, ClusterName: c.name}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(mustJSON(t, map[string]any{"clusterInfoList": list}))
	}))
	t.Cleanup(server.Close)
	labels, ok := parseBootstrapHost(seed)
	if !ok {
		t.Fatalf("test seed %q must parse as an MSK bootstrap host", seed)
	}
	d := &Discoverer{
		labels:   labels,
		endpoint: server.URL,
		client:   server.Client(),
		sign:     func(context.Context) (awsutil.RequestSigner, error) { return noopSign, nil },
	}
	return d, server
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDiscoverUniqueMatch(t *testing.T) {
	seed := "b-1.examplecluster.abc123.c2.kafka.us-west-2.amazonaws.com:9094"
	d, _ := discoveryServer(t, seed, []struct{ name, arn string }{
		{"example-cluster", "arn:aws:kafka:us-west-2:000000000000:cluster/example-cluster/11111111-1111-1111-1111-111111111111-2"},
		{"other-cluster", "arn:aws:kafka:us-west-2:000000000000:cluster/other-cluster/22222222-2222-2222-2222-222222222222-2"},
	}, 0)
	arn, reason, err := d.discover(context.Background())
	if err != nil || reason != "" || arn != "arn:aws:kafka:us-west-2:000000000000:cluster/example-cluster/11111111-1111-1111-1111-111111111111-2" {
		t.Fatalf("arn=%q reason=%q err=%v", arn, reason, err)
	}
}

func TestDiscoverNoMatch(t *testing.T) {
	seed := "b-1.examplecluster.abc123.c2.kafka.us-west-2.amazonaws.com:9094"
	d, _ := discoveryServer(t, seed, []struct{ name, arn string }{
		{"other-cluster", "arn:aws:kafka:us-west-2:000000000000:cluster/other-cluster/22222222-2222-2222-2222-222222222222-2"},
	}, 0)
	arn, reason, err := d.discover(context.Background())
	if err != nil || arn != "" || reason == "" {
		t.Fatalf("arn=%q reason=%q err=%v", arn, reason, err)
	}
}

func TestDiscoverAmbiguousMatch(t *testing.T) {
	seed := "b-1.examplecluster.abc123.c2.kafka.us-west-2.amazonaws.com:9094"
	d, _ := discoveryServer(t, seed, []struct{ name, arn string }{
		{"example-cluster", "arn:aws:kafka:us-west-2:000000000000:cluster/example-cluster/11111111-1111-1111-1111-111111111111-2"},
		{"example-cluster", "arn:aws:kafka:us-west-2:000000000000:cluster/example-cluster/33333333-3333-3333-3333-333333333333-2"},
	}, 0)
	arn, reason, err := d.discover(context.Background())
	if err != nil || arn != "" || reason == "" {
		t.Fatalf("arn=%q reason=%q err=%v", arn, reason, err)
	}
}

func TestDiscoverWrongGenerationIsNotAMatch(t *testing.T) {
	seed := "b-1.examplecluster.abc123.c2.kafka.us-west-2.amazonaws.com:9094"
	d, _ := discoveryServer(t, seed, []struct{ name, arn string }{
		// Same name, but the ARN's trailing generation suffix is -3, not -2.
		{"example-cluster", "arn:aws:kafka:us-west-2:000000000000:cluster/example-cluster/11111111-1111-1111-1111-111111111111-3"},
	}, 0)
	arn, reason, err := d.discover(context.Background())
	if err != nil || arn != "" || reason == "" {
		t.Fatalf("arn=%q reason=%q err=%v", arn, reason, err)
	}
}

func TestDiscoverNonMSKSeedsNeverCallAWS(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer server.Close()
	if _, ok := NewDiscoverer([]string{"kafka-1.example.internal:9092"}, "us-west-2", ""); ok {
		t.Fatal("non-MSK seeds must not produce a Discoverer")
	}
	if calls.Load() != 0 {
		t.Fatal("discovery must never be attempted for non-MSK seeds")
	}
}

func TestDiscoverDeniedPermissionNamesListClustersV2(t *testing.T) {
	seed := "b-1.examplecluster.abc123.c2.kafka.us-west-2.amazonaws.com:9094"
	d, _ := discoveryServer(t, seed, nil, 403)
	arn, reason, err := d.discover(context.Background())
	if err == nil || arn != "" {
		t.Fatalf("arn=%q err=%v", arn, err)
	}
	if !strings.Contains(reason, "kafka:ListClustersV2") || !strings.Contains(reason, "403") {
		t.Fatalf("reason must name the missing permission: %q", reason)
	}
}

func TestDiscovererCapabilitiesSurfacesTheDiscoveryReasonWhenUnresolved(t *testing.T) {
	seed := "b-1.examplecluster.abc123.c2.kafka.us-west-2.amazonaws.com:9094"
	d, _ := discoveryServer(t, seed, nil, 0)
	capability, err := d.Capabilities(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if capability.ManualReassignmentAllowed {
		t.Fatal("unresolved discovery must not allow manual reassignment")
	}
	if capability.RebalancingStatus != "UNKNOWN" || capability.Reason == "" {
		t.Fatalf("capability = %+v", capability)
	}
}

func TestDiscovererCapabilitiesDelegatesToTheDiscoveredClusterOnceResolved(t *testing.T) {
	seed := "b-1.examplecluster.abc123.c2.kafka.us-west-2.amazonaws.com:9094"
	arn := "arn:aws:kafka:us-west-2:000000000000:cluster/example-cluster/11111111-1111-1111-1111-111111111111-2"
	var requests, lists atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/api/v2/clusters" {
			lists.Add(1)
			_, _ = w.Write(mustJSON(t, map[string]any{"clusterInfoList": []map[string]string{{"clusterArn": arn, "clusterName": "example-cluster"}}}))
			return
		}
		fmt.Fprint(w, `{"clusterInfo":{"clusterType":"PROVISIONED","state":"ACTIVE","provisioned":{"brokerNodeGroupInfo":{"instanceType":"express.m7g.8xlarge"},"rebalancing":{"status":"PAUSED"}}}}`)
	}))
	defer server.Close()
	labels, _ := parseBootstrapHost(seed)
	d := &Discoverer{
		labels:   labels,
		endpoint: server.URL,
		client:   server.Client(),
		sign:     func(context.Context) (awsutil.RequestSigner, error) { return noopSign, nil },
	}
	capability, err := d.Capabilities(context.Background(), true)
	if err != nil || !capability.ManualReassignmentAllowed || capability.BrokerType != "express.m7g.8xlarge" {
		t.Fatalf("capability = %+v err = %v", capability, err)
	}
	if requests.Load() != 2 {
		t.Fatalf("expected one ListClustersV2 call and one DescribeClusterV2 call, got %d requests", requests.Load())
	}
	// A second call reuses the discovered checker, which answers from its cache.
	if _, err = d.Capabilities(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 {
		t.Fatalf("a cached second call should send no request, got %d requests total", requests.Load())
	}
	// A forced refresh describes the cluster again but never repeats discovery.
	if _, err = d.Capabilities(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 3 || lists.Load() != 1 {
		t.Fatalf("forced refresh: requests = %d, ListClustersV2 calls = %d; want 3 and 1", requests.Load(), lists.Load())
	}
}
