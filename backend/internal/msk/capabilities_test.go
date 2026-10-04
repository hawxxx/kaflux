package msk

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestExpressIntelligentRebalanceCapability(t *testing.T) {
	for _, test := range []struct {
		name, kind, broker, status string
		allowed                    bool
	}{
		{"active express", "PROVISIONED", "express.m7g.large", "ACTIVE", false},
		{"paused express", "PROVISIONED", "express.m7g.large", "PAUSED", true},
		{"unknown express", "PROVISIONED", "express.m7g.large", "", false},
		{"future status", "PROVISIONED", "express.m7g.large", "ENABLING", false},
		{"standard broker", "PROVISIONED", "kafka.m7g.large", "", true},
		{"serverless", "SERVERLESS", "", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := fmt.Sprintf(`{"clusterInfo":{"clusterType":%q,"state":"ACTIVE","provisioned":{"brokerNodeGroupInfo":{"instanceType":%q},"rebalancing":{"status":%q}}}}`, test.kind, test.broker, test.status)
			capability, err := ParseDescription([]byte(payload))
			if err != nil {
				t.Fatal(err)
			}
			if capability.ManualReassignmentAllowed != test.allowed {
				t.Fatalf("wrong capability %+v", capability)
			}
			if !test.allowed && capability.Reason == "" {
				t.Fatal("missing actionable reason")
			}
		})
	}
}

func TestCapabilityRefreshCatchesEnableAfterPlanAndFailsClosed(t *testing.T) {
	var requests atomic.Int32
	var active atomic.Bool
	var unavailable atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if unavailable.Load() {
			w.WriteHeader(403)
			return
		}
		status := "PAUSED"
		if active.Load() {
			status = "ACTIVE"
		}
		fmt.Fprintf(w, `{"clusterInfo":{"clusterType":"PROVISIONED","state":"ACTIVE","provisioned":{"brokerNodeGroupInfo":{"instanceType":"express.m7g.large"},"rebalancing":{"status":%q}}}}`, status)
	}))
	defer server.Close()
	checker, err := NewChecker("arn:aws:kafka:us-east-1:000000000000:cluster/test/id", server.URL, func(context.Context, *http.Request, []byte) error { return nil }, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	first, err := checker.Capabilities(context.Background(), false)
	if err != nil || !first.ManualReassignmentAllowed {
		t.Fatal(first, err)
	}
	active.Store(true)
	cached, _ := checker.Capabilities(context.Background(), false)
	if !cached.ManualReassignmentAllowed || requests.Load() != 1 {
		t.Fatal("cache not used")
	}
	refreshed, err := checker.Capabilities(context.Background(), true)
	if err != nil || refreshed.ManualReassignmentAllowed || refreshed.RebalancingStatus != "ACTIVE" {
		t.Fatal(refreshed, err)
	}
	unavailable.Store(true)
	failed, err := checker.Capabilities(context.Background(), true)
	if err == nil || failed.ManualReassignmentAllowed {
		t.Fatal("permission failure was not fail-closed")
	}
}

func TestUnknownDescriptionNeverAllowsManualReassignment(t *testing.T) {
	for _, payload := range []string{`{}`, `{"clusterInfo":{"clusterType":"PROVISIONED"}}`, `{"clusterInfo":{"clusterType":"PROVISIONED","state":"UPDATING","provisioned":{"brokerNodeGroupInfo":{"instanceType":"express.m7g.large"},"rebalancing":{"status":"PAUSED"}}}}`} {
		capability, _ := ParseDescription([]byte(payload))
		if capability.ManualReassignmentAllowed {
			t.Fatalf("unsafe description accepted %s", payload)
		}
	}
}
