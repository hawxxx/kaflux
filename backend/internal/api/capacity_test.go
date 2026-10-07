package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/capacity"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/msk"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

// configuredCluster stands for self-managed Kafka whose operator declared capacity.
type configuredCluster struct {
	*kafka.Demo
	c *capacity.Config
}

func (p configuredCluster) CapacityConfig() *capacity.Config { return p.c }

// mskCluster stands for a cluster that reports an Amazon MSK broker type.
type mskCluster struct {
	*kafka.Demo
	brokerType string
}

func (p mskCluster) Capabilities(context.Context, bool) (msk.Capabilities, error) {
	return msk.Capabilities{Kind: "MSK Express", BrokerType: p.brokerType}, nil
}

// both is MSK with an operator override.
type both struct {
	mskCluster
	c *capacity.Config
}

func (p both) CapacityConfig() *capacity.Config { return p.c }

// deniedMSKCluster stands for a cluster whose broker type could not be read
// because AWS denied the request, or whose MSK cluster could not be
// identified uniquely from the configured seeds; both report a specific
// reason alongside an error, rather than a generic unknown broker type.
type deniedMSKCluster struct {
	*kafka.Demo
	reason string
}

func (p deniedMSKCluster) Capabilities(context.Context, bool) (msk.Capabilities, error) {
	return msk.Capabilities{Kind: "MSK", RebalancingStatus: "UNKNOWN", Reason: p.reason}, errCapabilityDenied
}

var errCapabilityDenied = errors.New("capability lookup denied")

func balanceCapacity(t *testing.T, provider kafka.Provider) (known bool, report map[string]any) {
	t.Helper()
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": provider}, Clusters: []model.Cluster{{ID: "demo", Mode: "demo"}}})
	r := httptest.NewRecorder()
	a.ServeHTTP(r, httptest.NewRequest("GET", "/api/v1/clusters/demo/balance", nil))
	if r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	var body struct {
		Data struct {
			CapacityKnown bool           `json:"capacityKnown"`
			Capacity      map[string]any `json:"capacity"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err, r.Body.String())
	}
	return body.Data.CapacityKnown, body.Data.Capacity
}

func TestBalanceCapacityIsUnknownWithoutAnySourceAndSaysHowToFixIt(t *testing.T) {
	known, report := balanceCapacity(t, kafka.NewDemo())
	if known || report["known"] != false || report["status"] != "unknown" || report["reason"] != capacity.UnknownReason {
		t.Fatalf("known=%v report=%v", known, report)
	}
	if brokers, _ := report["brokers"].([]any); len(brokers) == 0 {
		t.Fatal("per-broker replica counts must be reported even when capacity is unknown")
	}
	if _, ok := report["medianReplicas"]; !ok {
		t.Fatal("median replica load must be reported even when capacity is unknown")
	}
}

func TestBalanceCapacityFromOperatorConfiguration(t *testing.T) {
	cfg := &capacity.Config{Label: "r6i.4xlarge x 9", PartitionsPerBroker: &capacity.Limits{Recommended: 4000, Maximum: 6000}, DiskBytesPerBroker: 1 << 40}
	known, report := balanceCapacity(t, configuredCluster{kafka.NewDemo(), cfg})
	if !known || report["source"] != "configured" || report["label"] != "r6i.4xlarge x 9" {
		t.Fatalf("known=%v report=%v", known, report)
	}
	limits, _ := report["partitionsPerBroker"].(map[string]any)
	if limits["recommended"] != float64(4000) || limits["maximum"] != float64(6000) {
		t.Fatalf("limits = %v", limits)
	}
	if report["status"] != "ok" {
		t.Fatalf("the demo cluster is far below 4000 replicas per broker, status = %v", report["status"])
	}
}

func TestBalanceCapacityFromTheMSKBrokerType(t *testing.T) {
	known, report := balanceCapacity(t, mskCluster{kafka.NewDemo(), "express.m7g.4xlarge"})
	limits, _ := report["partitionsPerBroker"].(map[string]any)
	if !known || report["source"] != "aws-msk" || report["label"] != "express.m7g.4xlarge" || limits["recommended"] != float64(6000) || limits["maximum"] != float64(8000) {
		t.Fatalf("known=%v report=%v", known, report)
	}
	if report["reference"] == "" || report["note"] == "" {
		t.Fatal("AWS limits must carry their source and caveat")
	}
}

func TestBalanceCapacityStaysUnknownForAnMSKTypeWithoutPublishedLimits(t *testing.T) {
	known, report := balanceCapacity(t, mskCluster{kafka.NewDemo(), "kafka.r5.large"})
	if known || report["status"] != "unknown" {
		t.Fatalf("an unlisted broker type must not be guessed: known=%v report=%v", known, report)
	}
	if report["reason"] != capacity.UnknownReason {
		t.Fatalf("an unlisted broker type keeps the generic reason, got %v", report["reason"])
	}
}

func TestOperatorConfigurationOverridesPublishedMSKLimits(t *testing.T) {
	cfg := &capacity.Config{Label: "tested limit", PartitionsPerBroker: &capacity.Limits{Recommended: 3000}}
	known, report := balanceCapacity(t, both{mskCluster{kafka.NewDemo(), "express.m7g.4xlarge"}, cfg})
	limits, _ := report["partitionsPerBroker"].(map[string]any)
	if !known || report["source"] != "configured" || limits["recommended"] != float64(3000) {
		t.Fatalf("known=%v report=%v", known, report)
	}
}

func TestEmptyOperatorConfigurationFallsBackToMSK(t *testing.T) {
	known, report := balanceCapacity(t, both{mskCluster{kafka.NewDemo(), "express.m7g.large"}, &capacity.Config{Label: "only a label"}})
	if !known || report["source"] != "aws-msk" {
		t.Fatalf("known=%v report=%v", known, report)
	}
}

// TestBalanceCapacityReasonNamesTheDeniedPermission covers the requirement
// that a 403 from AWS (surfaced here as a Capabilities error carrying a
// specific Reason) replaces the generic unknown-capacity reason with the
// exact permission that was denied, instead of staying generic.
func TestBalanceCapacityReasonNamesTheDeniedPermission(t *testing.T) {
	reason := "AWS denied the request to describe the MSK cluster (HTTP 403 AccessDenied). Grant the kafka:DescribeClusterV2 permission to the role Kaflux runs as."
	known, report := balanceCapacity(t, deniedMSKCluster{kafka.NewDemo(), reason})
	if known {
		t.Fatalf("a denied lookup must stay unknown: %v", report)
	}
	if report["reason"] != reason {
		t.Fatalf("reason = %v, want the specific permission reason %q", report["reason"], reason)
	}
}

// TestBalanceCapacityReasonNamesAmbiguousDiscovery covers the discovery
// variant of the same rule: no error is returned (discovery itself
// succeeded, it just did not find a unique cluster), but the reason must
// still be the specific discovery outcome, not the generic message.
func TestBalanceCapacityReasonNamesAmbiguousDiscovery(t *testing.T) {
	reason := `2 MSK clusters matched the bootstrap hostnames (name "examplecluster", generation c2). Set mskClusterArn to disambiguate.`
	known, report := balanceCapacity(t, ambiguousDiscoveryCluster{kafka.NewDemo(), reason})
	if known {
		t.Fatalf("ambiguous discovery must stay unknown: %v", report)
	}
	if report["reason"] != reason {
		t.Fatalf("reason = %v, want the specific discovery reason %q", report["reason"], reason)
	}
}

type ambiguousDiscoveryCluster struct {
	*kafka.Demo
	reason string
}

func (p ambiguousDiscoveryCluster) Capabilities(context.Context, bool) (msk.Capabilities, error) {
	return msk.Capabilities{Kind: "MSK", RebalancingStatus: "UNKNOWN", Reason: p.reason}, nil
}
