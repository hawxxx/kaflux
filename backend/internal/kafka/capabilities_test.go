package kafka

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"testing"
	"time"
)

func TestKnownMSKWithoutARNCannotReassign(t *testing.T) {
	p, e := NewNative(Config{Seeds: []string{"b-1.example.kafka.us-east-1.amazonaws.com:9098"}, TLS: true, SASL: "msk-iam", Region: "us-east-1"})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	cap, e := p.Capabilities(context.Background(), true)
	if e != nil || cap.ManualReassignmentAllowed || cap.RebalancingStatus != "UNKNOWN" {
		t.Fatalf("unverified MSK not blocked %+v %v", cap, e)
	}
	if p.Reassign(context.Background(), []model.Change{{Topic: "orders", Partition: 0, After: []int32{1}}}) == nil {
		t.Fatal("unverified manual reassignment accepted")
	}
}

// TestMSKBootstrapSeedsWithoutARNNeverBlockStartup confirms that discovery is
// attempted lazily (on the first Capabilities call) and not during
// NewNative, so a cluster with MSK-shaped seeds but no mskClusterArn still
// constructs immediately, even though real discovery would need network
// access this test never grants.
func TestMSKBootstrapSeedsWithoutARNNeverBlockStartup(t *testing.T) {
	started := time.Now()
	p, e := NewNative(Config{Seeds: []string{"b-1.examplecluster.abc123.c2.kafka.us-west-2.amazonaws.com:9094"}, TLS: true, SASL: "msk-iam", Region: "us-west-2"})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("construction must not perform discovery I/O, took %s", elapsed)
	}
}
