package kafka

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"testing"
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
