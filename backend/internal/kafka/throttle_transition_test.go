package kafka

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"testing"
)

func TestLiveThrottleTransitionPreservesOriginalSettings(t *testing.T) {
	ctx := context.Background()
	f := &configFixture{values: map[string]string{}}
	original, e := PrepareThrottle(ctx, f, []model.Change{{Topic: "orders", Partition: 0, Before: []int32{1}, After: []int32{2}}}, 100)
	if e != nil {
		t.Fatal(e)
	}
	if e = ApplyThrottle(ctx, f, original); e != nil {
		t.Fatal(e)
	}
	next, e := RetargetThrottle(original, 200)
	if e != nil {
		t.Fatal(e)
	}
	if original[0].Applied != "100" {
		t.Fatal("retarget mutated previous ownership")
	}
	if e = ApplyThrottle(ctx, f, next); e != nil {
		t.Fatal(e)
	}
	// Retry after a crash between application and durable acknowledgement.
	if e = ApplyThrottle(ctx, f, next); e != nil {
		t.Fatal(e)
	}
	finalized := FinalizeThrottle(next)
	if finalized[0].Previous != original[0].Previous || finalized[0].EffectiveBefore != original[0].EffectiveBefore {
		t.Fatal("original settings replaced")
	}
	if e = RestoreThrottle(ctx, f, finalized); e != nil {
		t.Fatal(e)
	}
	if len(f.values) != 0 {
		t.Fatal("updated throttle leaked")
	}
}

type missingThrottleBroker struct{ *configFixture }

func (f missingThrottleBroker) DescribeBrokerConfigs(context.Context, ...int32) (kadm.ResourceConfigs, error) {
	return nil, nil
}
func TestMissingBrokerIsNotAnInheritedThrottleDefault(t *testing.T) {
	f := missingThrottleBroker{&configFixture{values: map[string]string{}}}
	records := []ThrottleRecord{{Resource: "broker", Name: "1", Key: "leader.replication.throttled.rate", EffectiveBefore: "-1", Applied: "100"}}
	if RestoreThrottle(context.Background(), f, records) == nil {
		t.Fatal("missing broker incorrectly reported as restored")
	}
	if f.writes != 0 {
		t.Fatal("missing broker mutated")
	}
}

func TestInterruptedThrottleUpdateCanRestorePreviousOwnedRate(t *testing.T) {
	ctx := context.Background()
	f := &configFixture{values: map[string]string{}}
	records, e := PrepareThrottle(ctx, f, []model.Change{{Topic: "orders", Partition: 0, Before: []int32{1}, After: []int32{2}}}, 100)
	if e != nil {
		t.Fatal(e)
	}
	if e = ApplyThrottle(ctx, f, records); e != nil {
		t.Fatal(e)
	}
	next, e := RetargetThrottle(records, 200)
	if e != nil {
		t.Fatal(e)
	}
	// Transition persisted, broker RPC never sent: restore the old owned value.
	if e = RestoreThrottle(ctx, f, next); e != nil {
		t.Fatal(e)
	}
	if len(f.values) != 0 {
		t.Fatal("interrupted update was not restored")
	}
}

func TestLiveThrottleRejectsExternalTakeoverAndInvalidRates(t *testing.T) {
	ctx := context.Background()
	f := &configFixture{values: map[string]string{}}
	records, e := PrepareThrottle(ctx, f, []model.Change{{Topic: "orders", Partition: 0, Before: []int32{1}, After: []int32{2}}}, 100)
	if e != nil {
		t.Fatal(e)
	}
	if e = ApplyThrottle(ctx, f, records); e != nil {
		t.Fatal(e)
	}
	next, e := RetargetThrottle(records, 200)
	if e != nil {
		t.Fatal(e)
	}
	f.values["broker/1/leader.replication.throttled.rate"] = "999"
	if ApplyThrottle(ctx, f, next) == nil {
		t.Fatal("external setting taken over")
	}
	if RestoreThrottle(ctx, f, next) == nil {
		t.Fatal("external conflict hidden")
	}
	if f.values["broker/1/leader.replication.throttled.rate"] != "999" {
		t.Fatal("external rate erased")
	}
	for _, rate := range []int64{0, -1, 1000000000001} {
		if _, e = RetargetThrottle(records, rate); e == nil {
			t.Fatal("invalid rate accepted")
		}
	}
}
