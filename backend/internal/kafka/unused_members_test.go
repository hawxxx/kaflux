package kafka

import "testing"

func TestUsageFromMembers(t *testing.T) {
	parsable := []memberView{{assigned: []string{"a"}, assignedOK: true, subscribed: []string{"a", "b"}, subscribedOK: true}, {subscribed: []string{"b"}, subscribedOK: true}}
	got, e := usageFromMembers("Stable", parsable)
	if e != nil || got["a"] != 1 || got["b"] != 2 {
		t.Fatalf("parsable: %v %v", got, e)
	}
	unparseable := []memberView{{assigned: nil, assignedOK: false}}
	for _, state := range []string{"Stable", "PreparingRebalance", "CompletingRebalance"} {
		if _, e := usageFromMembers(state, unparseable); e == nil {
			t.Errorf("%s: unparseable members must fail closed", state)
		}
	}
	for _, state := range []string{"Empty", "Dead"} {
		if got, e := usageFromMembers(state, unparseable); e != nil || len(got) != 0 {
			t.Errorf("%s: %v %v", state, got, e)
		}
	}
	// One parsable and one unparseable member in a live group still fails.
	if _, e := usageFromMembers("Stable", append(parsable, unparseable...)); e == nil {
		t.Error("mixed members must fail closed")
	}
}
