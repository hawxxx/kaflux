package metrics

import (
	"strings"
	"testing"
)

func TestPromQLReplacementGroupsAndConfiguredRegexAnchorsAreNotVariables(t *testing.T) {
	expression := `label_replace(metric{instance=~"$instance"}, "broker", "$1", "instance", "(b-[0-9]+).*")`
	expanded, err := Expand(expression, "broker-1", "15m")
	if err != nil || !strings.Contains(expanded, `"$1"`) {
		t.Fatalf("PromQL replacement group damaged: %s %v", expanded, err)
	}
	expanded, err = expandConfiguredRegex(expression, `^broker-[0-9]+$`, "15m")
	if err != nil || !strings.Contains(expanded, `^broker-[0-9]+$`) {
		t.Fatalf("trusted regex anchor damaged: %s %v", expanded, err)
	}
}
