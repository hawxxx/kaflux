package kafka

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

func TestClusterInfoFromVersionsWithNoAnswerIsNil(t *testing.T) {
	if got := clusterInfoFromVersions(kadm.BrokersApiVersions{}); got != nil {
		t.Fatalf("want nil, got %+v", got)
	}
	failed := kadm.BrokersApiVersions{1: {NodeID: 1, Err: errors.New("down")}}
	if got := clusterInfoFromVersions(failed); got != nil {
		t.Fatalf("failed brokers must not produce info, got %+v", got)
	}
}
