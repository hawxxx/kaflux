package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
)

func TestGroupsServedFromCacheWithoutTouchingKafka(t *testing.T) {
	lag := int64(7)
	// A nil admin client would panic if Groups went to Kafka.
	n := &Native{groupsCache: []model.Group{{ID: "g1", Lag: &lag}}, groupsUntil: time.Now().Add(time.Minute)}

	got, err := n.Groups(context.Background())
	if err != nil || len(got) != 1 || got[0].ID != "g1" || *got[0].Lag != 7 {
		t.Fatalf("groups = %+v, err = %v", got, err)
	}
	got[0].ID = "mutated"
	again, _ := n.Groups(context.Background())
	if again[0].ID != "g1" {
		t.Fatal("a caller reordering or editing the slice must not change the cached list")
	}
}

func TestInvalidateGroupsExpiresTheCache(t *testing.T) {
	n := &Native{groupsCache: []model.Group{{ID: "g1"}}, groupsUntil: time.Now().Add(time.Minute)}
	n.invalidateGroups()
	if time.Now().Before(n.groupsUntil) {
		t.Fatal("an offset reset must force the next read to recompute lag")
	}
}
