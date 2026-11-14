package kafka

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"testing"
	"time"
)

func unusedTopic(name string, ends ...int64) model.Topic {
	t := model.Topic{Name: name}
	for i, e := range ends {
		start, end := int64(0), e
		t.Partitions = append(t.Partitions, model.Partition{ID: int32(i), StartOffset: &start, EndOffset: &end})
	}
	return t
}

func unusedGroup(id, state string, members int) model.TopicGroupUse {
	return model.TopicGroupUse{Group: id, State: state, Members: members, AssignedMembers: members, HasCommittedOffsets: true}
}

func TestClassifyUnused(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	age := 30 * 24 * time.Hour
	at := func(d time.Duration) *time.Time { v := now.Add(-d); return &v }
	internal := unusedTopic("_schemas", 5)
	flagged := unusedTopic("flagged", 5)
	flagged.Internal = true
	rows := []struct {
		name   string
		topic  model.Topic
		groups []model.TopicGroupUse
		last   *time.Time
		want   string
		empty  []int32
	}{
		{"empty without groups", unusedTopic("a", 0, 0), nil, nil, "unused", nil},
		{"empty topic even when young timestamp absent", unusedTopic("a", 0), nil, at(time.Hour), "unused", nil},
		{"old data without groups", unusedTopic("a", 4), nil, at(31 * 24 * time.Hour), "likely-unused", nil},
		{"exactly minAge is not older", unusedTopic("a", 4), nil, at(age), "", nil},
		{"young data without groups", unusedTopic("a", 4), nil, at(24 * time.Hour), "", nil},
		{"unknown timestamp without groups is never reported", unusedTopic("a", 4), nil, nil, "", nil},
		{"uncommitted group with members blocks even without commits", unusedTopic("a", 0), []model.TopicGroupUse{{Group: "g", State: "Stable", Members: 1, AssignedMembers: 1}}, nil, "", nil},
		{"uncommitted empty group is ignored", unusedTopic("a", 4), []model.TopicGroupUse{{Group: "g", State: "Empty"}}, at(60 * 24 * time.Hour), "likely-unused", nil},
		{"old data with only empty group", unusedTopic("a", 4), []model.TopicGroupUse{unusedGroup("g", "Empty", 0)}, at(40 * 24 * time.Hour), "idle", nil},
		{"dead group counts as empty", unusedTopic("a", 4), []model.TopicGroupUse{unusedGroup("g", "Dead", 0)}, at(40 * 24 * time.Hour), "idle", nil},
		{"empty group with young data", unusedTopic("a", 4), []model.TopicGroupUse{unusedGroup("g", "Empty", 0)}, at(time.Hour), "", nil},
		{"empty group with unknown timestamp", unusedTopic("a", 4), []model.TopicGroupUse{unusedGroup("g", "Empty", 0)}, nil, "", nil},
		{"active member blocks", unusedTopic("a", 4), []model.TopicGroupUse{unusedGroup("g", "Stable", 2)}, at(400 * 24 * time.Hour), "", nil},
		{"one active among empty blocks", unusedTopic("a", 0), []model.TopicGroupUse{unusedGroup("e", "Empty", 0), unusedGroup("g", "Stable", 1)}, nil, "", nil},
		{"empty topic with committed offsets is not unused", unusedTopic("a", 0), []model.TopicGroupUse{unusedGroup("g", "Empty", 0)}, nil, "", nil},
		{"underscore topic never reported", internal, nil, at(400 * 24 * time.Hour), "", nil},
		{"broker internal flag never reported", flagged, nil, at(400 * 24 * time.Hour), "", nil},
		{"empty partitions on non-empty topic", unusedTopic("a", 4, 0, 7, 0), nil, at(60 * 24 * time.Hour), "likely-unused", []int32{1, 3}},
	}
	for _, row := range rows {
		got, ok := ClassifyUnused(row.topic, row.groups, row.last, now, age)
		if row.want == "" {
			if ok {
				t.Errorf("%s: reported %+v", row.name, got)
			}
			continue
		}
		if !ok || got.Classification != row.want || len(got.Reasons) == 0 {
			t.Errorf("%s: got %+v ok=%v want %s", row.name, got, ok, row.want)
			continue
		}
		if len(got.EmptyPartitions) != len(row.empty) {
			t.Errorf("%s: empty partitions %v want %v", row.name, got.EmptyPartitions, row.empty)
		}
	}
	// The listing variant shows internal topics and flags them.
	got, ok := ClassifyUnusedListing(internal, nil, at(400*24*time.Hour), now, age)
	if !ok || !got.Internal || got.Classification != "likely-unused" {
		t.Errorf("listing internal: %+v %v", got, ok)
	}
}

func TestClassifyUnusedAgeKnownAndFingerprint(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	got, ok := ClassifyUnused(unusedTopic("a", 0), nil, nil, now, time.Hour)
	if !ok || got.AgeKnown {
		t.Fatalf("empty topic %+v", got)
	}
	old := now.Add(-48 * time.Hour)
	got, ok = ClassifyUnused(unusedTopic("a", 3), nil, &old, now, time.Hour)
	if !ok || !got.AgeKnown {
		t.Fatalf("non-empty topic %+v", got)
	}
	if f := OffsetsFingerprint(map[int32][2]int64{1: {0, 5}, 0: {2, 2}}); f != "0:2:2,1:0:5" {
		t.Fatal(f)
	}
}

func TestDemoGroupUsageReportsMembersAndCommits(t *testing.T) {
	d := NewDemo()
	usage, e := d.GroupUsage(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, u := range usage["orders.created"] {
		if u.Group == "demo-order-service" && u.AssignedMembers == 3 && u.HasCommittedOffsets {
			found = true
		}
	}
	if !found {
		t.Fatalf("usage %+v", usage["orders.created"])
	}
	offsets, e := d.FreshOffsets(context.Background(), "orders.created")
	if e != nil || len(offsets) != 12 || offsets[0] != [2]int64{0, 24} {
		t.Fatalf("offsets %v %v", offsets, e)
	}
}
