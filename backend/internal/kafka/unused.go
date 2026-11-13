package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"sort"
	"strings"
	"time"
)

// MaxUsageGroups bounds the consumer groups inspected for topic usage.
const MaxUsageGroups = 500

// ErrTooManyGroups is returned with an incomplete usage map when more than
// MaxUsageGroups groups exist; deletes must treat it as a failure.
var ErrTooManyGroups = errors.New("more consumer groups than can be inspected")

// GroupUsageProvider reports, per topic, every consumer group that references
// it, read fresh from the brokers (never cached).
type GroupUsageProvider interface {
	GroupUsage(ctx context.Context) (map[string][]model.TopicGroupUse, error)
}

// FreshOffsetsProvider reads partition start and end offsets straight from the
// brokers, bypassing the cached snapshot. Values are [start, end].
type FreshOffsetsProvider interface {
	FreshOffsets(ctx context.Context, topic string) (map[int32][2]int64, error)
}

// OffsetsFingerprint is a stable string of "<partition>:<start>:<end>" entries.
func OffsetsFingerprint(offsets map[int32][2]int64) string {
	ids := make([]int, 0, len(offsets))
	for id := range offsets {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		o := offsets[int32(id)]
		parts = append(parts, fmt.Sprintf("%d:%d:%d", id, o[0], o[1]))
	}
	return strings.Join(parts, ",")
}

// GroupUseActive reports whether a group may be consuming the topic right now.
func GroupUseActive(g model.TopicGroupUse) bool {
	return g.AssignedMembers > 0 || g.Members > 0 || (g.State != "Empty" && g.State != "Dead")
}

// ClassifyUnused decides whether a topic is a cleanup candidate. usage lists the
// consumer groups referencing the topic; lastRecord is the newest data record
// timestamp. A non-empty topic is reported only with positive evidence of age:
// an unknown lastRecord means not reported. Internal topics, topics with an
// active group and topics written within minAge are never reported.
func ClassifyUnused(topic model.Topic, usage []model.TopicGroupUse, lastRecord *time.Time, now time.Time, minAge time.Duration) (model.UnusedTopic, bool) {
	return classifyUnused(topic, usage, lastRecord, now, minAge, false)
}

// ClassifyUnusedListing is ClassifyUnused for listings that opt in to showing
// internal topics. The result carries Internal so callers can refuse to delete it.
func ClassifyUnusedListing(topic model.Topic, usage []model.TopicGroupUse, lastRecord *time.Time, now time.Time, minAge time.Duration) (model.UnusedTopic, bool) {
	return classifyUnused(topic, usage, lastRecord, now, minAge, true)
}

func classifyUnused(topic model.Topic, usage []model.TopicGroupUse, lastRecord *time.Time, now time.Time, minAge time.Duration, allowInternal bool) (model.UnusedTopic, bool) {
	internal := topic.IsInternal()
	if internal && !allowInternal {
		return model.UnusedTopic{}, false
	}
	out := model.UnusedTopic{Name: topic.Name, Partitions: len(topic.Partitions), EmptyPartitions: []int32{}, Records: -1, SizeBytes: topic.SizeBytes, LastRecordAt: lastRecord, ConsumerGroups: []string{}, Reasons: []string{}, Internal: internal}
	if n := topic.MessageCount(); n != nil {
		out.Records = *n
	}
	allEmpty := len(topic.Partitions) > 0
	for _, p := range topic.Partitions {
		if p.StartOffset == nil || p.EndOffset == nil || *p.StartOffset != *p.EndOffset {
			allEmpty = false
			continue
		}
		out.EmptyPartitions = append(out.EmptyPartitions, p.ID)
	}
	// Partitions cannot be deleted; empty ones matter only on a topic that has data.
	if len(out.EmptyPartitions) == len(topic.Partitions) {
		out.EmptyPartitions = []int32{}
	}
	committed := 0
	for _, g := range usage {
		if GroupUseActive(g) {
			return model.UnusedTopic{}, false
		}
		if g.HasCommittedOffsets {
			committed++
			out.ConsumerGroups = append(out.ConsumerGroups, g.Group)
		}
	}
	sort.Strings(out.ConsumerGroups)
	old := lastRecord != nil && now.Sub(*lastRecord) > minAge
	days := int(minAge / (24 * time.Hour))
	out.AgeKnown = !allEmpty
	switch {
	case allEmpty && committed == 0:
		out.Classification = "unused"
		out.Reasons = append(out.Reasons, "no records", "no consumer group has committed offsets", "age unknown")
	case allEmpty:
		return model.UnusedTopic{}, false
	case committed == 0 && old:
		out.Classification = "likely-unused"
		out.Reasons = append(out.Reasons, "no consumer group has committed offsets", fmt.Sprintf("last write older than %d days", days))
	case committed > 0 && old:
		out.Classification = "idle"
		out.Reasons = append(out.Reasons, "every consumer group is empty or dead", fmt.Sprintf("last write older than %d days", days))
	default:
		return model.UnusedTopic{}, false
	}
	return out, true
}
