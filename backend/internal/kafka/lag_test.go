package kafka

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func committed(topic string, partition int32, at int64) kadm.OffsetResponse {
	return kadm.OffsetResponse{Offset: kadm.Offset{Topic: topic, Partition: partition, At: at}}
}

func ended(topic string, partition int32, offset int64) kadm.ListedOffset {
	return kadm.ListedOffset{Topic: topic, Partition: partition, Offset: offset}
}

func fetch(group string, offsets kadm.OffsetResponses) kadm.FetchOffsetsResponse {
	return kadm.FetchOffsetsResponse{Group: group, Fetched: offsets}
}

var liveTopics = map[string]bool{"orders": true, "payments": true}

func TestExistingTopicsDropsTopicsMetadataReturnsWithAnError(t *testing.T) {
	got := existingTopics(kadm.TopicDetails{
		"orders": {Topic: "orders"},
		"broken": {Topic: "broken", Err: kerr.UnknownTopicOrPartition},
	})
	if !got["orders"] || got["broken"] || got["__msk_channel_remote_metadata_topic"] {
		t.Fatalf("existing = %v", got)
	}
}

func TestLagIsSummedOverCommittedPartitions(t *testing.T) {
	fetched := kadm.FetchOffsetsResponses{"g1": fetch("g1", kadm.OffsetResponses{
		"orders": {0: committed("orders", 0, 90), 1: committed("orders", 1, 40)},
	})}
	end := kadm.ListedOffsets{"orders": {0: ended("orders", 0, 100), 1: ended("orders", 1, 50)}}

	got := lagTotals(fetched, liveTopics, end)
	if got["g1"] == nil || *got["g1"] != 20 {
		t.Fatalf("lag = %v, want 20 (10 + 10)", got["g1"])
	}
}

func TestLagCountsPartitionsTheGroupNoLongerOwns(t *testing.T) {
	// Every committed partition counts. A group
	// whose members currently own only some partitions still lags on the rest,
	// which an assignment based calculation would leave out.
	fetched := kadm.FetchOffsetsResponses{"consumers": fetch("consumers", kadm.OffsetResponses{
		"orders": {0: committed("orders", 0, 10), 1: committed("orders", 1, 10), 2: committed("orders", 2, 10)},
	})}
	end := kadm.ListedOffsets{"orders": {0: ended("orders", 0, 110), 1: ended("orders", 1, 110), 2: ended("orders", 2, 110)}}
	if got := lagTotals(fetched, liveTopics, end); got["consumers"] == nil || *got["consumers"] != 300 {
		t.Fatalf("lag = %v, want 300", got["consumers"])
	}
}

func TestLagIsFlooredAtZeroPerPartition(t *testing.T) {
	// A commit beyond the log end (after truncation) must neither go negative
	// nor cancel lag on another partition.
	fetched := kadm.FetchOffsetsResponses{"g1": fetch("g1", kadm.OffsetResponses{
		"orders": {0: committed("orders", 0, 500), 1: committed("orders", 1, 40)},
	})}
	end := kadm.ListedOffsets{"orders": {0: ended("orders", 0, 100), 1: ended("orders", 1, 50)}}
	if got := lagTotals(fetched, liveTopics, end); got["g1"] == nil || *got["g1"] != 10 {
		t.Fatalf("lag = %v, want 10", got["g1"])
	}
}

func TestPartitionsWithoutACommitAreIgnored(t *testing.T) {
	fetched := kadm.FetchOffsetsResponses{"g1": fetch("g1", kadm.OffsetResponses{
		"orders": {0: committed("orders", 0, -1), 1: committed("orders", 1, 40)},
	})}
	end := kadm.ListedOffsets{"orders": {0: ended("orders", 0, 100), 1: ended("orders", 1, 50)}}
	if got := lagTotals(fetched, liveTopics, end); got["g1"] == nil || *got["g1"] != 10 {
		t.Fatalf("lag = %v, want 10", got["g1"])
	}
}

func TestCommitsOnTopicsMetadataDoesNotReturnAreSkipped(t *testing.T) {
	// __msk_channel_remote_metadata_topic is committed to by an MSK internal
	// group but absent from metadata, like a deleted topic.
	fetched := kadm.FetchOffsetsResponses{
		"mixed":      fetch("mixed", kadm.OffsetResponses{"orders": {0: committed("orders", 0, 90)}, "__msk_channel_remote_metadata_topic": {0: committed("__msk_channel_remote_metadata_topic", 0, 5)}}),
		"onlyHidden": fetch("onlyHidden", kadm.OffsetResponses{"__msk_channel_remote_metadata_topic": {0: committed("__msk_channel_remote_metadata_topic", 0, 5)}}),
	}
	end := kadm.ListedOffsets{"orders": {0: ended("orders", 0, 100)}}
	got := lagTotals(fetched, liveTopics, end)
	if got["mixed"] == nil || *got["mixed"] != 10 {
		t.Fatalf("mixed = %v, want 10", got["mixed"])
	}
	if got["onlyHidden"] == nil || *got["onlyHidden"] != 0 {
		t.Fatalf("onlyHidden = %v, want 0 because the hidden topic is skipped", got["onlyHidden"])
	}
}

func TestLagIsUnknownNotPartialWhenAnEndOffsetIsMissing(t *testing.T) {
	fetched := kadm.FetchOffsetsResponses{"g1": fetch("g1", kadm.OffsetResponses{
		"orders": {0: committed("orders", 0, 90), 1: committed("orders", 1, 40)},
	})}
	end := kadm.ListedOffsets{"orders": {0: ended("orders", 0, 100)}} // partition 1 has no end offset
	if got := lagTotals(fetched, liveTopics, end); got["g1"] != nil {
		t.Fatalf("a group with an unlistable partition must be unknown, got %d", *got["g1"])
	}
}

func TestOneFailingGroupDoesNotBlankTheOthers(t *testing.T) {
	failedCommit := committed("orders", 0, 90)
	failedCommit.Err = kerr.UnknownServerError
	brokenEnd := ended("payments", 0, 100)
	brokenEnd.Err = kerr.NotLeaderForPartition
	fetched := kadm.FetchOffsetsResponses{
		"good":        fetch("good", kadm.OffsetResponses{"orders": {0: committed("orders", 0, 60)}}),
		"badCommit":   fetch("badCommit", kadm.OffsetResponses{"orders": {0: failedCommit}}),
		"badEnd":      fetch("badEnd", kadm.OffsetResponses{"payments": {0: committed("payments", 0, 1)}}),
		"fetchFailed": {Group: "fetchFailed", Err: errors.New("coordinator unavailable")},
	}
	end := kadm.ListedOffsets{"orders": {0: ended("orders", 0, 100)}, "payments": {0: brokenEnd}}

	got := lagTotals(fetched, liveTopics, end)
	if got["good"] == nil || *got["good"] != 40 {
		t.Fatalf("good = %v, want 40: one bad group must not blank the others", got["good"])
	}
	for _, id := range []string{"badCommit", "badEnd", "fetchFailed"} {
		if got[id] != nil {
			t.Fatalf("%s must be unknown, got %d", id, *got[id])
		}
	}
}
