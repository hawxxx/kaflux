package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"
)

// offsetListChunk bounds how many topics go into one ListOffsets call.
const offsetListChunk = 1000

// groupLags returns the total lag per group, nil when any part of it is unknown.
//
// Lag uses the committed-offset definition: for every committed partition it is
// max(logEndOffset - committedOffset, 0), summed per group. Member assignment
// and log start offsets are not consulted, so the list page agrees with the
// group detail page and costs one FetchManyOffsets plus one end-offset listing.
//
// Commits on topics that cluster metadata does not return are ignored, like a
// deleted topic. This also matters for correctness of the whole request: MSK
// keeps commits for a hidden __msk_channel_remote_metadata_topic, and listing
// offsets for it makes the broker fail every topic in the request with
// UNKNOWN_SERVER_ERROR. kadm.Client.Lag included such topics, which left the
// lag of nearly every group unknown on a cluster with many groups.
func (n *Native) groupLags(ctx context.Context, described kadm.DescribedGroups) map[string]*int64 {
	ids := make([]string, 0, len(described))
	for id, g := range described {
		if g.Err == nil {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return map[string]*int64{}
	}
	fetched := n.admin.FetchManyOffsets(ctx, ids...)
	// Existence comes from the untargeted topic list: a targeted metadata
	// request can return the hidden topic without an error.
	details, e := n.admin.ListTopics(ctx)
	if e != nil {
		return map[string]*int64{}
	}
	existing := existingTopics(details)
	committed := []string{}
	for _, t := range fetched.CommittedPartitions().Topics() {
		if existing[t] {
			committed = append(committed, t)
		}
	}
	return lagTotals(fetched, existing, n.listEndOffsets(ctx, committed))
}

// existingTopics is the set of topics that cluster metadata returns without error.
func existingTopics(details kadm.TopicDetails) map[string]bool {
	out := make(map[string]bool, len(details))
	for t, d := range details {
		if d.Err == nil {
			out[t] = true
		}
	}
	return out
}

// listEndOffsets lists end offsets in chunks. A failed chunk leaves its topics
// absent, which lagTotals reports as unknown for the groups that use them
// instead of failing every group.
func (n *Native) listEndOffsets(ctx context.Context, topics []string) kadm.ListedOffsets {
	end := kadm.ListedOffsets{}
	for i := 0; i < len(topics); i += offsetListChunk {
		j := i + offsetListChunk
		if j > len(topics) {
			j = len(topics)
		}
		listed, _ := n.admin.ListEndOffsets(ctx, topics[i:j]...)
		for t, parts := range listed {
			end[t] = parts
		}
	}
	return end
}

// lagTotals sums lag per group over its committed partitions. A group whose
// offsets could not be fetched, or that has a committed partition whose end
// offset is missing or in error, is unknown (nil) rather than a partial sum.
func lagTotals(fetched kadm.FetchOffsetsResponses, existing map[string]bool, end kadm.ListedOffsets) map[string]*int64 {
	out := make(map[string]*int64, len(fetched))
	for id, f := range fetched {
		if f.Err != nil {
			continue
		}
		var total int64
		known := true
		f.Fetched.Each(func(o kadm.OffsetResponse) {
			if o.At < 0 && o.Err == nil {
				return // no commit on this partition
			}
			if !existing[o.Topic] {
				return // commits on a deleted or hidden topic carry no lag
			}
			e, ok := end.Lookup(o.Topic, o.Partition)
			if o.Err != nil || !ok || e.Err != nil {
				known = false
				return
			}
			if lag := e.Offset - o.At; lag > 0 {
				total += lag
			}
		})
		if known {
			v := total
			out[id] = &v
		}
	}
	return out
}
