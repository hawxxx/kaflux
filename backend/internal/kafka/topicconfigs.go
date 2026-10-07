package kafka

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The topic list shows each topic's cleanup policy and retention, which cluster metadata does not
// carry. They come from DescribeConfigs, asked for those two keys only: a full config describe of
// thousands of topics is a response of many megabytes for two values.
const (
	topicConfigTTL        = 5 * time.Minute  // a cached answer is served as is
	topicConfigMaxAge     = 30 * time.Minute // a stale answer is still served while refreshes fail
	topicConfigRetryDelay = 30 * time.Second // wait after a failed refresh
	topicConfigChunk      = 500              // topics per DescribeConfigs request
	topicConfigParallel   = 4                // concurrent requests while loading
	topicConfigLoadLimit  = 15 * time.Second // upper bound for loading every topic
)

var topicConfigKeys = []string{"cleanup.policy", "retention.ms"}

type topicSetting struct {
	cleanupPolicy string
	retentionMs   *int64
}

// topicSettings is one observation of those two values for every described topic.
type topicSettings struct {
	observedAt time.Time
	byTopic    map[string]topicSetting
}

// describeConfigRequests splits the topics into requests that ask for the two keys only.
func describeConfigRequests(names []string) []*kmsg.DescribeConfigsRequest {
	var out []*kmsg.DescribeConfigsRequest
	for start := 0; start < len(names); start += topicConfigChunk {
		end := min(start+topicConfigChunk, len(names))
		req := kmsg.NewPtrDescribeConfigsRequest()
		for _, name := range names[start:end] {
			r := kmsg.NewDescribeConfigsRequestResource()
			r.ResourceType = kmsg.ConfigResourceTypeTopic
			r.ResourceName = name
			r.ConfigNames = append([]string(nil), topicConfigKeys...)
			req.Resources = append(req.Resources, r)
		}
		out = append(out, req)
	}
	return out
}

// readTopicSettings extracts the two values from a response. Topics that failed (deleted while the
// request ran, or not permitted) are left out, so they show as unknown instead of as wrong.
func readTopicSettings(resp *kmsg.DescribeConfigsResponse, into map[string]topicSetting) {
	for _, res := range resp.Resources {
		if kerr.ErrorForCode(res.ErrorCode) != nil {
			continue
		}
		var s topicSetting
		for _, c := range res.Configs {
			if c.Value == nil || c.IsSensitive {
				continue
			}
			switch c.Name {
			case "cleanup.policy":
				s.cleanupPolicy = *c.Value
			case "retention.ms":
				if v, err := strconv.ParseInt(*c.Value, 10, 64); err == nil {
					s.retentionMs = &v
				}
			}
		}
		into[res.ResourceName] = s
	}
}

// apply fills the topics it knows about and reports how many of them it had no answer for.
func (t *topicSettings) apply(snap *model.Snapshot) (missing int) {
	for i := range snap.Topics {
		s, ok := t.byTopic[snap.Topics[i].Name]
		if !ok {
			missing++
			continue
		}
		snap.Topics[i].CleanupPolicy = s.cleanupPolicy
		snap.Topics[i].RetentionMs = s.retentionMs
	}
	return missing
}

// fetchTopicSettings is the Kafka call; Native.fetchSettings replaces it in tests.
func (n *Native) fetchTopicSettings(ctx context.Context, names []string) (map[string]topicSetting, error) {
	requests := describeConfigRequests(names)
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		firstErr error
		out      = make(map[string]topicSetting, len(names))
		slots    = make(chan struct{}, topicConfigParallel)
	)
	for _, req := range requests {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			mu.Lock()
			firstErr = errors.Join(firstErr, ctx.Err())
			mu.Unlock()
			wg.Wait()
			if len(out) == 0 {
				return nil, firstErr
			}
			return out, nil
		}
		wg.Add(1)
		go func(req *kmsg.DescribeConfigsRequest) {
			defer wg.Done()
			defer func() { <-slots }()
			c, done, err := n.bounded(ctx)
			if err != nil {
				mu.Lock()
				firstErr = errors.Join(firstErr, err)
				mu.Unlock()
				return
			}
			defer done()
			part := make(map[string]topicSetting, len(req.Resources))
			for _, shard := range n.describeShards(c, req) {
				if shard.Err != nil {
					mu.Lock()
					firstErr = errors.Join(firstErr, shard.Err)
					mu.Unlock()
					continue
				}
				readTopicSettings(shard.Resp.(*kmsg.DescribeConfigsResponse), part)
			}
			mu.Lock()
			for k, v := range part {
				out[k] = v
			}
			mu.Unlock()
		}(req)
	}
	wg.Wait()
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	// Some answers are better than none: the rest stay unknown and are retried at the next refresh.
	return out, nil
}

// describeShards sends one DescribeConfigs request and returns each broker's answer. Tests replace it.
func (n *Native) describeShards(ctx context.Context, req *kmsg.DescribeConfigsRequest) []kgo.ResponseShard {
	if n.describe != nil {
		return n.describe(ctx, req)
	}
	return n.client.RequestSharded(ctx, req)
}

// applyTopicSettings fills cleanup policy and retention on the snapshot. It never fails: when the
// brokers cannot be asked it serves the last good observation for a bounded time, and otherwise the
// two columns stay empty, which the interface shows as unknown.
//
// The first load waits for the answer, and listings that arrive together share that one load. After
// that a refresh runs in the background while the cached values keep being served, so the topic
// list is never held up by a config describe.
func (n *Native) applyTopicSettings(ctx context.Context, snap *model.Snapshot) {
	if n.applyCachedTopicSettings(snap) {
		return
	}
	// Nothing usable is cached. Load once even when several listings arrive at the same moment:
	// with thousands of topics each extra load is a full round of requests to the cluster.
	n.cfgLoadMu.Lock()
	defer n.cfgLoadMu.Unlock()
	if n.applyCachedTopicSettings(snap) {
		return // another listing loaded it while this one waited
	}
	n.cfgMu.Lock()
	backingOff := time.Now().Before(n.cfgRetryAt)
	generation := n.cfgGeneration
	n.cfgMu.Unlock()
	if backingOff {
		return
	}
	settings, err := n.loadTopicSettings(ctx, snapshotTopicNames(snap))
	n.cfgMu.Lock()
	defer n.cfgMu.Unlock()
	if err != nil {
		n.cfgRetryAt = time.Now().Add(topicConfigRetryDelay)
		return
	}
	// The answer is good for this listing either way, but if a topic was changed while it was being
	// read it may already be out of date, so it is not kept for the next listings.
	if generation == n.cfgGeneration {
		n.cfgCache = settings
	}
	settings.apply(snap)
}

// applyCachedTopicSettings serves what is cached and says whether it could. A cache within its TTL is
// served as is, an older one up to topicConfigMaxAge is served while a refresh runs in the background.
func (n *Native) applyCachedTopicSettings(snap *model.Snapshot) bool {
	n.cfgMu.Lock()
	defer n.cfgMu.Unlock()
	cache := n.cfgCache
	if cache == nil {
		return false
	}
	age := time.Since(cache.observedAt)
	switch {
	case age < topicConfigTTL:
		missing := cache.apply(snap)
		// A topic created after the last refresh has no answer yet; look again soon, not in 5 minutes.
		if missing > 0 && age > 10*time.Second {
			n.refreshTopicSettingsAsync(snapshotTopicNames(snap))
		}
		return true
	case age < topicConfigMaxAge:
		cache.apply(snap)
		n.refreshTopicSettingsAsync(snapshotTopicNames(snap))
		return true
	}
	return false
}

func (n *Native) loadTopicSettings(ctx context.Context, names []string) (*topicSettings, error) {
	fetch := n.fetchSettings
	if fetch == nil {
		fetch = n.fetchTopicSettings
	}
	// The whole load has a limit, whatever the number of topics, so a slow cluster cannot hold up a listing.
	ctx, cancel := context.WithTimeout(ctx, topicConfigLoadLimit)
	defer cancel()
	byTopic, err := fetch(ctx, names)
	if err != nil {
		return nil, err
	}
	return &topicSettings{observedAt: time.Now().UTC(), byTopic: byTopic}, nil
}

// refreshTopicSettingsAsync starts one background refresh at a time. Callers hold cfgMu.
func (n *Native) refreshTopicSettingsAsync(names []string) {
	if n.cfgRefreshing || time.Now().Before(n.cfgRetryAt) {
		return
	}
	n.cfgRefreshing = true
	generation := n.cfgGeneration
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		settings, err := n.loadTopicSettings(ctx, names)
		n.cfgMu.Lock()
		defer n.cfgMu.Unlock()
		n.cfgRefreshing = false
		if err != nil {
			n.cfgRetryAt = time.Now().Add(topicConfigRetryDelay)
			return
		}
		if generation == n.cfgGeneration {
			n.cfgCache = settings
		}
	}()
}

// invalidateTopicSettings drops the cache after a topic was created, deleted or reconfigured, so the
// next listing reads the new values instead of the old ones.
func (n *Native) invalidateTopicSettings() {
	n.cfgMu.Lock()
	n.cfgCache = nil
	n.cfgRetryAt = time.Time{}
	n.cfgGeneration++ // loads that started before this point must not be stored
	n.cfgMu.Unlock()
}

func snapshotTopicNames(snap *model.Snapshot) []string {
	names := make([]string, len(snap.Topics))
	for i := range snap.Topics {
		names[i] = snap.Topics[i].Name
	}
	return names
}
