package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func str(s string) *string { return &s }

func configResponse(entries map[string]map[string]*string, failed ...string) *kmsg.DescribeConfigsResponse {
	resp := kmsg.NewPtrDescribeConfigsResponse()
	for name, cfg := range entries {
		r := kmsg.NewDescribeConfigsResponseResource()
		r.ResourceName = name
		for k, v := range cfg {
			c := kmsg.NewDescribeConfigsResponseResourceConfig()
			c.Name, c.Value = k, v
			r.Configs = append(r.Configs, c)
		}
		resp.Resources = append(resp.Resources, r)
	}
	for _, name := range failed {
		r := kmsg.NewDescribeConfigsResponseResource()
		r.ResourceName = name
		r.ErrorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
		resp.Resources = append(resp.Resources, r)
	}
	return resp
}

func TestRequestsAskForTheTwoKeysOnlyInBoundedChunks(t *testing.T) {
	names := make([]string, 0, 1203)
	for i := 0; i < 1203; i++ {
		names = append(names, fmt.Sprintf("topic-%d", i))
	}
	reqs := describeConfigRequests(names)
	if len(reqs) != 3 {
		t.Fatalf("1203 topics should need 3 requests of at most %d, got %d", topicConfigChunk, len(reqs))
	}
	total := 0
	for _, r := range reqs {
		if len(r.Resources) > topicConfigChunk {
			t.Fatalf("chunk of %d topics", len(r.Resources))
		}
		for _, res := range r.Resources {
			total++
			if res.ResourceType != kmsg.ConfigResourceTypeTopic {
				t.Fatalf("not a topic resource: %v", res.ResourceType)
			}
			// A nil list would ask for every config of every topic, which is the heavy response to avoid.
			if len(res.ConfigNames) != 2 || res.ConfigNames[0] != "cleanup.policy" || res.ConfigNames[1] != "retention.ms" {
				t.Fatalf("config names = %v", res.ConfigNames)
			}
		}
	}
	if total != len(names) {
		t.Fatalf("%d of %d topics requested", total, len(names))
	}
	if len(describeConfigRequests(nil)) != 0 {
		t.Fatal("no topics should mean no request")
	}
}

func TestReadTopicSettingsHandlesOddAnswers(t *testing.T) {
	got := map[string]topicSetting{}
	readTopicSettings(configResponse(map[string]map[string]*string{
		"orders":   {"cleanup.policy": str("compact,delete"), "retention.ms": str("604800000")},
		"empty":    {"cleanup.policy": str("delete")},
		"badnum":   {"cleanup.policy": str("delete"), "retention.ms": str("not-a-number")},
		"nullval":  {"cleanup.policy": nil, "retention.ms": nil},
		"infinite": {"cleanup.policy": str("delete"), "retention.ms": str("-1")},
	}, "gone"), got)

	if s := got["orders"]; s.cleanupPolicy != "compact,delete" || s.retentionMs == nil || *s.retentionMs != 604800000 {
		t.Fatalf("orders = %+v", s)
	}
	if s := got["empty"]; s.cleanupPolicy != "delete" || s.retentionMs != nil {
		t.Fatalf("a topic without retention must keep it unknown, got %+v", s)
	}
	if s := got["badnum"]; s.cleanupPolicy != "delete" || s.retentionMs != nil {
		t.Fatalf("an unparsable retention must stay unknown, got %+v", s)
	}
	if s := got["nullval"]; s.cleanupPolicy != "" || s.retentionMs != nil {
		t.Fatalf("null values must not become empty text or zero, got %+v", s)
	}
	if s := got["infinite"]; s.retentionMs == nil || *s.retentionMs != -1 {
		t.Fatalf("-1 means unlimited retention and must be kept, got %+v", s)
	}
	if _, ok := got["gone"]; ok {
		t.Fatal("a topic that returned an error must be left out, not reported as empty")
	}
}

func snapshotOf(names ...string) *model.Snapshot {
	s := &model.Snapshot{}
	for _, n := range names {
		s.Topics = append(s.Topics, model.Topic{Name: n})
	}
	return s
}

func TestApplyFillsKnownTopicsAndCountsTheRest(t *testing.T) {
	r := int64(1000)
	cache := &topicSettings{observedAt: time.Now(), byTopic: map[string]topicSetting{"a": {cleanupPolicy: "delete", retentionMs: &r}}}
	snap := snapshotOf("a", "new")
	if missing := cache.apply(snap); missing != 1 {
		t.Fatalf("missing = %d", missing)
	}
	if snap.Topics[0].CleanupPolicy != "delete" || snap.Topics[0].RetentionMs == nil || *snap.Topics[0].RetentionMs != 1000 {
		t.Fatalf("a = %+v", snap.Topics[0])
	}
	if snap.Topics[1].CleanupPolicy != "" || snap.Topics[1].RetentionMs != nil {
		t.Fatalf("an unknown topic must stay empty: %+v", snap.Topics[1])
	}
}

// newSettingsNative returns a Native whose Kafka call is replaced by fetch.
func newSettingsNative(fetch func(context.Context, []string) (map[string]topicSetting, error)) *Native {
	return &Native{fetchSettings: fetch}
}

func policyFor(names []string, policy string) map[string]topicSetting {
	m := map[string]topicSetting{}
	for _, n := range names {
		m[n] = topicSetting{cleanupPolicy: policy}
	}
	return m
}

func TestFirstListingWaitsForTheValuesThenServesFromCache(t *testing.T) {
	var calls atomic.Int32
	n := newSettingsNative(func(_ context.Context, names []string) (map[string]topicSetting, error) {
		calls.Add(1)
		return policyFor(names, "delete"), nil
	})
	first := snapshotOf("a", "b")
	n.applyTopicSettings(context.Background(), first)
	if first.Topics[0].CleanupPolicy != "delete" || first.Topics[1].CleanupPolicy != "delete" {
		t.Fatalf("the first listing came back without values: %+v", first.Topics)
	}
	second := snapshotOf("a", "b")
	n.applyTopicSettings(context.Background(), second)
	if calls.Load() != 1 || second.Topics[0].CleanupPolicy != "delete" {
		t.Fatalf("a fresh cache must not ask Kafka again: calls=%d", calls.Load())
	}
}

func TestStaleValuesAreServedWhileRefreshingInTheBackground(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	n := newSettingsNative(func(_ context.Context, names []string) (map[string]topicSetting, error) {
		if calls.Add(1) > 1 {
			<-release
			return policyFor(names, "compact"), nil
		}
		return policyFor(names, "delete"), nil
	})
	n.applyTopicSettings(context.Background(), snapshotOf("a"))
	n.cfgMu.Lock()
	n.cfgCache.observedAt = time.Now().Add(-topicConfigTTL - time.Minute)
	n.cfgMu.Unlock()

	done := make(chan *model.Snapshot)
	go func() {
		s := snapshotOf("a")
		n.applyTopicSettings(context.Background(), s)
		done <- s
	}()
	select {
	case s := <-done:
		if s.Topics[0].CleanupPolicy != "delete" {
			t.Fatalf("the stale value should be served, got %q", s.Topics[0].CleanupPolicy)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the listing waited for the refresh instead of serving the cache")
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := snapshotOf("a")
		n.applyTopicSettings(context.Background(), s)
		if s.Topics[0].CleanupPolicy == "compact" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the background refresh never replaced the cached value")
}

func TestOnlyOneBackgroundRefreshRunsAtATime(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	n := newSettingsNative(func(_ context.Context, names []string) (map[string]topicSetting, error) {
		if calls.Add(1) > 1 {
			<-release
		}
		return policyFor(names, "delete"), nil
	})
	n.applyTopicSettings(context.Background(), snapshotOf("a"))
	n.cfgMu.Lock()
	n.cfgCache.observedAt = time.Now().Add(-topicConfigTTL - time.Minute) // stale but usable
	n.cfgMu.Unlock()
	for i := 0; i < 20; i++ {
		n.applyTopicSettings(context.Background(), snapshotOf("a"))
	}
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected the first load plus one background refresh, got %d fetches", got)
	}
	close(release)
}

func TestFailureLeavesValuesUnknownAndBacksOff(t *testing.T) {
	var calls atomic.Int32
	n := newSettingsNative(func(context.Context, []string) (map[string]topicSetting, error) {
		calls.Add(1)
		return nil, errors.New("broker unavailable")
	})
	for i := 0; i < 5; i++ {
		s := snapshotOf("a")
		n.applyTopicSettings(context.Background(), s)
		if s.Topics[0].CleanupPolicy != "" || s.Topics[0].RetentionMs != nil {
			t.Fatalf("values appeared out of nowhere: %+v", s.Topics[0])
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("a failing cluster was asked %d times; it must back off", calls.Load())
	}
}

func TestVeryOldValuesAreNotServedForever(t *testing.T) {
	var calls atomic.Int32
	n := newSettingsNative(func(_ context.Context, names []string) (map[string]topicSetting, error) {
		if calls.Add(1) > 1 {
			return nil, errors.New("down")
		}
		return policyFor(names, "delete"), nil
	})
	n.applyTopicSettings(context.Background(), snapshotOf("a"))
	n.cfgMu.Lock()
	n.cfgCache.observedAt = time.Now().Add(-topicConfigMaxAge - time.Minute)
	n.cfgMu.Unlock()
	s := snapshotOf("a")
	n.applyTopicSettings(context.Background(), s)
	if s.Topics[0].CleanupPolicy != "" {
		t.Fatalf("a value older than %s was served although the refresh failed: %q", topicConfigMaxAge, s.Topics[0].CleanupPolicy)
	}
}

func TestInvalidationMakesTheNextListingReadAgain(t *testing.T) {
	policy := "delete"
	n := newSettingsNative(func(_ context.Context, names []string) (map[string]topicSetting, error) {
		return policyFor(names, policy), nil
	})
	n.applyTopicSettings(context.Background(), snapshotOf("a"))
	policy = "compact"
	n.invalidateTopicSettings()
	s := snapshotOf("a")
	n.applyTopicSettings(context.Background(), s)
	if s.Topics[0].CleanupPolicy != "compact" {
		t.Fatalf("a changed config was not picked up after invalidation: %q", s.Topics[0].CleanupPolicy)
	}
}

func TestNewTopicsAreLookedUpWithoutWaitingForTheFullTTL(t *testing.T) {
	var calls atomic.Int32
	n := newSettingsNative(func(_ context.Context, names []string) (map[string]topicSetting, error) {
		calls.Add(1)
		return policyFor(names, "delete"), nil
	})
	n.applyTopicSettings(context.Background(), snapshotOf("a"))
	n.cfgMu.Lock()
	n.cfgCache.observedAt = time.Now().Add(-time.Minute) // fresh by TTL, but older than 10 s
	n.cfgMu.Unlock()
	n.applyTopicSettings(context.Background(), snapshotOf("a", "created-just-now"))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := snapshotOf("a", "created-just-now")
		n.applyTopicSettings(context.Background(), s)
		if s.Topics[1].CleanupPolicy == "delete" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("a topic created after the last refresh never got its values")
}

func TestListingsArrivingTogetherShareOneFirstLoad(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	n := newSettingsNative(func(_ context.Context, names []string) (map[string]topicSetting, error) {
		calls.Add(1)
		<-release
		return policyFor(names, "delete"), nil
	})
	const listings = 6
	results := make(chan string, listings)
	for i := 0; i < listings; i++ {
		go func() {
			s := snapshotOf("a")
			n.applyTopicSettings(context.Background(), s)
			results <- s.Topics[0].CleanupPolicy
		}()
	}
	time.Sleep(100 * time.Millisecond) // let every listing reach the loader
	close(release)
	for i := 0; i < listings; i++ {
		select {
		case got := <-results:
			if got != "delete" {
				t.Fatalf("a listing came back without values: %q", got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("a listing never finished")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("%d listings caused %d full loads; they should share one", listings, calls.Load())
	}
}

func TestLoadGivesUpAtItsDeadlineAndLeavesValuesUnknown(t *testing.T) {
	n := newSettingsNative(func(ctx context.Context, _ []string) (map[string]topicSetting, error) {
		<-ctx.Done() // a cluster that never answers
		return nil, ctx.Err()
	})
	// The package limit is long, so check the mechanism with a short parent deadline: the load must
	// honour the context it is given and then back off instead of retrying on every listing.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	s := snapshotOf("a")
	n.applyTopicSettings(ctx, s)
	if time.Since(start) > 2*time.Second {
		t.Fatalf("the listing waited %s for a cluster that does not answer", time.Since(start))
	}
	if s.Topics[0].CleanupPolicy != "" {
		t.Fatalf("a value appeared although nothing answered: %q", s.Topics[0].CleanupPolicy)
	}
	n.cfgMu.Lock()
	backoff := n.cfgRetryAt.After(time.Now())
	n.cfgMu.Unlock()
	if !backoff {
		t.Fatal("no back off after a failed load")
	}
}

func TestAnswerReadBeforeAnInvalidationIsNotKept(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	n := newSettingsNative(func(_ context.Context, names []string) (map[string]topicSetting, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release // the topic is changed while this answer is still being read
			return policyFor(names, "delete"), nil
		}
		return policyFor(names, "compact"), nil
	})
	loaded := make(chan *model.Snapshot)
	go func() {
		s := snapshotOf("a")
		n.applyTopicSettings(context.Background(), s)
		loaded <- s
	}()
	<-started
	n.invalidateTopicSettings()
	close(release)
	<-loaded
	// The old answer served that one listing, but it must not stand in for the changed topic.
	next := snapshotOf("a")
	n.applyTopicSettings(context.Background(), next)
	if next.Topics[0].CleanupPolicy != "compact" {
		t.Fatalf("the next listing served an answer read before the change: %q", next.Topics[0].CleanupPolicy)
	}
}

func TestBackgroundRefreshStartedBeforeAnInvalidationIsNotKept(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	n := newSettingsNative(func(_ context.Context, names []string) (map[string]topicSetting, error) {
		if calls.Add(1) == 2 {
			<-release
			return policyFor(names, "stale-from-before-the-change"), nil
		}
		return policyFor(names, "fresh"), nil
	})
	n.applyTopicSettings(context.Background(), snapshotOf("a"))
	n.cfgMu.Lock()
	n.cfgCache.observedAt = time.Now().Add(-topicConfigTTL - time.Minute)
	n.cfgMu.Unlock()
	n.applyTopicSettings(context.Background(), snapshotOf("a")) // starts the background refresh
	time.Sleep(50 * time.Millisecond)
	n.invalidateTopicSettings()
	close(release)
	time.Sleep(100 * time.Millisecond)
	s := snapshotOf("a")
	n.applyTopicSettings(context.Background(), s)
	if s.Topics[0].CleanupPolicy != "fresh" {
		t.Fatalf("a refresh that began before the change replaced newer data: %q", s.Topics[0].CleanupPolicy)
	}
}

func TestLoadStopsStartingRequestsOnceItsContextIsDone(t *testing.T) {
	var started atomic.Int32
	n := &Native{budget: make(chan struct{}, 64)}
	// Every request hangs until its context ends, which fills all the parallel slots.
	n.describe = func(ctx context.Context, _ *kmsg.DescribeConfigsRequest) []kgo.ResponseShard {
		started.Add(1)
		<-ctx.Done()
		return []kgo.ResponseShard{{Err: ctx.Err()}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	names := make([]string, 12*topicConfigChunk) // twelve requests, but only four may run at once
	for i := range names {
		names[i] = fmt.Sprintf("t%d", i)
	}
	start := time.Now()
	_, err := n.fetchTopicSettings(ctx, names)
	if err == nil {
		t.Fatal("a load whose requests all hung reported success")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("the load kept waiting for %s after its context ended", time.Since(start))
	}
	if got := started.Load(); got > topicConfigParallel {
		t.Fatalf("%d requests were started although only %d fit and the context had ended", got, topicConfigParallel)
	}
}
