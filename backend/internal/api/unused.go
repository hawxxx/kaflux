package api

import (
	"context"
	"errors"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	unusedMaxCandidates = 500
	unusedDeadline      = 10 * time.Second
	unusedWindow        = 32
	bulkDeadline        = 60 * time.Second
	usageFreshness      = 5 * time.Second
	unusedMaxBulk       = 100
)

// unusedTopics serves the unused topic finder and its safe bulk cleanup.
func (a *API) unusedTopics(w http.ResponseWriter, r *http.Request, u auth.User, id string, p kafka.Provider, parts []string) bool {
	switch {
	case len(parts) == 5 && parts[4] == "unused-topics":
		if r.Method != "GET" {
			fail(w, 405, "method_not_allowed", "GET required")
			return true
		}
		a.unusedReport(w, r, u, id, p)
	case len(parts) == 6 && parts[4] == "topics" && parts[5] == "bulk-delete":
		if r.Method != "POST" {
			fail(w, 405, "method_not_allowed", "POST required")
			return true
		}
		a.bulkDelete(w, r, u, id, p)
	default:
		return false
	}
	return true
}

func parseMinAgeDays(raw string) (int, bool) {
	if raw == "" {
		return 30, true
	}
	n, e := strconv.Atoi(raw)
	return n, e == nil && n >= 1 && n <= 3650
}

func (a *API) bulkTimeout() time.Duration {
	if a.bulkDeadline > 0 {
		return a.bulkDeadline
	}
	return bulkDeadline
}

func (a *API) unusedTimeout() time.Duration {
	if a.unusedDeadline > 0 {
		return a.unusedDeadline
	}
	return unusedDeadline
}

func (a *API) unusedReport(w http.ResponseWriter, r *http.Request, u auth.User, id string, p kafka.Provider) {
	if !(auth.Authorizer{Grants: a.o.Grants}).AnyAllowed(u, id, "read") {
		fail(w, 403, "forbidden", "Permission denied")
		return
	}
	q := r.URL.Query()
	days, ok := parseMinAgeDays(q.Get("minAgeDays"))
	if !ok {
		fail(w, 400, "invalid_request", "minAgeDays must be between 1 and 3650")
		return
	}
	var exclude *regexp.Regexp
	if pattern := q.Get("exclude"); pattern != "" {
		if len(pattern) > 200 {
			fail(w, 400, "invalid_request", "exclude must be at most 200 characters")
			return
		}
		var e error
		if exclude, e = regexp.Compile(pattern); e != nil {
			fail(w, 400, "invalid_request", "exclude is not a valid regular expression")
			return
		}
	}
	if _, ok := p.(kafka.GroupUsageProvider); !ok {
		fail(w, 422, "unsupported", "Provider cannot report consumer group usage")
		return
	}
	now := a.now()
	topics, partial, reason, e := a.evaluateUnused(r.Context(), p, now, time.Duration(days)*24*time.Hour, q.Get("includeInternal") == "true", exclude)
	if e != nil {
		if errors.Is(e, errUsageUnavailable) {
			fail(w, 503, "usage_unavailable", "Consumer group state could not be read; no topic can be judged unused")
			return
		}
		adminError(w, e)
		return
	}
	visible := topics[:0]
	for _, t := range topics {
		if (auth.Authorizer{Grants: a.o.Grants}).Allowed(u, id, "read", t.Name) {
			visible = append(visible, t)
		}
	}
	respond(w, map[string]any{"topics": visible, "evaluatedAt": now, "partial": partial, "partialReason": reason})
}

var errUsageUnavailable = errors.New("consumer state unavailable")

// freshTopic rebuilds the topic with partition offsets read straight from the
// brokers, returning the offsets fingerprint. Without a fresh reader the
// snapshot is returned with an empty fingerprint, which deletes refuse.
func freshTopic(ctx context.Context, p kafka.Provider, t model.Topic) (model.Topic, string, error) {
	fresh, ok := p.(kafka.FreshOffsetsProvider)
	if !ok {
		return t, "", nil
	}
	offsets, e := fresh.FreshOffsets(ctx, t.Name)
	if e != nil {
		return t, "", e
	}
	out := t
	out.Partitions = nil
	ids := make([]int, 0, len(offsets))
	for id := range offsets {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	for _, id := range ids {
		lo, hi := offsets[int32(id)][0], offsets[int32(id)][1]
		out.Partitions = append(out.Partitions, model.Partition{ID: int32(id), StartOffset: &lo, EndOffset: &hi})
	}
	return out, kafka.OffsetsFingerprint(offsets), nil
}

// evaluateUnused classifies every topic. Consumer usage is read once and fresh;
// offsets and the newest record are read per remaining topic, all under one
// deadline. A topic whose evidence cannot be read is left out, never reported.
func (a *API) evaluateUnused(parent context.Context, p kafka.Provider, now time.Time, minAge time.Duration, includeInternal bool, exclude *regexp.Regexp) ([]model.UnusedTopic, bool, string, error) {
	ctx, cancel := context.WithTimeout(parent, a.unusedTimeout())
	defer cancel()
	snapshot, e := p.Snapshot(ctx)
	if e != nil {
		return nil, false, "", e
	}
	usage, e := p.(kafka.GroupUsageProvider).GroupUsage(ctx)
	reasons := []string{}
	if errors.Is(e, kafka.ErrTooManyGroups) {
		reasons = append(reasons, fmt.Sprintf("more than %d consumer groups: consumer detection may be incomplete", kafka.MaxUsageGroups))
	} else if e != nil {
		return nil, false, "", errUsageUnavailable
	}
	candidates := []model.Topic{}
	for _, t := range snapshot.Topics {
		if (t.IsInternal() && !includeInternal) || (exclude != nil && exclude.MatchString(t.Name)) {
			continue
		}
		candidates = append(candidates, t)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Name < candidates[j].Name })
	if len(candidates) > unusedMaxCandidates {
		candidates = candidates[:unusedMaxCandidates]
		reasons = append(reasons, fmt.Sprintf("only the first %d topics were evaluated", unusedMaxCandidates))
	}
	out := []model.UnusedTopic{}
	unreadable, notReached := 0, 0
	for i, t := range candidates {
		if ctx.Err() != nil {
			notReached = len(candidates) - i
			break
		}
		if hasActiveUse(usage[t.Name]) {
			continue
		}
		fresh, fingerprint, e := freshTopic(ctx, p, t)
		if e != nil {
			unreadable++
			continue
		}
		last, ok := lastRecordTime(ctx, p, fresh, now)
		if !ok {
			unreadable++
			continue
		}
		if row, reported := kafka.ClassifyUnusedListing(fresh, usage[t.Name], last, now, minAge); reported {
			row.Fingerprint = fingerprint
			out = append(out, row)
		}
	}
	if ctx.Err() != nil && notReached == 0 {
		// The deadline hit during the last topic's reads.
		notReached, unreadable = 1, max(unreadable-1, 0)
	}
	if notReached > 0 {
		reasons = append(reasons, fmt.Sprintf("evaluation deadline reached: %d topics not evaluated", notReached))
	}
	if unreadable > 0 {
		reasons = append(reasons, fmt.Sprintf("offsets or last write time could not be read for %d topics, which are left out", unreadable))
	}
	return out, len(reasons) > 0, strings.Join(reasons, "; "), nil
}

func hasActiveUse(usage []model.TopicGroupUse) bool {
	for _, g := range usage {
		if kafka.GroupUseActive(g) {
			return true
		}
	}
	return false
}

// lastRecordTime reads the newest data record of every non-empty partition,
// looking back over a window so trailing transaction markers are stepped over.
// ok is false unless every non-empty partition yielded a timestamp. A nil time
// with ok means the topic has no records.
func lastRecordTime(ctx context.Context, p kafka.Provider, t model.Topic, now time.Time) (*time.Time, bool) {
	from := map[int32]int64{}
	for _, part := range t.Partitions {
		if part.StartOffset == nil || part.EndOffset == nil {
			return nil, false
		}
		if *part.EndOffset > *part.StartOffset {
			from[part.ID] = max(*part.StartOffset, *part.EndOffset-unusedWindow)
		}
	}
	if len(from) == 0 {
		return nil, true
	}
	reader, ok := p.(kafka.MultiReader)
	if !ok {
		return nil, false
	}
	msgs, e := reader.MessagesAt(ctx, t.Name, from, unusedWindow)
	if e != nil {
		return nil, false
	}
	return newestPlausible(msgs, from, now)
}

// earliestPlausible bounds record timestamps: producers can set any value, so
// anything before 2000 (including zero, negative and the epoch) is not evidence.
var earliestPlausible = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// newestPlausible returns the newest timestamp across partitions. It is not ok
// unless every partition in from yielded messages and every message carries a
// plausible timestamp: not before 2000 and not more than a day after now.
func newestPlausible(msgs []model.Message, from map[int32]int64, now time.Time) (*time.Time, bool) {
	newest := map[int32]time.Time{}
	for _, m := range msgs {
		if m.Timestamp.Before(earliestPlausible) || m.Timestamp.After(now.Add(24*time.Hour)) {
			return nil, false
		}
		if m.Timestamp.After(newest[m.Partition]) {
			newest[m.Partition] = m.Timestamp
		}
	}
	var last time.Time
	for pid := range from {
		ts, ok := newest[pid]
		if !ok {
			return nil, false
		}
		if ts.After(last) {
			last = ts
		}
	}
	return &last, true
}

type bulkSkip struct {
	Topic  string `json:"topic"`
	Reason string `json:"reason"`
}

func (a *API) bulkDelete(w http.ResponseWriter, r *http.Request, u auth.User, id string, p kafka.Provider) {
	if !(auth.Authorizer{Grants: a.o.Grants}).AnyAllowed(u, id, "delete") {
		fail(w, 403, "forbidden", "Permission denied")
		return
	}
	admin, ok := p.(kafka.AdminProvider)
	if !ok {
		fail(w, 422, "unsupported", "Provider does not support administration")
		return
	}
	var in struct {
		Topics                 []string          `json:"topics"`
		MinAgeDays             int               `json:"minAgeDays"`
		Confirmation           string            `json:"confirmation"`
		Fingerprints           map[string]string `json:"fingerprints"`
		IncludeEmptyUnknownAge bool              `json:"includeEmptyUnknownAge"`
	}
	if !decode(w, r, &in) {
		return
	}
	if len(in.Topics) == 0 || len(in.Topics) > unusedMaxBulk {
		fail(w, 400, "invalid_request", "Choose between 1 and 100 topics")
		return
	}
	if in.MinAgeDays < 1 || in.MinAgeDays > 3650 {
		fail(w, 400, "invalid_request", "minAgeDays must be between 1 and 3650")
		return
	}
	seen := map[string]bool{}
	names := []string{}
	for _, name := range in.Topics {
		if name == "" || len(name) > 249 {
			fail(w, 400, "invalid_request", "Topic names are invalid")
			return
		}
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	for _, name := range names {
		if in.Fingerprints[name] == "" {
			fail(w, 400, "invalid_request", "A fingerprint from the report is required for every topic: "+name)
			return
		}
	}
	if in.Confirmation != fmt.Sprintf("delete %d topics", len(names)) {
		fail(w, 400, "confirmation_required", fmt.Sprintf("Type \"delete %d topics\" to confirm", len(names)))
		return
	}
	for _, name := range names {
		if !a.allowed(u, id, "delete", name, w) {
			return
		}
	}
	snapshot, e := p.Snapshot(r.Context())
	if e != nil {
		adminError(w, e)
		return
	}
	known := map[string]model.Topic{}
	for _, t := range snapshot.Topics {
		known[t.Name] = t
	}
	minAge := time.Duration(in.MinAgeDays) * 24 * time.Hour
	deleted, skipped := []string{}, []bulkSkip{}
	// The whole evaluation and delete loop shares one deadline. A deletion that
	// has started is never cancelled; topics not yet started are skipped.
	reqCtx, cancelReq := context.WithTimeout(r.Context(), a.bulkTimeout())
	defer cancelReq()
	skipRest := func(rest []string, reason string) {
		for _, name := range rest {
			skipped = append(skipped, bulkSkip{name, reason})
		}
	}
	// Group usage is read once, fresh, before the loop and read again right
	// before a delete only when it is older than usageFreshness.
	gu, hasUsage := p.(kafka.GroupUsageProvider)
	var usage map[string][]model.TopicGroupUse
	var usageAt time.Time
	readUsage := func() error {
		if !hasUsage {
			return errUsageUnavailable
		}
		ctx, cancel := context.WithTimeout(reqCtx, a.unusedTimeout())
		defer cancel()
		u, e := gu.GroupUsage(ctx)
		if e != nil {
			return e
		}
		usage, usageAt = u, a.now()
		return nil
	}
	if e := readUsage(); e != nil {
		skipRest(names, "consumer state unavailable")
		respond(w, map[string]any{"deleted": deleted, "skipped": skipped})
		return
	}
	for i, name := range names {
		if reqCtx.Err() != nil {
			skipRest(names[i:], "request deadline reached")
			break
		}
		topic, exists := known[name]
		if !exists {
			skipped = append(skipped, bulkSkip{name, "topic not found"})
			continue
		}
		reason, row := a.recheckUnused(reqCtx, p, topic, usage[name], minAge, in.Fingerprints[name], in.IncludeEmptyUnknownAge)
		if reason == "" && a.now().Sub(usageAt) > usageFreshness {
			if e := readUsage(); e != nil {
				reason = "consumer state unavailable"
			} else if hasActiveUse(usage[name]) {
				reason = "topic has an active consumer"
			}
		}
		if reason == "" && reqCtx.Err() != nil {
			skipRest(names[i:], "request deadline reached")
			break
		}
		if reason != "" {
			skipped = append(skipped, bulkSkip{name, reason})
			continue
		}
		intent := map[string]any{"classification": row.Classification, "reasons": row.Reasons, "fingerprint": row.Fingerprint, "bulk": true}
		if e := a.adminAudit(r, u, id, "delete", name, "intent", intent, nil); e != nil {
			failCause(w, r, 503, "audit_unavailable", fmt.Sprintf("Mutation blocked because audit storage is unavailable; deleted before the failure: %v", deleted), e)
			return
		}
		derr := admin.DeleteTopic(r.Context(), name)
		result := "success"
		if derr != nil {
			result = "failed-or-uncertain"
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
		auditErr := a.adminAudit(r.WithContext(ctx), u, id, "delete", name, result, intent, nil)
		cancel()
		if derr != nil {
			skipped = append(skipped, bulkSkip{name, "delete failed: " + derr.Error()})
		} else {
			deleted = append(deleted, name)
		}
		if auditErr != nil {
			failCause(w, r, 503, "audit_uncertain", fmt.Sprintf("Broker operation attempted but result audit failed; deleted so far: %v", deleted), auditErr)
			return
		}
	}
	respond(w, map[string]any{"deleted": deleted, "skipped": skipped})
}

// recheckUnused re-evaluates one topic against fresh evidence at delete time.
// It returns a skip reason, or "" when the topic may be deleted. Any evidence
// that cannot be read fresh means the topic is skipped.
func (a *API) recheckUnused(parent context.Context, p kafka.Provider, topic model.Topic, usage []model.TopicGroupUse, minAge time.Duration, fingerprint string, allowEmpty bool) (string, model.UnusedTopic) {
	if topic.IsInternal() {
		return "internal topic", model.UnusedTopic{}
	}
	if _, ok := p.(kafka.FreshOffsetsProvider); !ok {
		return "cannot verify freshness", model.UnusedTopic{}
	}
	ctx, cancel := context.WithTimeout(parent, a.unusedTimeout())
	defer cancel()
	fresh, current, e := freshTopic(ctx, p, topic)
	if e != nil {
		return "fresh offsets could not be read", model.UnusedTopic{}
	}
	if current != fingerprint {
		return "changed since report", model.UnusedTopic{}
	}
	if hasActiveUse(usage) {
		return "topic has an active consumer", model.UnusedTopic{}
	}
	now := a.now()
	last, ok := lastRecordTime(ctx, p, fresh, now)
	if !ok {
		return "last write could not be read", model.UnusedTopic{}
	}
	row, reported := kafka.ClassifyUnused(fresh, usage, last, now, minAge)
	if !reported {
		return "topic is in use or was written recently", model.UnusedTopic{}
	}
	if !row.AgeKnown && !allowEmpty {
		return "empty topic with unknown age", model.UnusedTopic{}
	}
	row.Fingerprint = current
	return "", row
}
