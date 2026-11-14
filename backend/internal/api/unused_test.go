package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// unusedAPI is a demo API whose clock sits 43 days after the demo records.
func unusedAPI(t *testing.T) (*API, func(method, path, body string) *httptest.ResponseRecorder) {
	t.Helper()
	a, request := lifecycleAPI(t, nil)
	a.now = func() time.Time { return time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC) }
	if w := request("POST", "topics", `{"name":"scratch.empty","partitions":2,"replicationFactor":1}`); w.Code != 200 {
		t.Fatalf("create %d %s", w.Code, w.Body.String())
	}
	return a, request
}

type unusedReport struct {
	Data struct {
		Topics []struct {
			Name            string
			Classification  string
			Reasons         []string
			EmptyPartitions []int32
			ConsumerGroups  []string
			Internal        bool
			LastRecordAt    *time.Time
			Fingerprint     string
			AgeKnown        bool
		}
		Partial       bool
		PartialReason string
		EvaluatedAt   time.Time
	}
}

func unusedGet(t *testing.T, request func(method, path, body string) *httptest.ResponseRecorder, query string) map[string]string {
	t.Helper()
	w := request("GET", "unused-topics"+query, "")
	if w.Code != 200 {
		t.Fatalf("unused %d %s", w.Code, w.Body.String())
	}
	var out unusedReport
	if e := json.Unmarshal(w.Body.Bytes(), &out); e != nil {
		t.Fatal(e)
	}
	if out.Data.EvaluatedAt.IsZero() {
		t.Fatal("missing evaluatedAt")
	}
	m := map[string]string{}
	for _, row := range out.Data.Topics {
		m[row.Name] = row.Classification
	}
	return m
}

func TestUnusedTopicsDemoClassification(t *testing.T) {
	_, request := unusedAPI(t)
	got := unusedGet(t, request, "")
	if got["scratch.empty"] != "unused" || got["inventory.updated"] != "likely-unused" || got["shipping.dispatched"] != "likely-unused" {
		t.Fatalf("classification %v", got)
	}
	// orders.created has an active member (and an empty group); payments and audit have active members.
	for _, name := range []string{"orders.created", "payments.authorized", "events.audit"} {
		if _, ok := got[name]; ok {
			t.Fatalf("%s must be absent: %v", name, got)
		}
	}
}

func TestUnusedTopicsDemoCleanupFixtures(t *testing.T) {
	_, request := unusedAPI(t)
	got := unusedGet(t, request, "")
	want := map[string]string{
		"tmp.load-test": "unused", "scratch.schema-poc": "unused",
		"legacy.billing.v1": "likely-unused", "test.consumer-smoke": "likely-unused",
		"reports.nightly-export": "idle", "archive.clickstream-2025": "idle",
	}
	for name, class := range want {
		if got[name] != class {
			t.Errorf("%s: got %q want %q (%v)", name, got[name], class, got)
		}
	}
}

func TestUnusedTopicsExcludeInternalYoungAndBounds(t *testing.T) {
	a, request := unusedAPI(t)
	if w := request("POST", "topics", `{"name":"_internal.state","partitions":1,"replicationFactor":1}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	got := unusedGet(t, request, "")
	if _, ok := got["_internal.state"]; ok {
		t.Fatalf("internal topic reported: %v", got)
	}
	withInternal := unusedGet(t, request, "?includeInternal=true")
	if withInternal["_internal.state"] != "unused" {
		t.Fatalf("includeInternal %v", withInternal)
	}
	// A record written just now makes the topic young: never likely-unused.
	if w := request("POST", "topics", `{"name":"young.data","partitions":1,"replicationFactor":1}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if w := request("POST", "messages", `{"topic":"young.data","partition":0,"key":"k","value":"v"}`); w.Code != 200 {
		t.Fatalf("produce %d %s", w.Code, w.Body.String())
	}
	a.now = time.Now
	if _, ok := unusedGet(t, request, "")["young.data"]; ok {
		t.Fatal("young topic reported")
	}
	for _, q := range []string{"?minAgeDays=0", "?minAgeDays=3651", "?minAgeDays=abc", "?exclude=(", "?exclude=" + strings.Repeat("a", 201)} {
		if w := request("GET", "unused-topics"+q, ""); w.Code != 400 {
			t.Errorf("%s got %d", q, w.Code)
		}
	}
	if w := request("GET", "unused-topics?minAgeDays=3650&exclude=x", ""); w.Code != 200 {
		t.Fatalf("upper bound %d", w.Code)
	}
}

func TestUnusedTopicsExcludeRegex(t *testing.T) {
	_, request := unusedAPI(t)
	got := unusedGet(t, request, "?exclude=%5Escratch%5C.%7Cshipping")
	if _, ok := got["scratch.empty"]; ok {
		t.Fatalf("exclude ignored %v", got)
	}
	if _, ok := got["shipping.dispatched"]; ok {
		t.Fatalf("exclude ignored %v", got)
	}
	if got["inventory.updated"] == "" {
		t.Fatalf("non-matching topic removed %v", got)
	}
}

func TestUnusedTopicsReportEmptyPartitions(t *testing.T) {
	a2, request2 := unusedAPI(t)
	if w := request2("POST", "topics", `{"name":"half.used","partitions":2,"replicationFactor":1}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if w := request2("POST", "messages", `{"topic":"half.used","partition":0,"key":"k","value":"v"}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	a2.now = func() time.Time { return time.Now().Add(60 * 24 * time.Hour) }
	w := request2("GET", "unused-topics", "")
	var out unusedReport
	if e := json.Unmarshal(w.Body.Bytes(), &out); e != nil || w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, row := range out.Data.Topics {
		if row.Name == "half.used" {
			if row.Classification != "likely-unused" || len(row.EmptyPartitions) != 1 || row.EmptyPartitions[0] != 1 {
				t.Fatalf("half.used %+v", row)
			}
			return
		}
	}
	t.Fatalf("half.used missing: %s", w.Body.String())
}

func TestUnusedTopicsRequireRead(t *testing.T) {
	a, request := unusedAPI(t)
	a.o.Grants = []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "consume", Pattern: "*"}}
	a.demo.User.Roles = []string{"viewer"}
	if w := request("GET", "unused-topics", ""); w.Code != 403 {
		t.Fatalf("got %d", w.Code)
	}
}

// fingerprints returns the report's fingerprint per topic (with internal topics).
func fingerprints(t *testing.T, request func(method, path, body string) *httptest.ResponseRecorder) map[string]string {
	t.Helper()
	w := request("GET", "unused-topics?includeInternal=true", "")
	var out unusedReport
	if e := json.Unmarshal(w.Body.Bytes(), &out); e != nil || w.Code != 200 {
		t.Fatalf("report %d %s", w.Code, w.Body.String())
	}
	m := map[string]string{}
	for _, row := range out.Data.Topics {
		m[row.Name] = row.Fingerprint
	}
	return m
}

func bulkBody(topics []string, phrase string, fp map[string]string, flag bool) string {
	prints := map[string]string{}
	for _, name := range topics {
		prints[name] = "x"
		if v, ok := fp[name]; ok {
			prints[name] = v
		}
	}
	b, _ := json.Marshal(map[string]any{"topics": topics, "minAgeDays": 30, "confirmation": phrase, "fingerprints": prints, "includeEmptyUnknownAge": flag})
	return string(b)
}

type bulkResult struct {
	Data struct {
		Deleted []string
		Skipped []struct{ Topic, Reason string }
	}
}

func bulkSkips(t *testing.T, w *httptest.ResponseRecorder) (deleted []string, skipped map[string]string) {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("bulk %d %s", w.Code, w.Body.String())
	}
	var out bulkResult
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	skipped = map[string]string{}
	for _, s := range out.Data.Skipped {
		skipped[s.Topic] = s.Reason
	}
	return out.Data.Deleted, skipped
}

func TestBulkDeleteRejectsBadRequests(t *testing.T) {
	_, request := unusedAPI(t)
	fp := fingerprints(t, request)
	if w := request("POST", "topics/bulk-delete", bulkBody([]string{"scratch.empty"}, "delete 2 topics", fp, true)); w.Code != 400 {
		t.Fatalf("wrong phrase %d", w.Code)
	}
	if w := request("POST", "topics/bulk-delete", bulkBody([]string{"scratch.empty"}, "", fp, true)); w.Code != 400 {
		t.Fatalf("empty phrase %d", w.Code)
	}
	if w := request("POST", "topics/bulk-delete", `{"topics":["scratch.empty"],"minAgeDays":30,"fingerprints":{"scratch.empty":"x"}}`); w.Code != 400 {
		t.Fatalf("missing phrase %d", w.Code)
	}
	if w := request("POST", "topics/bulk-delete", `{"topics":["scratch.empty"],"minAgeDays":30,"confirmation":"delete 1 topics"}`); w.Code != 400 {
		t.Fatalf("missing fingerprints %d", w.Code)
	}
	if w := request("POST", "topics/bulk-delete", bulkBody([]string{}, "delete 0 topics", fp, true)); w.Code != 400 {
		t.Fatalf("no topics %d", w.Code)
	}
	many := make([]string, 101)
	for i := range many {
		many[i] = "t" + strings.Repeat("x", i%5) + string(rune('a'+i%26)) + string(rune('a'+i/26))
	}
	if w := request("POST", "topics/bulk-delete", bulkBody(many, "delete 101 topics", fp, true)); w.Code != 400 {
		t.Fatalf("too many %d", w.Code)
	}
	if w := request("POST", "topics/bulk-delete", `{"topics":["scratch.empty"],"minAgeDays":0,"confirmation":"delete 1 topics","fingerprints":{"scratch.empty":"x"}}`); w.Code != 400 {
		t.Fatalf("age %d", w.Code)
	}
	if got := unusedGet(t, request, ""); got["scratch.empty"] != "unused" {
		t.Fatal("topic deleted by rejected request")
	}
}

func TestBulkDeleteSkipsTopicsThatGainedConsumersAndInternal(t *testing.T) {
	a, request := unusedAPI(t)
	if w := request("POST", "topics", `{"name":"_internal.state","partitions":1,"replicationFactor":1}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	fp := fingerprints(t, request)
	if fp["inventory.updated"] == "" || fp["scratch.empty"] == "" {
		t.Fatalf("precondition: fingerprints %v", fp)
	}
	// A consumer starts on inventory.updated after the report was produced.
	a.o.Providers["demo"].(interface{ AddGroup(g model.Group) }).AddGroup(model.Group{ID: "late-consumer", State: "Stable", Members: 1, Topics: []string{"inventory.updated"}})
	w := request("POST", "topics/bulk-delete", bulkBody([]string{"scratch.empty", "inventory.updated", "_internal.state", "orders.created", "missing.topic"}, "delete 5 topics", fp, true))
	deleted, skipped := bulkSkips(t, w)
	if len(deleted) != 1 || deleted[0] != "scratch.empty" {
		t.Fatalf("deleted %v", deleted)
	}
	for _, name := range []string{"inventory.updated", "_internal.state", "orders.created", "missing.topic"} {
		if skipped[name] == "" {
			t.Fatalf("%s not skipped with a reason: %v", name, skipped)
		}
	}
	if _, ok := unusedGet(t, request, "")["scratch.empty"]; ok {
		t.Fatal("deleted topic still reported")
	}
	if w := request("GET", "topics?pageSize=200", ""); strings.Contains(w.Body.String(), `"scratch.empty"`) || !strings.Contains(w.Body.String(), `"inventory.updated"`) || !strings.Contains(w.Body.String(), `"_internal.state"`) {
		t.Fatalf("topic list after bulk delete: %s", w.Body.String())
	}
	events, _ := a.o.Store.Audits(context.Background())
	success := 0
	for _, e := range events {
		if e.Action == "delete" && e.Result == "success" {
			success++
			if e.Resource != "scratch.empty" {
				t.Fatalf("audit resource %s", e.Resource)
			}
		}
	}
	if success != 1 {
		t.Fatalf("success audits %d of %d events", success, len(events))
	}
}

func TestBulkDeleteViewerForbiddenBeforeDeletion(t *testing.T) {
	a, request := unusedAPI(t)
	fp := fingerprints(t, request)
	admin := []auth.Grant{{Role: "administrator", Cluster: "*", Action: "*", Pattern: "*"}}
	a.o.Grants = []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "read", Pattern: "*"}}
	a.demo.User.Roles = []string{"viewer"}
	if w := request("POST", "topics/bulk-delete", bulkBody([]string{"scratch.empty"}, "delete 1 topics", fp, true)); w.Code != 403 {
		t.Fatalf("got %d", w.Code)
	}
	a.demo.User.Roles = []string{"administrator"}
	a.o.Grants = admin
	if got := unusedGet(t, request, ""); got["scratch.empty"] != "unused" {
		t.Fatal("topic deleted for viewer")
	}
	a.o.Grants = []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "delete", Pattern: "scratch.*"}, {Role: "viewer", Cluster: "demo", Action: "read", Pattern: "*"}}
	a.demo.User.Roles = []string{"viewer"}
	if w := request("POST", "topics/bulk-delete", bulkBody([]string{"scratch.empty", "inventory.updated"}, "delete 2 topics", fp, true)); w.Code != 403 {
		t.Fatalf("partial permission %d", w.Code)
	}
	a.demo.User.Roles = []string{"administrator"}
	a.o.Grants = admin
	if got := unusedGet(t, request, ""); got["scratch.empty"] != "unused" {
		t.Fatal("topic deleted despite partial permission")
	}
}

// faultDemo wraps the demo provider to inject the failures a real broker produces.
type faultDemo struct {
	*kafka.Demo
	usageErr   error
	extraUse   map[string][]model.TopicGroupUse
	freshErr   error
	freshBlock bool
	noStamps   map[string]bool
	stamp      map[string]time.Time
	freshDelay time.Duration
	usageCalls int
	onDelete   func()
}

func (f *faultDemo) GroupUsage(ctx context.Context) (map[string][]model.TopicGroupUse, error) {
	f.usageCalls++
	if f.usageErr != nil {
		return nil, f.usageErr
	}
	out, e := f.Demo.GroupUsage(ctx)
	for topic, uses := range f.extraUse {
		out[topic] = append(out[topic], uses...)
	}
	return out, e
}

func (f *faultDemo) FreshOffsets(ctx context.Context, topic string) (map[int32][2]int64, error) {
	if f.freshBlock {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.freshDelay > 0 {
		time.Sleep(f.freshDelay)
	}
	if f.freshErr != nil {
		return nil, f.freshErr
	}
	return f.Demo.FreshOffsets(ctx, topic)
}

// MessagesAt returns nothing for flagged topics, as when every record in the
// look-back window is a transaction marker.
func (f *faultDemo) MessagesAt(ctx context.Context, topic string, from map[int32]int64, limit int) ([]model.Message, error) {
	if f.noStamps[topic] {
		return []model.Message{}, nil
	}
	msgs, e := f.Demo.MessagesAt(ctx, topic, from, limit)
	if ts, ok := f.stamp[topic]; ok {
		for i := range msgs {
			msgs[i].Timestamp = ts
		}
	}
	return msgs, e
}

func (f *faultDemo) DeleteTopic(ctx context.Context, name string) error {
	if f.onDelete != nil {
		f.onDelete()
	}
	return f.Demo.DeleteTopic(ctx, name)
}

func faultyAPI(t *testing.T) (*API, *faultDemo, func(method, path, body string) *httptest.ResponseRecorder) {
	t.Helper()
	a, request := unusedAPI(t)
	f := &faultDemo{Demo: a.o.Providers["demo"].(*kafka.Demo), noStamps: map[string]bool{}}
	a.o.Providers["demo"] = f
	return a, f, request
}

func TestUnusedLastWriteUnreadableIsNotReportedOrDeletable(t *testing.T) {
	_, f, request := faultyAPI(t)
	fp := fingerprints(t, request)
	if fp["inventory.updated"] == "" {
		t.Fatal("precondition")
	}
	f.noStamps["inventory.updated"] = true
	got := unusedGet(t, request, "")
	if _, ok := got["inventory.updated"]; ok || got["shipping.dispatched"] != "likely-unused" {
		t.Fatalf("report %v", got)
	}
	var out unusedReport
	_ = json.Unmarshal(request("GET", "unused-topics", "").Body.Bytes(), &out)
	if !out.Data.Partial || out.Data.PartialReason == "" {
		t.Fatalf("partial %+v", out.Data)
	}
	deleted, skipped := bulkSkips(t, request("POST", "topics/bulk-delete", bulkBody([]string{"inventory.updated"}, "delete 1 topics", fp, true)))
	if len(deleted) != 0 || !strings.Contains(skipped["inventory.updated"], "last write") {
		t.Fatalf("deleted %v skipped %v", deleted, skipped)
	}
}

func TestUnusedStableGroupWithMembersButNoCommitsBlocks(t *testing.T) {
	_, f, request := faultyAPI(t)
	fp := fingerprints(t, request)
	f.extraUse = map[string][]model.TopicGroupUse{"shipping.dispatched": {{Group: "fresh-app", State: "Stable", Members: 2, AssignedMembers: 2}}}
	if _, ok := unusedGet(t, request, "")["shipping.dispatched"]; ok {
		t.Fatal("topic with assigned members reported")
	}
	deleted, skipped := bulkSkips(t, request("POST", "topics/bulk-delete", bulkBody([]string{"shipping.dispatched"}, "delete 1 topics", fp, true)))
	if len(deleted) != 0 || skipped["shipping.dispatched"] != "topic has an active consumer" {
		t.Fatalf("deleted %v skipped %v", deleted, skipped)
	}
}

func TestUnusedGroupUsageErrorFailsClosed(t *testing.T) {
	_, f, request := faultyAPI(t)
	fp := fingerprints(t, request)
	f.usageErr = errors.New("describe failed")
	w := request("GET", "unused-topics", "")
	if w.Code != 503 || !strings.Contains(w.Body.String(), "usage_unavailable") {
		t.Fatalf("report %d %s", w.Code, w.Body.String())
	}
	deleted, skipped := bulkSkips(t, request("POST", "topics/bulk-delete", bulkBody([]string{"scratch.empty", "inventory.updated"}, "delete 2 topics", fp, true)))
	if len(deleted) != 0 || skipped["scratch.empty"] != "consumer state unavailable" || skipped["inventory.updated"] != "consumer state unavailable" {
		t.Fatalf("deleted %v skipped %v", deleted, skipped)
	}
}

func TestBulkDeleteSkipsChangedFingerprintAndFreshReadFailure(t *testing.T) {
	_, f, request := faultyAPI(t)
	fp := fingerprints(t, request)
	// A write lands after the report was produced.
	if w := request("POST", "messages", `{"topic":"scratch.empty","partition":0,"key":"k","value":"v"}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	deleted, skipped := bulkSkips(t, request("POST", "topics/bulk-delete", bulkBody([]string{"scratch.empty"}, "delete 1 topics", fp, true)))
	if len(deleted) != 0 || skipped["scratch.empty"] != "changed since report" {
		t.Fatalf("deleted %v skipped %v", deleted, skipped)
	}
	f.freshErr = errors.New("list offsets failed")
	deleted, skipped = bulkSkips(t, request("POST", "topics/bulk-delete", bulkBody([]string{"inventory.updated"}, "delete 1 topics", fp, true)))
	if len(deleted) != 0 || skipped["inventory.updated"] == "" {
		t.Fatalf("deleted %v skipped %v", deleted, skipped)
	}
	// With unreadable offsets the report leaves the topics out instead of guessing.
	var out unusedReport
	_ = json.Unmarshal(request("GET", "unused-topics", "").Body.Bytes(), &out)
	if len(out.Data.Topics) != 0 || !out.Data.Partial {
		t.Fatalf("report with failing offsets %+v", out.Data)
	}
}

func TestBulkDeleteEmptyTopicWithUnknownAge(t *testing.T) {
	_, request := unusedAPI(t)
	var out unusedReport
	_ = json.Unmarshal(request("GET", "unused-topics", "").Body.Bytes(), &out)
	for _, row := range out.Data.Topics {
		if row.Name == "scratch.empty" && row.AgeKnown {
			t.Fatal("empty topic must report ageKnown false")
		}
	}
	fp := fingerprints(t, request)
	deleted, skipped := bulkSkips(t, request("POST", "topics/bulk-delete", bulkBody([]string{"scratch.empty"}, "delete 1 topics", fp, false)))
	if len(deleted) != 0 || skipped["scratch.empty"] != "empty topic with unknown age" {
		t.Fatalf("deleted %v skipped %v", deleted, skipped)
	}
	deleted, _ = bulkSkips(t, request("POST", "topics/bulk-delete", bulkBody([]string{"scratch.empty"}, "delete 1 topics", fp, true)))
	if len(deleted) != 1 {
		t.Fatalf("deleted %v", deleted)
	}
}

func TestBulkDeleteDeletesNothingWhenAuditIsDown(t *testing.T) {
	a, request := unusedAPI(t)
	fp := fingerprints(t, request)
	broken, e := store.NewSQLite(context.Background(), filepath.Join(t.TempDir(), "audit.db"))
	if e != nil {
		t.Fatal(e)
	}
	broken.Close()
	a.o.Store = broken
	w := request("POST", "topics/bulk-delete", bulkBody([]string{"scratch.empty"}, "delete 1 topics", fp, true))
	if w.Code != 503 || !strings.Contains(w.Body.String(), "audit_unavailable") {
		t.Fatalf("got %d %s", w.Code, w.Body.String())
	}
	if _, ok := unusedGet(t, request, "")["scratch.empty"]; !ok {
		t.Fatal("topic deleted while audit storage was down")
	}
}

func TestUnusedReportStopsAtDeadlineAndSaysSo(t *testing.T) {
	a, f, request := faultyAPI(t)
	a.unusedDeadline = 50 * time.Millisecond
	f.freshBlock = true
	w := request("GET", "unused-topics", "")
	var out unusedReport
	if e := json.Unmarshal(w.Body.Bytes(), &out); e != nil || w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	if len(out.Data.Topics) != 0 || !out.Data.Partial || !strings.Contains(out.Data.PartialReason, "deadline") {
		t.Fatalf("report %+v", out.Data)
	}
}

func TestNewestPlausible(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	from := map[int32]int64{0: 0}
	at := func(ts time.Time, part int32) model.Message { return model.Message{Partition: part, Timestamp: ts} }
	good := now.Add(-72 * time.Hour)
	rows := []struct {
		name string
		msgs []model.Message
		ok   bool
	}{
		{"valid", []model.Message{at(good, 0)}, true},
		{"slightly in the future is tolerated", []model.Message{at(now.Add(time.Hour), 0)}, true},
		{"zero", []model.Message{at(time.Time{}, 0)}, false},
		{"negative", []model.Message{at(time.Unix(-5, 0), 0)}, false},
		{"epoch", []model.Message{at(time.Unix(0, 0), 0)}, false},
		{"before 2000", []model.Message{at(time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC), 0)}, false},
		{"far future", []model.Message{at(now.Add(25*time.Hour), 0)}, false},
		{"one bad among good", []model.Message{at(good, 0), at(time.Unix(0, 0), 0)}, false},
		{"partition without messages", []model.Message{at(good, 1)}, false},
		{"no messages", nil, false},
	}
	for _, row := range rows {
		got, ok := newestPlausible(row.msgs, from, now)
		if ok != row.ok || (ok && (got == nil || !got.Equal(row.msgs[0].Timestamp) && row.name == "valid")) {
			t.Errorf("%s: got %v ok=%v", row.name, got, ok)
		}
	}
}

func TestUnusedImplausibleTimestampTopicIsNotReportedOrDeletable(t *testing.T) {
	_, f, request := faultyAPI(t)
	fp := fingerprints(t, request)
	f.stamp = map[string]time.Time{"inventory.updated": time.Unix(0, 0)}
	if _, ok := unusedGet(t, request, "")["inventory.updated"]; ok {
		t.Fatal("topic with epoch timestamps reported")
	}
	deleted, skipped := bulkSkips(t, request("POST", "topics/bulk-delete", bulkBody([]string{"inventory.updated"}, "delete 1 topics", fp, true)))
	if len(deleted) != 0 || !strings.Contains(skipped["inventory.updated"], "last write") {
		t.Fatalf("deleted %v skipped %v", deleted, skipped)
	}
}

func createEmpty(t *testing.T, request func(method, path, body string) *httptest.ResponseRecorder, names ...string) {
	t.Helper()
	for _, n := range names {
		if w := request("POST", "topics", `{"name":"`+n+`","partitions":1,"replicationFactor":1}`); w.Code != 200 {
			t.Fatalf("create %s %d", n, w.Code)
		}
	}
}

func TestBulkDeleteRequestDeadlineSkipsRemainingTopics(t *testing.T) {
	a, f, request := faultyAPI(t)
	names := []string{"slow.a", "slow.b", "slow.c", "slow.d", "slow.e"}
	createEmpty(t, request, names...)
	fp := fingerprints(t, request)
	a.bulkDeadline = 100 * time.Millisecond
	f.freshDelay = 60 * time.Millisecond
	w := request("POST", "topics/bulk-delete", bulkBody(names, "delete 5 topics", fp, true))
	deleted, skipped := bulkSkips(t, w)
	late := []string{}
	for name, reason := range skipped {
		if reason == "request deadline reached" {
			late = append(late, name)
		}
	}
	if len(late) == 0 || len(deleted)+len(skipped) != len(names) {
		t.Fatalf("deleted %v skipped %v", deleted, skipped)
	}
	list := request("GET", "topics?pageSize=200", "").Body.String()
	for _, name := range late {
		if !strings.Contains(list, `"`+name+`"`) {
			t.Fatalf("%s was deleted after the deadline", name)
		}
	}
	for _, name := range deleted {
		if strings.Contains(list, `"`+name+`"`) {
			t.Fatalf("%s reported deleted but still listed", name)
		}
	}
}

func TestBulkDeleteReadsGroupUsageOnceUnlessStale(t *testing.T) {
	a, f, request := faultyAPI(t)
	names := []string{"once.a", "once.b", "once.c"}
	createEmpty(t, request, names...)
	fp := fingerprints(t, request)
	f.usageCalls = 0
	w := request("POST", "topics/bulk-delete", bulkBody(names, "delete 3 topics", fp, true))
	if deleted, _ := bulkSkips(t, w); len(deleted) != 3 || f.usageCalls != 1 {
		t.Fatalf("deleted %v usage reads %d", deleted, f.usageCalls)
	}
	// The clock jumps 6 s with every deletion: usage is re-read before the next delete.
	names = []string{"stale.a", "stale.b", "stale.c"}
	createEmpty(t, request, names...)
	fp = fingerprints(t, request)
	base := a.now()
	ticks := 0
	a.now = func() time.Time { return base.Add(time.Duration(ticks) * 6 * time.Second) }
	f.onDelete = func() { ticks++ }
	f.usageCalls = 0
	w = request("POST", "topics/bulk-delete", bulkBody(names, "delete 3 topics", fp, true))
	if deleted, _ := bulkSkips(t, w); len(deleted) != 3 || f.usageCalls < 3 {
		t.Fatalf("deleted %v usage reads %d", deleted, f.usageCalls)
	}
}

func TestBulkDeleteSkipsEverythingWhenInitialUsageReadFails(t *testing.T) {
	_, f, request := faultyAPI(t)
	fp := fingerprints(t, request)
	f.usageErr = errors.New("boom")
	deleted, skipped := bulkSkips(t, request("POST", "topics/bulk-delete", bulkBody([]string{"scratch.empty"}, "delete 1 topics", fp, true)))
	if len(deleted) != 0 || skipped["scratch.empty"] != "consumer state unavailable" {
		t.Fatalf("deleted %v skipped %v", deleted, skipped)
	}
}
