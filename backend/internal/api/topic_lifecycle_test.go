package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"github.com/twmb/franz-go/pkg/kerr"
	"net/http/httptest"
	"strings"
	"testing"
)

func lifecycleAPI(t *testing.T, grants []auth.Grant) (*API, func(method, path, body string) *httptest.ResponseRecorder) {
	t.Helper()
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo"}}, Grants: grants})
	return a, func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, "/api/v1/clusters/demo/"+path, strings.NewReader(body))
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		a.ServeHTTP(w, r)
		return w
	}
}

func topicRows(t *testing.T, w *httptest.ResponseRecorder) []map[string]any {
	t.Helper()
	var out struct{ Data []map[string]any }
	if e := json.Unmarshal(w.Body.Bytes(), &out); e != nil || w.Code != 200 {
		t.Fatalf("topics %d %s", w.Code, w.Body.String())
	}
	return out.Data
}

func TestTopicListReportsInternalTopicsAndMessageCounts(t *testing.T) {
	_, request := lifecycleAPI(t, nil)
	if w := request("POST", "topics", `{"name":"_schemas","partitions":1,"replicationFactor":1,"config":{"cleanup.policy":"compact"}}`); w.Code != 200 {
		t.Fatalf("create %d %s", w.Code, w.Body.String())
	}
	all := topicRows(t, request("GET", "topics?pageSize=200", ""))
	byName := map[string]map[string]any{}
	for _, row := range all {
		byName[row["name"].(string)] = row
	}
	if byName["_schemas"]["internal"] != true || byName["orders.created"]["internal"] != false {
		t.Fatalf("internal flags %v", all)
	}
	// Demo topics hold 24 records in each of 12 partitions.
	if byName["orders.created"]["messages"] != float64(288) {
		t.Fatalf("message count %v", byName["orders.created"]["messages"])
	}
	visible := topicRows(t, request("GET", "topics?pageSize=200&internal=false", ""))
	if len(visible) != len(all)-1 {
		t.Fatalf("internal topics not hidden: %d of %d", len(visible), len(all))
	}
}

func TestTruncateTopicRequiresConfirmationAndDeletePolicy(t *testing.T) {
	_, request := lifecycleAPI(t, nil)
	if w := request("POST", "topics/orders.created/truncate", `{"confirmation":"other"}`); w.Code != 400 {
		t.Fatalf("unconfirmed truncate %d", w.Code)
	}
	if w := request("POST", "topics/orders.created/truncate", `{"confirmation":"orders.created"}`); w.Code != 200 {
		t.Fatalf("truncate %d %s", w.Code, w.Body.String())
	}
	detail := request("GET", "topics/orders.created", "")
	if !strings.Contains(detail.Body.String(), `"messages":0`) {
		t.Fatalf("records remain %s", detail.Body.String())
	}
	request("POST", "topics", `{"name":"compacted","partitions":1,"replicationFactor":1,"config":{"cleanup.policy":"compact"}}`)
	if w := request("POST", "topics/compacted/truncate", `{"confirmation":"compacted"}`); w.Code != 422 {
		t.Fatalf("compacted topic truncated %d %s", w.Code, w.Body.String())
	}
}

func TestRecreateTopicKeepsLayoutAndOverrides(t *testing.T) {
	_, request := lifecycleAPI(t, nil)
	request("POST", "topics", `{"name":"payments.retry","partitions":4,"replicationFactor":2,"config":{"retention.ms":"1000","max.message.bytes":"2048"}}`)
	request("POST", "messages", `{"topic":"payments.retry","partition":0,"value":"x"}`)
	if w := request("POST", "topics/payments.retry/recreate", `{"confirmation":"payments.retry"}`); w.Code != 200 {
		t.Fatalf("recreate %d %s", w.Code, w.Body.String())
	}
	detail := request("GET", "topics/payments.retry", "")
	var envelope struct {
		Data struct {
			Partitions        []model.Partition `json:"partitions"`
			ReplicationFactor int               `json:"replicationFactor"`
			Messages          int64             `json:"messages"`
		}
	}
	_ = json.Unmarshal(detail.Body.Bytes(), &envelope)
	if len(envelope.Data.Partitions) != 4 || envelope.Data.ReplicationFactor != 2 || envelope.Data.Messages != 0 {
		t.Fatalf("recreated topic %s", detail.Body.String())
	}
	config := request("GET", "topics/payments.retry/config", "").Body.String()
	if !strings.Contains(config, `"name":"max.message.bytes","value":"2048","source":"DYNAMIC_TOPIC_CONFIG"`) {
		t.Fatalf("override lost %s", config)
	}
}

func TestCopyTopicValidatesAgainstSourceAndTargetPermission(t *testing.T) {
	_, request := lifecycleAPI(t, nil)
	if w := request("POST", "topics/orders.created/copy", `{"name":"orders.copy","partitions":2,"replicationFactor":1,"config":{"compression.type":"zstd"}}`); w.Code != 200 {
		t.Fatalf("copy %d %s", w.Code, w.Body.String())
	}
	if w := request("POST", "topics/orders.created/copy", `{"name":"orders.copy2","partitions":2,"replicationFactor":1,"config":{"not.a.key":"1"}}`); w.Code != 400 {
		t.Fatalf("unknown key accepted %d", w.Code)
	}
	if w := request("POST", "topics/orders.created/copy", `{"name":"__reserved","partitions":1,"replicationFactor":1}`); w.Code != 400 {
		t.Fatalf("reserved name accepted %d", w.Code)
	}

	// Reading the source is not enough to create the copy.
	a, limited := lifecycleAPI(t, []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "read", Pattern: "*"}})
	a.demo.User.Roles = []string{"viewer"}
	if w := limited("POST", "topics/orders.created/copy", `{"name":"orders.copy","partitions":1,"replicationFactor":1}`); w.Code != 403 {
		t.Fatalf("copy without create %d", w.Code)
	}
	for _, op := range []string{"truncate", "recreate"} {
		if w := limited("POST", "topics/orders.created/"+op, `{"confirmation":"orders.created"}`); w.Code != 403 {
			t.Fatalf("%s without delete %d", op, w.Code)
		}
	}
	// Recreate deletes and creates, so delete alone is not enough.
	a, deleter := lifecycleAPI(t, []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "read", Pattern: "*"}, {Role: "viewer", Cluster: "demo", Action: "delete", Pattern: "*"}})
	a.demo.User.Roles = []string{"viewer"}
	if w := deleter("POST", "topics/orders.created/recreate", `{"confirmation":"orders.created"}`); w.Code != 403 {
		t.Fatalf("recreate without create %d", w.Code)
	}
}

// pendingDelete answers TOPIC_ALREADY_EXISTS for a few creations, as a broker
// does while an asynchronous deletion is still in progress.
type pendingDelete struct {
	kafka.AdminProvider
	busy, creates int
}

func (p *pendingDelete) DeleteTopic(context.Context, string) error { return nil }
func (p *pendingDelete) CreateTopic(context.Context, model.TopicCreate) error {
	p.creates++
	if p.creates <= p.busy {
		return kerr.TopicAlreadyExists
	}
	return nil
}

func TestRecreateTopicRetriesWhileDeletionCompletes(t *testing.T) {
	p := &pendingDelete{busy: 2}
	if e := recreateTopic(context.Background(), p, model.TopicCreate{Name: "x"}); e != nil || p.creates != 3 {
		t.Fatalf("recreate %v after %d creates", e, p.creates)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := recreateTopic(ctx, &pendingDelete{busy: 100}, model.TopicCreate{Name: "x"}); !errors.Is(e, errRecreateIncomplete) {
		t.Fatalf("incomplete recreate reported as %v", e)
	}
}
