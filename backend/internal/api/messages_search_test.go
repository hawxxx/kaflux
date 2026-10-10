package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

func TestSearchMessages(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	defer s.Close()
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo"}}})
	post := func(method, body string) (int, kafka.SearchResult) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, "/api/v1/clusters/demo/messages/search", strings.NewReader(body))
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		a.ServeHTTP(w, r)
		var envelope struct{ Data kafka.SearchResult }
		_ = json.Unmarshal(w.Body.Bytes(), &envelope)
		return w.Code, envelope.Data
	}
	code, r := post("POST", `{"topic":"orders.created","match":{"in":"value","text":"ORD-000077"}}`)
	if code != 200 || !r.Done || len(r.Matches) != 1 || r.Matches[0].Partition != 3 {
		t.Fatalf("search: %d %+v", code, r)
	}
	code, r = post("POST", `{"topic":"orders.created","match":{"in":"key","op":"equals","text":"order-3","caseSensitive":true},"maxMatches":4}`)
	if code != 200 || r.Done || len(r.Matches) != 4 || r.StoppedBy != "matches" || len(r.Resume) == 0 {
		t.Fatalf("bounded search: %d %+v", code, r)
	}
	resume, _ := json.Marshal(r.Resume)
	code, r = post("POST", `{"topic":"orders.created","from":{"offsets":`+string(resume)+`},"match":{"in":"key","op":"equals","text":"order-3","caseSensitive":true},"maxMatches":50}`)
	if code != 200 || !r.Done || len(r.Matches) != 8 {
		t.Fatalf("resumed search: %d %+v", code, r)
	}
	for body, want := range map[string]int{
		`{"match":{"text":"x"}}`:                                                                                                                400,
		`{"topic":"orders.created","match":{"text":""}}`:                                                                                        400,
		`{"topic":"orders.created","match":{"op":"regex","text":"("}}`:                                                                          400,
		`{"topic":"orders.created","match":{"in":"body","text":"x"}}`:                                                                           400,
		`{"topic":"orders.created","partitions":[-1],"match":{"text":"x"}}`:                                                                     400,
		`{"topic":"orders.created","from":{"offsets":{"0":-1}},"match":{"text":"x"}}`:                                                           400,
		`{"topic":"orders.created","from":{"offsets":{"0":1}},"partitions":[0],"match":{"text":"x"}}`:                                           400,
		`{"topic":"orders.created","partitioner":"fnv","match":{"text":"x"}}`:                                                                   400,
		`{"topic":"orders.created","from":{"timestamp":"2026-10-01T12:00:05Z"},"to":{"timestamp":"2026-10-01T12:00:01Z"},"match":{"text":"x"}}`: 400,
		`{"topic":"orders.created","extra":1,"match":{"text":"x"}}`:                                                                             400,
		`{"topic":"orders.created","partitions":[99],"match":{"text":"x"}}`:                                                                     422,
		`{"topic":"missing.topic","match":{"text":"x"}}`:                                                                                        422,
	} {
		if code, _ = post("POST", body); code != want {
			t.Fatalf("%s: got %d, want %d", body, code, want)
		}
	}
	if code, _ = post("GET", ""); code != 405 {
		t.Fatalf("GET search: %d", code)
	}
}

func TestSearchMessagesNeedsConsumeOnTheTopic(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	defer s.Close()
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo"}}, Grants: []auth.Grant{
		{Role: "viewer", Cluster: "demo", Action: "consume", Pattern: "orders.*"},
		{Role: "producer", Cluster: "demo", Action: "produce", Pattern: "*"},
	}})
	search := func(roles []string, topic string) int {
		a.demo.User.Roles = roles
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/api/v1/clusters/demo/messages/search", strings.NewReader(`{"topic":"`+topic+`","match":{"text":"x"}}`))
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		a.ServeHTTP(w, r)
		return w.Code
	}
	if code := search([]string{"viewer"}, "orders.created"); code != 200 {
		t.Fatalf("granted topic: %d", code)
	}
	if code := search([]string{"viewer"}, "payments.authorized"); code != 403 {
		t.Fatalf("other topic: %d", code)
	}
	if code := search([]string{"producer"}, "orders.created"); code != 403 {
		t.Fatalf("produce grant must not allow searching: %d", code)
	}
}
