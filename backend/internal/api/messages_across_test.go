package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

func TestMessagesAcrossPartitions(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	defer s.Close()
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}})
	read := func(query string) (int, []model.Message) {
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/clusters/demo/messages?topic=orders.created&limit=2"+query, nil))
		var body struct{ Data []model.Message }
		_ = json.Unmarshal(w.Body.Bytes(), &body)
		return w.Code, body.Data
	}
	code, got := read("&offsets=0:0,1:0")
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	perPartition := map[int32]int{}
	for _, m := range got {
		perPartition[m.Partition]++
	}
	if len(perPartition) != 2 || perPartition[0] != 2 || perPartition[1] != 2 {
		t.Fatalf("expected two records from each of two partitions, got %v", perPartition)
	}
	if code, got = read("&offsets=1:1"); code != 200 || len(got) != 2 || got[0].Partition != 1 || got[0].Offset != 1 {
		t.Fatalf("offset ignored: %d %+v", code, got)
	}
	if code, got = read("&offsets=0:0,1:0&timestamp=2026-10-01T12:00:01Z"); code != 200 || len(got) != 4 || got[0].Offset != 1 {
		t.Fatalf("timestamp not applied: %d %+v", code, got)
	}
	for _, bad := range []string{"&offsets=0", "&offsets=a:1", "&offsets=-1:0", "&offsets=0:-1", "&offsets=0:0&timestamp=nope"} {
		if code, _ = read(bad); code != 400 {
			t.Fatalf("%s: %d", bad, code)
		}
	}
	for _, unknown := range []string{"&offsets=99:0", "&offsets=99:0&timestamp=2026-10-01T12:00:01Z"} {
		if code, _ = read(unknown); code != 422 {
			t.Fatalf("%s: unknown partition answered %d", unknown, code)
		}
	}
}

func TestMessagesLimitDefaultsToFifty(t *testing.T) {
	for raw, want := range map[string]int{"": 50, "0": 50, "-3": 50, "x": 50, "7": 7, "500": 100} {
		if got := readLimit(url.Values{"limit": {raw}}); got != want {
			t.Fatalf("limit %q: got %d, want %d", raw, got, want)
		}
	}
}
