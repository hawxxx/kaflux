package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

func TestBalanceReportsBrokerLogBytesWhenEveryBrokerIsSized(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo", Mode: "demo"}}})
	r := httptest.NewRecorder()
	a.ServeHTTP(r, httptest.NewRequest("GET", "/api/v1/clusters/demo/balance", nil))
	if r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	var body struct {
		Data struct {
			Dimensions   []string             `json:"dimensions"`
			Distribution []model.Distribution `json:"distribution"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err, r.Body.String())
	}
	if !slices.Contains(body.Data.Dimensions, "bytes") {
		t.Fatalf("dimensions=%v, want bytes", body.Data.Dimensions)
	}
	for _, d := range body.Data.Distribution {
		if d.Bytes == nil {
			t.Fatalf("broker %d has no bytes", d.Broker)
		}
	}
}
