package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	external "github.com/hawxxx/kaflux/backend/internal/integrations"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMessageDecodingPreservesBytesAndEnforcesSubjectScope(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch r.URL.Path {
		case "/schemas/ids/42/subjects":
			w.Write([]byte(`["orders-value"]`))
		case "/schemas/ids/42":
			w.Write([]byte(`{"schema":"\"string\""}`))
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	client, err := external.New(external.Config{ID: "sr", ClusterID: "demo", Kind: "schemas", URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	a := New(Options{Demo: true, Integrations: map[string]*external.Client{"sr": client}, Grants: []auth.Grant{{Role: "reader", Cluster: "demo", Action: "schema-read", Pattern: "orders-*"}}})
	wire := base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 0, 42, 6, 'a', 'b', 'c'})
	messages := []model.Message{{ValueBase64: wire}, {ValueBase64: wire}, {ValueBase64: wire, Truncated: true}, {ValueBase64: "bad"}}
	user := auth.User{Roles: []string{"reader"}}
	a.decodeMessages(context.Background(), user, "demo", client, messages)
	if string(messages[0].DecodedValue) != `"abc"` || messages[0].ValueBase64 != wire || calls != 2 {
		t.Fatalf("%+v calls=%d", messages, calls)
	}
	if messages[2].DecodeError == "" || messages[3].DecodeError == "" {
		t.Fatal("missing per-record errors")
	}
	denied := []model.Message{{ValueBase64: wire}}
	a.decodeMessages(context.Background(), auth.User{Roles: []string{"denied"}}, "demo", client, denied)
	if denied[0].DecodeError == "" || len(denied[0].DecodedValue) != 0 {
		t.Fatal("unauthorized schema decoded")
	}
	raw, err := json.Marshal(messages)
	if err != nil || len(raw) == 0 {
		t.Fatal(err)
	}
}

func TestMessageDecoderRequestValidationAndRegistryFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	client, _ := external.New(external.Config{ID: "sr", ClusterID: "demo", Kind: "schemas", URL: server.URL})
	defer client.Close()
	s, _ := store.New(context.Background(), "")
	defer s.Close()
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Integrations: map[string]*external.Client{"sr": client}})
	for _, sample := range []struct {
		query  string
		status int
	}{
		{"", 200}, {"&decoderTarget=invalid", 422}, {"&decoder=unsupported&registry=sr", 422}, {"&decoder=protobuf&registry=sr", 200}, {"&decoder=avro&registry=missing", 422}, {"&decoder=avro&registry=sr", 200},
	} {
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/clusters/demo/messages?topic=orders.created&partition=0&offset=0&limit=1"+sample.query, nil))
		if w.Code != sample.status {
			t.Fatalf("%s: %d %s", sample.query, w.Code, w.Body.String())
		}
	}
	records := []model.Message{{ValueBase64: base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 0, 42, 0})}}
	a.decodeMessages(context.Background(), a.demo.User, "demo", client, records)
	if records[0].DecodeError == "" || records[0].ValueBase64 == "" {
		t.Fatal("registry failure hid raw record")
	}
}

func TestMessageDecodingCancellationKeepsRawPreview(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a := New(Options{Demo: true})
	records := []model.Message{{ValueBase64: "AAAAACoGYWJj"}}
	a.decodeMessages(ctx, a.demo.User, "demo", nil, records)
	if records[0].DecodeError != "Decoding request cancelled or capacity unavailable" || records[0].ValueBase64 != "AAAAACoGYWJj" {
		t.Fatalf("%+v", records)
	}
}

func TestProtobufRegistryDecodingChecksFormatAndPreservesRaw(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schemas/ids/42/subjects":
			w.Write([]byte(`["orders-value"]`))
		case "/schemas/ids/42":
			w.Write([]byte(`{"schemaType":"PROTOBUF","schema":"syntax=\"proto3\"; message Event {string name=1;}"}`))
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	client, _ := external.New(external.Config{ID: "sr", ClusterID: "demo", Kind: "schemas", URL: server.URL})
	defer client.Close()
	a := New(Options{Demo: true, Integrations: map[string]*external.Client{"sr": client}})
	raw := base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 0, 42, 0, 10, 3, 'a', 'b', 'c'})
	messages := []model.Message{{ValueBase64: raw}}
	a.decodeMessages(context.Background(), a.demo.User, "demo", client, messages, "protobuf")
	if messages[0].DecodedFormat != "protobuf" || !strings.Contains(string(messages[0].DecodedValue), `"name"`) || messages[0].ValueBase64 != raw {
		t.Fatalf("%+v", messages)
	}
	avro := []model.Message{{ValueBase64: raw}}
	a.decodeMessages(context.Background(), a.demo.User, "demo", client, avro, "avro")
	if avro[0].DecodeError == "" || len(avro[0].DecodedValue) != 0 {
		t.Fatal("wrong decoder accepted schema")
	}
}
