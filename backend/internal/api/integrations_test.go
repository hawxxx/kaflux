package api

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	external "github.com/hawxxx/kaflux/backend/internal/integrations"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOptionalIntegrationsEnforceScopesAndRedactSecrets(t *testing.T) {
	calls := []string{}
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.RequestURI())
		switch r.URL.Path {
		case "/subjects", "/connectors":
			w.Write([]byte(`["orders.created","private.secret"]`))
		case "/connectors/orders.created/config":
			w.Write([]byte(`{"topics":"orders.created","sasl.jaas.config":"private credential","password":"secret"}`))
		case "/subjects/orders.created/versions/latest":
			w.Write([]byte(`{"subject":"orders.created","schema":"{}","schemaType":"AVRO"}`))
		default:
			w.Write([]byte(`{}`))
		}
	}))
	defer service.Close()
	s, _ := store.New(context.Background(), "")
	defer s.Close()
	schema, e := external.New(external.Config{ID: "sr", ClusterID: "demo", Kind: "schemas", URL: service.URL})
	if e != nil {
		t.Fatal(e)
	}
	defer schema.Close()
	connect, e := external.New(external.Config{ID: "connect", ClusterID: "demo", Kind: "connect", URL: service.URL})
	if e != nil {
		t.Fatal(e)
	}
	defer connect.Close()
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Integrations: map[string]*external.Client{"sr": schema, "connect": connect}, Grants: []auth.Grant{{Role: "scoped", Cluster: "demo", Action: "schema-read", Pattern: "orders.*"}, {Role: "scoped", Cluster: "demo", Action: "connector-read", Pattern: "orders.*"}, {Role: "operator", Cluster: "demo", Action: "connector-update", Pattern: "orders.*"}}})
	a.demo.User.Roles = []string{"scoped"}
	request := func(method, path, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/api/v1/clusters/demo/"+path, strings.NewReader(body))
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		return w
	}
	for _, path := range []string{"schemas/sr", "connectors/connect"} {
		w := request("GET", path, "")
		if w.Code != 200 || strings.Contains(w.Body.String(), "private.secret") {
			t.Fatalf("list scope %d %s", w.Code, w.Body.String())
		}
	}
	w := request("GET", "connectors/connect/orders.created/config", "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "private credential") || strings.Contains(w.Body.String(), `"secret"`) {
		t.Fatalf("redaction failed %s", w.Body.String())
	}
	before := len(calls)
	w = request("GET", "schemas/sr/private.secret", "")
	if w.Code != 403 || len(calls) != before {
		t.Fatal("unauthorized upstream call")
	}
	w = request("POST", "connectors/connect/orders.created/pause", `{"confirmation":true}`)
	if w.Code != 403 {
		t.Fatal("read-only mutation accepted")
	}
	a.demo.User.Roles = []string{"operator"}
	w = request("POST", "connectors/connect/orders.created/pause", `{"confirmation":false}`)
	if w.Code != 400 {
		t.Fatal("unconfirmed mutation accepted")
	}
	w = request("POST", "connectors/connect/orders.created/pause", `{"confirmation":true}`)
	if w.Code != 200 || calls[len(calls)-1] != "PUT /connectors/orders.created/pause" {
		t.Fatalf("wrong Connect method %d %+v", w.Code, calls)
	}
	audits, e := s.Audits(context.Background())
	if e != nil || len(audits) != 2 || audits[0].ClusterID != "demo" {
		t.Fatalf("missing mutation audit %+v", audits)
	}
	w = request("GET", "schemas/unknown", "")
	if w.Code != 404 {
		t.Fatal("unknown integration fallback")
	}
}

func TestOptionalIntegrationFailureDoesNotBlockKafka(t *testing.T) {
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "private upstream diagnostic", 503) }))
	defer service.Close()
	client, _ := external.New(external.Config{ID: "sr", ClusterID: "demo", Kind: "schemas", URL: service.URL})
	defer client.Close()
	s, _ := store.New(context.Background(), "")
	defer s.Close()
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Integrations: map[string]*external.Client{"sr": client}})
	for _, sample := range []struct {
		path   string
		status int
	}{{"schemas/sr", 503}, {"topics", 200}} {
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/clusters/demo/"+sample.path, nil))
		if w.Code != sample.status || strings.Contains(w.Body.String(), "private upstream diagnostic") {
			t.Fatal("optional integration failure leaked or blocked Kafka")
		}
	}
}
