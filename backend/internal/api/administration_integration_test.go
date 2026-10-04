package api

import (
	"context"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNativeAdministrationAPIIntegration(t *testing.T) {
	seed := os.Getenv("KAFLUX_TEST_KAFKA_SEED")
	if seed == "" {
		t.Skip("set KAFLUX_TEST_KAFKA_SEED for real Kafka integration")
	}
	p, e := kafka.NewNative(kafka.Config{Seeds: []string{seed}, AllowPlaintext: true})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	s, e := store.New(context.Background(), "")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	session := auth.NewSession(auth.User{ID: "integration-admin", Roles: []string{"administrator"}, Provider: "local"})
	if e = s.SaveSession(context.Background(), session); e != nil {
		t.Fatal(e)
	}
	a := New(Options{Store: s, Providers: map[string]kafka.Provider{"native": p}, Clusters: []model.Cluster{{ID: "native", Kind: "Kafka"}}})
	request := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, "/api/v1/clusters/native/"+path, strings.NewReader(body))
		r.AddCookie(&http.Cookie{Name: "kaflux_session", Value: session.ID})
		r.Header.Set("X-CSRF-Token", session.CSRF)
		a.ServeHTTP(w, r)
		return w
	}
	topic := fmt.Sprintf("kaflux-api-admin-%d", time.Now().UnixNano())
	defer p.DeleteTopic(context.Background(), topic)
	create := request("POST", "topics", fmt.Sprintf(`{"name":%q,"partitions":1,"replicationFactor":1,"config":{"retention.ms":"60000"}}`, topic))
	if create.Code != 200 {
		t.Fatalf("create %d %s", create.Code, create.Body.String())
	}
	var config *httptest.ResponseRecorder
	for attempt := 0; attempt < 50; attempt++ {
		config = request("GET", "topics/"+topic+"/config", "")
		if config.Code == 200 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if config.Code != 200 || !strings.Contains(config.Body.String(), `"retention.ms":"60000"`) {
		t.Fatalf("config %d %s", config.Code, config.Body.String())
	}
	diagnostic := request("POST", "test", `{"confirmation":true}`)
	if diagnostic.Code != 200 || !strings.Contains(diagnostic.Body.String(), `"brokerCount":3`) {
		t.Fatalf("diagnostic %d %s", diagnostic.Code, diagnostic.Body.String())
	}
	arbitraryURL := request("POST", "test", `{"url":"http://127.0.0.1/private"}`)
	if arbitraryURL.Code != 400 {
		t.Fatal("client diagnostic URL accepted")
	}
	aclList := request("GET", "acls", "")
	if aclList.Code != 422 || !strings.Contains(aclList.Body.String(), "acl_unsupported") {
		t.Fatalf("disabled authorizer list %d %s", aclList.Code, aclList.Body.String())
	}
	aclCreate := request("POST", "acls", aclTestBody)
	if aclCreate.Code != 422 || !strings.Contains(aclCreate.Body.String(), "acl_unsupported") {
		t.Fatalf("disabled authorizer mutation %d %s", aclCreate.Code, aclCreate.Body.String())
	}
	a.o.Grants = []auth.Grant{{Role: "administrator", Cluster: "native", Action: "read", Pattern: "*"}}
	denied := request("DELETE", "topics/"+topic, fmt.Sprintf(`{"confirmation":%q}`, topic))
	if denied.Code != 403 {
		t.Fatal("read-only role deleted native topic")
	}
	a.o.Grants = []auth.Grant{{Role: "administrator", Cluster: "native", Action: "*", Pattern: "*"}}
	deleted := request("DELETE", "topics/"+topic, fmt.Sprintf(`{"confirmation":%q}`, topic))
	if deleted.Code != 200 {
		t.Fatalf("delete %d %s", deleted.Code, deleted.Body.String())
	}
	events, _ := s.Audits(context.Background())
	if len(events) != 4 {
		t.Fatalf("expected four audits, got %d", len(events))
	}
	for _, event := range events {
		if event.ClusterID != "native" || event.Provider != "Kafka" || event.Actor != "integration-admin" {
			t.Fatalf("audit attribution %+v", event)
		}
	}
}
