package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	external "github.com/hawxxx/kaflux/backend/internal/integrations"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProtobufReferenceResolution(t *testing.T) {
	for _, scenario := range []string{"success", "denied", "cycle", "conflict", "depth", "count", "bytes"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				child := registryDecodeSchema{Type: "PROTOBUF", Schema: `syntax="proto3"; message Child {string name=1;}`}
				switch scenario {
				case "cycle":
					child.References = []protobufReference{{Name: "child.proto", Subject: "child", Version: 1}}
				case "depth":
					child.References = []protobufReference{{Name: fmt.Sprintf("next%d.proto", calls), Subject: "next", Version: calls + 1}}
				case "bytes":
					child.Schema = strings.Repeat(" ", 65537)
				}
				json.NewEncoder(w).Encode(child)
			}))
			defer server.Close()
			client, err := external.New(external.Config{ID: "sr", ClusterID: "demo", Kind: "schemas", URL: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			grants := []auth.Grant{{Role: "reader", Cluster: "demo", Action: "schema-read", Pattern: "*"}}
			if scenario == "denied" {
				grants = nil
			}
			resolver := protobufReferenceResolver{client: client, authorize: func(subject string) bool {
				return (auth.Authorizer{Grants: grants}).Allowed(auth.User{Roles: []string{"reader"}}, "demo", "schema-read", subject)
			}, cache: map[protobufReferenceKey]registryDecodeSchema{}}
			root := registryDecodeSchema{Type: "PROTOBUF", Schema: `syntax="proto3"; import "child.proto"; message Event {Child child=1;}`, References: []protobufReference{{Name: "child.proto", Subject: "child", Version: 1}}}
			if scenario == "success" {
				root.References = append(root.References, root.References[0])
			}
			if scenario == "conflict" {
				root.References = append(root.References, protobufReference{Name: "child.proto", Subject: "other", Version: 1})
			}
			if scenario == "count" {
				for i := 2; i <= 10; i++ {
					root.References = append(root.References, protobufReference{Name: strings.Repeat("x", i) + ".proto", Subject: "child", Version: i})
				}
			}
			sources, err := resolver.resolve(context.Background(), root)
			if scenario == "success" {
				if err != nil || len(sources) != 1 || calls != 1 {
					t.Fatalf("sources=%v calls=%d err=%v", sources, calls, err)
				}
			} else if err == nil {
				t.Fatal("unsafe references accepted")
			}
			if scenario == "denied" && calls != 0 {
				t.Fatal("unauthorized reference fetched")
			}
			if calls > 8 {
				t.Fatalf("reference budget exceeded: %d", calls)
			}
		})
	}
}

func TestProtobufReferencesThroughMessageFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schemas/ids/42/subjects":
			json.NewEncoder(w).Encode([]string{"orders-value"})
		case "/schemas/ids/42":
			json.NewEncoder(w).Encode(registryDecodeSchema{Type: "PROTOBUF", Schema: `syntax="proto3"; import "child.proto"; message Event {Child child=1;}`, References: []protobufReference{{Name: "child.proto", Subject: "child", Version: 1}}})
		case "/subjects/child/versions/1":
			json.NewEncoder(w).Encode(registryDecodeSchema{Type: "PROTOBUF", Schema: `syntax="proto3";message Child {string name=1;}`})
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	client, _ := external.New(external.Config{ID: "sr", ClusterID: "demo", Kind: "schemas", URL: server.URL})
	defer client.Close()
	a := New(Options{Demo: true, Grants: []auth.Grant{{Role: "reader", Cluster: "demo", Action: "schema-read", Pattern: "*"}}})
	wire := base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 0, 42, 0, 10, 3, 10, 1, 'x'})
	messages := []model.Message{{KeyBase64: wire, ValueBase64: wire}}
	a.decodeMessageFields(context.Background(), auth.User{Roles: []string{"reader"}}, "demo", client, messages, "protobuf", "both")
	if messages[0].KeyDecodeError != "" || messages[0].DecodeError != "" || !strings.Contains(string(messages[0].DecodedKey), `"name":"x"`) || !strings.Contains(string(messages[0].DecodedValue), `"name":"x"`) || messages[0].ValueBase64 != wire {
		t.Fatalf("%+v", messages)
	}
}

func TestProtobufReferenceSharedBudgetsAndCachedAuthorization(t *testing.T) {
	resolver := protobufReferenceResolver{authorize: func(string) bool { return true }, cache: map[protobufReferenceKey]registryDecodeSchema{}, fetches: 8}
	root := registryDecodeSchema{References: []protobufReference{{Name: "child.proto", Subject: "child", Version: 1}}}
	if _, err := resolver.resolve(context.Background(), root); err == nil {
		t.Fatal("reset shared fetch budget")
	}
	resolver.cache[protobufReferenceKey{"child", 1}] = registryDecodeSchema{Type: "PROTOBUF", Schema: `syntax="proto3";`}
	if _, err := resolver.resolve(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	resolver.authorize = func(string) bool { return false }
	if _, err := resolver.resolve(context.Background(), root); err == nil {
		t.Fatal("cached authorization bypass")
	}
}

func TestProtobufCachedReferenceDepth(t *testing.T) {
	resolver := protobufReferenceResolver{authorize: func(string) bool { return true }, cache: map[protobufReferenceKey]registryDecodeSchema{}}
	for i := 1; i <= 9; i++ {
		child := registryDecodeSchema{Type: "PROTOBUF", Schema: `syntax="proto3";`}
		if i < 9 {
			child.References = []protobufReference{{Name: fmt.Sprintf("level%d.proto", i+1), Subject: "child", Version: i + 1}}
		}
		resolver.cache[protobufReferenceKey{"child", i}] = child
	}
	root := registryDecodeSchema{References: []protobufReference{{Name: "level1.proto", Subject: "child", Version: 1}}}
	if _, err := resolver.resolve(context.Background(), root); err == nil {
		t.Fatal("accepted excessive cached graph depth")
	}
}
