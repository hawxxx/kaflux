package api

import (
	"context"
	"encoding/base64"
	external "github.com/hawxxx/kaflux/backend/internal/integrations"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestKeyDecodingPreservesBytesAndSharesSchemaLookup(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path == "/schemas/ids/42/subjects" {
			w.Write([]byte(`["orders-key","orders-value"]`))
		} else {
			w.Write([]byte(`{"schema":"\"string\""}`))
		}
	}))
	defer server.Close()
	client, _ := external.New(external.Config{ID: "sr", ClusterID: "demo", Kind: "schemas", URL: server.URL})
	defer client.Close()
	a := New(Options{Demo: true})
	raw := base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 0, 42, 6, 'a', 'b', 'c'})
	messages := []model.Message{{KeyBase64: raw, ValueBase64: raw}, {KeyBase64: "bad", ValueBase64: raw}}
	a.decodeMessageFields(context.Background(), a.demo.User, "demo", client, messages, "avro", "both")
	if string(messages[0].DecodedKey) != `"abc"` || string(messages[0].DecodedValue) != `"abc"` || messages[0].KeyBase64 != raw || calls != 2 {
		t.Fatalf("%+v calls=%d", messages, calls)
	}
	if messages[1].KeyDecodeError == "" || messages[1].DecodeError != "" || string(messages[1].DecodedValue) != `"abc"` {
		t.Fatal("key error affected value")
	}
	onlyKey := []model.Message{{KeyBase64: raw, ValueBase64: "raw"}}
	a.decodeMessageFields(context.Background(), a.demo.User, "demo", client, onlyKey, "avro", "key")
	if len(onlyKey[0].DecodedKey) == 0 || len(onlyKey[0].DecodedValue) != 0 || onlyKey[0].DecodeError != "" {
		t.Fatal("key-only selection decoded value")
	}
}
