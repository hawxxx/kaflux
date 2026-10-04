package api

import (
	"context"
	"encoding/base64"
	"fmt"
	external "github.com/hawxxx/kaflux/backend/internal/integrations"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestAvroKafkaAPIIntegration(t *testing.T)     { testSchemaKafkaAPI(t, false) }
func TestProtobufKafkaAPIIntegration(t *testing.T) { testSchemaKafkaAPI(t, true) }

func testSchemaKafkaAPI(t *testing.T, protobuf bool) {
	t.Helper()
	seed := os.Getenv("KAFLUX_TEST_KAFKA_SEED")
	if seed == "" {
		t.Skip("set KAFLUX_TEST_KAFKA_SEED for Kafka integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(seed))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)
	topic := fmt.Sprintf("kaflux-avro-%d", time.Now().UnixNano())
	created, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil {
		t.Fatal(err)
	}
	if err = created[topic].Err; err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		admin.DeleteTopics(cleanup, topic)
	}()
	provider, err := kafka.NewNative(kafka.Config{Seeds: []string{seed}, AllowPlaintext: true})
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	wire := []byte{0, 0, 0, 0, 42, 6, 'a', 'b', 'c'}
	format := "avro"
	schemaResponse := `{"schema":"\"string\""}`
	expected := `"decodedValue":"abc"`
	if protobuf {
		wire = []byte{0, 0, 0, 0, 42, 0, 10, 3, 'a', 'b', 'c'}
		format = "protobuf"
		schemaResponse = `{"schemaType":"PROTOBUF","schema":"syntax=\"proto3\";message Event {string name=1;}"}`
		expected = `"decodedFormat":"protobuf"`
	}
	record, err := provider.Produce(ctx, model.Message{Topic: topic, Partition: 0, Key: string(wire), Value: string(wire)})
	if err != nil {
		t.Fatal(err)
	}
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schemas/ids/42/subjects":
			fmt.Fprintf(w, "[%q]", topic+"-value")
		case "/schemas/ids/42":
			w.Write([]byte(schemaResponse))
		default:
			w.WriteHeader(404)
		}
	}))
	defer registry.Close()
	client, err := external.New(external.Config{ID: "sr", ClusterID: "demo", Kind: "schemas", URL: registry.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	state, err := store.New(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	a := New(Options{Demo: true, Store: state, Providers: map[string]kafka.Provider{"demo": provider}, Integrations: map[string]*external.Client{"sr": client}})
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", fmt.Sprintf("/api/v1/clusters/demo/messages?topic=%s&partition=0&offset=%d&limit=1&decoder=%s&registry=sr&decoderTarget=both", topic, record.Offset, format), nil)
	a.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), expected) || !strings.Contains(w.Body.String(), base64.StdEncoding.EncodeToString(wire)) {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"decodedKey":`) || !strings.Contains(w.Body.String(), `"keySchemaId":42`) || strings.Contains(w.Body.String(), `"keyDecodeError"`) {
		t.Fatalf("key not decoded: %s", w.Body.String())
	}
}
