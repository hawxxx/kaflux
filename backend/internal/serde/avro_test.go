package serde

import (
	"encoding/json"
	"testing"
)

func TestAvroWireRecord(t *testing.T) {
	schema := `{"type":"record","name":"Event","fields":[{"name":"name","type":"string"},{"name":"count","type":"long"}]}`
	got, err := DecodeAvro(schema, []byte{0, 0, 0, 0, 42, 6, 'a', 'b', 'c', 14})
	if err != nil || string(got) != `{"count":7,"name":"abc"}` {
		t.Fatalf("decoded=%s error=%v", got, err)
	}
	if !json.Valid(got) {
		t.Fatal("not JSON")
	}
}

func TestAvroRejectsMalformedAndUnboundedSchemas(t *testing.T) {
	for _, test := range []struct {
		schema string
		wire   []byte
	}{
		{`"string"`, []byte{1, 0, 0, 0, 1, 0}},
		{`"string"`, []byte{0, 0, 0, 0, 1, 6, 'a'}},
		{`"string"`, []byte{0, 0, 0, 0, 1, 0, 1}},
		{`{"type":"array","items":"null"}`, []byte{0, 0, 0, 0, 1, 0}},
		{`{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}`, []byte{0, 0, 0, 0, 1, 0}},
	} {
		if _, err := DecodeAvro(test.schema, test.wire); err == nil {
			t.Fatalf("accepted schema=%s wire=%v", test.schema, test.wire)
		}
	}
}

func TestAvroSchemaID(t *testing.T) {
	if id, err := SchemaID([]byte{0, 0, 0, 1, 0}); err != nil || id != 256 {
		t.Fatalf("%d %v", id, err)
	}
	for _, wire := range [][]byte{nil, {0, 0}, {1, 0, 0, 0, 1}, {0, 0, 0, 0, 0}} {
		if _, err := SchemaID(wire); err == nil {
			t.Fatal("accepted invalid envelope")
		}
	}
}
