package serde

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

const protoSchema = `syntax="proto3"; message Event {string name=1;int64 count=2; message Child {string id=1;} } message Other {string id=1;}`

func TestProtobufReferencedSources(t *testing.T) {
	root := `syntax="proto3"; import "types/common.proto"; message Event {Child child=1;}`
	decoder, err := NewProtobufDecoderWithSources(context.Background(), root, map[string]string{"types/common.proto": `syntax="proto3"; message Child {string name=1;}`})
	if err != nil {
		t.Fatal(err)
	}
	output, err := decoder.Decode([]byte{0, 0, 0, 0, 42, 0, 10, 3, 10, 1, 'x'})
	if err != nil || !strings.Contains(string(output), `"name":"x"`) {
		t.Fatalf("%s %v", output, err)
	}
	for _, name := range []string{"message.proto", "../private.proto", "/private.proto", `C:\private.proto`} {
		if _, err := NewProtobufDecoderWithSources(context.Background(), root, map[string]string{name: "syntax=\"proto3\";"}); err == nil {
			t.Fatalf("accepted %q", name)
		}
	}
}

func TestProtobufAggregateSourceLimit(t *testing.T) {
	sources := map[string]string{}
	for _, name := range []string{"a.proto", "b.proto", "c.proto", "d.proto"} {
		sources[name] = strings.Repeat(" ", 64*1024)
	}
	if _, err := NewProtobufDecoderWithSources(context.Background(), protoSchema, sources); err == nil {
		t.Fatal("accepted aggregate oversized source graph")
	}
}

func TestProtobufConfluentIndexes(t *testing.T) {
	decoder, err := NewProtobufDecoder(context.Background(), protoSchema)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		wire []byte
		want string
	}{
		{[]byte{0, 0, 0, 0, 42, 0, 10, 3, 'a', 'b', 'c', 16, 7}, `{"name":"abc","count":"7"}`},
		{[]byte{0, 0, 0, 0, 42, 2, 2, 10, 1, 'x'}, `{"id":"x"}`},
		{[]byte{0, 0, 0, 0, 42, 4, 0, 0, 10, 1, 'y'}, `{"id":"y"}`},
	} {
		result, err := decoder.Decode(test.wire)
		if err != nil {
			t.Fatal(err)
		}
		var got, want any
		json.Unmarshal(result, &got)
		json.Unmarshal([]byte(test.want), &want)
		actual, _ := json.Marshal(got)
		expected, _ := json.Marshal(want)
		if string(actual) != string(expected) {
			t.Fatalf("%s != %s", actual, expected)
		}
	}
}

func TestProtobufRejectsMalformedIndexesPayloadAndImports(t *testing.T) {
	decoder, err := NewProtobufDecoder(context.Background(), protoSchema)
	if err != nil {
		t.Fatal(err)
	}
	for _, wire := range [][]byte{
		{0, 0, 0, 0, 42}, {0, 0, 0, 0, 42, 1}, {0, 0, 0, 0, 42, 2, 6}, {0, 0, 0, 0, 42, 0, 10, 5, 'a'},
		{0, 0, 0, 0, 42, 0, 11, 12},
	} {
		if _, err := decoder.Decode(wire); err == nil {
			t.Fatalf("accepted %v", wire)
		}
	}
	for _, schema := range []string{`syntax="proto3"; import "private.proto";message Event {}`, strings.Repeat(" ", 65537), `this is not protobuf`} {
		if _, err := NewProtobufDecoder(context.Background(), schema); err == nil {
			t.Fatal("accepted invalid/imported/oversized schema")
		}
	}
}

func TestProtobufBoundsRepeatedValuesAndRecursion(t *testing.T) {
	decoder, err := NewProtobufDecoder(context.Background(), `syntax="proto3";message Node {Node child=1;repeated bool flags=2;}`)
	if err != nil {
		t.Fatal(err)
	}
	wire := []byte{0, 0, 0, 0, 42, 0, 18, 145, 78}
	wire = append(wire, make([]byte, 10001)...)
	if _, err := decoder.Decode(wire); err == nil {
		t.Fatal("accepted unbounded packed values")
	}
	nested := []byte{}
	for i := 0; i < 30; i++ {
		nested = append([]byte{10, byte(len(nested))}, nested...)
	}
	if _, err := decoder.Decode(append([]byte{0, 0, 0, 0, 42, 0}, nested...)); err == nil {
		t.Fatal("accepted excessive recursion")
	}
}
