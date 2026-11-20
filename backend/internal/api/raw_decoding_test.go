package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/serde"
	"google.golang.org/protobuf/encoding/protowire"
)

func testRawDecoder(t *testing.T) *RawDecoder {
	t.Helper()
	src := map[string]string{"r.proto": `syntax = "proto2"; package sts; message Report { optional string node_id = 1; }`}
	d, err := serde.NewRawProtobufDecoder(context.Background(), "r.proto", src, "sts.Report")
	if err != nil {
		t.Fatal(err)
	}
	return &RawDecoder{ID: "stats", Name: "Stats", Message: "sts.Report", Decoder: d}
}

func wireReport(node string) string {
	b := protowire.AppendTag(nil, 1, protowire.BytesType)
	return base64.StdEncoding.EncodeToString(protowire.AppendString(b, node))
}

func TestDecodeRawFieldsValueKeyAndFailures(t *testing.T) {
	d := testRawDecoder(t)
	msgs := []model.Message{
		{ValueBase64: wireReport("n1"), KeyBase64: wireReport("k1")},
		{ValueBase64: "!!", KeyBase64: wireReport("k2")},
		{ValueBase64: wireReport("n3"), Truncated: true},
	}
	decodeRawFields(msgs, d, "both")
	var v struct {
		NodeID string `json:"nodeId"`
	}
	if err := json.Unmarshal(msgs[0].DecodedValue, &v); err != nil || v.NodeID != "n1" || msgs[0].DecodedFormat != "protobuf-raw" {
		t.Fatalf("value %s %v", msgs[0].DecodedValue, err)
	}
	if err := json.Unmarshal(msgs[0].DecodedKey, &v); err != nil || v.NodeID != "k1" || msgs[0].KeyDecodedFormat != "protobuf-raw" {
		t.Fatalf("key %s %v", msgs[0].DecodedKey, err)
	}
	if msgs[1].DecodeError == "" || msgs[1].DecodedValue != nil || msgs[1].KeyDecodeError != "" {
		t.Fatalf("a bad value must fail alone: %+v", msgs[1])
	}
	if msgs[2].DecodeError == "" {
		t.Fatal("truncated preview was decoded")
	}
	valueOnly := []model.Message{{ValueBase64: wireReport("n"), KeyBase64: wireReport("k")}}
	decodeRawFields(valueOnly, d, "value")
	if valueOnly[0].DecodedKey != nil {
		t.Fatal("key decoded for target value")
	}
}

func TestDecodeRawFieldsSkipsEmptyKeysAndTombstones(t *testing.T) {
	msgs := []model.Message{{ValueBase64: wireReport("n"), KeyBase64: ""}, {ValueBase64: "", KeyBase64: wireReport("k")}}
	decodeRawFields(msgs, testRawDecoder(t), "both")
	if msgs[0].DecodedKey != nil || msgs[0].KeyDecodedFormat != "" || msgs[0].KeyDecodeError != "" {
		t.Fatalf("absent key was decoded: %+v", msgs[0])
	}
	if msgs[1].DecodedValue != nil || msgs[1].DecodedFormat != "" || msgs[1].DecodeError != "" {
		t.Fatalf("tombstone was decoded: %+v", msgs[1])
	}
	if msgs[0].DecodedValue == nil || msgs[1].DecodedKey == nil {
		t.Fatal("present bytes were not decoded")
	}
}

func TestRawDecoderLookup(t *testing.T) {
	a := &API{o: Options{RawDecoders: []RawDecoder{*testRawDecoder(t)}}}
	if a.rawDecoder("stats") == nil || a.rawDecoder("nope") != nil {
		t.Fatal("lookup by id failed")
	}
}
