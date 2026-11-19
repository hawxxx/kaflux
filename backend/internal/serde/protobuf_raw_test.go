package serde

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

const rawSchema = `syntax = "proto2";
package sts;
import "common.proto";
message Report {
  required string node_id = 1;
  repeated Client clients = 2;
  optional uint32 required_later = 3;
}
message Client { optional string mac = 1; optional sint32 rssi = 2; optional Detail detail = 3; }
`

func rawSources() map[string]string {
	return map[string]string{
		"stats.proto":  strings.Replace(rawSchema, `import "common.proto";`, `import "common.proto";`, 1),
		"common.proto": `syntax = "proto2"; package sts; message Detail { optional string band = 1; }`,
	}
}

func rawReport() []byte {
	var client []byte
	client = protowire.AppendTag(client, 1, protowire.BytesType)
	client = protowire.AppendString(client, "aa:bb:cc:dd:ee:ff")
	client = protowire.AppendTag(client, 2, protowire.VarintType)
	client = protowire.AppendVarint(client, protowire.EncodeZigZag(-42))
	var out []byte
	out = protowire.AppendTag(out, 1, protowire.BytesType)
	out = protowire.AppendString(out, "node-1")
	out = protowire.AppendTag(out, 2, protowire.BytesType)
	return protowire.AppendBytes(out, client)
}

func TestRawProtobufDecodesUnframedValue(t *testing.T) {
	d, err := NewRawProtobufDecoder(context.Background(), "stats.proto", rawSources(), "sts.Report")
	if err != nil {
		t.Fatal(err)
	}
	if d.MessageName() != "sts.Report" {
		t.Fatalf("message %q", d.MessageName())
	}
	out, err := d.Decode(rawReport())
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		NodeID  string `json:"nodeId"`
		Clients []struct {
			Mac  string `json:"mac"`
			Rssi int    `json:"rssi"`
		} `json:"clients"`
	}
	if err = json.Unmarshal(out, &got); err != nil || got.NodeID != "node-1" || len(got.Clients) != 1 || got.Clients[0].Rssi != -42 || got.Clients[0].Mac != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("decoded %s (%v)", out, err)
	}
}

func TestRawProtobufToleratesMissingRequiredFields(t *testing.T) {
	d, err := NewRawProtobufDecoder(context.Background(), "stats.proto", rawSources(), "sts.Report")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.Decode(nil); err != nil {
		t.Fatalf("empty report rejected: %v", err)
	}
}

func TestRawProtobufRejectsBadInput(t *testing.T) {
	ctx := context.Background()
	if _, err := NewRawProtobufDecoder(ctx, "stats.proto", rawSources(), "sts.Missing"); err == nil {
		t.Fatal("unknown message accepted")
	}
	if _, err := NewRawProtobufDecoder(ctx, "absent.proto", rawSources(), "sts.Report"); err == nil {
		t.Fatal("missing root accepted")
	}
	if _, err := NewRawProtobufDecoder(ctx, "stats.proto", map[string]string{"stats.proto": rawSchema}, "sts.Report"); err == nil {
		t.Fatal("unresolved import accepted")
	}
	if _, err := NewRawProtobufDecoder(ctx, "../x.proto", map[string]string{"../x.proto": "syntax = \"proto2\";"}, "x"); err == nil {
		t.Fatal("path escape accepted")
	}
	d, err := NewRawProtobufDecoder(ctx, "stats.proto", rawSources(), "sts.Report")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.Decode([]byte{0x0a, 0xff, 0x01}); err == nil {
		t.Fatal("truncated field accepted")
	}
}
