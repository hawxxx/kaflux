package serde

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	rawProtobufFieldBudget = 100000
	rawProtobufFileLimit   = 512 * 1024
	rawProtobufTotalLimit  = 2 * 1024 * 1024
	rawProtobufMaxFiles    = 64
)

// RawProtobufDecoder decodes unframed Protobuf values with one fixed message type.
// OpenSync telemetry is written this way: no Confluent header and no registry.
type RawProtobufDecoder struct{ message protoreflect.MessageDescriptor }

// NewRawProtobufDecoder compiles root and its imports from sources, then selects the
// message by its full name, for example sts.Report. Source names are slash paths
// relative to the schema directory.
func NewRawProtobufDecoder(ctx context.Context, root string, sources map[string]string, message string) (*RawProtobufDecoder, error) {
	if len(sources) == 0 || len(sources) > rawProtobufMaxFiles {
		return nil, errors.New("Protobuf schema file count is outside decoder limits")
	}
	total := 0
	for name, source := range sources {
		if !ValidProtobufImportName(name) || len(source) > rawProtobufFileLimit {
			return nil, errors.New("invalid or oversized Protobuf schema file")
		}
		total += len(source)
	}
	if total > rawProtobufTotalLimit {
		return nil, errors.New("Protobuf schema files exceed decoder limit")
	}
	if _, ok := sources[root]; !ok {
		return nil, errors.New("Protobuf root file is not among the schema files")
	}
	compiler := protocompile.Compiler{MaxParallelism: 1, Resolver: &protocompile.SourceResolver{Accessor: protocompile.SourceAccessorFromMap(sources)}}
	files, err := compiler.Compile(ctx, root)
	if err != nil || len(files) != 1 {
		return nil, errors.New("invalid Protobuf schema or unresolved import")
	}
	if err := checkProtobufSchema(files[0]); err != nil {
		return nil, err
	}
	d := findMessage(files[0].Messages(), protoreflect.FullName(message))
	if d == nil {
		return nil, errors.New("message " + message + " is not defined in " + root)
	}
	return &RawProtobufDecoder{message: d}, nil
}

func findMessage(messages protoreflect.MessageDescriptors, name protoreflect.FullName) protoreflect.MessageDescriptor {
	for i := 0; i < messages.Len(); i++ {
		m := messages.Get(i)
		if m.FullName() == name {
			return m
		}
		if inner := findMessage(m.Messages(), name); inner != nil {
			return inner
		}
	}
	return nil
}

// MessageName is the full name of the message this decoder produces.
func (d *RawProtobufDecoder) MessageName() string { return string(d.message.FullName()) }

func (d *RawProtobufDecoder) Decode(payload []byte) (json.RawMessage, error) {
	budget := rawProtobufFieldBudget
	if err := checkProtobufPayload(d.message, payload, 0, &budget); err != nil {
		return nil, err
	}
	message := dynamicpb.NewMessage(d.message)
	// Required fields of proto2 schemas may be absent in captured reports.
	if err := (proto.UnmarshalOptions{RecursionLimit: 24, AllowPartial: true}).Unmarshal(payload, message); err != nil {
		return nil, errors.New("malformed Protobuf payload")
	}
	output, err := (protojson.MarshalOptions{AllowPartial: true}).Marshal(message)
	if err != nil || len(output) > MaxBytes {
		return nil, errors.New("decoded payload exceeds JSON preview limit")
	}
	return output, nil
}
