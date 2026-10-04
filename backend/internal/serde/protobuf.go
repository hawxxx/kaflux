package serde

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"path"
	"strings"
)

type ProtobufDecoder struct{ file protoreflect.FileDescriptor }

func NewProtobufDecoder(ctx context.Context, schema string) (*ProtobufDecoder, error) {
	return NewProtobufDecoderWithSources(ctx, schema, nil)
}

// ValidProtobufImportName excludes ambiguous paths and the reserved root name.
func ValidProtobufImportName(name string) bool {
	return name != "" && name != "message.proto" && path.Clean(name) == name && !strings.HasPrefix(name, "/") && !strings.HasPrefix(name, "../") && !strings.ContainsAny(name, "\\:\x00") && strings.HasSuffix(name, ".proto")
}

func NewProtobufDecoderWithSources(ctx context.Context, schema string, sources map[string]string) (*ProtobufDecoder, error) {
	if len(schema) > 64*1024 {
		return nil, errors.New("schema exceeds decoder limit")
	}
	inputs := map[string]string{"message.proto": schema}
	total := len(schema)
	if len(sources) > 8 {
		return nil, errors.New("schema import count exceeds decoder limit")
	}
	for name, source := range sources {
		if !ValidProtobufImportName(name) || len(source) > 64*1024 {
			return nil, errors.New("invalid or oversized Protobuf import")
		}
		total += len(source)
		inputs[name] = source
	}
	if total > 256*1024 {
		return nil, errors.New("schema sources exceed decoder limit")
	}
	compiler := protocompile.Compiler{MaxParallelism: 1, Resolver: &protocompile.SourceResolver{Accessor: protocompile.SourceAccessorFromMap(inputs)}}
	files, err := compiler.Compile(ctx, "message.proto")
	if err != nil || len(files) != 1 {
		return nil, errors.New("invalid Protobuf schema or unsupported import")
	}
	nodes := 0
	var check func(protoreflect.MessageDescriptors, int) error
	check = func(messages protoreflect.MessageDescriptors, depth int) error {
		if depth > 24 {
			return errors.New("schema nesting exceeds decoder limit")
		}
		for i := 0; i < messages.Len(); i++ {
			m := messages.Get(i)
			nodes += 1 + m.Fields().Len()
			if nodes > 2048 || m.Fields().Len() > 256 {
				return errors.New("schema exceeds complexity limit")
			}
			if err := check(m.Messages(), depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	seen := map[string]bool{}
	var checkFile func(protoreflect.FileDescriptor, int) error
	checkFile = func(file protoreflect.FileDescriptor, depth int) error {
		if depth > 8 {
			return errors.New("schema import depth exceeds decoder limit")
		}
		if seen[file.Path()] {
			return nil
		}
		seen[file.Path()] = true
		if err := check(file.Messages(), 0); err != nil {
			return err
		}
		for i := 0; i < file.Imports().Len(); i++ {
			if err := checkFile(file.Imports().Get(i).FileDescriptor, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := checkFile(files[0], 0); err != nil {
		return nil, err
	}
	return &ProtobufDecoder{file: files[0]}, nil
}

func (d *ProtobufDecoder) Decode(wire []byte) (json.RawMessage, error) {
	if _, err := SchemaID(wire); err != nil {
		return nil, err
	}
	payload := wire[5:]
	readIndex := func() (int64, error) {
		v, n := protowire.ConsumeVarint(payload)
		if n < 0 {
			return 0, errors.New("malformed Protobuf message indexes")
		}
		payload = payload[n:]
		return protowire.DecodeZigZag(v), nil
	}
	count, err := readIndex()
	if err != nil {
		return nil, err
	}
	if count < 0 || count > 16 {
		return nil, errors.New("Protobuf message index depth exceeds limit")
	}
	indexes := []int64{0}
	if count != 0 {
		indexes = make([]int64, int(count))
		for i := range indexes {
			indexes[i], err = readIndex()
			if err != nil {
				return nil, err
			}
		}
	}
	messages := d.file.Messages()
	var descriptor protoreflect.MessageDescriptor
	for _, index := range indexes {
		if index < 0 || index >= int64(messages.Len()) {
			return nil, errors.New("Protobuf message index is outside schema")
		}
		descriptor = messages.Get(int(index))
		messages = descriptor.Messages()
	}
	budget := 10000
	if err := checkProtobufPayload(descriptor, payload, 0, &budget); err != nil {
		return nil, err
	}
	message := dynamicpb.NewMessage(descriptor)
	if err := (proto.UnmarshalOptions{RecursionLimit: 24}).Unmarshal(payload, message); err != nil {
		return nil, errors.New("malformed Protobuf payload")
	}
	output, err := protojson.Marshal(message)
	if err != nil || len(output) > MaxBytes {
		return nil, errors.New("decoded payload exceeds JSON preview limit")
	}
	return output, nil
}

// Validate allocation and nesting bounds before dynamic unmarshalling. Packed
// values also count towards the budget, including zero-byte nested messages.
func checkProtobufPayload(descriptor protoreflect.MessageDescriptor, payload []byte, depth int, budget *int) error {
	if depth > 24 {
		return errors.New("Protobuf payload nesting exceeds limit")
	}
	for len(payload) > 0 {
		*budget--
		if *budget < 0 {
			return errors.New("Protobuf field count exceeds decoder limit")
		}
		number, typ, n := protowire.ConsumeTag(payload)
		if n < 0 || number <= 0 || typ == protowire.StartGroupType || typ == protowire.EndGroupType {
			return errors.New("malformed or unsupported Protobuf wire field")
		}
		payload = payload[n:]
		consumed := protowire.ConsumeFieldValue(number, typ, payload)
		if consumed < 0 {
			return errors.New("malformed Protobuf wire value")
		}
		field := descriptor.Fields().ByNumber(protoreflect.FieldNumber(number))
		if typ == protowire.BytesType && field != nil {
			bytes, n := protowire.ConsumeBytes(payload)
			if n < 0 {
				return errors.New("malformed Protobuf bytes")
			}
			if field.Kind() == protoreflect.MessageKind {
				if err := checkProtobufPayload(field.Message(), bytes, depth+1, budget); err != nil {
					return err
				}
			}
			if field.IsList() && field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.StringKind && field.Kind() != protoreflect.BytesKind {
				switch field.Kind() {
				case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind, protoreflect.FloatKind:
					*budget -= len(bytes) / 4
				case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind, protoreflect.DoubleKind:
					*budget -= len(bytes) / 8
				default:
					for len(bytes) > 0 {
						_, n := protowire.ConsumeVarint(bytes)
						if n < 0 {
							return errors.New("malformed packed Protobuf value")
						}
						bytes = bytes[n:]
						*budget--
						if *budget < 0 {
							return errors.New("Protobuf field count exceeds decoder limit")
						}
					}
				}
				if *budget < 0 {
					return errors.New("Protobuf field count exceeds decoder limit")
				}
			}
		}
		payload = payload[consumed:]
	}
	return nil
}
