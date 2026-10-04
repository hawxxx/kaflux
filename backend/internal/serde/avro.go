// Package serde decodes bounded message previews without changing original bytes.
package serde

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/linkedin/goavro/v2"
)

const MaxBytes = 512 * 1024

func SchemaID(wire []byte) (uint32, error) {
	if len(wire) < 5 || len(wire) > MaxBytes || wire[0] != 0 {
		return 0, errors.New("invalid or oversized Confluent wire envelope")
	}
	id := binary.BigEndian.Uint32(wire[1:5])
	if id == 0 {
		return 0, errors.New("invalid schema ID")
	}
	return id, nil
}

// DecodeAvro supports bounded inline records, unions and primitive/enum/fixed values.
// Collections and named references are rejected before compilation: their decoded
// allocation and recursion cannot be bounded by the encoded preview size alone.
func DecodeAvro(schema string, wire []byte) (json.RawMessage, error) {
	if _, err := SchemaID(wire); err != nil {
		return nil, err
	}
	if len(schema) > 64*1024 {
		return nil, errors.New("schema exceeds decoder limit")
	}
	var definition any
	if json.Unmarshal([]byte(schema), &definition) != nil {
		return nil, errors.New("invalid Avro schema")
	}
	nodes := 0
	var check func(any, int) error
	check = func(value any, depth int) error {
		nodes++
		if depth > 24 || nodes > 2048 {
			return errors.New("schema exceeds complexity limit")
		}
		switch v := value.(type) {
		case string:
			switch v {
			case "null", "boolean", "int", "long", "float", "double", "bytes", "string":
				return nil
			}
			return errors.New("named references are not supported by the bounded decoder")
		case []any:
			if len(v) > 64 {
				return errors.New("union exceeds decoder limit")
			}
			for _, child := range v {
				if err := check(child, depth+1); err != nil {
					return err
				}
			}
		case map[string]any:
			typ, ok := v["type"].(string)
			if !ok {
				return errors.New("invalid Avro type")
			}
			switch typ {
			case "record":
				fields, ok := v["fields"].([]any)
				if !ok || len(fields) > 256 {
					return errors.New("record exceeds decoder limit")
				}
				for _, field := range fields {
					f, ok := field.(map[string]any)
					if !ok {
						return errors.New("invalid field")
					}
					if err := check(f["type"], depth+1); err != nil {
						return err
					}
				}
			case "enum":
				symbols, ok := v["symbols"].([]any)
				if !ok || len(symbols) > 256 {
					return errors.New("enum exceeds decoder limit")
				}
			case "fixed":
				size, ok := v["size"].(float64)
				if !ok || size < 1 || size > MaxBytes {
					return errors.New("fixed value exceeds decoder limit")
				}
			case "array", "map":
				return errors.New("Avro collections are not supported by the bounded decoder")
			default:
				return check(typ, depth+1)
			}
		default:
			return errors.New("invalid Avro schema")
		}
		return nil
	}
	if err := check(definition, 0); err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, errors.New("invalid or unsupported Avro schema")
	}
	value, remaining, err := codec.NativeFromBinary(wire[5:])
	if err != nil || len(remaining) != 0 {
		return nil, errors.New("malformed Avro payload or trailing bytes")
	}
	output, err := json.Marshal(value)
	if err != nil || len(output) > MaxBytes {
		return nil, errors.New("decoded payload exceeds JSON preview limit")
	}
	return output, nil
}
