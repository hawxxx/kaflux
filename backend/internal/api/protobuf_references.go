package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	external "github.com/hawxxx/kaflux/backend/internal/integrations"
	"github.com/hawxxx/kaflux/backend/internal/serde"
	"net/url"
)

type protobufReference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}
type registryDecodeSchema struct {
	Schema     string              `json:"schema"`
	Type       string              `json:"schemaType"`
	References []protobufReference `json:"references"`
}
type protobufReferenceKey struct {
	subject string
	version int
}

// Request-local state is shared by key/value and all root schema IDs. Fetches
// require subject authorization even when an integration response is cached.
type protobufReferenceResolver struct {
	client    *external.Client
	authorize func(string) bool
	cache     map[protobufReferenceKey]registryDecodeSchema
	fetches   int
	bytes     int
}

func (r *protobufReferenceResolver) resolve(ctx context.Context, root registryDecodeSchema) (map[string]string, error) {
	sources := map[string]string{}
	names := map[string]protobufReferenceKey{}
	visiting := map[protobufReferenceKey]bool{}
	completed := map[protobufReferenceKey]bool{}
	var walk func(registryDecodeSchema, int) error
	walk = func(schema registryDecodeSchema, depth int) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if depth > 8 || len(schema.References) > 16 {
			return errors.New("Protobuf reference graph exceeds decoder limit")
		}
		for _, ref := range schema.References {
			if !serde.ValidProtobufImportName(ref.Name) || ref.Subject == "" || len(ref.Subject) > 1024 || ref.Version <= 0 {
				return errors.New("Invalid Protobuf schema reference")
			}
			if !r.authorize(ref.Subject) {
				return errors.New("Schema registry unavailable or schema subject permission denied")
			}
			key := protobufReferenceKey{ref.Subject, ref.Version}
			if previous, ok := names[ref.Name]; ok && previous != key {
				return errors.New("Conflicting Protobuf import names")
			}
			names[ref.Name] = key
			if visiting[key] {
				return errors.New("Cyclic Protobuf schema references")
			}
			child, ok := r.cache[key]
			if !ok {
				if r.fetches >= 8 {
					return errors.New("Protobuf reference lookup limit reached")
				}
				r.fetches++
				raw, err := r.client.Do(ctx, "GET", fmt.Sprintf("/subjects/%s/versions/%d", url.PathEscape(ref.Subject), ref.Version), nil)
				if err != nil || json.Unmarshal(raw, &child) != nil {
					return errors.New("Schema registry unavailable or schema subject permission denied")
				}
				if child.Type != "PROTOBUF" || len(child.Schema) > 64*1024 || r.bytes+len(child.Schema) > 256*1024 {
					return errors.New("Invalid or oversized referenced Protobuf schema")
				}
				r.bytes += len(child.Schema)
				r.cache[key] = child
			}
			sources[ref.Name] = child.Schema
			if len(sources) > 8 {
				return errors.New("Protobuf import count exceeds decoder limit")
			}
			if completed[key] {
				continue
			}
			visiting[key] = true
			if err := walk(child, depth+1); err != nil {
				return err
			}
			delete(visiting, key)
			completed[key] = true
		}
		return nil
	}
	if err := walk(root, 0); err != nil {
		return nil, err
	}
	return sources, nil
}
