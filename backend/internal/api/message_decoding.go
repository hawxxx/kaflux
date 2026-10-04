package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	external "github.com/hawxxx/kaflux/backend/internal/integrations"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/serde"
	"time"
)

var decodingSlots = make(chan struct{}, 4)

// Keys and values share one deadline, concurrency slot, schema cache and output
// budget. A malformed key does not prevent inspection of its message value.
func (a *API) decodeMessageFields(ctx context.Context, user auth.User, cluster string, client *external.Client, messages []model.Message, format, target string) {
	if target == "" || target == "value" {
		a.decodeMessages(ctx, user, cluster, client, messages, format)
		return
	}
	previews := make([]model.Message, 0, len(messages)*2)
	for _, m := range messages {
		previews = append(previews, model.Message{ValueBase64: m.KeyBase64, Truncated: m.Truncated})
		if target == "both" {
			previews = append(previews, model.Message{ValueBase64: m.ValueBase64, Truncated: m.Truncated})
		}
	}
	a.decodeMessages(ctx, user, cluster, client, previews, format)
	index := 0
	for i := range messages {
		m, p := &messages[i], previews[index]
		index++
		m.DecodedKey = p.DecodedValue
		m.KeyDecodedFormat = p.DecodedFormat
		m.KeySchemaID = p.SchemaID
		m.KeyDecodeError = p.DecodeError
		if target == "both" {
			p = previews[index]
			index++
			m.DecodedValue = p.DecodedValue
			m.DecodedFormat = p.DecodedFormat
			m.SchemaID = p.SchemaID
			m.DecodeError = p.DecodeError
		}
	}
}

func (a *API) decodeMessages(ctx context.Context, user auth.User, cluster string, client *external.Client, messages []model.Message, formats ...string) {
	format := "avro"
	if len(formats) > 0 {
		format = formats[0]
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		for i := range messages {
			messages[i].DecodeError = "Decoding request cancelled or capacity unavailable"
		}
		return
	}
	select {
	case decodingSlots <- struct{}{}:
		defer func() { <-decodingSlots }()
	case <-ctx.Done():
		for i := range messages {
			messages[i].DecodeError = "Decoding request cancelled or capacity unavailable"
		}
		return
	}
	type resolved struct {
		schema   string
		problem  string
		protobuf *serde.ProtobufDecoder
	}
	schemas := map[uint32]resolved{}
	references := protobufReferenceResolver{client: client, authorize: func(subject string) bool {
		return (auth.Authorizer{Grants: a.o.Grants}).Allowed(user, cluster, "schema-read", subject)
	}, cache: map[protobufReferenceKey]registryDecodeSchema{}}
	budget := 2 * 1024 * 1024
	for i := range messages {
		m := &messages[i]
		if m.Truncated {
			m.DecodeError = "Truncated previews cannot be schema decoded"
			continue
		}
		wire, err := base64.StdEncoding.DecodeString(m.ValueBase64)
		if err != nil {
			m.DecodeError = "Invalid binary preview"
			continue
		}
		id, err := serde.SchemaID(wire)
		if err != nil {
			m.DecodeError = err.Error()
			continue
		}
		m.SchemaID = id
		entry, found := schemas[id]
		if !found {
			if len(schemas) >= 8 {
				m.DecodeError = "Schema lookup limit reached; reduce the record limit"
				continue
			}
			entry.problem = "Schema registry unavailable or schema subject permission denied"
			subjectsRaw, e := client.Do(ctx, "GET", fmt.Sprintf("/schemas/ids/%d/subjects", id), nil)
			var subjects []string
			allowed := false
			if e == nil && json.Unmarshal(subjectsRaw, &subjects) == nil {
				for _, subject := range subjects {
					if (auth.Authorizer{Grants: a.o.Grants}).Allowed(user, cluster, "schema-read", subject) {
						allowed = true
						break
					}
				}
			}
			if allowed {
				raw, e := client.Do(ctx, "GET", fmt.Sprintf("/schemas/ids/%d", id), nil)
				var schema registryDecodeSchema
				if e == nil && json.Unmarshal(raw, &schema) == nil {
					switch {
					case (format == "protobuf" && schema.Type != "PROTOBUF") || (format == "avro" && schema.Type != "" && schema.Type != "AVRO"):
						entry.problem = "Selected decoder does not match schema type"
					case len(schema.References) > 0 && format != "protobuf":
						entry.problem = "External schema references are not supported"
					default:
						entry.schema = schema.Schema
						entry.problem = ""
						if format == "protobuf" {
							var err error
							var sources map[string]string
							sources, err = references.resolve(ctx, schema)
							if err == nil {
								entry.protobuf, err = serde.NewProtobufDecoderWithSources(ctx, schema.Schema, sources)
							}
							if err != nil {
								entry.problem = err.Error()
							}
						}
					}
				}
			}
			schemas[id] = entry
		}
		if entry.problem != "" {
			m.DecodeError = entry.problem
			continue
		}
		if ctx.Err() != nil {
			m.DecodeError = "Decoding request timed out"
			continue
		}
		if budget <= 0 {
			m.DecodeError = "Decoded response limit reached; reduce the record limit"
			continue
		}
		var output json.RawMessage
		if format == "protobuf" {
			output, err = entry.protobuf.Decode(wire)
		} else {
			output, err = serde.DecodeAvro(entry.schema, wire)
		}
		if err != nil {
			m.DecodeError = err.Error()
			continue
		}
		if len(output) > budget {
			m.DecodeError = "Decoded response limit reached; reduce the record limit"
			continue
		}
		budget -= len(output)
		m.DecodedValue = output
		m.DecodedFormat = format
	}
}
