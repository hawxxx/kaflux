package api

import (
	"encoding/base64"
	"net/http"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/serde"
)

// RawDecoder decodes unframed Protobuf values with a schema the operator supplies,
// for producers that write no registry header (OpenSync telemetry, for example).
type RawDecoder struct {
	ID      string
	Name    string
	Message string
	// Topics are glob patterns (* only) that suggest this decoder in the console.
	Topics  []string
	Decoder *serde.RawProtobufDecoder
}

func (a *API) rawDecoder(id string) *RawDecoder {
	for i := range a.o.RawDecoders {
		if a.o.RawDecoders[i].ID == id {
			return &a.o.RawDecoders[i]
		}
	}
	return nil
}

func (a *API) rawDecoders(w http.ResponseWriter) {
	out := make([]map[string]any, 0, len(a.o.RawDecoders))
	for _, d := range a.o.RawDecoders {
		topics := d.Topics
		if topics == nil {
			topics = []string{}
		}
		out = append(out, map[string]any{"id": d.ID, "name": d.Name, "message": d.Message, "topics": topics})
	}
	respond(w, out)
}

// decodeRawFields decodes keys, values or both. A failure stays on its record and
// never hides the raw bytes. All records share one output budget.
func decodeRawFields(messages []model.Message, d *RawDecoder, target string) {
	budget := 2 * 1024 * 1024
	one := func(encoded string, truncated bool) (out []byte, problem string) {
		if truncated {
			return nil, "Truncated previews cannot be schema decoded"
		}
		wire, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, "Invalid binary preview"
		}
		if budget <= 0 {
			return nil, "Decoded response limit reached; reduce the record limit"
		}
		decoded, err := d.Decoder.Decode(wire)
		if err != nil {
			return nil, err.Error()
		}
		if len(decoded) > budget {
			return nil, "Decoded response limit reached; reduce the record limit"
		}
		budget -= len(decoded)
		return decoded, ""
	}
	for i := range messages {
		m := &messages[i]
		// An absent key or a tombstone value has no bytes to decode. An empty buffer would
		// otherwise decode as a valid empty message and show a misleading {}.
		if (target == "" || target == "value" || target == "both") && m.ValueBase64 != "" {
			m.DecodedValue, m.DecodeError = nil, ""
			if out, problem := one(m.ValueBase64, m.Truncated); problem != "" {
				m.DecodeError = problem
			} else {
				m.DecodedValue, m.DecodedFormat = out, "protobuf-raw"
			}
		}
		if (target == "key" || target == "both") && m.KeyBase64 != "" {
			m.DecodedKey, m.KeyDecodeError = nil, ""
			if out, problem := one(m.KeyBase64, m.Truncated); problem != "" {
				m.KeyDecodeError = problem
			} else {
				m.DecodedKey, m.KeyDecodedFormat = out, "protobuf-raw"
			}
		}
	}
}
