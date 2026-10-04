package kafka

import (
	"encoding/base64"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Bound both payloads and headers before allocating browser-facing representations.
func previewRecord(r *kgo.Record, budget int) (model.Message, int) {
	truncated := false
	used := 0
	take := func(data []byte, limit int) []byte {
		limit = min(limit, max(0, budget-used))
		if len(data) > limit {
			data = data[:limit]
			truncated = true
		}
		used += len(data)
		return data
	}
	key := take(r.Key, 64<<10)
	value := take(r.Value, 256<<10)
	headers := []model.Header{}
	for i, h := range r.Headers {
		if i >= 64 || used >= budget {
			truncated = true
			break
		}
		k := take([]byte(h.Key), 1024)
		v := take(h.Value, 4096)
		headers = append(headers, model.Header{Key: string(k), Value: string(v)})
	}
	return model.Message{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: string(key), Value: string(value), KeyBase64: base64.StdEncoding.EncodeToString(key), ValueBase64: base64.StdEncoding.EncodeToString(value), Truncated: truncated, Timestamp: r.Timestamp, Headers: headers}, used
}
