package kafka

import (
	"bytes"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

func TestPreviewBoundsHeadersAndPreservesBinary(t *testing.T) {
	r := &kgo.Record{Key: []byte{0, 255}, Value: []byte{254, 0}}
	for i := 0; i < 100; i++ {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: string(bytes.Repeat([]byte("k"), 2048)), Value: bytes.Repeat([]byte("v"), 8192)})
	}
	m, n := previewRecord(r, 8000)
	if n > 8000 || len(m.Headers) > 64 || !m.Truncated {
		t.Fatalf("unbounded preview: bytes=%d headers=%d", n, len(m.Headers))
	}
	if m.KeyBase64 != "AP8=" || m.ValueBase64 != "/gA=" {
		t.Fatal("binary bytes changed")
	}
	for _, h := range m.Headers {
		if len(h.Key) > 1024 || len(h.Value) > 4096 {
			t.Fatal("header exceeds limits")
		}
	}
}
