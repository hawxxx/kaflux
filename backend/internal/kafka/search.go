package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"regexp"
	"slices"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrUnknownTopic reports a topic the cluster does not have.
var ErrUnknownTopic = errors.New("unknown topic")

// ErrSearchBusy reports that the cluster already runs as many searches as it allows at once.
var ErrSearchBusy = errors.New("too many searches running")

// Searcher scans a topic on the server and returns only the records that match.
type Searcher interface {
	Search(ctx context.Context, q SearchQuery) (SearchResult, error)
}

// Fixed per-search budgets. A search that reaches one stops and reports where to resume.
var (
	searchRecordBudget  int64 = 2_000_000
	searchByteBudget    int64 = 2 << 30
	searchTimeBudget          = 30 * time.Second
	searchPreviewBudget       = 4 << 20
)

// SearchQuery is a bounded scan of one topic. Ranges are resolved per partition: FromOffsets
// resumes an earlier search and limits the scan to its partitions; otherwise FromTime, or the
// first retained record, starts each partition. Each partition ends before ToTime, or at the end
// offset it had when the scan started, so a search always terminates.
type SearchQuery struct {
	Topic       string
	Partitions  []int32
	FromOffsets map[int32]int64
	FromTime    *time.Time
	ToTime      *time.Time
	Match       Matcher
	// Partitioner names the producer's key partitioner (murmur2 or crc32). With a key equals
	// match it restricts the scan to the one partition that key is written to.
	Partitioner string
	MaxMatches  int
}

type SearchScanned struct {
	Records int64 `json:"records"`
	Bytes   int64 `json:"bytes"`
}

type SearchResult struct {
	Matches []model.Message `json:"matches"`
	Scanned SearchScanned   `json:"scanned"`
	// Resume holds, per partition not yet scanned to its end, the next offset to read.
	Resume    map[int32]int64 `json:"resume"`
	Done      bool            `json:"done"`
	StoppedBy string          `json:"stoppedBy"`
}

// Matcher tests a record's key, value or headers against the search text.
type Matcher struct {
	in, op        string
	text          []byte
	caseSensitive bool
	re            *regexp.Regexp
}

// NewMatcher validates a search. in is key, value, headers or any; op is contains, equals or regex.
func NewMatcher(in, op, text string, caseSensitive bool) (Matcher, error) {
	if in == "" {
		in = "any"
	}
	if op == "" {
		op = "contains"
	}
	if !slices.Contains([]string{"key", "value", "headers", "any"}, in) {
		return Matcher{}, errors.New("match.in must be key, value, headers or any")
	}
	if text == "" || len(text) > 1024 {
		return Matcher{}, errors.New("match.text must be 1 to 1024 bytes")
	}
	m := Matcher{in: in, op: op, text: []byte(text), caseSensitive: caseSensitive}
	switch op {
	case "contains", "equals":
		if !caseSensitive {
			m.text = bytes.ToLower(m.text)
		}
	case "regex":
		pattern := text
		if !caseSensitive {
			pattern = "(?i)" + pattern
		}
		re, e := regexp.Compile(pattern)
		if e != nil {
			return Matcher{}, errors.New("match.text is not a valid regular expression")
		}
		m.re = re
	default:
		return Matcher{}, errors.New("match.op must be contains, equals or regex")
	}
	return m, nil
}

func (m Matcher) field(b []byte) bool {
	switch m.op {
	case "regex":
		return m.re.Match(b)
	case "equals":
		if m.caseSensitive {
			return bytes.Equal(b, m.text)
		}
		return bytes.EqualFold(b, m.text)
	}
	if m.caseSensitive {
		return bytes.Contains(b, m.text)
	}
	return bytes.Contains(bytes.ToLower(b), m.text)
}

// Record matches the full record bytes, never a truncated preview.
func (m Matcher) Record(r *kgo.Record) bool {
	if (m.in == "key" || m.in == "any") && m.field(r.Key) {
		return true
	}
	if (m.in == "value" || m.in == "any") && m.field(r.Value) {
		return true
	}
	if m.in == "headers" || m.in == "any" {
		for _, h := range r.Headers {
			if m.field([]byte(h.Key)) || m.field(h.Value) {
				return true
			}
		}
	}
	return false
}

// keyPartition is the partition a producer using the named partitioner writes key to.
func keyPartition(partitioner string, key []byte, partitions int) (int32, error) {
	switch partitioner {
	case "murmur2":
		// The sticky key partitioner hashes keyed records exactly as the Java client does.
		return int32(kgo.StickyKeyPartitioner(nil).ForTopic("").Partition(&kgo.Record{Key: key}, partitions)), nil
	case "crc32":
		// librdkafka's default consistent_random partitioner.
		return int32(crc32.ChecksumIEEE(key) % uint32(partitions)), nil
	}
	return 0, fmt.Errorf("partitioner must be murmur2 or crc32")
}

// scan holds the progress of one search. Providers feed it records in offset order per
// partition; it decides what is scanned, what matches and when the search stops.
type scan struct {
	q           SearchQuery
	next, to    map[int32]int64
	result      SearchResult
	previewLeft int
	stoppedBy   string
}

// planScan resolves the query against a topic's start and end offsets. after returns, per
// partition, the first offset at or after a time, or -1 when there is none.
func planScan(q SearchQuery, starts, ends map[int32]int64, after func(time.Time) (map[int32]int64, error)) (*scan, error) {
	if len(ends) == 0 {
		return nil, ErrUnknownTopic
	}
	parts := q.Partitions
	if q.FromOffsets != nil {
		parts = make([]int32, 0, len(q.FromOffsets))
		for p := range q.FromOffsets {
			parts = append(parts, p)
		}
	} else if len(parts) == 0 {
		for p := range ends {
			parts = append(parts, p)
		}
	}
	for _, p := range parts {
		if _, ok := ends[p]; !ok {
			return nil, fmt.Errorf("partition %d: %w", p, ErrUnknownPartition)
		}
	}
	if q.Partitioner != "" && q.Match.in == "key" && q.Match.op == "equals" && q.Match.caseSensitive {
		only, e := keyPartition(q.Partitioner, q.Match.text, len(ends))
		if e != nil {
			return nil, e
		}
		parts = slices.DeleteFunc(parts, func(p int32) bool { return p != only })
	}
	resolve := func(at *time.Time) (map[int32]int64, error) {
		if at == nil {
			return nil, nil
		}
		offsets, e := after(*at)
		if e != nil {
			return nil, e
		}
		for p, o := range offsets {
			if o < 0 {
				offsets[p] = ends[p]
			}
		}
		return offsets, nil
	}
	fromTime, e := resolve(q.FromTime)
	if e != nil {
		return nil, e
	}
	toTime, e := resolve(q.ToTime)
	if e != nil {
		return nil, e
	}
	s := &scan{q: q, next: map[int32]int64{}, to: map[int32]int64{}, result: SearchResult{Matches: []model.Message{}}, previewLeft: searchPreviewBudget}
	for _, p := range parts {
		from := starts[p]
		if o, ok := q.FromOffsets[p]; ok {
			from = max(from, o)
		} else if o, ok := fromTime[p]; ok {
			from = o
		}
		to := ends[p]
		if o, ok := toTime[p]; ok {
			to = min(to, o)
		}
		if from < to {
			s.next[p], s.to[p] = from, to
		}
	}
	return s, nil
}

func (s *scan) active() []int32 {
	out := make([]int32, 0, len(s.next))
	for p := range s.next {
		out = append(out, p)
	}
	return out
}

func (s *scan) stop(reason string) {
	if s.stoppedBy == "" {
		s.stoppedBy = reason
	}
}

// offer scans one record and reports whether its partition is now finished.
func (s *scan) offer(r *kgo.Record) bool {
	p := r.Partition
	next, ok := s.next[p]
	if !ok || s.stoppedBy != "" || r.Offset < next {
		return false
	}
	if r.Offset >= s.to[p] {
		delete(s.next, p)
		return true
	}
	s.result.Scanned.Records++
	s.result.Scanned.Bytes += int64(len(r.Key) + len(r.Value))
	s.next[p] = r.Offset + 1
	if s.q.Match.Record(r) {
		m, used := previewRecord(r, s.previewLeft)
		s.previewLeft -= used
		s.result.Matches = append(s.result.Matches, m)
		if len(s.result.Matches) >= s.q.MaxMatches || s.previewLeft <= 0 {
			s.stop("matches")
		}
	}
	if s.result.Scanned.Records >= searchRecordBudget {
		s.stop("records")
	}
	if s.result.Scanned.Bytes >= searchByteBudget {
		s.stop("bytes")
	}
	if s.next[p] >= s.to[p] {
		delete(s.next, p)
		return true
	}
	return false
}

func (s *scan) finish() SearchResult {
	s.result.Resume = s.next
	s.result.Done = len(s.next) == 0
	s.stop("end")
	s.result.StoppedBy = s.stoppedBy
	return s.result
}
