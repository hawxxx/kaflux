package mcp

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Call results recorded in kaflux_mcp_calls_total.
const (
	resultOK       = "ok"
	resultPrepared = "prepared"
	resultError    = "error"
	resultDenied   = "denied"
)

// Denial reasons recorded in kaflux_mcp_denied_total. Label values are
// fixed strings; token ids, names and secrets never become labels.
const (
	deniedUnauthenticated = "unauthenticated"
	deniedOrigin          = "origin"
	deniedScope           = "scope"
	deniedWritesDisabled  = "writes_disabled"
	deniedToolset         = "toolset"
	deniedPermission      = "permission"
	deniedRateLimited     = "rate_limited"
)

// callBuckets are the latency histogram bounds in seconds.
var callBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

// metrics holds the MCP counters in memory, rendered in Prometheus text
// format by WriteMetrics. It is process-wide like the /metrics route.
type metrics struct {
	mu      sync.Mutex
	calls   map[[2]string]uint64 // {tool, result}
	denied  map[string]uint64
	buckets []uint64 // cumulative counts per callBuckets bound
	count   uint64
	sum     float64
}

var defaultMetrics = newMetrics()

func newMetrics() *metrics {
	return &metrics{calls: map[[2]string]uint64{}, denied: map[string]uint64{}, buckets: make([]uint64, len(callBuckets))}
}

// call records one finished tools/call. tool is always a catalog name.
func (m *metrics) call(tool, result string, d time.Duration) {
	s := d.Seconds()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls[[2]string{tool, result}]++
	m.count++
	m.sum += s
	for i, b := range callBuckets {
		if s <= b {
			m.buckets[i]++
		}
	}
}

func (m *metrics) deny(reason string) {
	m.mu.Lock()
	m.denied[reason]++
	m.mu.Unlock()
}

// WriteMetrics appends the MCP series to a Prometheus text exposition; pass
// it as api.Options.ExtraMetrics.
func WriteMetrics(w io.Writer) { defaultMetrics.write(w) }

func (m *metrics) write(w io.Writer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var b strings.Builder
	b.WriteString("# HELP kaflux_mcp_calls_total MCP tool calls by tool and result.\n# TYPE kaflux_mcp_calls_total counter\n")
	keys := make([][2]string, 0, len(m.calls))
	for k := range m.calls {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	for _, k := range keys {
		fmt.Fprintf(&b, "kaflux_mcp_calls_total{tool=%s,result=%s} %d\n", label(k[0]), label(k[1]), m.calls[k])
	}
	b.WriteString("# HELP kaflux_mcp_call_seconds MCP tool call latency.\n# TYPE kaflux_mcp_call_seconds histogram\n")
	for i, bound := range callBuckets {
		fmt.Fprintf(&b, "kaflux_mcp_call_seconds_bucket{le=\"%g\"} %d\n", bound, m.buckets[i])
	}
	fmt.Fprintf(&b, "kaflux_mcp_call_seconds_bucket{le=\"+Inf\"} %d\nkaflux_mcp_call_seconds_sum %g\nkaflux_mcp_call_seconds_count %d\n", m.count, m.sum, m.count)
	b.WriteString("# HELP kaflux_mcp_denied_total MCP requests and calls denied, by reason.\n# TYPE kaflux_mcp_denied_total counter\n")
	reasons := make([]string, 0, len(m.denied))
	for r := range m.denied {
		reasons = append(reasons, r)
	}
	sort.Strings(reasons)
	for _, r := range reasons {
		fmt.Fprintf(&b, "kaflux_mcp_denied_total{reason=%s} %d\n", label(r), m.denied[r])
	}
	_, _ = io.WriteString(w, b.String())
}

// label quotes a Prometheus label value.
func label(v string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return `"` + r.Replace(v) + `"`
}
