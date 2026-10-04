package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type SourceConfig struct {
	ID            string
	ClusterID     string
	URL           string
	Kind          string
	Headers       map[string]string
	Instance      string
	InstanceRegex string
	ClusterName   string
	Dimensions    map[string]string
	SignRequest   func(context.Context, *http.Request, []byte) error
	Simulate      func(context.Context, Query) (Result, error)
}
type Options struct {
	Demo                    bool
	TTL, Timeout            time.Duration
	MaxEntries, Concurrency int
	MaxResponseBytes        int64
	MaxSeries, MaxPoints    int
	Catalog                 []Definition
}
type Query struct {
	Source, Expression string
	Start, End         int64
	Step               int64
}
type Point struct {
	Time  float64 `json:"time"`
	Value float64 `json:"value"`
}
type Series struct {
	Labels map[string]string `json:"labels"`
	Points []Point           `json:"points"`
}
type Result struct {
	Status     string    `json:"status"`
	ObservedAt time.Time `json:"observedAt"`
	Series     []Series  `json:"series"`
	Cached     bool      `json:"cached"`
	Stale      bool      `json:"stale"`
	Error      string    `json:"error,omitempty"`
}
type entry struct {
	query            Query
	result           Result
	expires, touched time.Time
	pending          bool
	done             chan struct{}
}
type source struct {
	config   SourceConfig
	queue    chan *entry
	failures int
	retryAt  time.Time
}
type Gateway struct {
	mu                      sync.Mutex
	entries                 map[Query]*entry
	sources                 map[string]*source
	opts                    Options
	client                  *http.Client
	ctx                     context.Context
	cancel                  context.CancelFunc
	wg                      sync.WaitGroup
	closed                  bool
	hits, queries, failures atomic.Int64
}

func NewGateway(configs []SourceConfig, opts Options) (*Gateway, error) {
	if opts.TTL <= 0 {
		opts.TTL = 15 * time.Second
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.MaxEntries <= 0 {
		opts.MaxEntries = 512
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.Concurrency > 32 {
		return nil, errors.New("metrics concurrency exceeds 32")
	}
	if opts.MaxResponseBytes <= 0 {
		opts.MaxResponseBytes = 4 << 20
	}
	if opts.MaxSeries <= 0 {
		opts.MaxSeries = 200
	}
	if opts.MaxPoints <= 0 {
		opts.MaxPoints = 20000
	}
	if len(opts.Catalog) == 0 {
		opts.Catalog = DefaultCatalog()
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &Gateway{entries: make(map[Query]*entry), sources: make(map[string]*source), opts: opts, client: &http.Client{Timeout: opts.Timeout, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return errors.New("datasource redirects are disabled")
	}}, ctx: ctx, cancel: cancel}
	for _, cfg := range configs {
		u, err := url.Parse(cfg.URL)
		if cfg.Kind != "simulator" && (err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil) {
			cancel()
			return nil, fmt.Errorf("invalid metrics datasource %s", cfg.ID)
		}
		if cfg.ID == "" || g.sources[cfg.ID] != nil {
			cancel()
			return nil, errors.New("missing or duplicate datasource ID")
		}
		if cfg.Kind == "" {
			cfg.Kind = "prometheus"
		}
		if cfg.Kind != "prometheus" && cfg.Kind != "amp" && cfg.Kind != "cloudwatch" && cfg.Kind != "simulator" {
			cancel()
			return nil, fmt.Errorf("unsupported datasource kind %s", cfg.Kind)
		}
		if cfg.Kind == "simulator" && (!opts.Demo || cfg.Simulate == nil) {
			cancel()
			return nil, errors.New("simulated metrics require explicit development mode and adapter")
		}
		if (cfg.Kind == "amp" || cfg.Kind == "cloudwatch") && cfg.SignRequest == nil {
			cancel()
			return nil, errors.New("AWS metrics require a SigV4 signing provider")
		}
		clone := map[string]string{}
		for k, v := range cfg.Headers {
			clone[k] = v
		}
		cfg.Headers = clone
		g.sources[cfg.ID] = &source{config: cfg, queue: make(chan *entry, opts.MaxEntries)}
	}
	// Start only after all configurations validate; invalid configuration never leaks workers.
	for _, s := range g.sources {
		for i := 0; i < opts.Concurrency; i++ {
			g.wg.Add(1)
			go g.worker(s)
		}
	}
	return g, nil
}

func (g *Gateway) Close() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	g.cancel()
	g.mu.Unlock()
	g.wg.Wait()
}
func (g *Gateway) Catalog() []Definition { return append([]Definition(nil), g.opts.Catalog...) }
func (g *Gateway) Stats() map[string]int64 {
	return map[string]int64{"queries": g.queries.Load(), "cacheHits": g.hits.Load(), "errors": g.failures.Load()}
}

func (g *Gateway) Snapshot(q Query) Result {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.sources[q.Source]
	if s == nil {
		return Result{Status: "unavailable", Series: []Series{}, Error: "Metrics temporarily unavailable: datasource is not configured"}
	}
	if g.closed {
		return Result{Status: "unavailable", Series: []Series{}, Error: "Metrics gateway stopped"}
	}
	if err := validateQuery(q); err != nil {
		return Result{Status: "unavailable", Series: []Series{}, Error: err.Error()}
	}
	now := time.Now()
	e := g.entries[q]
	if e != nil {
		e.touched = now
		if now.Before(e.expires) {
			g.hits.Add(1)
			r := e.result
			r.Cached = true
			return r
		}
		if e.pending {
			r := e.result
			r.Stale = len(r.Series) > 0
			return r
		}
	}
	if now.Before(s.retryAt) {
		if e != nil && len(e.result.Series) > 0 {
			r := e.result
			r.Stale = true
			return r
		}
		return Result{Status: "unavailable", Series: []Series{}, Error: "Metrics temporarily unavailable: datasource circuit is open"}
	}
	if e == nil {
		if len(g.entries) >= g.opts.MaxEntries {
			var oldest Query
			var oldestTime time.Time
			found := false
			for k, v := range g.entries {
				if !v.pending && (!found || v.touched.Before(oldestTime)) {
					oldest = k
					oldestTime = v.touched
					found = true
				}
			}
			if !found {
				return Result{Status: "unavailable", Series: []Series{}, Error: "Metrics query capacity reached"}
			}
			delete(g.entries, oldest)
		}
		e = &entry{query: q, result: Result{Status: "pending", Series: []Series{}}, touched: now}
		g.entries[q] = e
	}
	e.pending = true
	e.done = make(chan struct{})
	select {
	case s.queue <- e:
	default:
		e.pending = false
		e.result.Status = "unavailable"
		e.result.Error = "Metrics query queue is full"
		close(e.done)
	}
	r := e.result
	r.Stale = len(r.Series) > 0
	return r
}

// Wait is for integration work and tests; HTTP navigation uses nonblocking Snapshot.
func (g *Gateway) Wait(ctx context.Context, q Query) (Result, error) {
	r := g.Snapshot(q)
	g.mu.Lock()
	e := g.entries[q]
	if e == nil || !e.pending {
		g.mu.Unlock()
		if r.Status == "unavailable" {
			return r, errors.New(r.Error)
		}
		return r, nil
	}
	done := e.done
	g.mu.Unlock()
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-g.ctx.Done():
		return Result{}, errors.New("gateway closed")
	case <-done:
	}
	g.mu.Lock()
	r = e.result
	g.mu.Unlock()
	if r.Status == "unavailable" {
		return r, errors.New(r.Error)
	}
	return r, nil
}

func validateQuery(q Query) error {
	if q.Expression == "" || len(q.Expression) > 16384 {
		return errors.New("invalid metric expression")
	}
	if q.Step < 1 || q.End < q.Start || q.End-q.Start > 86400 || (q.End-q.Start)/q.Step > 1440 {
		return errors.New("metric range or resolution exceeds limits")
	}
	return nil
}

func (g *Gateway) worker(s *source) {
	defer g.wg.Done()
	for {
		select {
		case <-g.ctx.Done():
			return
		case e := <-s.queue:
			ctx, cancel := context.WithTimeout(g.ctx, g.opts.Timeout)
			g.queries.Add(1)
			r, err := g.query(ctx, s.config, e.query)
			cancel()
			g.mu.Lock()
			if err != nil {
				g.failures.Add(1)
				s.failures++
				if s.failures >= 3 {
					backoff := time.Second * time.Duration(1<<min(s.failures-3, 6))
					jitter := time.Duration(time.Now().UnixNano() % int64(backoff/4+1))
					s.retryAt = time.Now().Add(backoff + jitter)
				}
				if len(e.result.Series) > 0 {
					e.result.Stale = true
					e.result.Error = "Metrics temporarily unavailable"
				} else {
					e.result = Result{Status: "unavailable", Series: []Series{}, Error: "Metrics temporarily unavailable"}
				}
				e.expires = time.Now().Add(time.Second)
			} else {
				s.failures = 0
				s.retryAt = time.Time{}
				e.result = r
				e.expires = time.Now().Add(g.opts.TTL)
			}
			e.pending = false
			close(e.done)
			g.mu.Unlock()
		}
	}
}

func (g *Gateway) query(ctx context.Context, c SourceConfig, q Query) (Result, error) {
	if c.Kind == "simulator" {
		return c.Simulate(ctx, q)
	}
	if c.Kind == "cloudwatch" {
		return g.queryCloudWatch(ctx, c, q)
	}
	u, err := url.Parse(strings.TrimRight(c.URL, "/") + "/api/v1/query_range")
	if err != nil {
		return Result{}, err
	}
	values := u.Query()
	values.Set("query", q.Expression)
	values.Set("start", strconv.FormatInt(q.Start, 10))
	values.Set("end", strconv.FormatInt(q.End, 10))
	values.Set("step", strconv.FormatInt(q.Step, 10))
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return Result{}, err
	}
	for k, v := range c.Headers {
		req.Header.Set(k, v)
	}
	if c.SignRequest != nil {
		if err := c.SignRequest(ctx, req, nil); err != nil {
			return Result{}, err
		}
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Result{}, fmt.Errorf("datasource returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, g.opts.MaxResponseBytes+1))
	if err != nil {
		return Result{}, err
	}
	if int64(len(body)) > g.opts.MaxResponseBytes {
		return Result{}, errors.New("metric response exceeds byte limit")
	}
	var decoded struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string    `json:"metric"`
				Values [][2]json.RawMessage `json:"values"`
				Value  []json.RawMessage    `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err = json.Unmarshal(body, &decoded); err != nil {
		return Result{}, err
	}
	if decoded.Status != "success" {
		return Result{}, errors.New("metric query failed")
	}
	if len(decoded.Data.Result) > g.opts.MaxSeries {
		return Result{}, errors.New("metric series exceeds limit")
	}
	r := Result{Status: "available", ObservedAt: time.Now().UTC(), Series: make([]Series, 0, len(decoded.Data.Result))}
	points := 0
	for _, item := range decoded.Data.Result {
		series := Series{Labels: item.Metric, Points: make([]Point, 0, len(item.Values))}
		pairs := item.Values
		if len(item.Value) == 2 {
			pairs = append(pairs, [2]json.RawMessage{item.Value[0], item.Value[1]})
		}
		for _, p := range pairs {
			points++
			if points > g.opts.MaxPoints {
				return Result{}, errors.New("metric points exceed limit")
			}
			var timestamp float64
			var value string
			if json.Unmarshal(p[0], &timestamp) != nil || json.Unmarshal(p[1], &value) != nil {
				continue
			}
			v, err := strconv.ParseFloat(value, 64)
			if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			series.Points = append(series.Points, Point{Time: timestamp, Value: v})
		}
		r.Series = append(r.Series, series)
	}
	return r, nil
}

func (g *Gateway) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet {
			w.WriteHeader(405)
			json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "METHOD_NOT_ALLOWED", "message": "GET required"}})
			return
		}
		switch strings.TrimRight(r.URL.Path, "/") {
		case "/catalog":
			json.NewEncoder(w).Encode(map[string]any{"data": g.Catalog()})
		case "/query":
			id := r.URL.Query().Get("metric")
			var def *Definition
			for i := range g.opts.Catalog {
				if g.opts.Catalog[i].ID == id {
					def = &g.opts.Catalog[i]
					break
				}
			}
			if def == nil {
				w.WriteHeader(404)
				json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "NOT_FOUND", "message": "Unknown metric"}})
				return
			}
			duration := r.URL.Query().Get("range")
			if duration == "" {
				duration = "15m"
			}
			instance := r.URL.Query().Get("instance")
			sourceID := r.URL.Query().Get("source")
			if sourceID == "" {
				sourceID = "prometheus"
				if def.Kind == "cloudwatch" {
					sourceID = "cloudwatch"
				}
			}
			if s := g.sources[sourceID]; s != nil && s.config.Instance != "" {
				instance = s.config.Instance
			}
			expr, err := Expand(def.Expression, instance, duration)
			if source := g.sources[sourceID]; source != nil && source.config.InstanceRegex != "" {
				expr, err = expandConfiguredRegex(def.Expression, source.config.InstanceRegex, duration)
			}
			if def.Kind == "cloudwatch" && err == nil {
				expr = "cloudwatch:" + def.ID
			}
			if err != nil {
				w.WriteHeader(400)
				json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "INVALID_QUERY", "message": err.Error()}})
				return
			}
			span, _ := time.ParseDuration(duration)
			end := time.Now().Unix() / 15 * 15
			step := max(int64(15), int64(span.Seconds())/300)
			result := g.Snapshot(Query{Source: sourceID, Expression: expr, Start: end - int64(span.Seconds()), End: end, Step: step})
			json.NewEncoder(w).Encode(map[string]any{"data": result})
		default:
			w.WriteHeader(404)
			json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "NOT_FOUND", "message": "Unknown metrics endpoint"}})
		}
	})
}
