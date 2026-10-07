package msk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/awsutil"
)

// CapabilityChecker is anything that resolves MSK capabilities for a cluster,
// either from a configured ARN (Checker) or by discovering one (Discoverer).
type CapabilityChecker interface {
	Capabilities(ctx context.Context, force bool) (Capabilities, error)
}

// bootstrapLabels is what a seed hostname implies about the MSK cluster that
// produced it, read from the bootstrap broker DNS name
// "b-<n>.<name-without-dashes>.<id>.c<generation>.kafka.<region>.<suffix>".
type bootstrapLabels struct {
	NameNoDashes string
	Generation   int
	Region       string
	Suffix       string // "amazonaws.com" or "amazonaws.com.cn"
}

var cLabel = regexp.MustCompile(`^c(\d+)$`)

// parseBootstrapHost reads the cluster name and ARN generation suffix encoded
// in an MSK bootstrap broker hostname. ok is false for anything that is not
// shaped like a provisioned MSK bootstrap broker, including serverless
// endpoints and self-managed hosts that merely contain ".kafka.".
func parseBootstrapHost(seed string) (bootstrapLabels, bool) {
	host := seed
	if h, _, err := net.SplitHostPort(seed); err == nil {
		host = h
	}
	labels := strings.Split(host, ".")
	for i, label := range labels {
		if label != "kafka" || i < 3 {
			continue
		}
		rest := labels[i+1:]
		var region, suffix string
		switch {
		case len(rest) == 3 && rest[1] == "amazonaws" && rest[2] == "com":
			region, suffix = rest[0], "amazonaws.com"
		case len(rest) == 4 && rest[1] == "amazonaws" && rest[2] == "com" && rest[3] == "cn":
			region, suffix = rest[0], "amazonaws.com.cn"
		default:
			continue
		}
		m := cLabel.FindStringSubmatch(labels[i-1])
		if m == nil {
			continue
		}
		generation, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		return bootstrapLabels{NameNoDashes: strings.ToLower(labels[i-3]), Generation: generation, Region: region, Suffix: suffix}, true
	}
	return bootstrapLabels{}, false
}

var alphanumericOnly = regexp.MustCompile(`[^a-z0-9]`)

func normalizeClusterName(name string) string {
	return alphanumericOnly.ReplaceAllString(strings.ToLower(name), "")
}

// arnGeneration returns the trailing "-<n>" numeric suffix of an MSK cluster
// ARN's resource ID, which is the same generation number that appears as the
// "c<n>" label in that cluster's bootstrap broker hostnames.
func arnGeneration(arn string) (int, bool) {
	i := strings.LastIndex(arn, "-")
	if i < 0 {
		return 0, false
	}
	n, err := strconv.Atoi(arn[i+1:])
	if err != nil {
		return 0, false
	}
	return n, true
}

// DiscoverSeeds extracts the cluster name and ARN generation an MSK bootstrap
// seed list implies. ok is false when no seed matches the bootstrap hostname
// shape, meaning discovery does not apply (self-managed Kafka, MSK Serverless,
// or a cluster already identified by a configured ARN).
func DiscoverSeeds(seeds []string) (bootstrapLabels, bool) {
	for _, s := range seeds {
		if l, ok := parseBootstrapHost(s); ok {
			return l, true
		}
	}
	return bootstrapLabels{}, false
}

// Discoverer finds the ARN of an MSK cluster from its bootstrap broker
// hostnames when the operator has not configured mskClusterArn, then behaves
// like a Checker for that ARN. Discovery itself happens at most once (cached
// for discoveryTTL) and only on the first Capabilities call: construction
// never performs I/O, so a misconfigured or slow AWS account cannot block
// startup.
type Discoverer struct {
	labels   bootstrapLabels
	endpoint string
	client   *http.Client
	sign     func(ctx context.Context) (awsutil.RequestSigner, error)

	mu       sync.Mutex
	resolved bool
	checker  *Checker
	cached   Capabilities
	lastErr  error
	expires  time.Time
	inflight chan struct{}
}

const discoveryTTL = 5 * time.Minute

// NewDiscoverer prepares lazy discovery for the cluster implied by seeds. It
// returns ok=false when the seeds do not look like MSK bootstrap brokers, in
// which case the caller should not attempt discovery at all.
func NewDiscoverer(seeds []string, region, roleARN string) (*Discoverer, bool) {
	labels, ok := DiscoverSeeds(seeds)
	if !ok {
		return nil, false
	}
	if region == "" {
		region = labels.Region
	}
	return &Discoverer{
		labels:   labels,
		endpoint: "https://kafka." + region + "." + labels.Suffix,
		client:   &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("MSK control-plane redirects disabled") }},
		sign: func(ctx context.Context) (awsutil.RequestSigner, error) {
			return awsutil.NewSigner(ctx, "kafka", region, roleARN)
		},
	}, true
}

// Capabilities resolves the cluster ARN on first use (or after discoveryTTL,
// or when force is set) and then delegates to the discovered Checker. While
// discovery has not produced a usable ARN, it reports an unknown capability
// that explains why, rather than guessing or blocking.
func (d *Discoverer) Capabilities(ctx context.Context, force bool) (Capabilities, error) {
	for {
		d.mu.Lock()
		if d.resolved && d.checker != nil {
			checker := d.checker
			d.mu.Unlock()
			return checker.Capabilities(ctx, force)
		}
		if !force && time.Now().Before(d.expires) {
			result, err := d.cached, d.lastErr
			d.mu.Unlock()
			return result, err
		}
		if d.inflight != nil {
			done := d.inflight
			d.mu.Unlock()
			select {
			case <-ctx.Done():
				return unknown(), ctx.Err()
			case <-done:
			}
			force = false // another caller just resolved or refreshed; use its result
			continue
		}
		d.inflight = make(chan struct{})
		done := d.inflight
		d.mu.Unlock()

		arn, reason, err := d.discover(ctx)

		d.mu.Lock()
		if err == nil && arn != "" {
			checker, checkerErr := NewChecker(arn, d.endpoint, func(c context.Context, r *http.Request, b []byte) error {
				sign, e := d.sign(c)
				if e != nil {
					return e
				}
				return sign(c, r, b)
			}, 15*time.Second)
			if checkerErr == nil {
				d.checker = checker
				d.resolved = true
				d.inflight = nil
				close(done)
				d.mu.Unlock()
				return checker.Capabilities(ctx, force)
			}
			err = checkerErr
			reason = "Discovered MSK cluster ARN is invalid"
		}
		result := Capabilities{Kind: "MSK", RebalancingStatus: "UNKNOWN", Reason: reason, ObservedAt: time.Now().UTC()}
		d.cached, d.lastErr = result, err
		d.expires = time.Now().Add(discoveryTTL)
		if err != nil {
			d.expires = time.Now().Add(5 * time.Second)
		}
		d.inflight = nil
		close(done)
		d.mu.Unlock()
		return result, err
	}
}

// discover calls kafka:ListClustersV2, paginating up to discoveryMaxPages
// times, and returns the ARN of the single cluster whose name (with
// non-alphanumeric characters removed) and ARN generation suffix both match
// the bootstrap hostnames. Any other outcome (no permission, zero matches,
// more than one match) returns no ARN and a reason explaining which.
func (d *Discoverer) discover(parent context.Context) (arn, reason string, err error) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	sign, err := d.sign(ctx)
	if err != nil {
		return "", "AWS credentials unavailable; cannot discover the MSK cluster", err
	}
	var matches []string
	nextToken := ""
	const discoveryMaxPages = 10
	for page := 0; page < discoveryMaxPages; page++ {
		q := url.Values{"maxResults": {"100"}}
		if nextToken != "" {
			q.Set("nextToken", nextToken)
		}
		request, e := http.NewRequestWithContext(ctx, "GET", d.endpoint+"/api/v2/clusters?"+q.Encode(), nil)
		if e != nil {
			return "", "Cannot build MSK cluster discovery request", e
		}
		if e = sign(ctx, request, nil); e != nil {
			return "", "AWS credentials unavailable; cannot discover the MSK cluster", e
		}
		response, e := d.client.Do(request)
		if e != nil {
			return "", "MSK cluster discovery unavailable; manual reassignment blocked", e
		}
		func() {
			defer response.Body.Close()
			if response.StatusCode == 403 {
				reason = "AWS denied the request to list MSK clusters (HTTP 403 AccessDenied). Grant the kafka:ListClustersV2 permission to the role Kaflux runs as."
				err = fmt.Errorf("MSK cluster discovery denied (HTTP 403); missing kafka:ListClustersV2")
				return
			}
			if response.StatusCode != 200 {
				reason = fmt.Sprintf("MSK cluster discovery returned HTTP %d; require kafka:ListClustersV2 permission", response.StatusCode)
				err = fmt.Errorf("MSK cluster discovery returned HTTP %d", response.StatusCode)
				return
			}
			body, e2 := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
			if e2 != nil || len(body) > 1<<20 {
				reason, err = "Invalid MSK cluster discovery response", errors.New("invalid MSK discovery response")
				return
			}
			var listed struct {
				ClusterInfoList []struct {
					ClusterArn  string `json:"clusterArn"`
					ClusterName string `json:"clusterName"`
				} `json:"clusterInfoList"`
				NextToken string `json:"nextToken"`
			}
			if e2 = json.Unmarshal(body, &listed); e2 != nil {
				reason, err = "Malformed MSK cluster discovery response", errors.New("malformed MSK discovery response")
				return
			}
			for _, c := range listed.ClusterInfoList {
				if normalizeClusterName(c.ClusterName) != d.labels.NameNoDashes {
					continue
				}
				if generation, ok := arnGeneration(c.ClusterArn); !ok || generation != d.labels.Generation {
					continue
				}
				matches = append(matches, c.ClusterArn)
			}
			nextToken = listed.NextToken
		}()
		if err != nil {
			return "", reason, err
		}
		if nextToken == "" {
			break
		}
	}
	switch len(matches) {
	case 0:
		return "", fmt.Sprintf("No MSK cluster matched the bootstrap hostnames (name %q, generation c%d). Set mskClusterArn to identify it explicitly.", d.labels.NameNoDashes, d.labels.Generation), nil
	case 1:
		return matches[0], "", nil
	default:
		return "", fmt.Sprintf("%d MSK clusters matched the bootstrap hostnames (name %q, generation c%d). Set mskClusterArn to disambiguate.", len(matches), d.labels.NameNoDashes, d.labels.Generation), nil
	}
}
