// Package msk inspects AWS control-plane restrictions before Kafka mutations.
package msk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/awsutil"
)

type Capabilities struct {
	ManualReassignmentAllowed bool `json:"manualReassignmentAllowed"`
	// PlanningAllowed means plans may be generated and validated (read-only work) even though
	// running one is not allowed. It is also true whenever manual reassignment is allowed.
	PlanningAllowed   bool      `json:"planningAllowed"`
	RebalancingStatus string    `json:"rebalancingStatus"`
	Reason            string    `json:"reason,omitempty"`
	Kind              string    `json:"kind"`
	BrokerType        string    `json:"brokerType,omitempty"`
	ObservedAt        time.Time `json:"observedAt"`
}

type Checker struct {
	arn, endpoint string
	sign          awsutil.RequestSigner
	client        *http.Client
	ttl           time.Duration
	mu            sync.Mutex
	cached        Capabilities
	expires       time.Time
	inflight      chan struct{}
	lastErr       error
}

func New(ctx context.Context, arn, region, roleARN string) (*Checker, error) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "kafka" || !strings.HasPrefix(parts[5], "cluster/") {
		return nil, errors.New("valid MSK cluster ARN required for capability detection")
	}
	if region == "" {
		region = parts[3]
	}
	if region != parts[3] {
		return nil, errors.New("MSK region does not match cluster ARN")
	}
	suffix := "amazonaws.com"
	if parts[1] == "aws-cn" {
		suffix = "amazonaws.com.cn"
	}
	signer, err := awsutil.NewSigner(ctx, "kafka", region, roleARN)
	if err != nil {
		return nil, err
	}
	return NewChecker(arn, "https://kafka."+region+"."+suffix, signer, 15*time.Second)
}

func NewChecker(arn, endpoint string, signer awsutil.RequestSigner, ttl time.Duration) (*Checker, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || signer == nil || arn == "" {
		return nil, errors.New("invalid MSK capability checker configuration")
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	return &Checker{arn: arn, endpoint: strings.TrimRight(endpoint, "/"), sign: signer, ttl: ttl, client: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("MSK control-plane redirects disabled") }}}, nil
}

func (c *Checker) Capabilities(ctx context.Context, force bool) (Capabilities, error) {
	c.mu.Lock()
	if !force && time.Now().Before(c.expires) {
		result, err := c.cached, c.lastErr
		c.mu.Unlock()
		return result, err
	}
	if c.inflight != nil {
		done := c.inflight
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return unknown(), ctx.Err()
		case <-done:
		}
		c.mu.Lock()
		result, err := c.cached, c.lastErr
		c.mu.Unlock()
		return result, err
	}
	c.inflight = make(chan struct{})
	done := c.inflight
	c.mu.Unlock()
	result, err := c.fetch(ctx)
	c.mu.Lock()
	c.cached = result
	c.lastErr = err
	c.expires = time.Now().Add(c.ttl)
	if err != nil {
		c.expires = time.Now().Add(time.Second)
	}
	c.inflight = nil
	close(done)
	c.mu.Unlock()
	return result, err
}

func (c *Checker) fetch(parent context.Context) (Capabilities, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "GET", c.endpoint+"/api/v2/clusters/"+url.PathEscape(c.arn), nil)
	if err != nil {
		return unknown(), err
	}
	if err = c.sign(ctx, request, nil); err != nil {
		return unknown(), errors.New("AWS credentials unavailable; cannot verify MSK rebalance capability")
	}
	response, err := c.client.Do(request)
	if err != nil {
		return unknown(), errors.New("MSK capability lookup unavailable; manual reassignment blocked")
	}
	defer response.Body.Close()
	if response.StatusCode == 403 {
		return deniedCapability("kafka:DescribeClusterV2", "describe the MSK cluster"), fmt.Errorf("MSK capability lookup denied (HTTP 403); missing kafka:DescribeClusterV2")
	}
	if response.StatusCode != 200 {
		return unknown(), fmt.Errorf("MSK capability lookup returned HTTP %d; require kafka:DescribeClusterV2 permission", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return unknown(), errors.New("invalid MSK capability response")
	}
	return ParseDescription(body)
}

func unknown() Capabilities {
	return Capabilities{Kind: "MSK", RebalancingStatus: "UNKNOWN", Reason: "MSK intelligent rebalancing status is unverified; manual reassignment is blocked", ObservedAt: time.Now().UTC()}
}

// deniedCapability reports the specific IAM action AWS denied so the operator
// can grant it, instead of a generic unknown result. action is the exact IAM
// action string (e.g. "kafka:DescribeClusterV2"); purpose is a short
// human-readable description of what that action was needed for.
func deniedCapability(action, purpose string) Capabilities {
	return Capabilities{Kind: "MSK", RebalancingStatus: "UNKNOWN", Reason: fmt.Sprintf("AWS denied the request to %s (HTTP 403 AccessDenied). Grant the %s permission to the role Kaflux runs as.", purpose, action), ObservedAt: time.Now().UTC()}
}

func ParseDescription(body []byte) (Capabilities, error) {
	var description struct {
		ClusterInfo struct {
			ClusterType string `json:"clusterType"`
			State       string `json:"state"`
			Provisioned *struct {
				BrokerNodeGroupInfo struct {
					InstanceType string `json:"instanceType"`
				} `json:"brokerNodeGroupInfo"`
				Rebalancing *struct {
					Status string `json:"status"`
				} `json:"rebalancing"`
			} `json:"provisioned"`
		} `json:"clusterInfo"`
	}
	if err := json.Unmarshal(body, &description); err != nil {
		return unknown(), errors.New("malformed MSK capability description")
	}
	cluster := description.ClusterInfo
	result := unknown()
	if cluster.ClusterType == "SERVERLESS" {
		result.Kind = "MSK Serverless"
		result.Reason = "MSK Serverless manages partition placement; manual reassignment is unavailable"
		return result, nil
	}
	if cluster.ClusterType != "PROVISIONED" || cluster.Provisioned == nil {
		return result, errors.New("MSK cluster type is unknown")
	}
	result.Kind = "MSK Provisioned"
	result.BrokerType = cluster.Provisioned.BrokerNodeGroupInfo.InstanceType
	if cluster.Provisioned.Rebalancing != nil {
		result.RebalancingStatus = strings.ToUpper(cluster.Provisioned.Rebalancing.Status)
	}
	if cluster.State != "ACTIVE" {
		result.Reason = "MSK cluster is not ACTIVE; manual reassignment is blocked"
		return result, nil
	}
	if strings.HasPrefix(strings.ToLower(result.BrokerType), "express.") {
		result.Kind = "MSK Express"
	}
	if result.RebalancingStatus == "ACTIVE" {
		// Planning only reads metadata, so it stays available. Running a plan does not.
		result.PlanningAllowed = true
		result.Reason = "Amazon MSK intelligent rebalancing is ACTIVE. Manual partition reassignment is blocked by AWS: plans can be generated and validated here, but an authorized operator must pause intelligent rebalancing in MSK before one can run."
		return result, nil
	}
	if strings.HasPrefix(strings.ToLower(result.BrokerType), "express.") {
		result.Kind = "MSK Express"
		if result.RebalancingStatus != "PAUSED" {
			result.RebalancingStatus = "UNKNOWN"
			result.Reason = "MSK Express intelligent rebalancing status is unknown; manual reassignment is blocked"
			return result, nil
		}
	} else if result.BrokerType == "" {
		return result, errors.New("MSK broker type is unknown")
	}
	result.ManualReassignmentAllowed = true
	result.PlanningAllowed = true
	result.Reason = ""
	if result.RebalancingStatus == "" || result.RebalancingStatus == "UNKNOWN" {
		result.RebalancingStatus = "NOT_APPLICABLE"
	}
	return result, nil
}
