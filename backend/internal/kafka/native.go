package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/msk"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	saslaws "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Config struct {
	ID                   string   `json:"id"`
	Name                 string   `json:"name"`
	Environment          string   `json:"environment"`
	Seeds                []string `json:"seeds"`
	TLS                  bool     `json:"tls"`
	CAFile               string   `json:"caFile"`
	CertFile             string   `json:"certFile"`
	KeyFile              string   `json:"keyFile"`
	SASL                 string   `json:"sasl"`
	User                 string   `json:"user"`
	PasswordEnv          string   `json:"passwordEnv"`
	AllowPlaintext       bool     `json:"allowPlaintext"`
	Region               string   `json:"region"`
	RoleARN              string   `json:"roleArn"`
	MSKClusterARN        string   `json:"mskClusterArn"`
	OAuthTokenEnv        string   `json:"oauthTokenEnv"`
	OAuthTokenEndpoint   string   `json:"oauthTokenEndpoint"`
	OAuthClientID        string   `json:"oauthClientId"`
	OAuthClientSecretEnv string   `json:"oauthClientSecretEnv"`
	OAuthScopes          []string `json:"oauthScopes"`
	OAuthCAFile          string   `json:"oauthCAFile"`
}
type Native struct {
	client      *kgo.Client
	admin       *kadm.Client
	opts        []kgo.Opt
	budget      chan struct{}
	mu          sync.Mutex
	cached      model.Snapshot
	expires     time.Time
	msk         *msk.Checker
	knownMSK    bool
	oauthTokens *oauthTokens
	sizeMu      sync.Mutex
	sizeCache   *logSizes
	sizeRetryAt time.Time
	groupsMu    sync.Mutex
	groupsCache []model.Group
	groupsUntil time.Time
}

func NewNative(c Config) (*Native, error) {
	var tokens *oauthTokens
	if len(c.Seeds) == 0 {
		return nil, fmt.Errorf("seeds required")
	}
	opts := []kgo.Opt{kgo.SeedBrokers(c.Seeds...), kgo.ClientID("kaflux"), kgo.RequestTimeoutOverhead(5 * time.Second), kgo.DialTimeout(5 * time.Second), kgo.RetryTimeout(8 * time.Second), kgo.FetchMaxBytes(1 << 20), kgo.RecordPartitioner(kgo.ManualPartitioner())}
	if c.TLS {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if c.CAFile != "" {
			b, e := os.ReadFile(c.CAFile)
			if e != nil {
				return nil, e
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(b) {
				return nil, fmt.Errorf("invalid CA")
			}
			tc.RootCAs = pool
		}
		if c.CertFile != "" {
			cert, e := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
			if e != nil {
				return nil, e
			}
			tc.Certificates = []tls.Certificate{cert}
		}
		opts = append(opts, kgo.DialTLSConfig(tc))
	} else if !c.AllowPlaintext {
		return nil, fmt.Errorf("plaintext requires explicit allowPlaintext")
	}
	password := os.Getenv(c.PasswordEnv)
	switch c.SASL {
	case "", "none":
	case "plain":
		if !c.TLS {
			return nil, fmt.Errorf("PLAIN requires TLS")
		}
		opts = append(opts, kgo.SASL(plain.Auth{User: c.User, Pass: password}.AsMechanism()))
	case "scram-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.User, Pass: password}.AsSha256Mechanism()))
	case "scram-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.User, Pass: password}.AsSha512Mechanism()))
	case "msk-iam":
		if !c.TLS {
			return nil, fmt.Errorf("MSK IAM requires TLS")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cfg, e := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(c.Region))
		if e != nil {
			return nil, e
		}
		credentials := cfg.Credentials
		if c.RoleARN != "" {
			credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), c.RoleARN))
		}
		opts = append(opts, kgo.SASL(saslaws.ManagedStreamingIAM(func(ctx context.Context) (saslaws.Auth, error) {
			v, e := credentials.Retrieve(ctx)
			return saslaws.Auth{AccessKey: v.AccessKeyID, SecretKey: v.SecretAccessKey, SessionToken: v.SessionToken, UserAgent: "kaflux"}, e
		})))
	case "oauthbearer":
		if !c.TLS {
			return nil, fmt.Errorf("OAUTHBEARER requires TLS")
		}
		if c.OAuthTokenEndpoint != "" {
			var err error
			tokens, err = newOAuthTokens(c)
			if err != nil {
				return nil, err
			}
			opts = append(opts, kgo.SASL(oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
				token, err := tokens.Token(ctx)
				return oauth.Auth{Token: token}, err
			})))
			break
		}
		if c.OAuthClientID != "" || c.OAuthClientSecretEnv != "" || len(c.OAuthScopes) > 0 || c.OAuthCAFile != "" {
			return nil, fmt.Errorf("OAuth client credentials require a token endpoint")
		}
		if c.OAuthTokenEnv == "" || os.Getenv(c.OAuthTokenEnv) == "" {
			return nil, fmt.Errorf("OAUTHBEARER requires a configured token environment reference")
		}
		opts = append(opts, kgo.SASL(oauth.Oauth(func(context.Context) (oauth.Auth, error) {
			token := os.Getenv(c.OAuthTokenEnv)
			if token == "" {
				return oauth.Auth{}, fmt.Errorf("OAuth bearer token is unavailable")
			}
			return oauth.Auth{Token: token}, nil
		})))
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q", c.SASL)
	}
	cl, e := kgo.NewClient(opts...)
	if e != nil {
		if tokens != nil {
			tokens.Close()
		}
		return nil, e
	}
	n := &Native{client: cl, admin: kadm.NewClient(cl), opts: opts, budget: make(chan struct{}, 8), oauthTokens: tokens}
	n.knownMSK = c.SASL == "msk-iam" || c.MSKClusterARN != ""
	for _, seed := range c.Seeds {
		if strings.Contains(seed, ".kafka.") || strings.Contains(seed, ".kafka-serverless.") {
			n.knownMSK = true
		}
	}
	if c.MSKClusterARN != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		checker, e := msk.New(ctx, c.MSKClusterARN, c.Region, c.RoleARN)
		if e != nil {
			n.Close()
			return nil, e
		}
		n.msk = checker
	}
	return n, nil
}
func (n *Native) bounded(ctx context.Context) (context.Context, func(), error) {
	select {
	case n.budget <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	c, cancel := context.WithTimeout(ctx, 10*time.Second)
	return c, func() { cancel(); <-n.budget }, nil
}
func (n *Native) Snapshot(ctx context.Context) (model.Snapshot, error) {
	n.mu.Lock()
	if time.Now().Before(n.expires) {
		s := n.cached
		n.mu.Unlock()
		return s, nil
	}
	n.mu.Unlock()
	c, done, e := n.bounded(ctx)
	if e != nil {
		return model.Snapshot{}, e
	}
	m, e := n.admin.Metadata(c)
	// Release the slot before sizes() takes its own: holding one while waiting
	// for another could exhaust the budget under concurrent refreshes.
	done()
	if e != nil {
		return model.Snapshot{}, e
	}
	s := snapshotFromMetadata(m, time.Now().UTC())
	n.sizes(ctx).apply(&s)
	n.mu.Lock()
	n.cached = s
	n.expires = time.Now().Add(5 * time.Second)
	n.mu.Unlock()
	return s, nil
}

// snapshotFromMetadata keeps partitions that report errors such as
// LEADER_NOT_AVAILABLE or REPLICA_NOT_AVAILABLE: brokers return them while a
// broker restarts, which is exactly when the cluster view must stay usable.
// Their leader (-1 when offline) and ISR still describe the degraded state.
// Topics whose metadata failed as a whole (deleted or still being created
// mid-request) carry no partitions and are skipped.
func snapshotFromMetadata(m kadm.Metadata, observedAt time.Time) model.Snapshot {
	s := model.Snapshot{Brokers: []model.Broker{}, Topics: []model.Topic{}, ObservedAt: observedAt}
	if m.Controller >= 0 {
		controller := m.Controller
		s.Controller = &controller
	}
	for _, b := range m.Brokers {
		rack := ""
		if b.Rack != nil {
			rack = *b.Rack
		}
		s.Brokers = append(s.Brokers, model.Broker{ID: b.NodeID, Host: b.Host, Port: b.Port, Rack: rack})
	}
	for _, t := range m.Topics {
		if t.Err != nil {
			continue
		}
		x := model.Topic{Name: t.Topic, Partitions: []model.Partition{}, ObservedAt: observedAt}
		for _, p := range t.Partitions {
			x.Partitions = append(x.Partitions, model.Partition{ID: p.Partition, Leader: p.Leader, Replicas: p.Replicas, ISR: p.ISR})
			if len(p.ISR) < len(p.Replicas) {
				x.URP++
			}
		}
		sort.Slice(x.Partitions, func(i, j int) bool { return x.Partitions[i].ID < x.Partitions[j].ID })
		// Partitions come from a map, so take the factor from the lowest-numbered one after
		// sorting. Taking the first one visited made it depend on map iteration order.
		if len(x.Partitions) > 0 {
			x.ReplicationFactor = len(x.Partitions[0].Replicas)
		}
		s.Topics = append(s.Topics, x)
	}
	sort.Slice(s.Topics, func(i, j int) bool { return s.Topics[i].Name < s.Topics[j].Name })
	brokerIndex := make(map[int32]int, len(s.Brokers))
	for i, b := range s.Brokers {
		brokerIndex[b.ID] = i
	}
	for _, t := range s.Topics {
		for _, p := range t.Partitions {
			if i, ok := brokerIndex[p.Leader]; ok {
				s.Brokers[i].Leaders++
			}
			for _, id := range p.Replicas {
				if i, ok := brokerIndex[id]; ok {
					s.Brokers[i].Partitions++
				}
			}
		}
	}
	return s
}

// groupsTTL bounds how often the full group list and its lag are computed. The
// list page and the overview both read it on every poll.
const groupsTTL = 10 * time.Second

// Groups lists all consumer groups with lag. Concurrent callers share one
// computation, and the result is reused for groupsTTL or until offsets change.
func (n *Native) Groups(ctx context.Context) ([]model.Group, error) {
	n.groupsMu.Lock()
	defer n.groupsMu.Unlock()
	if time.Now().Before(n.groupsUntil) {
		return append([]model.Group(nil), n.groupsCache...), nil
	}
	out, e := n.loadGroups(ctx)
	if e != nil {
		return nil, e
	}
	n.groupsCache, n.groupsUntil = out, time.Now().Add(groupsTTL)
	return append([]model.Group(nil), out...), nil
}

// invalidateGroups drops the cached list so the next read reflects a change
// made through Kaflux, such as an offset reset.
func (n *Native) invalidateGroups() {
	n.groupsMu.Lock()
	n.groupsUntil = time.Time{}
	n.groupsMu.Unlock()
}
func (n *Native) loadGroups(ctx context.Context) ([]model.Group, error) {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	// A ShardErrors result means some coordinators (e.g. a restarting broker)
	// did not answer; the groups they own are omitted but the rest are valid.
	g, e := n.admin.DescribeGroups(c)
	var shardErrs *kadm.ShardErrors
	if e != nil && !(errors.As(e, &shardErrs) && len(g) > 0) {
		return nil, e
	}
	out := []model.Group{}
	lags := n.groupLags(c, g)
	for _, x := range g {
		if x.Err != nil {
			out = append(out, model.Group{ID: x.Group, State: "Unavailable", Topics: []string{}})
			continue
		}
		topics := []string{}
		for t := range x.AssignedPartitions() {
			topics = append(topics, t)
		}
		sort.Strings(topics)
		out = append(out, model.Group{ID: x.Group, State: x.State, Members: len(x.Members), Topics: topics, Lag: lags[x.Group]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (n *Native) Messages(ctx context.Context, t string, p int32, o int64, l int) ([]model.Message, error) {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	opts := append([]kgo.Opt{}, n.opts...)
	opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{t: {p: kgo.NewOffset().At(o)}}), kgo.FetchMaxWait(500*time.Millisecond), kgo.BrokerMaxReadBytes(4<<20))
	cl, e := kgo.NewClient(opts...)
	if e != nil {
		return nil, e
	}
	defer cl.Close()
	f := cl.PollRecords(c, l)
	if e = f.Err(); e != nil && e != context.DeadlineExceeded {
		return nil, e
	}
	out := []model.Message{}
	previewBytes := 0
	f.EachRecord(func(r *kgo.Record) {
		if previewBytes >= 1<<20 {
			return
		}
		message, bytes := previewRecord(r, (1<<20)-previewBytes)
		previewBytes += bytes
		out = append(out, message)
	})
	return out, nil
}
func (n *Native) Produce(ctx context.Context, m model.Message) (model.Message, error) {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return m, e
	}
	defer done()
	r := &kgo.Record{Topic: m.Topic, Partition: m.Partition, Key: []byte(m.Key), Value: []byte(m.Value)}
	for _, h := range m.Headers {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: h.Key, Value: []byte(h.Value)})
	}
	e = n.client.ProduceSync(c, r).FirstErr()
	m.Offset = r.Offset
	m.Partition = r.Partition
	m.Timestamp = r.Timestamp
	m.KeyBase64 = base64.StdEncoding.EncodeToString(r.Key)
	m.ValueBase64 = base64.StdEncoding.EncodeToString(r.Value)
	return m, e
}
func (n *Native) Reassign(ctx context.Context, changes []model.Change) error {
	capabilities, capErr := n.Capabilities(ctx, true)
	if capErr != nil || !capabilities.ManualReassignmentAllowed {
		return &msk.BlockedError{Reason: capabilities.Reason}
	}
	c, done, e := n.bounded(ctx)
	if e != nil {
		return e
	}
	defer done()
	r := kadm.AlterPartitionAssignmentsReq{}
	for _, x := range changes {
		r.Assign(x.Topic, x.Partition, x.After)
	}
	out, e := n.admin.AlterPartitionAssignments(c, r)
	n.mu.Lock()
	n.expires = time.Time{}
	n.mu.Unlock()
	if e != nil {
		return e
	}
	return out.Error()
}
func (n *Native) Pending(ctx context.Context) (map[string][]int32, error) {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	r, e := n.admin.ListPartitionReassignments(c, nil)
	if e != nil {
		return nil, e
	}
	out := map[string][]int32{}
	for t, parts := range r {
		for p, x := range parts {
			out[fmt.Sprintf("%s/%d", t, p)] = x.Replicas
		}
	}
	return out, nil
}
func (n *Native) Close() {
	n.client.Close()
	if n.oauthTokens != nil {
		n.oauthTokens.Close()
	}
}
func (n *Native) Capabilities(ctx context.Context, force bool) (msk.Capabilities, error) {
	if n.msk != nil {
		return n.msk.Capabilities(ctx, force)
	}
	if n.knownMSK {
		return msk.Capabilities{Kind: "MSK", RebalancingStatus: "UNKNOWN", Reason: "Configure mskClusterArn to verify intelligent rebalance status", ObservedAt: time.Now().UTC()}, nil
	}
	return msk.Capabilities{Kind: "Kafka", ManualReassignmentAllowed: true, RebalancingStatus: "NOT_APPLICABLE", ObservedAt: time.Now().UTC()}, nil
}
func (n *Native) FreshSnapshot(ctx context.Context) (model.Snapshot, error) {
	n.mu.Lock()
	n.expires = time.Time{}
	n.mu.Unlock()
	return n.Snapshot(ctx)
}
