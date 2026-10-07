// Package capacity describes how much a broker is meant to hold and compares
// it with what each broker holds now.
//
// Kaflux never guesses capacity. A profile comes from the operator's cluster
// configuration or from limits a platform publishes for a broker type it can
// read, such as Amazon MSK. Any field that neither source provides stays unset
// and is reported as unknown.
package capacity

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hawxxx/kaflux/backend/internal/model"
)

// Sources of a profile.
const (
	SourceConfigured = "configured"
	SourceAWSMSK     = "aws-msk"
)

// Status of a broker or of the cluster against its partition limits.
const (
	StatusOK               = "ok"
	StatusAboveRecommended = "above-recommended"
	StatusOverMaximum      = "over-maximum"
	StatusUnknown          = "unknown"
)

// Limits are partition counts per broker. Replicas count, leaders and
// followers alike, which is how Amazon MSK states its limits. Zero means not set.
type Limits struct {
	Recommended int `json:"recommended,omitempty" yaml:"recommended"`
	Maximum     int `json:"maximum,omitempty" yaml:"maximum"`
}

// Config is the capacity an operator declares for a cluster. It is the way to
// describe self-managed Kafka on EC2 or Kubernetes, where the platform offers
// no broker type to read and nobody publishes limits for it.
type Config struct {
	// Label is shown in the UI, for example the instance type.
	Label string `json:"label,omitempty" yaml:"label"`
	// PartitionsPerBroker are limits for replicas hosted on one broker.
	PartitionsPerBroker *Limits `json:"partitionsPerBroker,omitempty" yaml:"partitionsPerBroker"`
	// DiskBytesPerBroker is the usable log storage of each broker.
	DiskBytesPerBroker int64 `json:"diskBytesPerBroker,omitempty" yaml:"diskBytesPerBroker"`
}

// Validate rejects values that cannot describe a broker.
func (c *Config) Validate() error {
	if c == nil {
		return nil
	}
	if c.DiskBytesPerBroker < 0 {
		return fmt.Errorf("capacity diskBytesPerBroker must not be negative")
	}
	if l := c.PartitionsPerBroker; l != nil {
		if l.Recommended < 0 || l.Maximum < 0 {
			return fmt.Errorf("capacity partitionsPerBroker limits must not be negative")
		}
		if l.Recommended > 0 && l.Maximum > 0 && l.Recommended > l.Maximum {
			return fmt.Errorf("capacity partitionsPerBroker recommended (%d) exceeds maximum (%d)", l.Recommended, l.Maximum)
		}
	}
	if c.PartitionsPerBroker == nil && c.DiskBytesPerBroker == 0 {
		return fmt.Errorf("capacity needs partitionsPerBroker or diskBytesPerBroker")
	}
	return nil
}

// Profile is the capacity of one broker. Unset fields are unknown.
type Profile struct {
	Source     string
	Label      string
	Reference  string
	Note       string
	Partitions *Limits
	DiskBytes  int64
}

// Known reports whether the profile has any number to compare against.
func (p Profile) Known() bool {
	return (p.Partitions != nil && (p.Partitions.Recommended > 0 || p.Partitions.Maximum > 0)) || p.DiskBytes > 0
}

// FromConfig turns an operator declaration into a profile.
func FromConfig(c *Config) (Profile, bool) {
	if c == nil {
		return Profile{}, false
	}
	p := Profile{Source: SourceConfigured, Label: c.Label, Partitions: c.PartitionsPerBroker, DiskBytes: c.DiskBytesPerBroker}
	if p.Label == "" {
		p.Label = "Configured capacity"
	}
	return p, p.Known()
}

// The tables below are the limits Amazon MSK documents per broker size, read
// from the AWS documentation on 2026-10-06. AWS can change them, so a cluster's
// own capacity configuration takes precedence over these.
const (
	expressReference  = "https://docs.aws.amazon.com/msk/latest/developerguide/limits.html#msk-express-broker-partition-quota"
	standardReference = "https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html"
	expressNote       = "The recommended count is guidance. MSK does not allow an Express broker to exceed the maximum."
	standardNote      = "The recommended count is guidance. Above the maximum, MSK blocks updates such as configuration changes and moving to a smaller broker size."
)

var expressLimits = map[string]Limits{
	"express.m7g.large":    {Recommended: 1000, Maximum: 1500},
	"express.m7g.xlarge":   {Recommended: 1000, Maximum: 2000},
	"express.m7g.2xlarge":  {Recommended: 2500, Maximum: 4000},
	"express.m7g.4xlarge":  {Recommended: 6000, Maximum: 8000},
	"express.m7g.8xlarge":  {Recommended: 12000, Maximum: 16000},
	"express.m7g.12xlarge": {Recommended: 16000, Maximum: 24000},
	"express.m7g.16xlarge": {Recommended: 20000, Maximum: 32000},
}

var standardLimits = map[string]Limits{
	"kafka.t3.small":     {Recommended: 300, Maximum: 300},
	"kafka.m5.large":     {Recommended: 1000, Maximum: 1500},
	"kafka.m5.xlarge":    {Recommended: 1000, Maximum: 1500},
	"kafka.m5.2xlarge":   {Recommended: 2000, Maximum: 3000},
	"kafka.m5.4xlarge":   {Recommended: 4000, Maximum: 6000},
	"kafka.m5.8xlarge":   {Recommended: 4000, Maximum: 6000},
	"kafka.m5.12xlarge":  {Recommended: 4000, Maximum: 6000},
	"kafka.m5.16xlarge":  {Recommended: 4000, Maximum: 6000},
	"kafka.m5.24xlarge":  {Recommended: 4000, Maximum: 6000},
	"kafka.m7g.large":    {Recommended: 1000, Maximum: 1500},
	"kafka.m7g.xlarge":   {Recommended: 1000, Maximum: 1500},
	"kafka.m7g.2xlarge":  {Recommended: 2000, Maximum: 3000},
	"kafka.m7g.4xlarge":  {Recommended: 4000, Maximum: 6000},
	"kafka.m7g.8xlarge":  {Recommended: 4000, Maximum: 6000},
	"kafka.m7g.12xlarge": {Recommended: 4000, Maximum: 6000},
	"kafka.m7g.16xlarge": {Recommended: 4000, Maximum: 6000},
}

// FromMSK returns the limits Amazon MSK documents for a broker type such as
// "express.m7g.8xlarge" or "kafka.m5.large". Types AWS has not published
// limits for, or that this table does not list yet, are not guessed.
func FromMSK(brokerType string) (Profile, bool) {
	key := strings.ToLower(strings.TrimSpace(brokerType))
	if l, ok := expressLimits[key]; ok {
		return Profile{Source: SourceAWSMSK, Label: key, Reference: expressReference, Note: expressNote, Partitions: &l}, true
	}
	if l, ok := standardLimits[key]; ok {
		return Profile{Source: SourceAWSMSK, Label: key, Reference: standardReference, Note: standardNote, Partitions: &l}, true
	}
	return Profile{}, false
}

// Broker is one broker's load against the profile.
type Broker struct {
	ID                   int32    `json:"broker"`
	Replicas             int      `json:"replicas"`
	PercentOfRecommended *float64 `json:"percentOfRecommended,omitempty"`
	PercentOfMaximum     *float64 `json:"percentOfMaximum,omitempty"`
	DiskBytes            *int64   `json:"diskBytes,omitempty"`
	PercentOfDisk        *float64 `json:"percentOfDisk,omitempty"`
	Status               string   `json:"status"`
}

// Report is what the Balance page shows.
type Report struct {
	Known      bool    `json:"known"`
	Source     string  `json:"source,omitempty"`
	Label      string  `json:"label,omitempty"`
	Reference  string  `json:"reference,omitempty"`
	Note       string  `json:"note,omitempty"`
	Reason     string  `json:"reason,omitempty"`
	Partitions *Limits `json:"partitionsPerBroker,omitempty"`
	// DiskBytesPerBroker is the configured log storage of one broker.
	DiskBytesPerBroker int64 `json:"diskBytesPerBroker,omitempty"`
	// Status is the worst status of any broker, or unknown without limits.
	Status  string  `json:"status"`
	Busiest *Broker `json:"busiest,omitempty"`
	// MedianReplicas is the median replica count across every broker in the
	// snapshot. Reported alongside Busiest regardless of whether a profile is
	// known, since replica counts always come from cluster metadata.
	MedianReplicas *float64 `json:"medianReplicas,omitempty"`
	Brokers        []Broker `json:"brokers"`
}

// UnknownReason tells the operator how to make capacity known.
const UnknownReason = "Capacity is not known for this cluster. Set capacity in the cluster configuration. For Amazon MSK, set mskClusterArn so the broker type can be read."

var severity = map[string]int{StatusUnknown: 0, StatusOK: 1, StatusAboveRecommended: 2, StatusOverMaximum: 3}

func percent(part, whole int64) *float64 {
	v := float64(part) * 100 / float64(whole)
	return &v
}

func median(replicas []int) *float64 {
	if len(replicas) == 0 {
		return nil
	}
	sorted := append([]int(nil), replicas...)
	sort.Ints(sorted)
	n := len(sorted)
	var v float64
	if n%2 == 1 {
		v = float64(sorted[n/2])
	} else {
		v = float64(sorted[n/2-1]+sorted[n/2]) / 2
	}
	return &v
}

// Evaluate compares every broker in the snapshot with the profile. Replica
// counts come from cluster metadata and exist for every cluster, so the load
// is reported even when the profile is unknown.
func Evaluate(snap model.Snapshot, p Profile) Report {
	r := Report{Known: p.Known(), Source: p.Source, Label: p.Label, Reference: p.Reference, Note: p.Note, Partitions: p.Partitions, DiskBytesPerBroker: p.DiskBytes, Status: StatusUnknown, Brokers: []Broker{}}
	if !r.Known {
		r.Source, r.Label, r.Reference, r.Note, r.Partitions, r.DiskBytesPerBroker = "", "", "", "", nil, 0
		r.Reason = UnknownReason
	}
	replicas := make([]int, 0, len(snap.Brokers))
	for _, b := range snap.Brokers {
		x := Broker{ID: b.ID, Replicas: b.Partitions, Status: StatusUnknown}
		replicas = append(replicas, b.Partitions)
		if l := r.Partitions; r.Known && l != nil {
			x.Status = StatusOK
			if l.Recommended > 0 {
				x.PercentOfRecommended = percent(int64(b.Partitions), int64(l.Recommended))
				if b.Partitions > l.Recommended {
					x.Status = StatusAboveRecommended
				}
			}
			if l.Maximum > 0 {
				x.PercentOfMaximum = percent(int64(b.Partitions), int64(l.Maximum))
				if b.Partitions > l.Maximum {
					x.Status = StatusOverMaximum
				}
			}
		}
		if r.Known && p.DiskBytes > 0 && b.SizeBytes != nil {
			used := *b.SizeBytes
			x.DiskBytes = &used
			x.PercentOfDisk = percent(used, p.DiskBytes)
		}
		r.Brokers = append(r.Brokers, x)
	}
	sort.Slice(r.Brokers, func(i, j int) bool { return r.Brokers[i].ID < r.Brokers[j].ID })
	r.MedianReplicas = median(replicas)
	for i := range r.Brokers {
		b := &r.Brokers[i]
		if r.Busiest == nil || b.Replicas > r.Busiest.Replicas {
			r.Busiest = b
		}
		if severity[b.Status] > severity[r.Status] {
			r.Status = b.Status
		}
	}
	return r
}
