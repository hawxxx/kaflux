package capacity

import (
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/model"
)

func size(v int64) *int64 { return &v }

func snapshot(replicas ...int) model.Snapshot {
	s := model.Snapshot{}
	for i, r := range replicas {
		s.Brokers = append(s.Brokers, model.Broker{ID: int32(i + 1), Partitions: r})
	}
	return s
}

func TestConfigValidate(t *testing.T) {
	for name, tc := range map[string]struct {
		c  *Config
		ok bool
	}{
		"absent":                   {nil, true},
		"limits only":              {&Config{PartitionsPerBroker: &Limits{Recommended: 4000, Maximum: 6000}}, true},
		"recommended only":         {&Config{PartitionsPerBroker: &Limits{Recommended: 4000}}, true},
		"disk only":                {&Config{DiskBytesPerBroker: 1 << 40}, true},
		"empty":                    {&Config{Label: "r6i.4xlarge"}, false},
		"negative disk":            {&Config{DiskBytesPerBroker: -1}, false},
		"negative limit":           {&Config{PartitionsPerBroker: &Limits{Recommended: -5}}, false},
		"recommended over maximum": {&Config{PartitionsPerBroker: &Limits{Recommended: 7000, Maximum: 6000}}, false},
	} {
		if err := tc.c.Validate(); (err == nil) != tc.ok {
			t.Errorf("%s: err = %v, want ok=%v", name, err, tc.ok)
		}
	}
}

func TestFromMSKUsesTheLimitsAWSPublishes(t *testing.T) {
	for brokerType, want := range map[string]Limits{
		"express.m7g.large":      {1000, 1500},
		"express.m7g.xlarge":     {1000, 2000},
		"express.m7g.2xlarge":    {2500, 4000},
		"express.m7g.4xlarge":    {6000, 8000},
		"express.m7g.8xlarge":    {12000, 16000},
		"express.m7g.12xlarge":   {16000, 24000},
		"express.m7g.16xlarge":   {20000, 32000},
		"kafka.t3.small":         {300, 300},
		"kafka.m5.large":         {1000, 1500},
		"kafka.m5.2xlarge":       {2000, 3000},
		"kafka.m5.24xlarge":      {4000, 6000},
		"kafka.m7g.xlarge":       {1000, 1500},
		"kafka.m7g.2xlarge":      {2000, 3000},
		"kafka.m7g.16xlarge":     {4000, 6000},
		"  Express.M7G.8xlarge ": {12000, 16000},
	} {
		p, ok := FromMSK(brokerType)
		if !ok || p.Partitions == nil || *p.Partitions != want || p.Source != SourceAWSMSK || p.Reference == "" || p.Note == "" {
			t.Errorf("%q: got %+v ok=%v, want %+v", brokerType, p, ok, want)
		}
	}
}

func TestFromMSKDoesNotGuessUnlistedTypes(t *testing.T) {
	for _, brokerType := range []string{"", "m7g.8xlarge", "kafka.r5.large", "express.m7g.32xlarge", "kafka.m9g.large", "serverless"} {
		if p, ok := FromMSK(brokerType); ok || p.Known() {
			t.Errorf("%q must stay unknown, got %+v", brokerType, p)
		}
	}
}

func TestFromConfig(t *testing.T) {
	if _, ok := FromConfig(nil); ok {
		t.Fatal("absent config must be unknown")
	}
	p, ok := FromConfig(&Config{PartitionsPerBroker: &Limits{Recommended: 4000}})
	if !ok || p.Source != SourceConfigured || p.Label != "Configured capacity" {
		t.Fatalf("got %+v ok=%v", p, ok)
	}
	p, ok = FromConfig(&Config{Label: "r6i.4xlarge", DiskBytesPerBroker: 1 << 40})
	if !ok || p.Label != "r6i.4xlarge" || p.DiskBytes != 1<<40 {
		t.Fatalf("disk-only config: %+v ok=%v", p, ok)
	}
}

func TestEvaluateWithoutAProfileStillReportsLoadAndHowToFixIt(t *testing.T) {
	r := Evaluate(snapshot(300, 500, 400), Profile{})
	if r.Known || r.Status != StatusUnknown || r.Reason != UnknownReason || r.Source != "" || r.Partitions != nil {
		t.Fatalf("report = %+v", r)
	}
	if len(r.Brokers) != 3 || r.Busiest == nil || r.Busiest.ID != 2 || r.Busiest.Replicas != 500 {
		t.Fatalf("load must still be reported, busiest = %+v brokers = %+v", r.Busiest, r.Brokers)
	}
	for _, b := range r.Brokers {
		if b.Status != StatusUnknown || b.PercentOfRecommended != nil || b.PercentOfMaximum != nil {
			t.Fatalf("broker %d must not get invented percentages: %+v", b.ID, b)
		}
	}
}

func TestEvaluateClassifiesEachBrokerAndReportsTheWorst(t *testing.T) {
	p := Profile{Source: SourceConfigured, Label: "r6i.4xlarge", Partitions: &Limits{Recommended: 1000, Maximum: 1500}}
	r := Evaluate(snapshot(400, 1000, 1200, 1501), p)
	want := []string{StatusOK, StatusOK, StatusAboveRecommended, StatusOverMaximum}
	for i, b := range r.Brokers {
		if b.Status != want[i] {
			t.Errorf("broker %d status = %s, want %s", b.ID, b.Status, want[i])
		}
	}
	if !r.Known || r.Status != StatusOverMaximum || r.Busiest.ID != 4 || r.Label != "r6i.4xlarge" {
		t.Fatalf("report = %+v", r)
	}
	if got := *r.Brokers[0].PercentOfRecommended; got != 40 {
		t.Errorf("40%% of recommended expected, got %v", got)
	}
	if got := *r.Brokers[1].PercentOfMaximum; got < 66.6 || got > 66.7 {
		t.Errorf("1000/1500 should be about 66.7%% of maximum, got %v", got)
	}
}

func TestEvaluateWithOnlyARecommendedLimit(t *testing.T) {
	p := Profile{Source: SourceConfigured, Partitions: &Limits{Recommended: 1000}}
	r := Evaluate(snapshot(500, 2000), p)
	if r.Brokers[1].Status != StatusAboveRecommended || r.Status != StatusAboveRecommended {
		t.Fatalf("report = %+v", r)
	}
	if r.Brokers[0].PercentOfMaximum != nil {
		t.Fatal("no maximum was configured, so no percentage of it may be shown")
	}
}

func TestEvaluateAtTheLimitIsNotOver(t *testing.T) {
	p := Profile{Partitions: &Limits{Recommended: 1000, Maximum: 1500}}
	r := Evaluate(snapshot(1000, 1500), p)
	if r.Brokers[0].Status != StatusOK || r.Brokers[1].Status != StatusAboveRecommended {
		t.Fatalf("limits are inclusive: %+v", r.Brokers)
	}
}

func TestEvaluateDiskUsesMeasuredBrokerBytes(t *testing.T) {
	s := snapshot(10, 10)
	s.Brokers[0].SizeBytes = size(250)
	p := Profile{Source: SourceConfigured, DiskBytes: 1000}
	r := Evaluate(s, p)
	if !r.Known || r.DiskBytesPerBroker != 1000 {
		t.Fatalf("report = %+v", r)
	}
	if r.Brokers[0].PercentOfDisk == nil || *r.Brokers[0].PercentOfDisk != 25 || *r.Brokers[0].DiskBytes != 250 {
		t.Fatalf("broker 1 = %+v", r.Brokers[0])
	}
	if r.Brokers[1].PercentOfDisk != nil {
		t.Fatal("a broker whose size is unknown must not get a disk percentage")
	}
	if r.Status != StatusUnknown {
		t.Fatalf("without partition limits the partition status is unknown, got %s", r.Status)
	}
}

func TestEvaluateEmptyCluster(t *testing.T) {
	r := Evaluate(model.Snapshot{}, Profile{Partitions: &Limits{Recommended: 10}})
	if r.Status != StatusUnknown || r.Busiest != nil || len(r.Brokers) != 0 || r.MedianReplicas != nil {
		t.Fatalf("report = %+v", r)
	}
}

func TestEvaluateMedianReplicas(t *testing.T) {
	if got := *Evaluate(snapshot(10, 20, 30), Profile{}).MedianReplicas; got != 20 {
		t.Errorf("odd count median = %v, want 20", got)
	}
	if got := *Evaluate(snapshot(10, 20, 30, 40), Profile{}).MedianReplicas; got != 25 {
		t.Errorf("even count median = %v, want 25", got)
	}
	if got := *Evaluate(snapshot(7), Profile{}).MedianReplicas; got != 7 {
		t.Errorf("single broker median = %v, want 7", got)
	}
}
