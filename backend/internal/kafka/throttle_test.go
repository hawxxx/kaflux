package kafka

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type configFixture struct {
	values map[string]string
	writes int
	mu     sync.Mutex
}
type ConfigSource = kmsg.ConfigSource

func (f *configFixture) describe(kind string, names []string) kadm.ResourceConfigs {
	out := kadm.ResourceConfigs{}
	for _, name := range names {
		r := kadm.ResourceConfig{Name: name}
		keys := []string{"leader.replication.throttled.rate", "follower.replication.throttled.rate"}
		if kind == "topic" {
			keys = []string{"leader.replication.throttled.replicas", "follower.replication.throttled.replicas"}
		}
		for _, key := range keys {
			v, ok := f.values[kind+"/"+name+"/"+key]
			source := int8(5)
			if !ok {
				if kind == "topic" {
					v = ""
				} else {
					v = "-1"
				}
			} else if kind == "topic" {
				source = 1
			} else {
				source = 2
			}
			r.Configs = append(r.Configs, kadm.Config{Key: key, Value: &v, Source: ConfigSource(source)})
		}
		out = append(out, r)
	}
	return out
}
func (f *configFixture) DescribeBrokerConfigs(ctx context.Context, ids ...int32) (kadm.ResourceConfigs, error) {
	names := []string{}
	for _, id := range ids {
		names = append(names, strconv.Itoa(int(id)))
	}
	return f.describe("broker", names), nil
}
func (f *configFixture) DescribeTopicConfigs(ctx context.Context, names ...string) (kadm.ResourceConfigs, error) {
	return f.describe("topic", names), nil
}
func (f *configFixture) alter(kind string, configs []kadm.AlterConfig, names []string) (kadm.AlterConfigsResponses, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := kadm.AlterConfigsResponses{}
	for _, name := range names {
		for _, c := range configs {
			k := kind + "/" + name + "/" + c.Name
			if c.Op == kadm.DeleteConfig {
				delete(f.values, k)
			} else {
				f.values[k] = *c.Value
			}
			f.writes++
		}
		out = append(out, kadm.AlterConfigsResponse{Name: name})
	}
	return out, nil
}
func (f *configFixture) AlterBrokerConfigs(ctx context.Context, c []kadm.AlterConfig, ids ...int32) (kadm.AlterConfigsResponses, error) {
	names := []string{}
	for _, id := range ids {
		names = append(names, strconv.Itoa(int(id)))
	}
	return f.alter("broker", c, names)
}
func (f *configFixture) AlterTopicConfigs(ctx context.Context, c []kadm.AlterConfig, names ...string) (kadm.AlterConfigsResponses, error) {
	return f.alter("topic", c, names)
}

func TestThrottleCapturesBeforeApplyAndRestoresOnlyOwnedSettings(t *testing.T) {
	f := &configFixture{values: map[string]string{}}
	changes := []model.Change{{Topic: "orders", Partition: 0, Before: []int32{1, 2}, After: []int32{2, 3}}}
	records, err := PrepareThrottle(context.Background(), f, changes, 1000000)
	if err != nil {
		t.Fatal(err)
	}
	if f.writes != 0 {
		t.Fatal("prepare mutated Kafka")
	}
	if len(records) != 8 {
		t.Fatalf("expected 6 broker rates and 2 topic lists, got %d", len(records))
	}
	if err = ApplyThrottle(context.Background(), f, records); err != nil {
		t.Fatal(err)
	}
	// A different tool changes a setting while reassignment runs. Never erase its change.
	f.values["broker/1/leader.replication.throttled.rate"] = "2000000"
	if err = RestoreThrottle(context.Background(), f, records); err == nil {
		t.Fatal("external conflict should be reported")
	}
	if f.values["broker/1/leader.replication.throttled.rate"] != "2000000" {
		t.Fatal("unrelated throttle overwritten")
	}
	if _, exists := f.values["broker/2/leader.replication.throttled.rate"]; exists {
		t.Fatal("original inherited config should be restored by delete")
	}
}

func TestExistingThrottleCannotBeTakenOver(t *testing.T) {
	f := &configFixture{values: map[string]string{"broker/1/leader.replication.throttled.rate": "1234"}}
	if _, err := PrepareThrottle(context.Background(), f, []model.Change{{Topic: "a", Partition: 0, Before: []int32{1}, After: []int32{2}}}, 100); err == nil {
		t.Fatal("existing throttle accepted")
	}
	if f.writes != 0 {
		t.Fatal("failed prepare mutated Kafka")
	}
}

func TestResumeApplyIsIdempotent(t *testing.T) {
	f := &configFixture{values: map[string]string{}}
	records, err := PrepareThrottle(context.Background(), f, []model.Change{{Topic: "a", Partition: 0, Before: []int32{1}, After: []int32{2}}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err = ApplyThrottle(context.Background(), f, records); err != nil {
		t.Fatal(err)
	}
	writes := f.writes
	if err = ApplyThrottle(context.Background(), f, records); err != nil {
		t.Fatal(err)
	}
	if writes != f.writes {
		t.Fatal("resume repeated already-applied settings")
	}
	if err = RestoreThrottle(context.Background(), f, records); err != nil {
		t.Fatal(err)
	}
	if len(f.values) != 0 {
		t.Fatal("throttle configs leaked")
	}
}
