package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ThrottleRecord must be durably stored before changing broker configuration.
type ThrottleRecord struct {
	Resource        string  `json:"resource"`
	Name            string  `json:"name"`
	Key             string  `json:"key"`
	Previous        *string `json:"previous"`
	EffectiveBefore string  `json:"effectiveBefore"`
	Applied         string  `json:"applied"`
	TransitionFrom  string  `json:"transitionFrom,omitempty"`
}

// RetargetThrottle must be persisted before applying the new rate. Original
// configuration remains unchanged, including inherited/default settings.
func RetargetThrottle(records []ThrottleRecord, rate int64) ([]ThrottleRecord, error) {
	if rate < 1 || rate > 1000000000000 {
		return nil, errors.New("throttle must be between 1 and 1000000000000 bytes/sec")
	}
	if len(records) == 0 {
		return nil, errors.New("throttle ownership is unavailable")
	}
	next := append([]ThrottleRecord(nil), records...)
	brokers := 0
	for i := range next {
		r := &next[i]
		if r.Resource != "broker" {
			continue
		}
		if r.Key != "leader.replication.throttled.rate" && r.Key != "follower.replication.throttled.rate" {
			return nil, errors.New("invalid owned throttle rate")
		}
		brokers++
		value := strconv.FormatInt(rate, 10)
		if value == r.Applied {
			continue
		}
		if r.TransitionFrom != "" {
			return nil, errors.New("previous throttle transition must be reconciled first")
		}
		r.TransitionFrom = r.Applied
		r.Applied = value
	}
	if brokers == 0 {
		return nil, errors.New("no broker rate ownership was captured")
	}
	return next, nil
}
func FinalizeThrottle(records []ThrottleRecord) []ThrottleRecord {
	next := append([]ThrottleRecord(nil), records...)
	for i := range next {
		next[i].TransitionFrom = ""
	}
	return next
}

type ThrottleProvider interface {
	PrepareThrottle(context.Context, []model.Change, int64) ([]ThrottleRecord, error)
	ApplyThrottle(context.Context, []ThrottleRecord) error
	RestoreThrottle(context.Context, []ThrottleRecord) error
}

type ConfigAdmin interface {
	DescribeBrokerConfigs(context.Context, ...int32) (kadm.ResourceConfigs, error)
	DescribeTopicConfigs(context.Context, ...string) (kadm.ResourceConfigs, error)
	AlterBrokerConfigs(context.Context, []kadm.AlterConfig, ...int32) (kadm.AlterConfigsResponses, error)
	AlterTopicConfigs(context.Context, []kadm.AlterConfig, ...string) (kadm.AlterConfigsResponses, error)
}

func (n *Native) PrepareThrottle(ctx context.Context, changes []model.Change, rate int64) ([]ThrottleRecord, error) {
	c, done, err := n.bounded(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return PrepareThrottle(c, n.admin, changes, rate)
}
func (n *Native) ApplyThrottle(ctx context.Context, records []ThrottleRecord) error {
	capability, e := n.Capabilities(ctx, true)
	if e != nil || !capability.ManualReassignmentAllowed {
		return fmt.Errorf("manual throttle change blocked: %s", capability.Reason)
	}
	c, done, err := n.bounded(ctx)
	if err != nil {
		return err
	}
	defer done()
	return ApplyThrottle(c, n.admin, records)
}
func (n *Native) RestoreThrottle(ctx context.Context, records []ThrottleRecord) error {
	c, done, err := n.bounded(ctx)
	if err != nil {
		return err
	}
	defer done()
	return RestoreThrottle(c, n.admin, records)
}

func PrepareThrottle(ctx context.Context, admin ConfigAdmin, changes []model.Change, rate int64) ([]ThrottleRecord, error) {
	if rate <= 0 {
		return nil, errors.New("a positive throttle bytes/sec is required")
	}
	brokerSet := map[int32]bool{}
	topicReplicas := map[string]map[string]bool{}
	for _, change := range changes {
		if change.Topic == "" || change.Partition < 0 {
			return nil, errors.New("invalid throttle partition")
		}
		if topicReplicas[change.Topic] == nil {
			topicReplicas[change.Topic] = map[string]bool{}
		}
		for _, brokers := range [][]int32{change.Before, change.After} {
			for _, id := range brokers {
				if id < 0 {
					return nil, errors.New("invalid throttle broker")
				}
				brokerSet[id] = true
				topicReplicas[change.Topic][fmt.Sprintf("%d:%d", change.Partition, id)] = true
			}
		}
	}
	brokers := []int32{}
	for id := range brokerSet {
		brokers = append(brokers, id)
	}
	sort.Slice(brokers, func(i, j int) bool { return brokers[i] < brokers[j] })
	topics := []string{}
	for name := range topicReplicas {
		topics = append(topics, name)
	}
	sort.Strings(topics)
	records := []ThrottleRecord{}
	clusterDefaults, err := admin.DescribeBrokerConfigs(ctx)
	if err != nil {
		return nil, err
	}
	for _, resource := range clusterDefaults {
		if resource.Err != nil {
			return nil, resource.Err
		}
		for _, config := range resource.Configs {
			if config.Key == "leader.replication.throttled.rate" || config.Key == "follower.replication.throttled.rate" {
				if config.Value != nil {
					value, err := strconv.ParseInt(*config.Value, 10, 64)
					if err != nil || value >= 0 {
						return nil, errors.New("cluster-level replication throttle is already active")
					}
				}
			}
		}
	}
	brokerConfigs, err := admin.DescribeBrokerConfigs(ctx, brokers...)
	if err != nil {
		return nil, err
	}
	for _, id := range brokers {
		for _, key := range []string{"leader.replication.throttled.rate", "follower.replication.throttled.rate"} {
			r, err := capture(brokerConfigs, "broker", strconv.Itoa(int(id)), key, strconv.FormatInt(rate, 10))
			if err != nil {
				return nil, err
			}
			effective, err := strconv.ParseInt(r.EffectiveBefore, 10, 64)
			if err != nil || effective >= 0 {
				return nil, fmt.Errorf("broker %d already has an active or unknown replication throttle", id)
			}
			records = append(records, r)
		}
	}
	topicConfigs, err := admin.DescribeTopicConfigs(ctx, topics...)
	if err != nil {
		return nil, err
	}
	for _, name := range topics {
		replicas := []string{}
		for replica := range topicReplicas[name] {
			replicas = append(replicas, replica)
		}
		sort.Strings(replicas)
		for _, key := range []string{"leader.replication.throttled.replicas", "follower.replication.throttled.replicas"} {
			r, err := capture(topicConfigs, "topic", name, key, strings.Join(replicas, ","))
			if err != nil {
				return nil, err
			}
			if r.EffectiveBefore != "" {
				return nil, fmt.Errorf("topic %s already has replication throttle selections", name)
			}
			records = append(records, r)
		}
	}
	return records, nil
}

func capture(configs kadm.ResourceConfigs, kind, name, key, applied string) (ThrottleRecord, error) {
	for _, resource := range configs {
		if resource.Name != name {
			continue
		}
		if resource.Err != nil {
			return ThrottleRecord{}, resource.Err
		}
		for _, config := range resource.Configs {
			if config.Key != key {
				continue
			}
			if config.Sensitive || config.Value == nil {
				return ThrottleRecord{}, errors.New("throttle configuration is not readable")
			}
			record := ThrottleRecord{Resource: kind, Name: name, Key: key, EffectiveBefore: *config.Value, Applied: applied}
			if (kind == "topic" && config.Source == kmsg.ConfigSourceDynamicTopicConfig) || (kind == "broker" && config.Source == kmsg.ConfigSourceDynamicBrokerConfig) {
				value := *config.Value
				record.Previous = &value
			}
			return record, nil
		}
	}
	// Kafka omits unset dynamic broker quota keys. Their documented default is -1.
	for _, resource := range configs {
		if resource.Name == name && resource.Err == nil {
			if value, ok := defaultThrottle(kind, key); ok {
				return ThrottleRecord{Resource: kind, Name: name, Key: key, EffectiveBefore: value, Applied: applied}, nil
			}
		}
	}
	return ThrottleRecord{}, fmt.Errorf("throttle configuration %s unavailable for %s", key, name)
}

func defaultThrottle(kind, key string) (string, bool) {
	if kind == "broker" && (key == "leader.replication.throttled.rate" || key == "follower.replication.throttled.rate") {
		return "-1", true
	}
	if kind == "topic" && (key == "leader.replication.throttled.replicas" || key == "follower.replication.throttled.replicas") {
		return "", true
	}
	return "", false
}

func currentConfigs(ctx context.Context, admin ConfigAdmin, records []ThrottleRecord) (map[string]string, error) {
	brokers := map[int32]bool{}
	topics := map[string]bool{}
	for _, r := range records {
		switch r.Resource {
		case "broker":
			id, err := strconv.ParseInt(r.Name, 10, 32)
			if err != nil {
				return nil, err
			}
			brokers[int32(id)] = true
		case "topic":
			topics[r.Name] = true
		default:
			return nil, errors.New("invalid throttle resource")
		}
	}
	ids := []int32{}
	for id := range brokers {
		ids = append(ids, id)
	}
	names := []string{}
	for name := range topics {
		names = append(names, name)
	}
	out := map[string]string{}
	seenResources := map[string]bool{}
	wanted := map[string]bool{}
	for _, r := range records {
		wanted[r.Resource+"/"+r.Name+"/"+r.Key] = true
	}
	for _, kind := range []string{"broker", "topic"} {
		var configs kadm.ResourceConfigs
		var err error
		if kind == "broker" && len(ids) > 0 {
			configs, err = admin.DescribeBrokerConfigs(ctx, ids...)
		} else if kind == "topic" && len(names) > 0 {
			configs, err = admin.DescribeTopicConfigs(ctx, names...)
		}
		if err != nil {
			return nil, err
		}
		for _, resource := range configs {
			if resource.Err != nil {
				return nil, resource.Err
			}
			seenResources[kind+"/"+resource.Name] = true
			for _, config := range resource.Configs {
				key := kind + "/" + resource.Name + "/" + config.Key
				if wanted[key] && (config.Sensitive || config.Value == nil) {
					return nil, errors.New("owned throttle configuration is unreadable")
				}
				if config.Value != nil {
					out[kind+"/"+resource.Name+"/"+config.Key] = *config.Value
				}
			}
		}
	}
	for _, r := range records {
		if !seenResources[r.Resource+"/"+r.Name] {
			return nil, errors.New("owned throttle resource is unavailable")
		}
		key := r.Resource + "/" + r.Name + "/" + r.Key
		if _, exists := out[key]; !exists {
			if value, ok := defaultThrottle(r.Resource, r.Key); ok {
				out[key] = value
			}
		}
	}
	return out, nil
}

func ApplyThrottle(ctx context.Context, admin ConfigAdmin, records []ThrottleRecord) error {
	current, err := currentConfigs(ctx, admin, records)
	if err != nil {
		return err
	}
	updates := []configUpdate{}
	for _, r := range records {
		value, ok := current[r.Resource+"/"+r.Name+"/"+r.Key]
		if !ok {
			return errors.New("throttle configuration disappeared")
		}
		if value == r.Applied {
			continue
		}
		if value != r.EffectiveBefore && (r.TransitionFrom == "" || value != r.TransitionFrom) {
			return fmt.Errorf("throttle changed externally on %s %s; refusing takeover", r.Resource, r.Name)
		}
		v := r.Applied
		updates = append(updates, configUpdate{record: r, operation: kadm.SetConfig, value: &v})
	}
	if err = writeConfigs(ctx, admin, updates); err != nil {
		return err
	}
	return verifyConfigs(ctx, admin, updates, false)
}

func RestoreThrottle(ctx context.Context, admin ConfigAdmin, records []ThrottleRecord) error {
	current, err := currentConfigs(ctx, admin, records)
	if err != nil {
		return err
	}
	updates := []configUpdate{}
	conflicts := []error{}
	for _, r := range records {
		value, ok := current[r.Resource+"/"+r.Name+"/"+r.Key]
		if !ok {
			conflicts = append(conflicts, errors.New("throttle resource unavailable during restore"))
			continue
		}
		if value == r.EffectiveBefore {
			continue
		}
		if value != r.Applied && (r.TransitionFrom == "" || value != r.TransitionFrom) {
			conflicts = append(conflicts, fmt.Errorf("external throttle on %s %s preserved", r.Resource, r.Name))
			continue
		}
		op := kadm.SetConfig
		if r.Previous == nil {
			op = kadm.DeleteConfig
		}
		updates = append(updates, configUpdate{record: r, operation: op, value: r.Previous})
	}
	if err = writeConfigs(ctx, admin, updates); err != nil {
		conflicts = append(conflicts, err)
	} else if err = verifyConfigs(ctx, admin, updates, true); err != nil {
		conflicts = append(conflicts, err)
	}
	return errors.Join(conflicts...)
}

type configUpdate struct {
	record    ThrottleRecord
	operation kadm.IncrementalOp
	value     *string
}

// KRaft acknowledges config changes before every broker has applied the metadata record.
func verifyConfigs(ctx context.Context, admin ConfigAdmin, updates []configUpdate, restore bool) error {
	if len(updates) == 0 {
		return nil
	}
	records := make([]ThrottleRecord, len(updates))
	for i, u := range updates {
		records[i] = u.record
	}
	verification, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	for {
		current, err := currentConfigs(verification, admin, records)
		if err != nil {
			return err
		}
		complete := true
		for _, r := range records {
			expected := r.Applied
			if restore {
				expected = r.EffectiveBefore
			}
			if current[r.Resource+"/"+r.Name+"/"+r.Key] != expected {
				complete = false
				break
			}
		}
		if complete {
			return nil
		}
		select {
		case <-verification.Done():
			return errors.New("throttle configuration propagation not yet verified; retain ownership for reconciliation")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Resource configurations use a fixed pool, never one goroutine per broker or topic.
func writeConfigs(ctx context.Context, admin ConfigAdmin, updates []configUpdate) error {
	byResource := map[string][]configUpdate{}
	for _, u := range updates {
		key := u.record.Resource + "/" + u.record.Name
		byResource[key] = append(byResource[key], u)
	}
	groups := make(chan []configUpdate, 8)
	var mu sync.Mutex
	failures := []error{}
	var wg sync.WaitGroup
	workers := min(8, len(byResource))
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for group := range groups {
				if len(group) == 0 {
					continue
				}
				configs := []kadm.AlterConfig{}
				for _, u := range group {
					configs = append(configs, kadm.AlterConfig{Op: u.operation, Name: u.record.Key, Value: u.value})
				}
				var result kadm.AlterConfigsResponses
				var err error
				r := group[0].record
				if r.Resource == "broker" {
					id, e := strconv.ParseInt(r.Name, 10, 32)
					if e != nil {
						err = e
					} else {
						result, err = admin.AlterBrokerConfigs(ctx, configs, int32(id))
					}
				} else {
					result, err = admin.AlterTopicConfigs(ctx, configs, r.Name)
				}
				if err == nil {
					for _, response := range result {
						if response.Err != nil {
							err = response.Err
							break
						}
					}
				}
				if err != nil {
					mu.Lock()
					failures = append(failures, err)
					mu.Unlock()
				}
			}
		}()
	}
	for _, group := range byResource {
		select {
		case groups <- group:
		case <-ctx.Done():
			mu.Lock()
			failures = append(failures, ctx.Err())
			mu.Unlock()
			close(groups)
			wg.Wait()
			return errors.Join(failures...)
		}
	}
	close(groups)
	wg.Wait()
	return errors.Join(failures...)
}
