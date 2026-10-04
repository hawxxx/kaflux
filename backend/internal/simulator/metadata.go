// Package simulator generates deterministic, explicitly synthetic metadata for benchmarks.
package simulator

import (
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"time"
)

func Metadata(brokers, topics, partitions, replicas int) (model.Snapshot, error) {
	if brokers < replicas || replicas < 1 || brokers > 1000 || topics < 1 || topics > 100000 || partitions < topics || partitions > 1000000 {
		return model.Snapshot{}, fmt.Errorf("invalid or excessive simulation dimensions")
	}
	s := model.Snapshot{ObservedAt: time.Unix(1700000000, 0).UTC(), Brokers: make([]model.Broker, brokers), Topics: make([]model.Topic, topics)}
	controller := int32(1)
	s.Controller = &controller
	for i := range s.Brokers {
		s.Brokers[i] = model.Broker{ID: int32(i + 1), Host: fmt.Sprintf("simulated-broker-%d.invalid", i+1), Port: 9092, Rack: fmt.Sprintf("az-%d", i%3)}
	}
	cursor := 0
	for i := range s.Topics {
		count := partitions / topics
		if i < partitions%topics {
			count++
		}
		t := model.Topic{Name: fmt.Sprintf("simulated.topic.%05d", i), ReplicationFactor: replicas, ObservedAt: s.ObservedAt, Partitions: make([]model.Partition, count)}
		var topicSize int64
		for j := range t.Partitions {
			ids := make([]int32, replicas)
			for k := range ids {
				index := (cursor + k) % brokers
				ids[k] = int32(index + 1)
				s.Brokers[index].Partitions++
				size := int64(64<<20) * (1 + int64(cursor%11))
				if s.Brokers[index].SizeBytes == nil {
					v := int64(0)
					s.Brokers[index].SizeBytes = &v
				}
				*s.Brokers[index].SizeBytes += size
			}
			leader := (cursor % brokers)
			s.Brokers[leader].Leaders++
			size := int64(64<<20) * (1 + int64(cursor%11))
			start, end := int64(0), int64(100000+cursor)
			t.Partitions[j] = model.Partition{ID: int32(j), Leader: ids[0], Replicas: ids, ISR: append([]int32(nil), ids...), SizeBytes: &size, StartOffset: &start, EndOffset: &end}
			topicSize += size
			cursor++
		}
		t.SizeBytes = &topicSize
		s.Topics[i] = t
	}
	return s, nil
}
