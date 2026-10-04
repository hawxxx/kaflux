package metrics

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
)

// DemoSource is a deterministic development adapter. It never replaces failed real telemetry.
func DemoSource(cluster string, snapshot func(context.Context) (model.Snapshot, error)) SourceConfig {
	return SourceConfig{ID: "simulator", ClusterID: cluster, Kind: "simulator", Simulate: func(ctx context.Context, q Query) (Result, error) {
		inventory, err := snapshot(ctx)
		if err != nil {
			return Result{}, err
		}
		result := Result{Status: "available", ObservedAt: time.Now().UTC(), Series: []Series{}}
		value := float64(0)
		variable := false
		switch {
		case strings.Contains(q.Expression, "BytesInPerSec"):
			value = float64(len(inventory.Brokers)) * 4.5e6
			variable = true
		case strings.Contains(q.Expression, "BytesOutPerSec"):
			value = float64(len(inventory.Brokers)) * 8.7e6
			variable = true
		case strings.Contains(q.Expression, "RequestsPerSec"):
			value = float64(len(inventory.Brokers)) * 2800
			variable = true
		case strings.Contains(q.Expression, "MessagesInPerSec"):
			value = 32000
			variable = true
		case strings.Contains(q.Expression, "CurrentControllerId"):
			if len(inventory.Brokers) > 0 {
				value = float64(inventory.Brokers[0].ID)
			}
		case strings.Contains(q.Expression, "count(kafka_server_ReplicaManager"):
			value = float64(len(inventory.Brokers))
		case strings.Contains(q.Expression, "count(count by (topic)"):
			value = float64(len(inventory.Topics))
		case strings.Contains(q.Expression, "PartitionCount"):
			for _, broker := range inventory.Brokers {
				value += float64(broker.Partitions)
			}
		case strings.Contains(q.Expression, "LeaderCount"):
			for _, broker := range inventory.Brokers {
				value += float64(broker.Leaders)
			}
		case strings.Contains(q.Expression, "name=\"Size\""):
			for _, broker := range inventory.Brokers {
				if broker.SizeBytes != nil {
					value += float64(*broker.SizeBytes)
				}
			}
		case strings.Contains(q.Expression, "SumOffsetLag"):
			value = 1324
			variable = true
		case strings.Contains(q.Expression, "HeapMemoryUsage_used"):
			value = 1.8e9
			variable = true
		case strings.Contains(q.Expression, "HeapMemoryUsage_max"):
			value = 4e9
		case strings.Contains(q.Expression, "LocalTimeMs"):
			value = 2.4
			variable = true
		case strings.Contains(q.Expression, "UnderReplicated"):
			for _, topic := range inventory.Topics {
				value += float64(topic.URP)
			}
		case strings.Contains(q.Expression, "OfflineReplicaCount"):
			for _, topic := range inventory.Topics {
				for _, partition := range topic.Partitions {
					if partition.Leader < 0 {
						value++
					}
				}
			}
		default:
			return Result{}, errors.New("this metric is not provided by the development simulator")
		}
		series := Series{Labels: map[string]string{"source": "simulator", "cluster": cluster}, Points: []Point{}}
		for timestamp := q.Start; timestamp <= q.End; timestamp += q.Step {
			v := value
			if variable {
				v *= 1 + .12*math.Sin(float64(timestamp)/67) + .06*math.Sin(float64(timestamp)/23)
			}
			series.Points = append(series.Points, Point{Time: float64(timestamp), Value: v})
		}
		if strings.Contains(q.Expression, "PartitionCount") && !strings.Contains(q.Expression, "sum(") {
			for _, broker := range inventory.Brokers {
				points := make([]Point, len(series.Points))
				for i, p := range series.Points {
					points[i] = Point{Time: p.Time, Value: float64(broker.Partitions)}
				}
				result.Series = append(result.Series, Series{Labels: map[string]string{"source": "simulator", "instance": "broker-" + strconv.Itoa(int(broker.ID))}, Points: points})
			}
		} else {
			result.Series = append(result.Series, series)
		}
		return result, nil
	}}
}
