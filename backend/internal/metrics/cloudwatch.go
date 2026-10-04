package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"
)

func (g *Gateway) queryCloudWatch(ctx context.Context, c SourceConfig, q Query) (Result, error) {
	id := strings.TrimPrefix(q.Expression, "cloudwatch:")
	var def *CloudWatchDefinition
	for _, d := range g.opts.Catalog {
		if d.ID == id {
			def = d.CloudWatch
			break
		}
	}
	if def == nil {
		return Result{}, errors.New("unknown CloudWatch metric")
	}
	dimensions := make([]map[string]string, 0, len(def.Dimensions))
	keys := make([]string, 0, len(def.Dimensions))
	for k := range def.Dimensions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := def.Dimensions[key]
		switch value {
		case "$cluster":
			value = c.ClusterName
		case "$broker":
			value = c.Dimensions[key]
		case "$configured":
			value = c.Dimensions[key]
		}
		if value == "" || strings.HasPrefix(value, "$") {
			return Result{}, fmt.Errorf("CloudWatch dimension %s is not configured", key)
		}
		dimensions = append(dimensions, map[string]string{"Name": key, "Value": value})
	}
	statistic := def.Statistic
	if statistic == "" {
		statistic = "Average"
	}
	input := map[string]any{"StartTime": q.Start, "EndTime": q.End, "ScanBy": "TimestampAscending", "MaxDatapoints": min(g.opts.MaxPoints, 1440), "MetricDataQueries": []any{map[string]any{"Id": "m0", "ReturnData": true, "MetricStat": map[string]any{"Period": max(int64(60), q.Step/60*60), "Stat": statistic, "Metric": map[string]any{"Namespace": def.Namespace, "MetricName": def.MetricName, "Dimensions": dimensions}}}}}
	payload, err := json.Marshal(input)
	if err != nil {
		return Result{}, err
	}
	request, err := http.NewRequestWithContext(ctx, "POST", c.URL, bytes.NewReader(payload))
	if err != nil {
		return Result{}, err
	}
	request.Header.Set("Content-Type", "application/x-amz-json-1.1")
	request.Header.Set("X-Amz-Target", "GraniteServiceVersion20100801.GetMetricData")
	if c.SignRequest == nil {
		return Result{}, errors.New("CloudWatch signing is missing")
	}
	if err = c.SignRequest(ctx, request, payload); err != nil {
		return Result{}, err
	}
	response, err := g.client.Do(request)
	if err != nil {
		return Result{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return Result{}, fmt.Errorf("CloudWatch HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, g.opts.MaxResponseBytes+1))
	if err != nil {
		return Result{}, err
	}
	if int64(len(body)) > g.opts.MaxResponseBytes {
		return Result{}, errors.New("CloudWatch response exceeds byte limit")
	}
	var decoded struct {
		NextToken         string
		MetricDataResults []struct {
			Id, Label, StatusCode string
			Timestamps            []float64
			Values                []float64
		}
	}
	if err = json.Unmarshal(body, &decoded); err != nil {
		return Result{}, err
	}
	if decoded.NextToken != "" {
		return Result{}, errors.New("CloudWatch result exceeds bounded query; narrow the range")
	}
	if len(decoded.MetricDataResults) > g.opts.MaxSeries {
		return Result{}, errors.New("CloudWatch series limit exceeded")
	}
	result := Result{Status: "available", ObservedAt: time.Now().UTC(), Series: []Series{}}
	count := 0
	for _, series := range decoded.MetricDataResults {
		if series.StatusCode != "Complete" {
			return Result{}, errors.New("CloudWatch returned incomplete data")
		}
		if len(series.Values) != len(series.Timestamps) {
			return Result{}, errors.New("CloudWatch returned mismatched samples")
		}
		item := Series{Labels: map[string]string{"metric": def.MetricName, "label": series.Label}, Points: []Point{}}
		for i, v := range series.Values {
			count++
			if count > g.opts.MaxPoints {
				return Result{}, errors.New("CloudWatch point limit exceeded")
			}
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			item.Points = append(item.Points, Point{Time: series.Timestamps[i], Value: v})
		}
		result.Series = append(result.Series, item)
	}
	return result, nil
}
