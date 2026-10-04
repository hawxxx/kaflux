package metrics

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type CloudWatchDefinition struct {
	Namespace  string            `json:"namespace"`
	MetricName string            `json:"metricName"`
	Statistic  string            `json:"statistic"`
	Dimensions map[string]string `json:"dimensions"`
}

type Definition struct {
	ID         string                `json:"id"`
	Title      string                `json:"title"`
	Category   string                `json:"category"`
	Kind       string                `json:"kind"`
	Expression string                `json:"expression,omitempty"`
	Unit       string                `json:"unit,omitempty"`
	Legend     string                `json:"legend,omitempty"`
	CloudWatch *CloudWatchDefinition `json:"cloudWatch,omitempty"`
}

type dashboard struct {
	Panels    []panel    `json:"panels"`
	Dashboard *dashboard `json:"dashboard"`
}
type panel struct {
	ID          int             `json:"id"`
	Title       string          `json:"title"`
	Datasource  json.RawMessage `json:"datasource"`
	Panels      []panel         `json:"panels"`
	FieldConfig struct {
		Defaults struct {
			Unit string `json:"unit"`
		} `json:"defaults"`
	} `json:"fieldConfig"`
	Targets []struct {
		Expr       string                     `json:"expr"`
		RefID      string                     `json:"refId"`
		Legend     string                     `json:"legendFormat"`
		Namespace  string                     `json:"namespace"`
		MetricName string                     `json:"metricName"`
		Statistic  string                     `json:"statistic"`
		Dimensions map[string]json.RawMessage `json:"dimensions"`
	} `json:"targets"`
}

// ImportDashboard imports expressions and display metadata, never datasource URLs or identifiers.
func ImportDashboard(reader io.Reader) ([]Definition, error) {
	var d dashboard
	dec := json.NewDecoder(io.LimitReader(reader, 8<<20))
	if err := dec.Decode(&d); err != nil {
		return nil, fmt.Errorf("invalid dashboard: %w", err)
	}
	if d.Dashboard != nil {
		d = *d.Dashboard
	}
	definitions := make([]Definition, 0)
	var walk func([]panel) error
	walk = func(panels []panel) error {
		for _, p := range panels {
			for i, target := range p.Targets {
				if len(definitions) >= 2048 {
					return errors.New("dashboard exceeds 2048 metric definitions")
				}
				def := Definition{ID: fmt.Sprintf("panel-%d-%d", p.ID, i), Title: p.Title, Category: category(p.Title), Unit: p.FieldConfig.Defaults.Unit, Legend: target.Legend}
				if target.Expr != "" {
					if len(target.Expr) > 16384 {
						return errors.New("metric expression too long")
					}
					def.Kind = "prometheus"
					def.Expression = target.Expr
				} else if target.Namespace != "" && target.MetricName != "" {
					def.Kind = "cloudwatch"
					def.CloudWatch = &CloudWatchDefinition{Namespace: target.Namespace, MetricName: target.MetricName, Statistic: target.Statistic, Dimensions: map[string]string{}}
					for key := range target.Dimensions {
						switch key {
						case "Cluster Name":
							def.CloudWatch.Dimensions[key] = "$cluster"
						case "Broker ID":
							def.CloudWatch.Dimensions[key] = "$broker"
						default:
							def.CloudWatch.Dimensions[key] = "$configured"
						}
					}
				} else {
					continue
				}
				definitions = append(definitions, def)
			}
			if err := walk(p.Panels); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(d.Panels); err != nil {
		return nil, err
	}
	if len(definitions) == 0 {
		return nil, errors.New("dashboard contains no supported metric targets")
	}
	seen := map[string]int{}
	for i := range definitions {
		key := definitions[i].ID
		seen[key]++
		if seen[key] > 1 {
			definitions[i].ID = fmt.Sprintf("%s-%d", key, seen[key])
		}
	}
	return definitions, nil
}

func category(title string) string {
	lower := strings.ToLower(title)
	for _, c := range []struct{ pattern, name string }{{"kraft", "kraft"}, {"metadata", "kraft"}, {"consumer", "consumer"}, {"lag", "consumer"}, {"produce", "producer"}, {"authentication", "authentication"}, {"replica", "replication"}, {"isr", "replication"}, {"log", "storage"}, {"heap", "brokers"}, {"cpu", "brokers"}, {"topic", "topics"}, {"broker", "brokers"}} {
		if strings.Contains(lower, c.pattern) {
			return c.name
		}
	}
	return "cluster"
}

func Expand(expression, instance, duration string) (string, error) {
	d, err := time.ParseDuration(duration)
	if err != nil || d < time.Minute || d > 24*time.Hour {
		return "", errors.New("range must be between 1m and 24h")
	}
	if len(instance) > 512 {
		return "", errors.New("instance selector too long")
	}
	if instance == "" {
		instance = ".*"
	} else {
		instance = regexp.QuoteMeta(instance)
	}
	quoted := strconv.Quote(instance)
	escaped := quoted[1 : len(quoted)-1]
	out := strings.ReplaceAll(expression, "$instance", escaped)
	out = strings.ReplaceAll(out, "$__range", duration)
	if containsTemplateVariable(out) {
		return "", errors.New("metric uses unsupported dashboard variables")
	}
	return out, nil
}

var templateVariable = regexp.MustCompile(`\$(?:[A-Za-z_][A-Za-z_0-9]*|\{[^}]+\})`)

func containsTemplateVariable(expression string) bool {
	return templateVariable.MatchString(expression)
}

//go:embed default-catalog.json
var dashboardCatalog []byte

func DefaultCatalog() []Definition {
	var definitions []Definition
	if err := json.Unmarshal(dashboardCatalog, &definitions); err != nil {
		return CuratedCatalog()
	}
	// Keep stable identifiers for overview queries while preserving every imported target.
	for _, alias := range CuratedCatalog() {
		for i := range definitions {
			if definitions[i].Expression == alias.Expression {
				definitions[i].ID = alias.ID
				break
			}
		}
	}
	return definitions
}

func CuratedCatalog() []Definition {
	items := []struct{ id, title, unit, expression string }{
		{"brokers", "Broker Count", "none", `count(kafka_server_ReplicaManager_Value{instance=~"$instance",name="LeaderCount"})`},
		{"topics", "Topic Count", "none", `count(count by (topic) (kafka_cluster_Partition_Value{instance=~"$instance"}))`},
		{"partitions", "Partition Count", "none", `sum(kafka_server_ReplicaManager_Value{instance=~"$instance",name="PartitionCount"})`},
		{"data-size", "Total Data Size", "bytes", `sum(kafka_log_Log_Value{instance=~"$instance",name="Size"})`},
		{"controller", "Active Controller", "none", `max(kafka_server_MetadataLoader_Value{instance=~"$instance",name="CurrentControllerId"})`},
		{"urp", "Under Replicated Partitions", "none", `sum(kafka_cluster_Partition_Value{instance=~"$instance",name="UnderReplicated"})`},
		{"offline", "Offline Replicas", "none", `sum(kafka_server_ReplicaManager_Value{instance=~"$instance",name="OfflineReplicaCount"})`},
		{"under-min-isr", "Under Min ISR", "none", `sum(kafka_cluster_Partition_Value{instance=~"$instance",name="UnderMinIsr"})`},
		{"requests", "Total Requests", "reqps", `sum(kafka_network_RequestMetrics_OneMinuteRate{instance=~"$instance",name="RequestsPerSec"})`},
		{"ingress", "Bytes In", "Bps", `sum(kafka_server_BrokerTopicMetrics_OneMinuteRate{instance=~"$instance",name="BytesInPerSec",topic=""})`},
		{"egress", "Bytes Out", "Bps", `sum(kafka_server_BrokerTopicMetrics_OneMinuteRate{instance=~"$instance",name="BytesOutPerSec",topic=""})`},
		{"partition-distribution", "Partitions Per Broker", "none", `kafka_server_ReplicaManager_Value{instance=~"$instance",name="PartitionCount"}`},
		{"leader-distribution", "Leaders Per Broker", "none", `kafka_server_ReplicaManager_Value{instance=~"$instance",name="LeaderCount"}`},
		{"storage-distribution", "Log Size Per Broker", "bytes", `sum by (instance) (kafka_log_Log_Value{instance=~"$instance",name="Size"})`},
		{"topic-size", "Log Size By Topic", "bytes", `sort_desc(sum by (topic) (kafka_log_Log_Value{instance=~"$instance",name="Size"}))`},
		{"topic-ingress", "Ingress By Topic", "Bps", `sort_desc(sum by (topic) (kafka_server_BrokerTopicMetrics_OneMinuteRate{instance=~"$instance",name="BytesInPerSec",topic!=""}))`},
		{"consumer-lag", "Consumer Lag By Group", "none", `sort_desc(sum by (groupId) (kafka_consumer_group_ConsumerLagMetrics_Value{instance=~"$instance",name="SumOffsetLag"}))`},
		{"produce-latency", "Produce Processing Time", "ms", `avg(kafka_network_RequestMetrics_Mean{instance=~"$instance",name="LocalTimeMs",request="Produce"})`},
		{"fetch-queue", "Consumer Fetch Queue Time", "ms", `avg(kafka_network_RequestMetrics_Mean{instance=~"$instance",name="RequestQueueTimeMs",request="FetchConsumer"})`},
		{"heap-used", "JVM Heap Used", "bytes", `java_lang_Memory_HeapMemoryUsage_used{instance=~"$instance"}`},
		{"heap-max", "JVM Heap Max", "bytes", `java_lang_Memory_HeapMemoryUsage_max{instance=~"$instance"}`},
		{"non-heap", "JVM Non Heap", "bytes", `java_lang_Memory_NonHeapMemoryUsage_used{instance=~"$instance"}`},
		{"metadata-lag", "KRaft Metadata Apply Lag", "ms", `kafka_server_broker_metadata_metrics_last_applied_record_lag_ms{instance=~"$instance"}`},
		{"auth-failures", "Failed Authentications", "none", `sum by (instance,listener) (kafka_server_socket_server_metrics_failed_authentication_rate{instance=~"$instance"})`},
		{"request-queue", "Request Queue Size", "none", `kafka_network_RequestChannel_Value{instance=~"$instance",name="RequestQueueSize"}`},
		{"offline-logs", "Offline Log Directories", "none", `sum by (instance) (kafka_log_LogManager_Value{instance=~"$instance",name="OfflineLogDirectoryCount"})`},
	}
	out := make([]Definition, 0, len(items))
	for _, x := range items {
		out = append(out, Definition{ID: x.id, Title: x.title, Category: category(x.title), Kind: "prometheus", Unit: x.unit, Expression: x.expression})
	}
	return out
}
