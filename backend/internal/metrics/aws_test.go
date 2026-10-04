package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestCloudWatchUsesConfiguredDimensionsAndSignedBoundedRequest(t *testing.T) {
	var signed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !signed.Load() || r.Header.Get("X-Amz-Target") != "GraniteServiceVersion20100801.GetMetricData" {
			t.Error("unsigned or wrong CloudWatch action")
		}
		var input map[string]any
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
		}
		encoded, _ := json.Marshal(input)
		if !strings.Contains(string(encoded), "configured-cluster") {
			t.Error("missing configured cluster binding")
		}
		if strings.Contains(string(encoded), "$cluster") {
			t.Error("unresolved private binding")
		}
		fmt.Fprint(w, `{"MetricDataResults":[{"Id":"m0","Label":"Controller","Timestamps":[100,200],"Values":[1,1],"StatusCode":"Complete"}]}`)
	}))
	defer server.Close()
	definitions := []Definition{{ID: "cw-controller", Kind: "cloudwatch", CloudWatch: &CloudWatchDefinition{Namespace: "AWS/Kafka", MetricName: "ActiveControllerCount", Statistic: "Maximum", Dimensions: map[string]string{"Cluster Name": "$cluster"}}}}
	g, err := NewGateway([]SourceConfig{{ID: "cw", URL: server.URL, Kind: "cloudwatch", ClusterName: "configured-cluster", SignRequest: func(ctx context.Context, r *http.Request, body []byte) error { signed.Store(true); return nil }}}, Options{Catalog: definitions})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	result, err := g.Wait(context.Background(), Query{Source: "cw", Expression: "cloudwatch:cw-controller", Start: 100, End: 200, Step: 60})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Series) != 1 || len(result.Series[0].Points) != 2 {
		t.Fatalf("bad CloudWatch data %+v", result)
	}
}

func TestAWSMetricsRequireSigner(t *testing.T) {
	for _, kind := range []string{"amp", "cloudwatch"} {
		g, err := NewGateway([]SourceConfig{{ID: "a", URL: "https://metrics.example.com", Kind: kind}}, Options{})
		if err == nil {
			g.Close()
			t.Fatalf("%s accepted without signing", kind)
		}
	}
}

func TestAMPCallsSigV4Hook(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"status":"success","data":{"result":[]}}`)
	}))
	defer server.Close()
	g, err := NewGateway([]SourceConfig{{ID: "amp", URL: server.URL, Kind: "amp", SignRequest: func(ctx context.Context, r *http.Request, b []byte) error { calls.Add(1); return nil }}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if _, err = g.Wait(context.Background(), Query{Source: "amp", Expression: "up", Start: 100, End: 200, Step: 60}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("AMP signing skipped")
	}
}
