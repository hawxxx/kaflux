package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/jobs"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/crypto/bcrypt"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestProductionAPIPlanExecuteAndRollback(t *testing.T) {
	seed, dbURL := os.Getenv("KAFLUX_TEST_KAFKA_SEED"), os.Getenv("KAFLUX_TEST_DATABASE_URL")
	if seed == "" || dbURL == "" {
		t.Skip("requires real Kafka and PostgreSQL integration fixtures")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cl, e := kgo.NewClient(kgo.SeedBrokers(seed))
	if e != nil {
		t.Fatal(e)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)
	topic := fmt.Sprintf("kaflux-api-integration-%d", time.Now().UnixNano())
	created, e := admin.CreateTopics(ctx, 3, 2, nil, topic)
	if e != nil {
		t.Fatal(e)
	}
	if e = created[topic].Err; e != nil {
		t.Fatal(e)
	}
	defer admin.DeleteTopics(context.Background(), topic)
	provider, e := kafka.NewNative(kafka.Config{Seeds: []string{seed}, AllowPlaintext: true})
	if e != nil {
		t.Fatal(e)
	}
	defer provider.Close()
	st, e := store.New(ctx, dbURL)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	clusterID := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	defer st.DB.Exec(context.Background(), "DELETE FROM kaflux_job_throttles WHERE job_id IN (SELECT id FROM kaflux_jobs WHERE cluster_id=$1)", clusterID)
	defer st.DB.Exec(context.Background(), "DELETE FROM kaflux_jobs WHERE cluster_id=$1", clusterID)
	hash, _ := bcrypt.GenerateFromPassword([]byte("local-integration-only"), bcrypt.MinCost)
	api := New(Options{Store: st, AdminUser: "integration", AdminHash: string(hash), Providers: map[string]kafka.Provider{clusterID: provider}, Clusters: []model.Cluster{{ID: clusterID, Mode: "live"}}})
	server := httptest.NewTLSServer(api)
	defer server.Close()
	client := server.Client()
	jar, _ := cookiejar.New(nil)
	client.Jar = jar
	csrf := ""
	request := func(method, path string, body any, status int, out any) {
		t.Helper()
		var b []byte
		if body != nil {
			b, _ = json.Marshal(body)
		}
		req, e := http.NewRequestWithContext(ctx, method, server.URL+path, bytes.NewReader(b))
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("Content-Type", "application/json")
		if csrf != "" {
			req.Header.Set("X-CSRF-Token", csrf)
		}
		res, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		if res.StatusCode != status {
			var v any
			_ = json.NewDecoder(res.Body).Decode(&v)
			t.Fatalf("%s %s returned %d: %v", method, path, res.StatusCode, v)
		}
		if out != nil {
			var envelope struct {
				Data json.RawMessage `json:"data"`
			}
			if e = json.NewDecoder(res.Body).Decode(&envelope); e != nil {
				t.Fatal(e)
			}
			if e = json.Unmarshal(envelope.Data, out); e != nil {
				t.Fatal(e)
			}
		}
	}
	base := "/api/v1/clusters/" + clusterID
	request("GET", base+"/topics", nil, 401, nil)
	var session struct {
		CSRF string `json:"csrfToken"`
	}
	request("POST", "/api/v1/auth/login", map[string]string{"username": "integration", "password": "local-integration-only"}, 200, &session)
	request("POST", base+"/rebalances", map[string]any{"topics": []string{topic}}, 403, nil)
	csrf = session.CSRF
	original, e := provider.FreshSnapshot(ctx)
	if e != nil {
		t.Fatal(e)
	}
	var before []model.Partition
	for _, x := range original.Topics {
		if x.Name == topic {
			before = x.Partitions
		}
	}
	var plan model.Plan
	request("POST", base+"/rebalances", map[string]any{"topics": []string{topic}, "brokers": []int32{1, 2}, "throttleBytesPerSec": 1048576}, 200, &plan)
	dry, e := provider.FreshSnapshot(ctx)
	if e != nil {
		t.Fatal(e)
	}
	for _, x := range dry.Topics {
		if x.Name == topic && !reflect.DeepEqual(before, x.Partitions) {
			t.Fatal("dry-run mutated Kafka")
		}
	}
	request("POST", base+"/rebalances/"+plan.ID+"/execute", map[string]any{"confirmation": true, "planHash": "tampered"}, 409, nil)
	request("POST", base+"/rebalances/"+plan.ID+"/execute", map[string]any{"confirmation": true, "planHash": plan.PlanHash}, 200, nil)
	worker := jobs.Worker{Store: st, Providers: map[string]kafka.Provider{clusterID: provider}, Owner: "integration-worker"}
	await := func(id string) model.Plan {
		t.Helper()
		deadline := time.Now().Add(40 * time.Second)
		for time.Now().Before(deadline) {
			worker.Step(ctx)
			p, e := st.Job(ctx, id)
			if e != nil {
				t.Fatal(e)
			}
			if p.State == "completed" {
				return p
			}
			if p.State == "failed" {
				t.Fatalf("job failed: %s", p.Error)
			}
			time.Sleep(150 * time.Millisecond)
		}
		t.Fatal("job did not finish")
		return model.Plan{}
	}
	completed := await(plan.ID)
	var rollback model.Plan
	request("POST", base+"/rebalances/"+completed.ID+"/rollback", map[string]any{"confirmation": true, "planHash": completed.PlanHash}, 200, &rollback)
	request("POST", base+"/rebalances/"+rollback.ID+"/execute", map[string]any{"confirmation": true, "planHash": rollback.PlanHash}, 200, nil)
	await(rollback.ID)
	after, e := provider.FreshSnapshot(ctx)
	if e != nil {
		t.Fatal(e)
	}
	for _, x := range after.Topics {
		if x.Name == topic {
			for i, p := range x.Partitions {
				if !reflect.DeepEqual(p.Replicas, before[i].Replicas) {
					t.Fatal("rollback did not restore original assignments")
				}
			}
		}
	}
}
