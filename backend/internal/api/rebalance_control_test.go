package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/msk"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

var reassignmentAllowed = msk.Capabilities{ManualReassignmentAllowed: true, PlanningAllowed: true}

func get(a *API, path string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", path, nil)
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)
	return w
}

func confirm(p model.Plan, extra string) string {
	return fmt.Sprintf(`{"confirmation":true,"planHash":%q%s}`, p.PlanHash, extra)
}

func TestPauseResumeAndCancelRollbackFollowJobState(t *testing.T) {
	a, _, s := rebalanceAPI(t, reassignmentAllowed)
	ctx := context.Background()
	plan := savedPlan(t, s, &capsProvider{Demo: kafka.NewDemo()}, "job", "running")
	base := "/api/v1/clusters/demo/rebalances/job/"
	if w := post(a, base+"resume", confirm(plan, "")); w.Code != 409 {
		t.Fatalf("resume running = %d", w.Code)
	}
	if w := post(a, base+"pause", confirm(plan, "")); w.Code != 200 {
		t.Fatalf("pause = %d %s", w.Code, w.Body.String())
	}
	got, _ := s.Job(ctx, "job")
	if !got.PauseRequested {
		t.Fatal("pause not recorded")
	}
	got.State = "paused"
	_ = s.SaveJob(ctx, got)
	if w := post(a, base+"resume", confirm(plan, `,"skip":true`)); w.Code != 200 {
		t.Fatalf("resume = %d %s", w.Code, w.Body.String())
	}
	got, _ = s.Job(ctx, "job")
	if got.State != "running" || got.CurrentStep != 1 || got.Steps[0].State != model.StepSkipped {
		t.Fatalf("resume skip = %+v", got)
	}
	if w := post(a, base+"cancel-rollback", confirm(plan, "")); w.Code != 200 {
		t.Fatalf("cancel-rollback = %d %s", w.Code, w.Body.String())
	}
	got, _ = s.Job(ctx, "job")
	if !got.CancellationRequested || !got.RollbackRequested || got.RollbackRequestedBy == "" {
		t.Fatalf("cancel-rollback = %+v", got)
	}
	if w := post(a, base+"pause", confirm(plan, "")); w.Code != 409 || !strings.Contains(w.Body.String(), "invalid_state") {
		t.Fatalf("pause while canceling = %d %s", w.Code, w.Body.String())
	}
	if w := post(a, base+"pause", `{"confirmation":true,"planHash":"wrong"}`); w.Code != 409 {
		t.Fatalf("pause without matching hash = %d", w.Code)
	}
	w := get(a, base+"events")
	var body struct {
		Data struct {
			Events []model.JobEvent `json:"events"`
			Last   int64            `json:"last"`
		} `json:"data"`
	}
	if e := json.Unmarshal(w.Body.Bytes(), &body); e != nil || len(body.Data.Events) != 3 || body.Data.Last != 3 {
		t.Fatalf("events = %s", w.Body.String())
	}
	if !strings.Contains(body.Data.Events[1].Message, "skipped") {
		t.Fatalf("skip not logged: %+v", body.Data.Events)
	}
	if w := get(a, base+"events?after=2"); !strings.Contains(w.Body.String(), "roll back requested") || strings.Contains(w.Body.String(), "Pause requested") {
		t.Fatalf("paged events = %s", w.Body.String())
	}
}

func TestExecuteWithoutThrottleUnlessClusterRequiresIt(t *testing.T) {
	for _, required := range []bool{false, true} {
		s, _ := store.New(context.Background(), "")
		p := &capsProvider{Demo: kafka.NewDemo(), caps: reassignmentAllowed}
		a := New(Options{Store: s, Providers: map[string]kafka.Provider{"demo": p}, Clusters: []model.Cluster{{ID: "demo", RequireThrottle: required}}})
		a.o.Demo = true // demo session, live policy
		plan := savedPlan(t, s, p, "job", "planned")
		w := post(a, "/api/v1/clusters/demo/rebalances/job/execute", confirm(plan, ""))
		if required && (w.Code != 422 || !strings.Contains(w.Body.String(), "throttle_required")) {
			t.Fatalf("required: %d %s", w.Code, w.Body.String())
		}
		if !required && w.Code != 200 {
			t.Fatalf("optional: %d %s", w.Code, w.Body.String())
		}
	}
}

func TestBackupAndReportDownloads(t *testing.T) {
	a, p, s := rebalanceAPI(t, reassignmentAllowed)
	plan := savedPlan(t, s, p, "job", "completed")
	w := get(a, "/api/v1/clusters/demo/rebalances/job/backup.json")
	var backup struct {
		Version    int `json:"version"`
		Partitions []struct {
			Topic     string  `json:"topic"`
			Partition int32   `json:"partition"`
			Replicas  []int32 `json:"replicas"`
		} `json:"partitions"`
	}
	if e := json.Unmarshal(w.Body.Bytes(), &backup); e != nil || backup.Version != 1 || len(backup.Partitions) != len(plan.Changes) {
		t.Fatalf("backup = %s", w.Body.String())
	}
	if fmt.Sprint(backup.Partitions[0].Replicas) != fmt.Sprint(plan.Changes[0].Before) {
		t.Fatal("backup must hold the original replicas")
	}
	if !strings.Contains(w.Header().Get("Content-Disposition"), "job-backup.json") {
		t.Fatalf("disposition = %s", w.Header().Get("Content-Disposition"))
	}
	report := get(a, "/api/v1/clusters/demo/rebalances/job/report.csv").Body.String()
	if !strings.HasPrefix(report, "topic,partition,before,after,topicState,finishedAt\n") || strings.Count(report, "\n") != len(plan.Changes)+1 {
		t.Fatalf("report = %q", report)
	}
}
