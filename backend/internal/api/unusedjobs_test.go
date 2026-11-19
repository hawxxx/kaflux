package api

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type unusedJobBody struct {
	Data struct {
		ID     string
		Status string
		Done   int
		Total  int
		Topics []struct{ Name, Classification string }
		Error  struct{ Code string }
	}
}

func jobCall(t *testing.T, request func(method, path, body string) *httptest.ResponseRecorder, method, path string, code int) unusedJobBody {
	t.Helper()
	w := request(method, path, "")
	if w.Code != code {
		t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
	}
	var out unusedJobBody
	if e := json.Unmarshal(w.Body.Bytes(), &out); e != nil {
		t.Fatal(e)
	}
	return out
}

func waitJob(t *testing.T, request func(method, path, body string) *httptest.ResponseRecorder, id string) unusedJobBody {
	t.Helper()
	for i := 0; i < 200; i++ {
		out := jobCall(t, request, "GET", "unused-topics/jobs/"+id, 200)
		if out.Data.Status != "running" {
			return out
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job did not finish")
	return unusedJobBody{}
}

func TestUnusedJobMatchesSynchronousReport(t *testing.T) {
	_, request := unusedAPI(t)
	start := jobCall(t, request, "POST", "unused-topics", 200)
	if start.Data.ID == "" || start.Data.Status != "running" {
		t.Fatalf("start %+v", start.Data)
	}
	done := waitJob(t, request, start.Data.ID)
	if done.Data.Status != "done" || done.Data.Total == 0 || done.Data.Done != done.Data.Total {
		t.Fatalf("done %+v", done.Data)
	}
	want := unusedGet(t, request, "")
	if len(done.Data.Topics) != len(want) {
		t.Fatalf("job %d topics, report %d", len(done.Data.Topics), len(want))
	}
	for _, row := range done.Data.Topics {
		if want[row.Name] != row.Classification {
			t.Fatalf("%s: job %s, report %s", row.Name, row.Classification, want[row.Name])
		}
	}
}

func TestUnusedJobReusesIdenticalRunningJobAndRejectsBadInput(t *testing.T) {
	a, request := unusedAPI(t)
	if w := request("POST", "unused-topics?minAgeDays=0", ""); w.Code != 400 {
		t.Fatalf("bad days %d", w.Code)
	}
	if w := request("POST", "unused-topics?exclude=%28", ""); w.Code != 400 {
		t.Fatalf("bad regex %d", w.Code)
	}
	first := jobCall(t, request, "POST", "unused-topics", 200)
	second := jobCall(t, request, "POST", "unused-topics", 200)
	waitJob(t, request, first.Data.ID)
	if second.Data.ID != first.Data.ID && second.Data.Status == "running" {
		t.Fatalf("running duplicate started a new job: %s vs %s", first.Data.ID, second.Data.ID)
	}
	a.mu.Lock()
	n := len(a.unusedJobs)
	a.mu.Unlock()
	if n == 0 || n > 2 {
		t.Fatalf("jobs held %d", n)
	}
}

func TestUnusedJobUnknownAndCancel(t *testing.T) {
	_, request := unusedAPI(t)
	if w := request("GET", "unused-topics/jobs/nope", ""); w.Code != 404 {
		t.Fatalf("unknown %d", w.Code)
	}
	if w := request("PUT", "unused-topics/jobs/nope", ""); w.Code != 405 {
		t.Fatalf("method %d", w.Code)
	}
	start := jobCall(t, request, "POST", "unused-topics?minAgeDays=7", 200)
	got := jobCall(t, request, "DELETE", "unused-topics/jobs/"+start.Data.ID, 200)
	if got.Data.Status != "canceled" && got.Data.Status != "done" {
		t.Fatalf("cancel status %q", got.Data.Status)
	}
	time.Sleep(50 * time.Millisecond)
	again := jobCall(t, request, "GET", "unused-topics/jobs/"+start.Data.ID, 200)
	if got.Data.Status == "canceled" && again.Data.Status != "canceled" {
		t.Fatalf("canceled job changed to %q", again.Data.Status)
	}
	if strings.Contains(again.Data.Error.Code, "failed") {
		t.Fatal("unexpected failure")
	}
}
