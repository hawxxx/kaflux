package api

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/jobs"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
)

// executeActions change a running job and need the same permission as execution.
var executeActions = map[string]bool{"cancel": true, "throttle": true, "pause": true, "resume": true, "cancel-rollback": true}

// controlJob handles pause, resume, cancel and cancel & roll back. It reports
// whether action was one of them (the response has then been written).
func (a *API) controlJob(w http.ResponseWriter, r *http.Request, u auth.User, cluster string, provider kafka.Provider, p model.Plan, action string, skip bool) bool {
	var run func() error
	var after any
	var line string
	switch action {
	case "cancel":
		run = func() error { return a.o.Store.RequestCancel(r.Context(), p.ID) }
		after = map[string]bool{"cancellationRequested": true}
		line = "Cancellation requested by " + u.ID
	case "cancel-rollback":
		run = func() error { return a.o.Store.RequestCancelRollback(r.Context(), p.ID, u.ID) }
		after = map[string]bool{"cancellationRequested": true, "rollbackRequested": true}
		line = "Cancel & roll back requested by " + u.ID
	case "pause":
		run = func() error { return a.o.Store.RequestPause(r.Context(), p.ID, u.ID) }
		after = map[string]bool{"pauseRequested": true}
		line = "Pause requested by " + u.ID + " · pausing after the current topic"
	case "resume":
		if p.State != "paused" {
			fail(w, 409, "invalid_state", "Only paused jobs can be resumed")
			return true
		}
		if cap, e := jobs.Capabilities(r.Context(), provider, true); e != nil || !cap.ManualReassignmentAllowed {
			fail(w, 409, "manual_reassignment_blocked", cap.Reason)
			return true
		}
		run = func() error { _, e := a.o.Store.Resume(r.Context(), p.ID, skip); return e }
		after = map[string]bool{"skip": skip}
		line = "Resumed by " + u.ID
		if skip && p.CurrentStep < len(p.Steps) {
			line = fmt.Sprintf("Topic %s skipped by %s · resuming", p.Steps[p.CurrentStep].Topic, u.ID)
		}
	default:
		return false
	}
	if !a.adminMutation(w, r, u, cluster, action, p.ID, map[string]string{"state": p.State}, after, run) {
		return true
	}
	a.logJob(r, p.ID, "info", "%s", line)
	updated, e := a.o.Store.Job(r.Context(), p.ID)
	if e != nil {
		failCause(w, r, 503, "store_unavailable", "Request recorded; job status unavailable", e)
		return true
	}
	respond(w, updated)
	return true
}

// logJob writes a user action to the job's activity log; the log never blocks the action.
func (a *API) logJob(r *http.Request, jobID, level, format string, args ...any) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
	defer cancel()
	_ = a.o.Store.AppendEvent(ctx, jobID, level, fmt.Sprintf(format, args...))
}

func (a *API) requiresThrottle(cluster string) bool {
	for _, c := range a.o.Clusters {
		if c.ID == cluster {
			return c.RequireThrottle
		}
	}
	return false
}

// jobRead serves the activity log and the job's downloads.
func (a *API) jobRead(w http.ResponseWriter, r *http.Request, p model.Plan, what string) {
	switch what {
	case "events":
		after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
		events, e := a.o.Store.Events(r.Context(), p.ID, after, 500)
		if e != nil {
			failCause(w, r, 503, "store_unavailable", "Activity log unavailable", e)
			return
		}
		last := after
		if len(events) > 0 {
			last = events[len(events)-1].Seq
		}
		respond(w, map[string]any{"events": events, "last": last})
	case "events.txt":
		events, e := a.o.Store.Events(r.Context(), p.ID, 0, 1<<30)
		if e != nil {
			failCause(w, r, 503, "store_unavailable", "Activity log unavailable", e)
			return
		}
		var b strings.Builder
		for _, ev := range events {
			fmt.Fprintf(&b, "%s %-5s %s\n", ev.At.Format(time.RFC3339), strings.ToUpper(ev.Level), ev.Message)
		}
		download(w, p.ID+"-activity.log", "text/plain; charset=utf-8", []byte(b.String()))
	case "backup.json":
		type replica struct {
			Topic     string  `json:"topic"`
			Partition int32   `json:"partition"`
			Replicas  []int32 `json:"replicas"`
		}
		backup := struct {
			Version    int       `json:"version"`
			Partitions []replica `json:"partitions"`
		}{Version: 1, Partitions: []replica{}}
		for _, c := range p.Changes {
			backup.Partitions = append(backup.Partitions, replica{Topic: c.Topic, Partition: c.Partition, Replicas: c.Before})
		}
		// The kafka-reassign-partitions.sh format, so the CLI can restore it if Kaflux is down.
		body, _ := json.MarshalIndent(backup, "", "  ")
		download(w, p.ID+"-backup.json", "application/json", append(body, '\n'))
	case "report.csv":
		state := map[string]string{}
		for _, s := range p.Steps {
			state[s.Topic] = s.State
		}
		var b strings.Builder
		out := csv.NewWriter(&b)
		_ = out.Write([]string{"topic", "partition", "before", "after", "topicState", "finishedAt"})
		for _, c := range p.Changes {
			finished := ""
			if c.FinishedAt != nil {
				finished = c.FinishedAt.UTC().Format(time.RFC3339)
			}
			_ = out.Write([]string{c.Topic, strconv.Itoa(int(c.Partition)), brokers(c.Before), brokers(c.After), state[c.Topic], finished})
		}
		out.Flush()
		download(w, p.ID+"-report.csv", "text/csv; charset=utf-8", []byte(b.String()))
	default:
		fail(w, 404, "not_found", "Unknown resource")
	}
}

func download(w http.ResponseWriter, name, contentType string, body []byte) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(body)
}

func brokers(ids []int32) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.Itoa(int(id))
	}
	return strings.Join(parts, " ")
}
