package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
)

const (
	unusedJobTimeout       = 3 * time.Minute
	unusedJobMaxCandidates = 10000
	unusedJobRetention     = 10 * time.Minute
	unusedJobsRunningMax   = 8
	unusedJobsMax          = 64
)

const (
	jobRunning  = "running"
	jobDone     = "done"
	jobFailed   = "failed"
	jobCanceled = "canceled"
)

// unusedJob is one background evaluation. It is owned by the user who started
// it and only that user can read or cancel it. Results are kept in memory for a
// short retention and are never persisted.
type unusedJob struct {
	id, owner, cluster, key string
	cancel                  context.CancelFunc

	mu          sync.Mutex
	status      string
	done, total int
	topics      []model.UnusedTopic
	partial     bool
	reason      string
	evaluatedAt time.Time
	errCode     string
	errMessage  string
	finished    time.Time
	canceled    bool
}

func (j *unusedJob) progress(done, total int) {
	j.mu.Lock()
	// Workers report concurrently, so updates can arrive out of order.
	j.done, j.total = max(j.done, done), total
	j.mu.Unlock()
}

func (j *unusedJob) finish(status string, now time.Time) {
	j.status, j.finished = status, now
}

// unusedJobView is the wire form. Topics are sent once, when the job is done.
func (a *API) unusedJobView(j *unusedJob, u auth.User) map[string]any {
	j.mu.Lock()
	defer j.mu.Unlock()
	v := map[string]any{"id": j.id, "status": j.status, "done": j.done, "total": j.total, "evaluatedAt": j.evaluatedAt}
	switch j.status {
	case jobDone:
		v["topics"] = a.visibleUnused(u, j.cluster, j.topics)
		v["partial"] = j.partial
		v["partialReason"] = j.reason
	case jobFailed:
		v["error"] = map[string]string{"code": j.errCode, "message": j.errMessage}
	}
	return v
}

func newJobID() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}

// startUnusedJob begins a background evaluation, or returns the caller's
// identical running one so repeated clicks do not stack load on the brokers.
func (a *API) startUnusedJob(w http.ResponseWriter, r *http.Request, u auth.User, id string, p kafka.Provider) {
	days, exclude, ok := a.unusedParams(w, r, u, id, p)
	if !ok {
		return
	}
	internal := r.URL.Query().Get("includeInternal") == "true"
	key := r.URL.RawQuery
	now := a.now()
	a.mu.Lock()
	if a.unusedJobs == nil {
		a.unusedJobs = map[string]*unusedJob{}
	}
	running, finished := 0, []*unusedJob{}
	for jid, j := range a.unusedJobs {
		j.mu.Lock()
		status, done := j.status, j.finished
		j.mu.Unlock()
		switch {
		case status == jobRunning:
			running++
			if j.owner == u.ID && j.cluster == id && j.key == key {
				a.mu.Unlock()
				respond(w, a.unusedJobView(j, u))
				return
			}
		case now.Sub(done) > unusedJobRetention:
			delete(a.unusedJobs, jid)
		default:
			finished = append(finished, j)
		}
	}
	if running >= unusedJobsRunningMax {
		a.mu.Unlock()
		fail(w, 429, "too_many_jobs", "Too many evaluations are running; try again shortly")
		return
	}
	if len(a.unusedJobs) >= unusedJobsMax {
		sort.Slice(finished, func(i, k int) bool { return finished[i].finished.Before(finished[k].finished) })
		for _, j := range finished {
			if len(a.unusedJobs) < unusedJobsMax {
				break
			}
			delete(a.unusedJobs, j.id)
		}
		if len(a.unusedJobs) >= unusedJobsMax {
			a.mu.Unlock()
			fail(w, 429, "too_many_jobs", "Too many evaluations are running; try again shortly")
			return
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	j := &unusedJob{id: newJobID(), owner: u.ID, cluster: id, key: key, cancel: cancel, status: jobRunning, evaluatedAt: now}
	a.unusedJobs[j.id] = j
	a.mu.Unlock()
	go a.runUnusedJob(ctx, j, p, now, time.Duration(days)*24*time.Hour, internal, exclude)
	respond(w, a.unusedJobView(j, u))
}

func (a *API) runUnusedJob(ctx context.Context, j *unusedJob, p kafka.Provider, now time.Time, minAge time.Duration, internal bool, exclude *regexp.Regexp) {
	defer j.cancel()
	defer func() {
		if rec := recover(); rec != nil {
			j.mu.Lock()
			j.errCode, j.errMessage = "evaluation_failed", "Evaluation failed unexpectedly"
			j.finish(jobFailed, a.now())
			j.mu.Unlock()
		}
	}()
	topics, partial, reason, e := a.evaluateUnusedWith(ctx, p, now, minAge, internal, exclude, unusedEval{timeout: unusedJobTimeout, maxCandidates: unusedJobMaxCandidates, progress: j.progress})
	j.mu.Lock()
	defer j.mu.Unlock()
	switch {
	case j.canceled:
		j.finish(jobCanceled, a.now())
	case e != nil && errors.Is(e, errUsageUnavailable):
		j.errCode, j.errMessage = "usage_unavailable", "Consumer group state could not be read; no topic can be judged unused"
		j.finish(jobFailed, a.now())
	case e != nil:
		j.errCode, j.errMessage = "evaluation_failed", "Topics could not be evaluated"
		j.finish(jobFailed, a.now())
	default:
		j.topics, j.partial, j.reason = topics, partial, reason
		j.finish(jobDone, a.now())
	}
}

func (a *API) ownedUnusedJob(w http.ResponseWriter, u auth.User, cluster, jobID string) *unusedJob {
	a.mu.Lock()
	j := a.unusedJobs[jobID]
	a.mu.Unlock()
	if j == nil || j.owner != u.ID || j.cluster != cluster {
		fail(w, 404, "not_found", "Evaluation not found or expired")
		return nil
	}
	return j
}

func (a *API) unusedJobStatus(w http.ResponseWriter, u auth.User, cluster, jobID string) {
	if j := a.ownedUnusedJob(w, u, cluster, jobID); j != nil {
		respond(w, a.unusedJobView(j, u))
	}
}

func (a *API) cancelUnusedJob(w http.ResponseWriter, u auth.User, cluster, jobID string) {
	j := a.ownedUnusedJob(w, u, cluster, jobID)
	if j == nil {
		return
	}
	j.mu.Lock()
	if j.status == jobRunning {
		j.canceled = true
		j.cancel()
		j.finish(jobCanceled, a.now())
	}
	j.mu.Unlock()
	respond(w, a.unusedJobView(j, u))
}
