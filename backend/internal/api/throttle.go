package api

import (
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/jobs"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"net/http"
	"time"
)

func (a *API) requestThrottle(w http.ResponseWriter, r *http.Request, user auth.User, cluster string, provider kafka.Provider, p model.Plan) {
	var input struct {
		Confirmation bool   `json:"confirmation"`
		PlanHash     string `json:"planHash"`
		BytesPerSec  int64  `json:"bytesPerSec"`
	}
	if !decode(w, r, &input) {
		return
	}
	if !input.Confirmation || input.PlanHash != p.PlanHash {
		fail(w, 409, "approval_required", "Explicit confirmation and matching plan hash required")
		return
	}
	if input.BytesPerSec < 1 || input.BytesPerSec > 1000000000000 {
		fail(w, 422, "invalid_throttle", "Throttle must be between 1 and 1000000000000 bytes/sec")
		return
	}
	if p.State != "running" || p.CleanupPending || p.CancellationRequested || p.ThrottleRequest != nil {
		fail(w, 409, "throttle_conflict", "Only a running job without pending changes or cancellation can change throttle")
		return
	}
	capability, e := jobs.Capabilities(r.Context(), provider, true)
	if e != nil || !capability.ManualReassignmentAllowed {
		fail(w, 409, "manual_reassignment_blocked", capability.Reason)
		return
	}
	if _, ok := provider.(kafka.ThrottleProvider); !ok {
		simulation, ok := provider.(interface{ Simulated() bool })
		if !ok || !simulation.Simulated() {
			fail(w, 422, "throttle_unsupported", "Provider does not support live throttle changes")
			return
		}
	}
	request := model.ThrottleRequest{Revision: auth.Token(), BytesPerSec: input.BytesPerSec, Actor: user.ID, Provider: user.Provider, At: time.Now().UTC()}
	if !a.adminMutation(w, r, user, cluster, "throttle-request", p.ID, map[string]int64{"bytesPerSec": p.ThrottleBytesPerSec}, request, func() error { return a.o.Store.RequestThrottle(r.Context(), p.ID, request) }) {
		return
	}
	updated, e := a.o.Store.Job(r.Context(), p.ID)
	if e != nil {
		fail(w, 503, "store_unavailable", "Throttle change requested; job status unavailable")
		return
	}
	respond(w, updated)
}
