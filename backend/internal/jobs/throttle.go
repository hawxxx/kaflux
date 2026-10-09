package jobs

import (
	"context"
	"errors"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"time"
)

func (w *Worker) applyRequestedThrottle(ctx context.Context, p model.Plan, provider kafka.Provider) error {
	request := p.ThrottleRequest
	if request == nil {
		return nil
	}
	capability, e := Capabilities(ctx, provider, true)
	if e != nil || !capability.ManualReassignmentAllowed {
		return fmt.Errorf("Throttle change blocked: %s", capability.Reason)
	}
	if throttle, ok := provider.(kafka.ThrottleProvider); ok {
		records, e := w.Store.LoadThrottle(ctx, p.ID)
		if e != nil {
			return e
		}
		if len(records) == 0 {
			// Between topics, or on a job that started unthrottled, there is
			// nothing to retarget: a moving topic gets throttled now, and the
			// acknowledged rate applies from the next topic either way.
			if p.CurrentStep < len(p.Steps) && p.Steps[p.CurrentStep].State == model.StepMoving {
				changes := topicChanges(p.Changes, p.Steps[p.CurrentStep].Topic)
				if records, e = throttle.PrepareThrottle(ctx, changes, request.BytesPerSec); e != nil {
					return e
				}
				if e = w.Store.SaveThrottle(ctx, p.ID, records); e != nil {
					return e
				}
				if e = throttle.ApplyThrottle(ctx, records); e != nil {
					return e
				}
			}
			if e = w.throttleAudit(ctx, p, "applied"); e != nil {
				return e
			}
			return w.Store.AcknowledgeThrottle(ctx, p.ID, w.Owner, request.Revision, request.BytesPerSec)
		}
		next, e := kafka.RetargetThrottle(records, request.BytesPerSec)
		if e != nil {
			return e
		}
		if e = w.Store.ReplaceThrottle(ctx, p.ID, w.Owner, request.Revision, next); e != nil {
			return e
		}
		if e = throttle.ApplyThrottle(ctx, next); e != nil {
			return e
		}
		if e = w.Store.ReplaceThrottle(ctx, p.ID, w.Owner, request.Revision, kafka.FinalizeThrottle(next)); e != nil {
			return e
		}
	} else if simulation, ok := provider.(interface{ Simulated() bool }); !ok || !simulation.Simulated() {
		return errors.New("Kafka provider does not support live throttle changes")
	}
	if e = w.throttleAudit(ctx, p, "applied"); e != nil {
		return e
	}
	return w.Store.AcknowledgeThrottle(ctx, p.ID, w.Owner, request.Revision, request.BytesPerSec)
}
func (w *Worker) throttleAudit(ctx context.Context, p model.Plan, result string) error {
	request := p.ThrottleRequest
	if request == nil {
		return nil
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return w.Store.Audit(bounded, store.Audit{Actor: request.Actor, Provider: request.Provider, ClusterID: p.ClusterID, Action: "throttle", Resource: p.ID, RequestID: request.Revision, Result: result, AdminBefore: map[string]int64{"bytesPerSec": p.ThrottleBytesPerSec}, AdminAfter: map[string]int64{"bytesPerSec": request.BytesPerSec}})
}
