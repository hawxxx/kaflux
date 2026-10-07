package api

import (
	"context"

	"github.com/hawxxx/kaflux/backend/internal/capacity"
	"github.com/hawxxx/kaflux/backend/internal/jobs"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/msk"
)

// capacityReport resolves what a broker of this cluster is meant to hold and
// compares it with the snapshot.
//
// Order: capacity the operator declared, then limits a platform publishes for
// the broker type it reports (Amazon MSK), otherwise unknown. Nothing is
// guessed, so self-managed clusters stay unknown until the operator says more.
//
// When the broker type cannot be read because AWS denied the request or the
// MSK cluster could not be uniquely discovered from the configured seeds, the
// report's reason names that specific cause instead of the generic "set
// capacity" message, so an operator can tell a permission problem from a
// cluster that was simply never described.
func (a *API) capacityReport(ctx context.Context, provider kafka.Provider, snap model.Snapshot) capacity.Report {
	profile, caps := a.capacityProfile(ctx, provider)
	report := capacity.Evaluate(snap, profile)
	if !report.Known && caps.BrokerType == "" && caps.Reason != "" {
		report.Reason = caps.Reason
	}
	return report
}

func (a *API) capacityProfile(ctx context.Context, provider kafka.Provider) (capacity.Profile, msk.Capabilities) {
	if c, ok := provider.(interface{ CapacityConfig() *capacity.Config }); ok {
		if p, known := capacity.FromConfig(c.CapacityConfig()); known {
			return p, msk.Capabilities{}
		}
	}
	caps, err := jobs.Capabilities(ctx, provider, false)
	if err != nil {
		return capacity.Profile{}, caps
	}
	if p, known := capacity.FromMSK(caps.BrokerType); known {
		return p, caps
	}
	return capacity.Profile{}, caps
}
