package kafka

import (
	"context"
	"strings"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kversion"
)

const (
	// Kafka protocol API keys that only one metadata mode advertises.
	apiKeyLeaderAndIsr   = 4  // inter-broker, ZooKeeper-managed brokers only
	apiKeyDescribeQuorum = 55 // KRaft only

	clusterInfoTTL     = 10 * time.Minute
	clusterInfoBackoff = time.Minute
)

// applyClusterInfo adds the Kafka version and metadata mode. Software versions
// change rarely, so the answer is cached far longer than the snapshot; a failure
// leaves the snapshot without it rather than failing the whole view.
func (n *Native) applyClusterInfo(ctx context.Context, snap *model.Snapshot) {
	n.infoMu.Lock()
	defer n.infoMu.Unlock()
	if time.Now().Before(n.infoUntil) {
		snap.Info = n.info
		return
	}
	c, done, e := n.bounded(ctx)
	if e != nil {
		return
	}
	defer done()
	versions, e := n.admin.ApiVersions(c)
	info := clusterInfoFromVersions(versions)
	if e != nil || info == nil {
		n.infoUntil = time.Now().Add(clusterInfoBackoff)
		snap.Info = n.info
		return
	}
	n.info, n.infoUntil = info, time.Now().Add(clusterInfoTTL)
	snap.Info = info
}

// clusterInfoFromVersions infers the release and metadata mode from the APIs the
// brokers support. Brokers do not report a release number, so the version is the
// guess for the first answering broker (patch level is not detectable).
func clusterInfoFromVersions(versions kadm.BrokersApiVersions) *model.ClusterInfo {
	var info model.ClusterInfo
	for _, v := range versions.Sorted() {
		if v.Err != nil {
			continue
		}
		var opts []kversion.VersionGuessOpt
		if _, ok := v.KeyMaxVersion(apiKeyLeaderAndIsr); ok {
			info.MetadataMode = "zookeeper"
		} else if _, ok := v.KeyMaxVersion(apiKeyDescribeQuorum); ok {
			info.MetadataMode = "kraft"
			opts = append(opts, kversion.TryRaftBroker())
		}
		if info.KafkaVersion == "" {
			if guess := v.VersionGuess(opts...); guess != "" && guess != "unknown" {
				info.KafkaVersion = strings.TrimPrefix(guess, "v")
			}
		}
	}
	if info.KafkaVersion == "" && info.MetadataMode == "" {
		return nil
	}
	return &info
}
