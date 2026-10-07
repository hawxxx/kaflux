package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/hawxxx/kaflux/backend/internal/api"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/awsutil"
	"github.com/hawxxx/kaflux/backend/internal/config"
	"github.com/hawxxx/kaflux/backend/internal/httpgzip"
	"github.com/hawxxx/kaflux/backend/internal/integrations"
	"github.com/hawxxx/kaflux/backend/internal/jobs"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/metrics"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"github.com/hawxxx/kaflux/backend/internal/telemetry"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if e := run(); e != nil {
		slog.Error("Kaflux startup failed", "error", e)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	shutdownTelemetry, e := telemetry.Setup(ctx)
	if e != nil {
		return e
	}
	defer func() {
		flush, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTelemetry(flush)
	}()
	cfg, e := config.Load(os.Getenv("KAFLUX_CONFIG"))
	if e != nil {
		return e
	}
	if e := validateAuthentication(cfg); e != nil {
		return e
	}
	s, e := openStore(ctx, cfg)
	if e != nil {
		return e
	}
	defer s.Close()
	providers := map[string]kafka.Provider{}
	clusters := []model.Cluster{}
	if cfg.Demo {
		providers["demo"] = kafka.NewDemo()
		clusters = append(clusters, model.Cluster{ID: "demo", Name: "Development simulator", Environment: "development", Kind: "Kafka", Mode: "demo"})
	} else {
		configs := []kafka.Config{}
		for _, c := range cfg.Clusters {
			b, _ := json.Marshal(c)
			var v kafka.Config
			if e = json.Unmarshal(b, &v); e != nil {
				return e
			}
			configs = append(configs, v)
		}
		if cfg.ClustersFile != "" {
			b, e := os.ReadFile(cfg.ClustersFile)
			if e != nil {
				return e
			}
			if e = json.Unmarshal(b, &configs); e != nil {
				return e
			}
		}
		if len(configs) == 0 {
			return errors.New("at least one cluster must be configured")
		}
		for _, c := range configs {
			if c.ID == "" || providers[c.ID] != nil {
				return errors.New("cluster IDs must be nonempty and unique")
			}
			if e = c.Capacity.Validate(); e != nil {
				return errors.New("cluster " + c.ID + ": " + e.Error())
			}
			p, e := kafka.NewNative(c)
			if e != nil {
				return e
			}
			providers[c.ID] = p
			clusters = append(clusters, model.Cluster{ID: c.ID, Name: c.Name, Environment: c.Environment, Kind: "Kafka", Mode: "live"})
		}
	}
	defer func() {
		for _, p := range providers {
			p.Close()
		}
	}()
	var source []metrics.SourceConfig
	metricsCluster := os.Getenv("KAFLUX_METRICS_CLUSTER_ID")
	if metricsCluster == "" && len(clusters) == 1 {
		metricsCluster = clusters[0].ID
	}
	if (cfg.PrometheusURL != "" || cfg.AmpURL != "" || cfg.CloudWatchRegion != "") && providers[metricsCluster] == nil {
		return errors.New("metrics datasource requires KAFLUX_METRICS_CLUSTER_ID identifying a configured cluster")
	}
	if cfg.PrometheusURL != "" {
		source = append(source, metrics.SourceConfig{ID: "prometheus", ClusterID: metricsCluster, URL: cfg.PrometheusURL, Kind: "prometheus"})
	}
	if cfg.AmpURL != "" {
		sign, e := awsutil.NewSigner(ctx, "aps", cfg.CloudWatchRegion, cfg.AWSRoleARN)
		if e != nil {
			return e
		}
		source = append(source, metrics.SourceConfig{ID: "amp", ClusterID: metricsCluster, URL: cfg.AmpURL, Kind: "amp", SignRequest: sign})
	}
	if cfg.CloudWatchRegion != "" {
		sign, e := awsutil.NewSigner(ctx, "monitoring", cfg.CloudWatchRegion, cfg.AWSRoleARN)
		if e != nil {
			return e
		}
		source = append(source, metrics.SourceConfig{ID: "cloudwatch", ClusterID: metricsCluster, URL: "https://monitoring." + cfg.CloudWatchRegion + ".amazonaws.com", Kind: "cloudwatch", ClusterName: cfg.CloudWatchClusterName, SignRequest: sign})
	}
	if cfg.Demo && len(source) == 0 {
		source = append(source, metrics.DemoSource("demo", providers["demo"].Snapshot))
	}
	gateway, e := metrics.NewGateway(source, metrics.Options{Demo: cfg.Demo})
	if e != nil {
		return e
	}
	defer gateway.Close()
	identityProviders := map[string]*auth.OIDC{}
	if len(cfg.OIDC) > 0 && s.IsPersistent() {
		if e = s.EnsureOIDC(ctx); e != nil {
			return e
		}
	}
	for _, c := range cfg.OIDC {
		p, e := auth.NewOIDC(ctx, c)
		if e != nil {
			return e
		}
		if s.IsPersistent() {
			p.SetFlowStore(s)
		}
		identityProviders[c.ID] = p
	}
	grants := []auth.Grant{}
	for _, g := range cfg.Grants {
		b, _ := json.Marshal(g)
		var v auth.Grant
		if e = json.Unmarshal(b, &v); e != nil {
			return e
		}
		grants = append(grants, v)
	}
	external := map[string]*integrations.Client{}
	defer func() {
		for _, client := range external {
			client.Close()
		}
	}()
	for _, config := range cfg.Integrations {
		if external[config.ID] != nil || providers[config.ClusterID] == nil {
			return errors.New("integration ID must be unique and clusterID must identify a configured cluster")
		}
		client, e := integrations.New(config)
		if e != nil {
			return e
		}
		external[config.ID] = client
	}
	handler := api.New(api.Options{SessionLifetime: cfg.SessionLifetime, Demo: cfg.Demo, Store: s, Providers: providers, Clusters: clusters, AdminUser: cfg.AdminUser, AdminHash: cfg.AdminPasswordHash, Metrics: gateway.Handler(), ClusterMetrics: gateway.HandlerForCluster, StaticDir: cfg.StaticDirectory, OIDC: identityProviders, LDAP: cfg.LDAP, Grants: grants, Integrations: external})
	worker := jobs.Worker{Store: s, Providers: providers}
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); worker.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-workerDone:
		case <-time.After(12 * time.Second):
			slog.Error("background worker shutdown timed out")
		}
	}()
	server := &http.Server{Addr: cfg.Listen, Handler: telemetry.NewHTTP(httpgzip.Handler(handler), gateway.Stats), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	go func() {
		<-ctx.Done()
		shutdown, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = server.Shutdown(shutdown)
	}()
	slog.Info("Kaflux listening", "address", cfg.Listen, "demo", cfg.Demo)
	e = server.ListenAndServe()
	if errors.Is(e, http.ErrServerClosed) {
		return nil
	}
	return e
}

func validateAuthentication(cfg config.Config) error {
	if !cfg.Demo && cfg.AdminPasswordHash == "" && len(cfg.OIDC) == 0 && len(cfg.LDAP) == 0 {
		return errors.New("production requires configured authentication")
	}
	return nil
}

func openStore(ctx context.Context, cfg config.Config) (*store.Store, error) {
	switch cfg.StorageBackend {
	case "sqlite":
		return store.NewSQLite(ctx, cfg.SQLitePath)
	case "postgres":
		return store.New(ctx, cfg.DatabaseURL)
	case "memory":
		return store.New(ctx, "")
	default:
		return nil, errors.New("unsupported storage backend")
	}
}
