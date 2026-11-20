package main

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/api"
	"github.com/hawxxx/kaflux/backend/internal/config"
	"github.com/hawxxx/kaflux/backend/internal/mcp"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

// mcpWriteSlack is added to the per-call timeout for a /mcp response's write
// deadline, so a call that ends at its timeout can still send its result.
const mcpWriteSlack = 10 * time.Second

// newAPI builds the API and, when runtime.mcp.enabled is set, mounts /mcp on it.
// MCP settings are reported to the token endpoints either way.
func newAPI(cfg config.Config, o api.Options) *api.API {
	o.MCPEnabled = cfg.MCP.Enabled
	o.MCPWritesEnabled = cfg.MCP.Enabled && cfg.MCP.Writes
	if cfg.MCP.Enabled {
		o.ExtraMetrics = mcp.WriteMetrics
	}
	a := api.New(o)
	if h := buildMCP(cfg, a, o.Store); h != nil {
		a.SetMCP(h)
	}
	logMCP(cfg)
	return a
}

// buildMCP returns the /mcp handler, or nil when MCP is disabled. Tool calls
// dispatch into a; the handler records audit rows in s and keys write
// confirmations with a random per-process key.
func buildMCP(cfg config.Config, a *api.API, s *store.Store) http.Handler {
	if !cfg.MCP.Enabled {
		return nil
	}
	h := mcp.NewHandler(mcp.Options{
		Store:          s,
		Handler:        a,
		Grants:         a.ConfiguredGrants,
		Writes:         cfg.MCP.Writes,
		AllowedOrigins: cfg.MCP.AllowedOrigins,
		Limits: mcp.Limits{
			MaxBytes:    cfg.MCP.MaxResponseBytes,
			Timeout:     cfg.MCP.CallTimeout,
			PerMinute:   cfg.MCP.RateLimitPerMinute,
			Concurrency: cfg.MCP.ConcurrencyPerToken,
		},
		Demo: cfg.Demo,
	})
	return extendWriteDeadline(h, cfg.MCP.CallTimeout+mcpWriteSlack)
}

// extendWriteDeadline lifts the server-wide write timeout for h's responses
// to d, since MCP calls may run longer than ordinary API requests.
func extendWriteDeadline(h http.Handler, d time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(d))
		h.ServeHTTP(w, r)
	})
}

// mcpToolCount is the number of tools the catalog exposes for the writes mode.
func mcpToolCount(writes bool) int {
	n := 0
	for _, t := range mcp.Catalog() {
		if writes || !t.IsWrite() {
			n++
		}
	}
	return n
}

func logMCP(cfg config.Config) {
	if !cfg.MCP.Enabled {
		slog.Info("MCP server disabled")
		return
	}
	writes := "disabled"
	if cfg.MCP.Writes {
		writes = "enabled"
	}
	slog.Info("MCP server enabled", "path", "/mcp", "writes", writes, "tools", mcpToolCount(cfg.MCP.Writes), "allowed_origins", len(cfg.MCP.AllowedOrigins), "demo", cfg.Demo)
	if cfg.StorageBackend == "postgres" {
		slog.Warn("MCP write confirmations are per-process; with several replicas route /mcp with sticky sessions so a confirmation reaches the replica that issued it")
	}
}
