package metrics

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// HandlerForCluster cannot be redirected to another cluster's configured datasource.
// The API must authorize the cluster before invoking this handler.
func (g *Gateway) HandlerForCluster(clusterID string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.TrimRight(r.URL.Path, "/") == "/catalog" {
			g.Handler().ServeHTTP(w, r)
			return
		}
		metric := r.URL.Query().Get("metric")
		kind := "prometheus"
		for _, def := range g.opts.Catalog {
			if def.ID == metric {
				kind = def.Kind
				break
			}
		}
		sourceID := ""
		candidates := []string{}
		for id, s := range g.sources {
			if s.config.ClusterID == clusterID && ((kind == "cloudwatch" && s.config.Kind == "cloudwatch") || (kind == "prometheus" && (s.config.Kind == "prometheus" || s.config.Kind == "amp" || s.config.Kind == "simulator"))) {
				candidates = append(candidates, id)
			}
		}
		sort.Strings(candidates)
		if len(candidates) > 0 {
			sourceID = candidates[0]
		}
		if sourceID == "" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"data": Result{Status: "unavailable", Series: []Series{}, Error: "Metrics temporarily unavailable: no datasource is bound to this cluster"}})
			return
		}
		clone := r.Clone(r.Context())
		urlCopy := *r.URL
		values := urlCopy.Query()
		values.Set("source", sourceID)
		values.Del("instance")
		urlCopy.RawQuery = values.Encode()
		clone.URL = &urlCopy
		g.Handler().ServeHTTP(w, clone)
	})
}

func expandConfiguredRegex(expression, pattern, duration string) (string, error) {
	if len(pattern) > 1024 {
		return "", errors.New("instance regex exceeds limit")
	}
	if _, err := regexp.Compile(pattern); err != nil {
		return "", err
	}
	out, err := Expand(expression, "", duration)
	if err != nil {
		return "", err
	}
	quoted := strconv.Quote(pattern)
	// Expand replaces the empty instance with .*; replace only its original variable site.
	expr := strings.ReplaceAll(expression, "$instance", quoted[1:len(quoted)-1])
	expr = strings.ReplaceAll(expr, "$__range", duration)
	if containsTemplateVariable(expr) {
		return out, errors.New("unsupported variable")
	}
	return expr, nil
}
