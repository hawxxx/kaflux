package api

import (
	"encoding/json"
	"errors"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	external "github.com/hawxxx/kaflux/backend/internal/integrations"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

func (a *API) integrationRoutes(w http.ResponseWriter, r *http.Request, user auth.User, cluster string, parts []string) bool {
	domain := parts[4]
	kind := "schemas"
	if domain == "connectors" {
		kind = "connect"
	} else if domain != "schemas" {
		return false
	}
	action := "schema-read"
	if kind == "connect" {
		action = "connector-read"
	}
	authorizer := auth.Authorizer{Grants: a.o.Grants}
	if len(parts) == 5 {
		if r.Method != "GET" {
			fail(w, 405, "method_not_allowed", "GET required")
			return true
		}
		if !authorizer.AnyAllowed(user, cluster, action) {
			fail(w, 403, "forbidden", "Permission denied")
			return true
		}
		list := []map[string]string{}
		for id, c := range a.o.Integrations {
			if c.ClusterID() == cluster && c.Kind() == kind {
				list = append(list, map[string]string{"id": id, "kind": kind})
			}
		}
		sort.Slice(list, func(i, j int) bool { return list[i]["id"] < list[j]["id"] })
		respond(w, list)
		return true
	}
	client := a.o.Integrations[parts[5]]
	if client == nil || client.ClusterID() != cluster || client.Kind() != kind {
		fail(w, 404, "integration_not_found", "Integration not configured for this cluster")
		return true
	}
	resource := "*"
	if len(parts) > 6 {
		resource = parts[6]
	}
	if len(resource) > 249 || strings.ContainsAny(resource, "/\\\x00") || resource == "." || resource == ".." {
		fail(w, 400, "invalid_resource", "Invalid resource name")
		return true
	}
	method := r.Method
	path := ""
	mutation := method != "GET"
	payload := []byte(nil)
	if mutation {
		action = "schema-update"
		if kind == "connect" {
			action = "connector-update"
		}
	}
	if len(parts) == 6 {
		if method != "GET" {
			fail(w, 405, "method_not_allowed", "GET required")
			return true
		}
		if !authorizer.AnyAllowed(user, cluster, action) {
			fail(w, 403, "forbidden", "Permission denied")
			return true
		}
		path = "/subjects"
		if kind == "connect" {
			path = "/connectors"
		}
	} else {
		if !a.allowed(user, cluster, action, resource, w) {
			return true
		}
		name := url.PathEscape(resource)
		if kind == "schemas" {
			path = "/subjects/" + name + "/versions/latest"
			if len(parts) == 7 && method == "POST" {
				path = "/subjects/" + name + "/versions"
			} else if len(parts) == 7 && method == "DELETE" {
				path = "/subjects/" + name
			} else if len(parts) == 8 {
				switch parts[7] {
				case "versions":
					if method != "GET" {
						path = ""
					} else {
						path = "/subjects/" + name + "/versions"
					}
				case "configuration":
					path = "/config/" + name
					if method != "GET" && method != "PUT" {
						path = ""
					}
				case "compatibility":
					if method == "POST" {
						path = "/compatibility/subjects/" + name + "/versions/latest"
					} else {
						path = ""
					}
				default:
					path = ""
				}
			} else if len(parts) == 9 && parts[7] == "versions" {
				version, e := strconv.ParseInt(parts[8], 10, 32)
				if (method != "GET" && method != "DELETE") || (parts[8] != "latest" && (e != nil || version < 1)) {
					path = ""
				} else {
					path = "/subjects/" + name + "/versions/" + parts[8]
				}
			} else if len(parts) > 7 {
				path = ""
			}
			if len(parts) == 7 && method != "GET" && method != "POST" && method != "DELETE" {
				path = ""
			}
		} else {
			path = "/connectors/" + name
			switch {
			case len(parts) == 7 && method == "GET":
			case len(parts) == 7 && method == "DELETE":
			case len(parts) == 7 && method == "PUT":
				path += "/config"
			case len(parts) == 8:
				operation := parts[7]
				switch operation {
				case "status":
					if method == "GET" {
						path += "/status"
					} else {
						path = ""
					}
				case "config":
					if method == "GET" || method == "PUT" {
						path += "/config"
					} else {
						path = ""
					}
				case "pause", "resume":
					if method == "POST" {
						method = "PUT"
						path += "/" + operation
					} else {
						path = ""
					}
				case "restart":
					if method == "POST" {
						path += "/restart?includeTasks=true&onlyFailed=false"
					} else {
						path = ""
					}
				default:
					path = ""
				}
			case len(parts) == 10 && parts[7] == "tasks" && parts[9] == "restart" && method == "POST":
				if n, e := strconv.Atoi(parts[8]); e == nil && n >= 0 {
					path += "/tasks/" + parts[8] + "/restart"
				} else {
					path = ""
				}
			default:
				path = ""
			}
		}
	}
	if path == "" {
		fail(w, 400, "invalid_operation", "Unsupported integration operation")
		return true
	}
	if mutation {
		var input struct {
			Confirmation bool            `json:"confirmation"`
			Payload      json.RawMessage `json:"payload"`
		}
		if !decode(w, r, &input) {
			return true
		}
		if !input.Confirmation {
			fail(w, 400, "confirmation_required", "Confirmation is required")
			return true
		}
		payload = input.Payload
		if (method == "PUT" && strings.HasSuffix(path, "/config")) || (kind == "schemas" && (method == "POST" || method == "PUT")) {
			var object map[string]any
			if json.Unmarshal(payload, &object) != nil || object == nil {
				fail(w, 400, "invalid_payload", "A JSON object payload is required")
				return true
			}
		}
		if !a.audit(w, r, user, action, resource, "intent") {
			return true
		}
	}
	if kind == "connect" && method == "PUT" && strings.HasSuffix(path, "/config") && strings.Contains(string(payload), "[REDACTED]") {
		current, e := client.Do(r.Context(), "GET", path, nil)
		if e != nil {
			a.audit(w, r, user, action, resource, "failed")
			fail(w, 503, "configuration_unavailable", "Cannot preserve existing credentials; configuration was not changed")
			return true
		}
		merged, e := external.PreserveRedacted(payload, current)
		if e != nil {
			a.audit(w, r, user, action, resource, "failed")
			fail(w, 400, "invalid_payload", e.Error())
			return true
		}
		payload = merged
	}
	raw, e := client.Do(r.Context(), method, path, payload)
	if e != nil {
		if mutation {
			a.audit(w, r, user, action, resource, "failed")
		}
		var upstream *external.UpstreamError
		if errors.As(e, &upstream) && upstream.Status >= 400 && upstream.Status < 500 {
			fail(w, upstream.Status, "integration_error", e.Error())
		} else {
			fail(w, 503, "integration_unavailable", e.Error())
		}
		return true
	}
	if len(parts) == 6 {
		var names []string
		if json.Unmarshal(raw, &names) != nil {
			fail(w, 502, "invalid_response", "Integration returned unexpected resource list")
			return true
		}
		filtered := []string{}
		for _, name := range names {
			if authorizer.Allowed(user, cluster, action, name) {
				filtered = append(filtered, name)
			}
		}
		sort.Strings(filtered)
		respond(w, filtered)
		return true
	}
	safe, e := external.Redact(raw)
	if e != nil {
		fail(w, 502, "invalid_response", "Integration returned invalid response")
		return true
	}
	if mutation && !a.audit(w, r, user, action, resource, "success") {
		return true
	}
	respond(w, json.RawMessage(safe))
	return true
}
