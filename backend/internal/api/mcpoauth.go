package api

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

// MCP-native OAuth: an authorization-code flow with PKCE for MCP clients that
// would rather sign in than be handed a token. A signed-in console user
// approves a request on the consent screen; the client then exchanges the code
// for a short-lived access token and a rotating refresh token. Both are rows in
// the MCP token store, so /mcp authenticates them like any other token.
//
// Clients register themselves (RFC 7591) as public clients. Registrations,
// pending requests and authorization codes are held in memory and signed with a
// key made at startup: a restart invalidates them, and a client simply registers
// again. Issued tokens are stored and survive a restart.
const (
	oauthAccessTTL  = time.Hour
	oauthRefreshTTL = 30 * 24 * time.Hour
	oauthRequestTTL = 10 * time.Minute
	oauthCodeTTL    = time.Minute
	oauthMaxPending = 1000

	oauthRequestsBase = "/api/v1/mcp/oauth/requests/"
)

var oauthRequestID = regexp.MustCompile(`^[A-Za-z0-9_-]{16,128}$`)

// oauthClient is what a client_id carries: the registration itself, signed.
type oauthClient struct {
	Name         string   `json:"n"`
	RedirectURIs []string `json:"r"`
}

type oauthRequest struct {
	client      oauthClient
	clientID    string
	redirectURI string
	state       string
	challenge   string
	scopes      []string
	created     time.Time
	// owner is the console user who first opened the request; nobody else may decide it.
	owner string
}

type oauthCode struct {
	req      oauthRequest
	owner    auth.User
	scopes   []string
	clusters []string
	expires  time.Time
}

type oauthState struct {
	once     sync.Once
	key      []byte
	mu       sync.Mutex
	requests map[string]oauthRequest
	codes    map[string]oauthCode
	// refreshMu serializes refresh-token rotation so one token cannot be spent twice.
	refreshMu sync.Mutex
}

func (o *oauthState) init() {
	o.once.Do(func() {
		o.key = make([]byte, 32)
		if _, e := rand.Read(o.key); e != nil {
			panic(e)
		}
		o.requests = map[string]oauthRequest{}
		o.codes = map[string]oauthCode{}
	})
}

func randomURLToken(n int) string {
	b := make([]byte, n)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// signClient turns a registration into a client_id.
func (a *API) signClient(c oauthClient) string {
	a.oauth.init()
	body, _ := json.Marshal(c)
	enc := base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, a.oauth.key)
	mac.Write([]byte(enc))
	return "kfc_" + enc + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// verifyClient returns the registration behind a client_id, or false if it was
// not issued by this server since it started.
func (a *API) verifyClient(id string) (oauthClient, bool) {
	a.oauth.init()
	rest, ok := strings.CutPrefix(id, "kfc_")
	if !ok {
		return oauthClient{}, false
	}
	enc, sig, ok := strings.Cut(rest, ".")
	if !ok {
		return oauthClient{}, false
	}
	got, e := base64.RawURLEncoding.DecodeString(sig)
	if e != nil {
		return oauthClient{}, false
	}
	mac := hmac.New(sha256.New, a.oauth.key)
	mac.Write([]byte(enc))
	if !hmac.Equal(got, mac.Sum(nil)) {
		return oauthClient{}, false
	}
	body, e := base64.RawURLEncoding.DecodeString(enc)
	var c oauthClient
	if e != nil || json.Unmarshal(body, &c) != nil || len(c.RedirectURIs) == 0 {
		return oauthClient{}, false
	}
	return c, true
}

// externalOrigin is the scheme and host the client reached us on.
func externalOrigin(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

func oauthJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func oauthError(w http.ResponseWriter, status int, code, description string) {
	oauthJSON(w, status, map[string]string{"error": code, "error_description": description})
}

var blockedRedirectSchemes = map[string]bool{"javascript": true, "data": true, "file": true, "vbscript": true, "about": true, "blob": true, "ftp": true}

// validRedirectURI accepts https, loopback http (native apps) and private-use
// schemes, and refuses anything that can carry a fragment or run script.
func validRedirectURI(raw string) bool {
	if len(raw) > 512 {
		return false
	}
	u, e := url.Parse(raw)
	if e != nil || u.Fragment != "" || u.Scheme == "" || blockedRedirectSchemes[strings.ToLower(u.Scheme)] {
		return false
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return u.Host != "" && u.User == nil
	case "http":
		host := u.Hostname()
		ip := net.ParseIP(host)
		return host == "localhost" || (ip != nil && ip.IsLoopback())
	default:
		return true
	}
}

// mcpOAuthPublic serves the endpoints an MCP client reaches without a console
// session. It returns false for any other path, or when MCP is not mounted.
func (a *API) mcpOAuthPublic(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path
	switch path {
	case "/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp",
		"/.well-known/oauth-authorization-server", "/oauth/register", "/oauth/authorize", "/oauth/token":
	default:
		return false
	}
	if a.o.MCP == nil {
		return false
	}
	a.oauth.init()
	origin := externalOrigin(r)
	switch path {
	case "/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp":
		oauthJSON(w, 200, map[string]any{"resource": origin + "/mcp", "authorization_servers": []string{origin}, "bearer_methods_supported": []string{"header"}, "scopes_supported": auth.MCPScopes})
	case "/.well-known/oauth-authorization-server":
		oauthJSON(w, 200, map[string]any{
			"issuer": origin, "authorization_endpoint": origin + "/oauth/authorize", "token_endpoint": origin + "/oauth/token", "registration_endpoint": origin + "/oauth/register",
			"response_types_supported": []string{"code"}, "grant_types_supported": []string{"authorization_code", "refresh_token"},
			"code_challenge_methods_supported": []string{"S256"}, "token_endpoint_auth_methods_supported": []string{"none"}, "scopes_supported": auth.MCPScopes,
		})
	case "/oauth/register":
		a.oauthRegister(w, r)
	case "/oauth/authorize":
		a.oauthAuthorize(w, r)
	case "/oauth/token":
		a.oauthToken(w, r)
	}
	return true
}

func (a *API) oauthRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		oauthError(w, 405, "invalid_request", "POST required")
		return
	}
	var body struct {
		ClientName   string   `json:"client_name"`
		RedirectURIs []string `json:"redirect_uris"`
		AuthMethod   string   `json:"token_endpoint_auth_method"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	if e := json.NewDecoder(r.Body).Decode(&body); e != nil {
		oauthError(w, 400, "invalid_client_metadata", "Body must be a JSON registration")
		return
	}
	if len(body.RedirectURIs) == 0 || len(body.RedirectURIs) > 5 {
		oauthError(w, 400, "invalid_redirect_uri", "Provide 1 to 5 redirect_uris")
		return
	}
	for _, u := range body.RedirectURIs {
		if !validRedirectURI(u) {
			oauthError(w, 400, "invalid_redirect_uri", "Redirect URIs must be https, loopback http or an app scheme")
			return
		}
	}
	if body.AuthMethod != "" && body.AuthMethod != "none" {
		oauthError(w, 400, "invalid_client_metadata", "Only public clients (token_endpoint_auth_method none) are supported")
		return
	}
	name := strings.TrimSpace(body.ClientName)
	if name == "" || !validMCPTokenName(name) {
		name = "MCP client"
	}
	c := oauthClient{Name: name, RedirectURIs: body.RedirectURIs}
	oauthJSON(w, 201, map[string]any{
		"client_id": a.signClient(c), "client_name": name, "redirect_uris": body.RedirectURIs, "token_endpoint_auth_method": "none",
		"grant_types": []string{"authorization_code", "refresh_token"}, "response_types": []string{"code"}, "client_id_issued_at": time.Now().Unix(),
	})
}

func redirectWith(base string, params map[string]string) string {
	u, e := url.Parse(base)
	if e != nil {
		return base
	}
	q := u.Query()
	for k, v := range params {
		if v != "" {
			q.Set(k, v)
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (a *API) oauthAuthorize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		oauthError(w, 405, "invalid_request", "GET required")
		return
	}
	q := r.URL.Query()
	client, ok := a.verifyClient(q.Get("client_id"))
	redirect := q.Get("redirect_uri")
	// Until the client and redirect are trusted, errors are shown, never redirected.
	if !ok || !slices.Contains(client.RedirectURIs, redirect) {
		oauthError(w, 400, "invalid_request", "Unknown client or redirect_uri. Register the client again and retry.")
		return
	}
	state := q.Get("state")
	reject := func(code, description string) {
		http.Redirect(w, r, redirectWith(redirect, map[string]string{"error": code, "error_description": description, "state": state}), http.StatusFound)
	}
	if q.Get("response_type") != "code" {
		reject("unsupported_response_type", "response_type must be code")
		return
	}
	challenge := q.Get("code_challenge")
	if q.Get("code_challenge_method") != "S256" || len(challenge) < 43 || len(challenge) > 128 {
		reject("invalid_request", "PKCE with code_challenge_method S256 is required")
		return
	}
	scopes := []string{}
	for _, sc := range auth.MCPScopes {
		if slices.Contains(strings.Fields(q.Get("scope")), sc) {
			scopes = append(scopes, sc)
		}
	}
	if len(scopes) == 0 {
		scopes = []string{"read"}
	}
	now := time.Now()
	a.oauth.mu.Lock()
	for id, p := range a.oauth.requests {
		if now.Sub(p.created) > oauthRequestTTL {
			delete(a.oauth.requests, id)
		}
	}
	if len(a.oauth.requests) >= oauthMaxPending {
		a.oauth.mu.Unlock()
		reject("temporarily_unavailable", "Too many pending requests; retry shortly")
		return
	}
	id := randomURLToken(24)
	a.oauth.requests[id] = oauthRequest{client: client, clientID: q.Get("client_id"), redirectURI: redirect, state: state, challenge: challenge, scopes: scopes, created: now}
	a.oauth.mu.Unlock()
	http.Redirect(w, r, "/oauth/consent?request="+id, http.StatusFound)
}

// takeOAuthRequest returns a pending request for user u, binding it to u the first time it is opened.
// A request first opened by someone else looks the same as one that expired.
func (a *API) takeOAuthRequest(id string, u auth.User, consume bool) (oauthRequest, bool) {
	a.oauth.init()
	a.oauth.mu.Lock()
	defer a.oauth.mu.Unlock()
	p, ok := a.oauth.requests[id]
	if !ok || time.Since(p.created) > oauthRequestTTL {
		delete(a.oauth.requests, id)
		return oauthRequest{}, false
	}
	who := u.Provider + "\x00" + u.ID
	if p.owner == "" {
		p.owner = who
		a.oauth.requests[id] = p
	} else if p.owner != who {
		return oauthRequest{}, false
	}
	if consume {
		delete(a.oauth.requests, id)
	}
	return p, true
}

// heldMCPScopes returns the scopes u may grant on the given clusters (every
// configured cluster when none are named), using the same rule as token creation.
func (a *API) heldMCPScopes(az auth.Authorizer, u auth.User, clusters []string) []string {
	targets := clusters
	if len(targets) == 0 {
		targets = []string{"*"}
		for _, c := range a.o.Clusters {
			targets = append(targets, c.ID)
		}
	}
	out := []string{}
	for _, sc := range auth.MCPScopes {
		for _, action := range auth.MCPScopeActions[sc] {
			if slices.ContainsFunc(targets, func(c string) bool { return az.AnyAllowed(u, c, action) }) {
				out = append(out, sc)
				break
			}
		}
	}
	return out
}

func clientHost(c oauthClient) string {
	u, e := url.Parse(c.RedirectURIs[0])
	if e != nil {
		return ""
	}
	if u.Host != "" {
		return u.Host
	}
	return u.Scheme + "://"
}

// mcpOAuthConsole serves the consent screen's API for a signed-in console user.
func (a *API) mcpOAuthConsole(w http.ResponseWriter, r *http.Request, s auth.Session) bool {
	if !strings.HasPrefix(r.URL.Path, oauthRequestsBase) {
		return false
	}
	if _, viaMCP := mcpFrom(r.Context()); viaMCP {
		fail(w, 403, "forbidden", "MCP tokens cannot approve MCP sign-ins; use the console")
		return true
	}
	rest := strings.TrimPrefix(r.URL.Path, oauthRequestsBase)
	id, action, _ := strings.Cut(rest, "/")
	if !oauthRequestID.MatchString(id) {
		fail(w, 404, "not_found", "This request has expired or was already used")
		return true
	}
	az := auth.Authorizer{Grants: a.configuredGrants()}
	if !az.AnyAllowed(s.User, "*", "manage-mcp") {
		fail(w, 403, "forbidden", "Connecting an MCP client requires the manage-mcp permission. Ask an administrator to grant it.")
		return true
	}
	switch {
	case action == "" && r.Method == http.MethodGet:
		p, ok := a.takeOAuthRequest(id, s.User, false)
		if !ok {
			fail(w, 404, "not_found", "This request has expired or was already used")
			return true
		}
		held := a.heldMCPScopes(az, s.User, nil)
		clusters := []map[string]string{}
		for _, c := range a.o.Clusters {
			if len(a.heldMCPScopes(az, s.User, []string{c.ID})) > 0 {
				clusters = append(clusters, map[string]string{"id": c.ID, "name": c.Name})
			}
		}
		respond(w, map[string]any{"client": map[string]string{"name": p.client.Name, "host": clientHost(p.client)}, "scopes": p.scopes, "clusters": clusters, "ownerScopes": held, "accessTokenMinutes": int(oauthAccessTTL / time.Minute), "refreshTokenDays": int(oauthRefreshTTL / (24 * time.Hour))})
	case action == "approve" && r.Method == http.MethodPost:
		a.oauthApprove(w, r, s, az, id)
	case action == "deny" && r.Method == http.MethodPost:
		p, ok := a.takeOAuthRequest(id, s.User, true)
		if !ok {
			fail(w, 404, "not_found", "This request has expired or was already used")
			return true
		}
		_ = a.oauthAudit(r, s.User, "mcp.oauth.deny", p.client.Name, "success", nil)
		respond(w, map[string]string{"redirect": redirectWith(p.redirectURI, map[string]string{"error": "access_denied", "error_description": "The user denied the request", "state": p.state})})
	default:
		fail(w, 405, "method_not_allowed", "Unsupported method")
	}
	return true
}

func (a *API) oauthAudit(r *http.Request, u auth.User, action, client, result string, details any) error {
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return a.o.Store.Audit(r.Context(), store.Audit{Actor: u.ID, Provider: u.Provider, ClusterID: "*", Action: action, Resource: "oauth client (" + client + ")", Result: result, RequestID: requestID(r), SourceIP: ip, AdminAfter: details})
}

func (a *API) oauthApprove(w http.ResponseWriter, r *http.Request, s auth.Session, az auth.Authorizer, id string) {
	var body struct {
		Scopes   []string `json:"scopes"`
		Clusters []string `json:"clusters"`
	}
	if !decode(w, r, &body) {
		return
	}
	p, ok := a.takeOAuthRequest(id, s.User, false)
	if !ok {
		fail(w, 404, "not_found", "This request has expired or was already used")
		return
	}
	clusters := []string{}
	for _, c := range body.Clusters {
		if !slices.ContainsFunc(a.o.Clusters, func(k model.Cluster) bool { return k.ID == c }) {
			fail(w, 400, "invalid_cluster", fmt.Sprintf("Unknown cluster %q", c))
			return
		}
		if !slices.Contains(clusters, c) {
			clusters = append(clusters, c)
		}
	}
	held := a.heldMCPScopes(az, s.User, clusters)
	scopes := []string{}
	for _, sc := range auth.MCPScopes {
		if slices.Contains(body.Scopes, sc) {
			if !slices.Contains(p.scopes, sc) {
				fail(w, 400, "invalid_scope", fmt.Sprintf("The client did not request the %s scope", sc))
				return
			}
			if !slices.Contains(held, sc) {
				fail(w, 403, "scope_not_permitted", fmt.Sprintf("You hold no grant for the %s scope on the selected clusters", sc))
				return
			}
			scopes = append(scopes, sc)
		}
	}
	if len(scopes) == 0 {
		fail(w, 400, "invalid_scope", "Choose at least one scope")
		return
	}
	details := map[string]any{"scopes": scopes, "clusters": clusters}
	if e := a.oauthAudit(r, s.User, "mcp.oauth.approve", p.client.Name, "success", details); e != nil {
		failCause(w, r, 503, "audit_unavailable", "Approval blocked because audit storage is unavailable", e)
		return
	}
	// Only the first approval of a request wins.
	if _, ok := a.takeOAuthRequest(id, s.User, true); !ok {
		fail(w, 404, "not_found", "This request has expired or was already used")
		return
	}
	code := randomURLToken(32)
	a.oauth.mu.Lock()
	a.oauth.codes[code] = oauthCode{req: p, owner: s.User, scopes: scopes, clusters: clusters, expires: time.Now().Add(oauthCodeTTL)}
	a.oauth.mu.Unlock()
	respond(w, map[string]string{"redirect": redirectWith(p.redirectURI, map[string]string{"code": code, "state": p.state})})
}

func pkceMatches(verifier, challenge string) bool {
	if len(verifier) < 43 || len(verifier) > 128 {
		return false
	}
	sum := sha256.Sum256([]byte(verifier))
	want := base64.RawURLEncoding.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(want), []byte(challenge)) == 1
}

func (a *API) oauthToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		oauthError(w, 405, "invalid_request", "POST required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	if e := r.ParseForm(); e != nil {
		oauthError(w, 400, "invalid_request", "Body must be application/x-www-form-urlencoded")
		return
	}
	clientID := r.PostForm.Get("client_id")
	if _, ok := a.verifyClient(clientID); !ok {
		oauthError(w, 401, "invalid_client", "Unknown client. Register the client again and retry.")
		return
	}
	switch r.PostForm.Get("grant_type") {
	case "authorization_code":
		a.oauthExchangeCode(w, r, clientID)
	case "refresh_token":
		a.oauthRefresh(w, r, clientID)
	default:
		oauthError(w, 400, "unsupported_grant_type", "grant_type must be authorization_code or refresh_token")
	}
}

func (a *API) oauthExchangeCode(w http.ResponseWriter, r *http.Request, clientID string) {
	a.oauth.mu.Lock()
	c, ok := a.oauth.codes[r.PostForm.Get("code")]
	delete(a.oauth.codes, r.PostForm.Get("code")) // single use, even when the checks below fail
	a.oauth.mu.Unlock()
	if !ok || time.Now().After(c.expires) || c.req.clientID != clientID || c.req.redirectURI != r.PostForm.Get("redirect_uri") || !pkceMatches(r.PostForm.Get("code_verifier"), c.req.challenge) {
		oauthError(w, 400, "invalid_grant", "The code is invalid, expired or already used")
		return
	}
	grant := auth.Token()[:32]
	a.oauthIssue(w, r, c.owner, grant, clientID, c.req.client.Name, c.scopes, c.clusters, "")
}

func (a *API) oauthRefresh(w http.ResponseWriter, r *http.Request, clientID string) {
	secret := r.PostForm.Get("refresh_token")
	if !strings.HasPrefix(secret, "kfr_") {
		oauthError(w, 400, "invalid_grant", "The refresh token is invalid, expired or revoked")
		return
	}
	a.oauth.refreshMu.Lock()
	defer a.oauth.refreshMu.Unlock()
	old, e := a.o.Store.MCPTokenByHash(r.Context(), store.HashMCPSecret(secret))
	if errors.Is(e, store.ErrMCPTokenNotFound) || (e == nil && (old.Kind != store.MCPKindRefresh || old.ClientID != clientID)) {
		oauthError(w, 400, "invalid_grant", "The refresh token is invalid, expired or revoked")
		return
	}
	if e != nil {
		oauthError(w, 503, "temporarily_unavailable", "Token store unavailable; retry")
		return
	}
	owner := auth.User{ID: old.OwnerID, Provider: old.OwnerProvider, Roles: old.OwnerRoles}
	a.oauthIssue(w, r, owner, old.Grant, clientID, old.ClientName, old.Scopes, old.Clusters, old.ID)
}

// oauthIssue mints a refresh and an access token for a grant and answers the
// token request. A non-empty replaces is the refresh token being rotated out.
func (a *API) oauthIssue(w http.ResponseWriter, r *http.Request, owner auth.User, grant, clientID, clientName string, scopes, clusters []string, replaces string) {
	ctx := r.Context()
	now := time.Now().UTC().Truncate(time.Millisecond)
	refresh := store.MCPToken{ID: auth.Token()[:32], Name: clientName, OwnerID: owner.ID, OwnerProvider: owner.Provider, OwnerRoles: slices.Clone(owner.Roles), Scopes: scopes, Clusters: clusters, Created: now, Expires: now.Add(oauthRefreshTTL), Kind: store.MCPKindRefresh, Grant: grant, ClientID: clientID, ClientName: clientName}
	access := refresh
	access.ID, access.Expires, access.Kind = auth.Token()[:32], now.Add(oauthAccessTTL), store.MCPKindAccess
	refreshSecret, _ := store.NewMCPSecret()
	refreshSecret = "kfr_" + strings.TrimPrefix(refreshSecret, "kfx_")
	accessSecret, accessHash := store.NewMCPSecret()
	if e := a.o.Store.CreateMCPToken(ctx, refresh, store.HashMCPSecret(refreshSecret)); e != nil {
		failOAuth(w, r, e)
		return
	}
	if e := a.o.Store.CreateMCPToken(ctx, access, accessHash); e != nil {
		_ = a.o.Store.RevokeMCPToken(ctx, refresh.ID)
		failOAuth(w, r, e)
		return
	}
	if replaces != "" {
		if e := a.o.Store.RevokeMCPToken(ctx, replaces); e != nil && !errors.Is(e, store.ErrMCPTokenNotFound) {
			_ = a.o.Store.RevokeMCPToken(ctx, refresh.ID)
			_ = a.o.Store.RevokeMCPToken(ctx, access.ID)
			failOAuth(w, r, e)
			return
		}
	}
	action := "mcp.oauth.token"
	if replaces != "" {
		action = "mcp.oauth.refresh"
	}
	if e := a.oauthAudit(r, owner, action, clientName, "success", map[string]any{"scopes": scopes, "clusters": clusters}); e != nil {
		// Never hand out tokens whose issue is not on record.
		_ = a.o.Store.RevokeMCPToken(context.WithoutCancel(ctx), refresh.ID)
		_ = a.o.Store.RevokeMCPToken(context.WithoutCancel(ctx), access.ID)
		failOAuth(w, r, e)
		return
	}
	oauthJSON(w, 200, map[string]any{"access_token": accessSecret, "token_type": "Bearer", "expires_in": int(oauthAccessTTL / time.Second), "refresh_token": refreshSecret, "scope": strings.Join(scopes, " ")})
}

func failOAuth(w http.ResponseWriter, r *http.Request, cause error) {
	slog.Error("mcp oauth token issue failed", "request_id", requestID(r), "error", cause)
	oauthError(w, 503, "temporarily_unavailable", "Token issue unavailable; retry")
}
