package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/store"
)

const oauthVerifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"

func oauthChallenge() string {
	sum := sha256.Sum256([]byte(oauthVerifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func (f *tokenFixture) oauthFixture() {
	f.a.SetMCP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
}

func (f *tokenFixture) public(method, path, contentType, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.RemoteAddr = "192.0.2.1:1234"
	r.Host = "kaflux.test"
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	w := httptest.NewRecorder()
	f.a.ServeHTTP(w, r)
	return w
}

func (f *tokenFixture) register(redirect string) string {
	f.t.Helper()
	w := f.public("POST", "/oauth/register", "application/json", `{"client_name":"Atlas","redirect_uris":["`+redirect+`"],"token_endpoint_auth_method":"none"}`)
	if w.Code != 201 {
		f.t.Fatalf("register %d %s", w.Code, w.Body.String())
	}
	var out struct {
		ClientID string `json:"client_id"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return out.ClientID
}

// authorize starts a request and returns the request id the consent screen uses.
func (f *tokenFixture) authorize(clientID, redirect, scope string) string {
	f.t.Helper()
	q := url.Values{"response_type": {"code"}, "client_id": {clientID}, "redirect_uri": {redirect}, "code_challenge": {oauthChallenge()}, "code_challenge_method": {"S256"}, "state": {"xyz"}, "scope": {scope}}
	w := f.public("GET", "/oauth/authorize?"+q.Encode(), "", "")
	if w.Code != 302 {
		f.t.Fatalf("authorize %d %s", w.Code, w.Body.String())
	}
	loc, _ := url.Parse(w.Header().Get("Location"))
	if loc.Path != "/oauth/consent" || loc.Query().Get("request") == "" {
		f.t.Fatalf("location %s", loc)
	}
	return loc.Query().Get("request")
}

func (f *tokenFixture) approve(user, id, body string) (int, string) {
	w := f.call(user, "POST", oauthRequestsBase+id+"/approve", body)
	var env struct {
		Data struct{ Redirect string }
	}
	_ = json.Unmarshal(w.Body.Bytes(), &env)
	if w.Code != 200 {
		return w.Code, w.Body.String()
	}
	loc, _ := url.Parse(env.Data.Redirect)
	return w.Code, loc.Query().Get("code")
}

func (f *tokenFixture) token(form url.Values) (int, map[string]any) {
	w := f.public("POST", "/oauth/token", "application/x-www-form-urlencoded", form.Encode())
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out
}

func TestOAuthDiscoveryAndRegistration(t *testing.T) {
	f := newTokenFixture(t)
	f.oauthFixture()
	w := f.public("GET", "/.well-known/oauth-authorization-server", "", "")
	var meta map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &meta)
	if w.Code != 200 || meta["token_endpoint"] != "http://kaflux.test/oauth/token" || meta["code_challenge_methods_supported"].([]any)[0] != "S256" {
		t.Fatalf("metadata %d %v", w.Code, meta)
	}
	w = f.public("GET", "/.well-known/oauth-protected-resource", "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"resource":"http://kaflux.test/mcp"`) {
		t.Fatalf("resource %d %s", w.Code, w.Body.String())
	}
	for _, bad := range []string{"javascript:alert(1)", "http://evil.example/cb", "https://app.example/cb#frag"} {
		if w := f.public("POST", "/oauth/register", "application/json", `{"redirect_uris":["`+bad+`"]}`); w.Code != 400 {
			t.Fatalf("redirect %q accepted: %d", bad, w.Code)
		}
	}
	for _, ok := range []string{"https://app.example/cb", "http://127.0.0.1:8123/cb", "cursor://callback"} {
		f.register(ok)
	}
}

func TestOAuthDisabledWhenMCPIsNotMounted(t *testing.T) {
	f := newTokenFixture(t)
	if w := f.public("GET", "/.well-known/oauth-authorization-server", "", ""); w.Code == 200 {
		t.Fatalf("metadata served while MCP is off: %d", w.Code)
	}
}

func TestOAuthFullFlow(t *testing.T) {
	f := newTokenFixture(t)
	f.oauthFixture()
	redirect := "https://app.example/cb"
	client := f.register(redirect)
	id := f.authorize(client, redirect, "read messages")

	// The consent screen sees the client and what this user may grant.
	w := f.call("admin", "GET", oauthRequestsBase+id, "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"name":"Atlas"`) || !strings.Contains(w.Body.String(), `"host":"app.example"`) {
		t.Fatalf("request %d %s", w.Code, w.Body.String())
	}
	// A user without manage-mcp cannot approve.
	if code, _ := f.approve("viewer", id, `{"scopes":["read"]}`); code != 403 {
		t.Fatalf("viewer approve %d", code)
	}
	// A scope the client did not ask for is refused.
	if code, _ := f.approve("admin", id, `{"scopes":["write"]}`); code != 400 {
		t.Fatalf("unrequested scope %d", code)
	}
	code, authCode := f.approve("admin", id, `{"scopes":["read"]}`)
	if code != 200 || authCode == "" {
		t.Fatalf("approve %d %s", code, authCode)
	}
	// The request is spent.
	if code, _ := f.approve("admin", id, `{"scopes":["read"]}`); code != 404 {
		t.Fatalf("second approve %d", code)
	}

	form := url.Values{"grant_type": {"authorization_code"}, "client_id": {client}, "code": {authCode}, "redirect_uri": {redirect}, "code_verifier": {"wrong-verifier-wrong-verifier-wrong-verifier-xx"}}
	if status, out := f.token(form); status != 400 || out["error"] != "invalid_grant" {
		t.Fatalf("wrong verifier %d %v", status, out)
	}
	// A failed attempt still spends the code.
	form.Set("code_verifier", oauthVerifier)
	if status, _ := f.token(form); status != 400 {
		t.Fatalf("code reuse %d", status)
	}
	id = f.authorize(client, redirect, "read")
	_, authCode = f.approve("admin", id, `{"scopes":["read"]}`)
	form.Set("code", authCode)
	status, out := f.token(form)
	if status != 200 || out["token_type"] != "Bearer" || out["scope"] != "read" || out["expires_in"].(float64) != 3600 {
		t.Fatalf("token %d %v", status, out)
	}
	access, refresh := out["access_token"].(string), out["refresh_token"].(string)
	if !strings.HasPrefix(access, "kfx_") || !strings.HasPrefix(refresh, "kfr_") {
		t.Fatalf("secret shapes %q %q", access, refresh)
	}
	// The access token authenticates; the refresh token never does.
	if _, e := f.st.MCPTokenByHash(t.Context(), store.HashMCPSecret(access)); e != nil {
		t.Fatalf("access token rejected: %v", e)
	}

	// The connection is listed once, as OAuth, with no secret.
	l := f.list("admin")
	if len(l.Data) != 1 || l.Data[0].Name != "Atlas" || l.Data[0].Status != "active" {
		t.Fatalf("list %+v", l.Data)
	}
	if strings.Contains(f.call("admin", "GET", tokensPath, "").Body.String(), refresh) {
		t.Fatal("refresh secret leaked into the list")
	}

	// Rotation: a refresh token works once.
	rf := url.Values{"grant_type": {"refresh_token"}, "client_id": {client}, "refresh_token": {refresh}}
	status, out = f.token(rf)
	if status != 200 || out["refresh_token"] == refresh {
		t.Fatalf("refresh %d %v", status, out)
	}
	if status, out = f.token(rf); status != 400 || out["error"] != "invalid_grant" {
		t.Fatalf("refresh reuse %d %v", status, out)
	}
	newAccess := out["access_token"]
	_ = newAccess

	// Revoking the connection ends its access tokens.
	conn := f.list("admin").Data
	if len(conn) != 1 {
		t.Fatalf("after rotation %+v", conn)
	}
	if w := f.call("admin", "DELETE", tokensPath+"/"+conn[0].ID, ""); w.Code != 200 {
		t.Fatalf("revoke %d %s", w.Code, w.Body.String())
	}
	if _, e := f.st.MCPTokenByHash(t.Context(), store.HashMCPSecret(access)); e == nil {
		t.Fatal("access token survived revoking the connection")
	}
	if l := f.list("admin"); len(l.Data) != 0 {
		t.Fatalf("connection still listed %+v", l.Data)
	}
}

func TestOAuthAuthorizeRejectsUntrustedRedirects(t *testing.T) {
	f := newTokenFixture(t)
	f.oauthFixture()
	client := f.register("https://app.example/cb")
	q := url.Values{"response_type": {"code"}, "client_id": {client}, "redirect_uri": {"https://evil.example/cb"}, "code_challenge": {oauthChallenge()}, "code_challenge_method": {"S256"}}
	if w := f.public("GET", "/oauth/authorize?"+q.Encode(), "", ""); w.Code != 400 || w.Header().Get("Location") != "" {
		t.Fatalf("unregistered redirect %d %q", w.Code, w.Header().Get("Location"))
	}
	if w := f.public("GET", "/oauth/authorize?"+url.Values{"client_id": {"kfc_forged.sig"}, "redirect_uri": {"https://app.example/cb"}}.Encode(), "", ""); w.Code != 400 {
		t.Fatalf("forged client %d", w.Code)
	}
	// Without PKCE the client is sent back with an error instead of a request.
	q.Set("redirect_uri", "https://app.example/cb")
	q.Del("code_challenge")
	w := f.public("GET", "/oauth/authorize?"+q.Encode(), "", "")
	if w.Code != 302 || !strings.Contains(w.Header().Get("Location"), "error=invalid_request") {
		t.Fatalf("no pkce %d %q", w.Code, w.Header().Get("Location"))
	}
}

func TestOAuthDenyAndTokenCannotApprove(t *testing.T) {
	f := newTokenFixture(t)
	f.oauthFixture()
	client := f.register("https://app.example/cb")
	id := f.authorize(client, "https://app.example/cb", "read")
	w := f.call("admin", "POST", oauthRequestsBase+id+"/deny", "{}")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "access_denied") {
		t.Fatalf("deny %d %s", w.Code, w.Body.String())
	}
	if w := f.call("admin", "GET", oauthRequestsBase+id, ""); w.Code != 404 {
		t.Fatalf("denied request still open %d", w.Code)
	}
	if w := f.call("admin", "GET", oauthRequestsBase+"short", ""); w.Code != 404 {
		t.Fatalf("bad id %d", w.Code)
	}
}
