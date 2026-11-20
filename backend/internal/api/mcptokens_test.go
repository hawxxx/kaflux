package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

type tokenFixture struct {
	t     *testing.T
	a     *API
	st    *store.Store
	users map[string]auth.Session
}

// newTokenFixture signs in an administrator, a viewer (read only, no
// manage-mcp), and a "builder" holding manage-mcp plus read only.
func newTokenFixture(t *testing.T) *tokenFixture {
	t.Helper()
	st, e := store.New(context.Background(), "")
	if e != nil {
		t.Fatal(e)
	}
	f := &tokenFixture{t: t, st: st, users: map[string]auth.Session{}}
	for name, roles := range map[string][]string{"admin": {"administrator"}, "viewer": {"viewer"}, "builder": {"builder"}, "builder2": {"builder"}} {
		s := auth.NewSession(auth.User{ID: name, Username: name, Roles: roles, Provider: "local"})
		if e := st.SaveSession(context.Background(), s); e != nil {
			t.Fatal(e)
		}
		f.users[name] = s
	}
	f.a = New(Options{Store: st, Clusters: []model.Cluster{{ID: "c1"}, {ID: "c2"}}, Grants: []auth.Grant{
		{Role: "administrator", Cluster: "*", Action: "*", Pattern: "*"},
		{Role: "viewer", Cluster: "*", Action: "read", Pattern: "*"},
		{Role: "builder", Cluster: "*", Action: "manage-mcp", Pattern: "*"},
		{Role: "builder", Cluster: "*", Action: "read", Pattern: "*"},
	}})
	return f
}

func (f *tokenFixture) call(user, method, path, body string) *httptest.ResponseRecorder {
	f.t.Helper()
	s := f.users[user]
	return f.raw(user, method, path, body, s.CSRF)
}

func (f *tokenFixture) raw(user, method, path, body, csrf string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.RemoteAddr = "192.0.2.1:1234"
	if s, ok := f.users[user]; ok {
		r.AddCookie(&http.Cookie{Name: "kaflux_session", Value: s.ID})
	}
	if csrf != "" {
		r.Header.Set("X-CSRF-Token", csrf)
	}
	w := httptest.NewRecorder()
	f.a.ServeHTTP(w, r)
	return w
}

type tokenView struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	OwnerID       string   `json:"ownerId"`
	OwnerProvider string   `json:"ownerProvider"`
	Scopes        []string `json:"scopes"`
	Clusters      []string `json:"clusters"`
	Created       string   `json:"created"`
	Expires       string   `json:"expires"`
	LastUsed      *string  `json:"lastUsed"`
	Revoked       *string  `json:"revoked"`
	Status        string   `json:"status"`
	Secret        string   `json:"secret"`
}

const tokensPath = "/api/v1/admin/mcp-tokens"

func (f *tokenFixture) create(user, body string) (int, tokenView, string) {
	f.t.Helper()
	w := f.call(user, "POST", tokensPath, body)
	var env struct{ Data tokenView }
	_ = json.Unmarshal(w.Body.Bytes(), &env)
	return w.Code, env.Data, w.Body.String()
}

type tokenList struct {
	Data []tokenView
	Meta struct {
		Enabled       bool `json:"enabled"`
		WritesEnabled bool `json:"writesEnabled"`
	}
}

func (f *tokenFixture) list(user string) tokenList {
	f.t.Helper()
	w := f.call(user, "GET", tokensPath, "")
	if w.Code != 200 {
		f.t.Fatalf("list %d %s", w.Code, w.Body.String())
	}
	var l tokenList
	if e := json.Unmarshal(w.Body.Bytes(), &l); e != nil {
		f.t.Fatal(e)
	}
	return l
}

func errCode(body string) string {
	var env struct {
		Error struct{ Code, Message string }
	}
	_ = json.Unmarshal([]byte(body), &env)
	return env.Error.Code
}

func TestCreateTokenReturnsSecretOnce(t *testing.T) {
	f := newTokenFixture(t)
	code, tok, body := f.create("admin", `{"name":"ci agent","scopes":["read","messages"],"expiresInDays":30}`)
	if code != 201 {
		t.Fatalf("create %d %s", code, body)
	}
	if !strings.HasPrefix(tok.Secret, "kfx_") || tok.ID == "" || tok.Status != "active" || tok.OwnerID != "admin" || tok.OwnerProvider != "local" {
		t.Fatalf("created %+v", tok)
	}
	exp, e := time.Parse(time.RFC3339Nano, tok.Expires)
	if e != nil || exp.Sub(time.Now().Add(30*24*time.Hour)).Abs() > time.Minute {
		t.Fatalf("expires %q %v", tok.Expires, e)
	}
	if strings.Contains(body, store.HashMCPSecret(tok.Secret)) || strings.Contains(body, `"hash"`) {
		t.Fatal("create response exposes the hash")
	}
	got, e := f.st.MCPTokenByHash(context.Background(), store.HashMCPSecret(tok.Secret))
	if e != nil || got.ID != tok.ID {
		t.Fatalf("secret does not authenticate: %v", e)
	}
	w := f.call("admin", "GET", tokensPath, "")
	if strings.Contains(w.Body.String(), tok.Secret) || strings.Contains(w.Body.String(), store.HashMCPSecret(tok.Secret)) || strings.Contains(w.Body.String(), `"secret"`) || strings.Contains(w.Body.String(), `"hash"`) {
		t.Fatalf("list exposes secret material: %s", w.Body.String())
	}
	l := f.list("admin")
	if len(l.Data) != 1 || l.Data[0].ID != tok.ID || !slices.Equal(l.Data[0].Scopes, []string{"read", "messages"}) || l.Data[0].Clusters == nil {
		t.Fatalf("list %+v", l.Data)
	}
	if l.Meta.Enabled || l.Meta.WritesEnabled {
		t.Fatalf("MCP reported enabled by default: %+v", l.Meta)
	}
	f.a.o.MCPEnabled, f.a.o.MCPWritesEnabled = true, true
	if l = f.list("admin"); !l.Meta.Enabled || !l.Meta.WritesEnabled {
		t.Fatalf("enabled flags not reported: %+v", l.Meta)
	}
}

func TestCreateRequiresExpiry(t *testing.T) {
	f := newTokenFixture(t)
	for _, body := range []string{
		`{"name":"x","scopes":["read"]}`,
		`{"name":"x","scopes":["read"],"expiresInDays":null}`,
		`{"name":"x","scopes":["read"],"expiresInDays":"30"}`,
		`{"name":"x","scopes":["read"],"expiresInDays":7.5}`,
		`{"name":"x","scopes":["read"],"expiresInDays":0}`,
	} {
		if code, _, b := f.create("admin", body); code != 400 {
			t.Fatalf("%s: %d %s", body, code, b)
		}
	}
	if len(f.list("admin").Data) != 0 {
		t.Fatal("token stored without valid expiry")
	}
}

func TestExpiryMax90Days(t *testing.T) {
	f := newTokenFixture(t)
	if code, _, b := f.create("admin", `{"name":"x","scopes":["read"],"expiresInDays":91}`); code != 400 || errCode(b) != "invalid_expiry" {
		t.Fatalf("91 days: %d %s", code, b)
	}
	if code, _, b := f.create("admin", `{"name":"x","scopes":["read"],"expiresInDays":90}`); code != 201 {
		t.Fatalf("90 days: %d %s", code, b)
	}
	if code, _, b := f.create("admin", `{"name":"y","scopes":["read"],"expiresInDays":1}`); code != 201 {
		t.Fatalf("1 day: %d %s", code, b)
	}
}

func TestScopesValidated(t *testing.T) {
	f := newTokenFixture(t)
	code, _, b := f.create("admin", `{"name":"x","scopes":["read","admin"],"expiresInDays":30}`)
	if code != 400 || errCode(b) != "invalid_scope" || !strings.Contains(b, "read, messages, write, destructive") {
		t.Fatalf("unknown scope: %d %s", code, b)
	}
	if code, _, b = f.create("admin", `{"name":"x","scopes":[],"expiresInDays":30}`); code != 400 {
		t.Fatalf("empty scopes: %d %s", code, b)
	}
	if code, _, b = f.create("admin", `{"name":"x","expiresInDays":30}`); code != 400 {
		t.Fatalf("missing scopes: %d %s", code, b)
	}
	// Write scopes are accepted while writes are disabled; the list reports it.
	code, tok, b := f.create("admin", `{"name":"x","scopes":["destructive","read","write","read"],"expiresInDays":30}`)
	if code != 201 || !slices.Equal(tok.Scopes, []string{"read", "write", "destructive"}) {
		t.Fatalf("write scopes: %d %s", code, b)
	}
	if f.list("admin").Meta.WritesEnabled {
		t.Fatal("writesEnabled true by default")
	}
}

func TestScopeNotPermittedForCreator(t *testing.T) {
	f := newTokenFixture(t)
	for _, scope := range []string{"messages", "write", "destructive"} {
		code, _, b := f.create("builder", fmt.Sprintf(`{"name":"x","scopes":["read",%q],"expiresInDays":30}`, scope))
		if code != 403 || errCode(b) != "scope_not_permitted" {
			t.Fatalf("%s: %d %s", scope, code, b)
		}
	}
	if code, _, b := f.create("builder", `{"name":"x","scopes":["read"],"expiresInDays":30}`); code != 201 {
		t.Fatalf("read: %d %s", code, b)
	}
}

func TestCreateValidatesName(t *testing.T) {
	f := newTokenFixture(t)
	for _, name := range []string{"", "   ", "a/b", "tab\there", strings.Repeat("n", 65)} {
		body, _ := json.Marshal(map[string]any{"name": name, "scopes": []string{"read"}, "expiresInDays": 30})
		if code, _, b := f.create("admin", string(body)); code != 400 || errCode(b) != "invalid_name" {
			t.Fatalf("name %q: %d %s", name, code, b)
		}
	}
	body, _ := json.Marshal(map[string]any{"name": strings.Repeat("n", 64), "scopes": []string{"read"}, "expiresInDays": 30})
	if code, _, b := f.create("admin", string(body)); code != 201 {
		t.Fatalf("64 chars: %d %s", code, b)
	}
}

func TestCreateValidatesClusters(t *testing.T) {
	f := newTokenFixture(t)
	for _, clusters := range []string{`["c1","nope"]`, `["*"]`} {
		code, _, b := f.create("admin", `{"name":"x","scopes":["read"],"clusters":`+clusters+`,"expiresInDays":30}`)
		if code != 400 || errCode(b) != "invalid_cluster" {
			t.Fatalf("%s: %d %s", clusters, code, b)
		}
	}
	code, tok, b := f.create("admin", `{"name":"x","scopes":["read"],"clusters":["c2","c2"],"expiresInDays":30}`)
	if code != 201 || !slices.Equal(tok.Clusters, []string{"c2"}) {
		t.Fatalf("known cluster: %d %s", code, b)
	}
}

func TestCreateLimitsActiveTokens(t *testing.T) {
	f := newTokenFixture(t)
	var first string
	for i := 0; i < 25; i++ {
		code, tok, b := f.create("builder", fmt.Sprintf(`{"name":"t%d","scopes":["read"],"expiresInDays":30}`, i))
		if code != 201 {
			t.Fatalf("token %d: %d %s", i, code, b)
		}
		if i == 0 {
			first = tok.ID
		}
	}
	if code, _, b := f.create("builder", `{"name":"t25","scopes":["read"],"expiresInDays":30}`); code != 409 || errCode(b) != "token_limit" {
		t.Fatalf("26th: %d %s", code, b)
	}
	// Another owner is unaffected; revoking frees a slot.
	if code, _, b := f.create("builder2", `{"name":"t0","scopes":["read"],"expiresInDays":30}`); code != 201 {
		t.Fatalf("other owner: %d %s", code, b)
	}
	if w := f.call("builder", "DELETE", tokensPath+"/"+first, ""); w.Code != 200 {
		t.Fatalf("revoke %d %s", w.Code, w.Body.String())
	}
	if code, _, b := f.create("builder", `{"name":"t25","scopes":["read"],"expiresInDays":30}`); code != 201 {
		t.Fatalf("after revoke: %d %s", code, b)
	}
}

func TestCreateSnapshotsOwnerRoles(t *testing.T) {
	f := newTokenFixture(t)
	code, tok, b := f.create("builder", `{"name":"x","scopes":["read"],"expiresInDays":30}`)
	if code != 201 {
		t.Fatalf("%d %s", code, b)
	}
	got, e := f.st.MCPTokenByHash(context.Background(), store.HashMCPSecret(tok.Secret))
	if e != nil || !slices.Equal(got.OwnerRoles, []string{"builder"}) || got.OwnerID != "builder" || got.OwnerProvider != "local" {
		t.Fatalf("stored %+v %v", got, e)
	}
}

func TestUserSeesOwnTokensAdminSeesAll(t *testing.T) {
	f := newTokenFixture(t)
	// A viewer's token is seeded directly: viewers cannot create one.
	_, hash := store.NewMCPSecret()
	now := time.Now().UTC().Truncate(time.Millisecond)
	if e := f.st.CreateMCPToken(context.Background(), store.MCPToken{ID: "viewertok", Name: "mine", OwnerID: "viewer", OwnerProvider: "local", OwnerRoles: []string{"viewer"}, Scopes: []string{"read"}, Expires: now.Add(-time.Hour), Created: now.Add(-2 * time.Hour)}, hash); e != nil {
		t.Fatal(e)
	}
	for _, u := range []string{"admin", "builder"} {
		if code, _, b := f.create(u, `{"name":"x","scopes":["read"],"expiresInDays":30}`); code != 201 {
			t.Fatalf("%s: %d %s", u, code, b)
		}
	}
	owners := func(l tokenList) []string {
		out := []string{}
		for _, v := range l.Data {
			out = append(out, v.OwnerID)
		}
		slices.Sort(out)
		return out
	}
	if got := owners(f.list("viewer")); !slices.Equal(got, []string{"viewer"}) {
		t.Fatalf("viewer sees %v", got)
	}
	if l := f.list("viewer"); l.Data[0].Status != "expired" {
		t.Fatalf("status %q", l.Data[0].Status)
	}
	// builder holds manage-mcp, so sees every token.
	if got := owners(f.list("builder")); !slices.Equal(got, []string{"admin", "builder", "viewer"}) {
		t.Fatalf("builder sees %v", got)
	}
	if got := owners(f.list("admin")); !slices.Equal(got, []string{"admin", "builder", "viewer"}) {
		t.Fatalf("admin sees %v", got)
	}
	if w := f.raw("", "GET", tokensPath, "", ""); w.Code != 401 {
		t.Fatalf("anonymous list %d", w.Code)
	}
}

func TestRevokeOwnOrAdmin(t *testing.T) {
	f := newTokenFixture(t)
	_, adminTok, _ := f.create("admin", `{"name":"a","scopes":["read"],"expiresInDays":30}`)
	_, b1, _ := f.create("builder", `{"name":"b1","scopes":["read"],"expiresInDays":30}`)
	_, b2, _ := f.create("builder", `{"name":"b2","scopes":["read"],"expiresInDays":30}`)
	_, hash := store.NewMCPSecret()
	now := time.Now().UTC().Truncate(time.Millisecond)
	if e := f.st.CreateMCPToken(context.Background(), store.MCPToken{ID: "viewertok", Name: "v", OwnerID: "viewer", OwnerProvider: "local", Scopes: []string{"read"}, Expires: now.Add(time.Hour), Created: now}, hash); e != nil {
		t.Fatal(e)
	}
	// A viewer without manage-mcp cannot see or revoke others' tokens: 404, as for unknown ids.
	for _, id := range []string{adminTok.ID, "doesnotexist", "bad%2Fid"} {
		if w := f.call("viewer", "DELETE", tokensPath+"/"+id, ""); w.Code != 404 {
			t.Fatalf("viewer revoke %s: %d %s", id, w.Code, w.Body.String())
		}
	}
	// Own token, twice: idempotent.
	for i := 0; i < 2; i++ {
		if w := f.call("viewer", "DELETE", tokensPath+"/viewertok", ""); w.Code != 200 {
			t.Fatalf("own revoke %d: %d %s", i, w.Code, w.Body.String())
		}
	}
	if w := f.call("builder", "DELETE", tokensPath+"/"+b1.ID, ""); w.Code != 200 {
		t.Fatalf("builder own revoke %d", w.Code)
	}
	// manage-mcp revokes any token.
	if w := f.call("admin", "DELETE", tokensPath+"/"+b2.ID, ""); w.Code != 200 {
		t.Fatalf("admin revoke %d %s", w.Code, w.Body.String())
	}
	if w := f.call("admin", "DELETE", tokensPath+"/doesnotexist", ""); w.Code != 404 {
		t.Fatalf("admin unknown %d", w.Code)
	}
	status := map[string]string{}
	for _, v := range f.list("admin").Data {
		status[v.ID] = v.Status
		if v.Status == "revoked" && v.Revoked == nil {
			t.Fatalf("revoked without time %+v", v)
		}
	}
	if status[adminTok.ID] != "active" || status[b1.ID] != "revoked" || status[b2.ID] != "revoked" || status["viewertok"] != "revoked" {
		t.Fatalf("status %v", status)
	}
	if w := f.call("admin", "PUT", tokensPath+"/"+adminTok.ID, "{}"); w.Code != 405 {
		t.Fatalf("PUT %d", w.Code)
	}
	if w := f.call("admin", "DELETE", tokensPath+"/"+adminTok.ID+"/x", ""); w.Code != 404 {
		t.Fatalf("nested %d", w.Code)
	}
}

func TestCannotCreateTokenWithoutPermission(t *testing.T) {
	f := newTokenFixture(t)
	code, _, b := f.create("viewer", `{"name":"x","scopes":["read"],"expiresInDays":30}`)
	if code != 403 || errCode(b) != "forbidden" {
		t.Fatalf("viewer create %d %s", code, b)
	}
	if len(f.list("admin").Data) != 0 {
		t.Fatal("token created without permission")
	}
}

func TestTokenCannotCreateToken(t *testing.T) {
	f := newTokenFixture(t)
	code, tok, _ := f.create("admin", `{"name":"x","scopes":["read","messages","write","destructive"],"expiresInDays":30}`)
	if code != 201 {
		t.Fatal(code)
	}
	// A bearer token on the REST route is never a credential.
	for _, method := range []string{"GET", "POST", "DELETE"} {
		path := tokensPath
		if method == "DELETE" {
			path += "/" + tok.ID
		}
		r := httptest.NewRequest(method, path, strings.NewReader(`{"name":"y","scopes":["read"],"expiresInDays":30}`))
		r.Header.Set("Authorization", "Bearer "+tok.Secret)
		w := httptest.NewRecorder()
		f.a.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatalf("bearer %s = %d", method, w.Code)
		}
	}
	// Even an MCP session acting as an administrator with every grant is refused.
	s := auth.Session{ID: "mcp:" + tok.ID, User: auth.User{ID: "admin", Roles: []string{"administrator"}, Provider: "local"}, Expires: time.Now().Add(time.Hour)}
	grants := []auth.Grant{{Role: "administrator", Cluster: "*", Action: "*", Pattern: "*"}}
	for _, method := range []string{"GET", "POST", "DELETE"} {
		path := tokensPath
		if method == "DELETE" {
			path += "/" + tok.ID
		}
		r := httptest.NewRequest(method, path, strings.NewReader(`{"name":"y","scopes":["read"],"expiresInDays":30}`))
		w := httptest.NewRecorder()
		f.a.ServeHTTP(w, WithMCPSession(r, s, grants))
		if w.Code != 403 || errCode(w.Body.String()) != "forbidden" {
			t.Fatalf("MCP session %s = %d %s", method, w.Code, w.Body.String())
		}
	}
	l := f.list("admin")
	if len(l.Data) != 1 || l.Data[0].Status != "active" {
		t.Fatalf("MCP session changed tokens: %+v", l.Data)
	}
}

func TestCreateAuditsAndCSRF(t *testing.T) {
	f := newTokenFixture(t)
	body := `{"name":"ci","scopes":["read"],"expiresInDays":30}`
	if w := f.raw("admin", "POST", tokensPath, body, ""); w.Code != 403 || errCode(w.Body.String()) != "csrf_failed" {
		t.Fatalf("no CSRF %d %s", w.Code, w.Body.String())
	}
	if w := f.raw("admin", "POST", tokensPath, body, "wrong"); w.Code != 403 {
		t.Fatalf("bad CSRF %d", w.Code)
	}
	code, tok, b := f.create("admin", body)
	if code != 201 {
		t.Fatalf("%d %s", code, b)
	}
	if w := f.raw("admin", "DELETE", tokensPath+"/"+tok.ID, "", ""); w.Code != 403 {
		t.Fatalf("revoke without CSRF %d", w.Code)
	}
	if w := f.call("admin", "DELETE", tokensPath+"/"+tok.ID, ""); w.Code != 200 {
		t.Fatalf("revoke %d", w.Code)
	}
	audits, _ := f.st.Audits(context.Background())
	var seen []string
	for _, a := range audits {
		raw, _ := json.Marshal(a)
		if strings.Contains(string(raw), tok.Secret) || strings.Contains(string(raw), store.HashMCPSecret(tok.Secret)) {
			t.Fatal("audit contains secret material")
		}
		if strings.HasPrefix(a.Action, "mcp.token.") {
			if a.Actor != "admin" || a.Provider != "local" || a.ClusterID != "*" || !strings.Contains(a.Resource, tok.ID) || !strings.Contains(a.Resource, "ci") {
				t.Fatalf("audit %+v", a)
			}
			seen = append(seen, a.Action+":"+a.Result)
		}
	}
	slices.Sort(seen)
	want := []string{"mcp.token.create:intent", "mcp.token.create:success", "mcp.token.revoke:intent", "mcp.token.revoke:success"}
	if !slices.Equal(seen, want) {
		t.Fatalf("audits %v", seen)
	}
}

func TestCreateBlockedWhenAuditDown(t *testing.T) {
	f := newTokenFixture(t)
	broken, e := store.NewSQLite(context.Background(), filepath.Join(t.TempDir(), "audit.db"))
	if e != nil {
		t.Fatal(e)
	}
	broken.Close()
	// Demo mode authenticates without the store, so the request reaches the
	// handler and every later store call, the audit write included, fails.
	f.a.o.Demo, f.a.o.Store = true, broken
	r := httptest.NewRequest("POST", tokensPath, strings.NewReader(`{"name":"x","scopes":["read"],"expiresInDays":30}`))
	r.Header.Set("X-CSRF-Token", f.a.demo.CSRF)
	w := httptest.NewRecorder()
	f.a.ServeHTTP(w, r)
	if w.Code != 503 || strings.Contains(w.Body.String(), "kfx_") {
		t.Fatalf("got %d %s", w.Code, w.Body.String())
	}
}
