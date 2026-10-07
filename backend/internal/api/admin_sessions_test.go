package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAdminSessionInventoryAndRevocation(t *testing.T) {
	st, _ := store.New(context.Background(), "")
	admin := auth.NewSession(auth.User{ID: "admin", Roles: []string{"administrator"}, Provider: "local"})
	target := auth.NewSession(auth.User{ID: "other", Roles: []string{"viewer"}, Provider: "oidc"})
	expired := auth.NewSession(auth.User{ID: "expired"})
	expired.Expires = time.Now().Add(-time.Hour)
	for _, s := range []auth.Session{admin, target, expired} {
		if e := st.SaveSession(context.Background(), s); e != nil {
			t.Fatal(e)
		}
	}
	a := New(Options{Store: st})
	call := func(method, path, body, csrf string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.AddCookie(&http.Cookie{Name: "kaflux_session", Value: admin.ID})
		r.Header.Set("X-CSRF-Token", csrf)
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		return w
	}
	w := call("GET", "/api/v1/admin/sessions?limit=1", "", "")
	var env struct {
		Data []struct {
			Handle, UserID string
			Current        bool
		}
		Meta struct{ HasMore bool }
	}
	if w.Code != 200 {
		t.Fatalf("inventory %d %s", w.Code, w.Body.String())
	}
	if e := json.Unmarshal(w.Body.Bytes(), &env); e != nil {
		t.Fatal(e)
	}
	if len(env.Data) != 1 || !env.Meta.HasMore {
		t.Fatalf("pagination %s", w.Body.String())
	}
	for _, secret := range []string{admin.ID, target.ID, admin.CSRF, target.CSRF, "expired"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("inventory exposed secret or expired session")
		}
	}
	handle := fmt.Sprintf("%x", sha256.Sum256([]byte(target.ID)))
	for _, tc := range []struct {
		body, csrf string
		status     int
	}{{`{"confirm":true}`, "", 403}, {`{"confirm":false}`, admin.CSRF, 400}, {`{"confirm":true}`, admin.CSRF, 200}} {
		w = call("POST", "/api/v1/admin/sessions/"+handle+"/revoke", tc.body, tc.csrf)
		if w.Code != tc.status {
			t.Fatalf("revoke %d %s", w.Code, w.Body.String())
		}
	}
	r := httptest.NewRequest("GET", "/api/v1/auth/session", nil)
	r.AddCookie(&http.Cookie{Name: "kaflux_session", Value: target.ID})
	w = httptest.NewRecorder()
	a.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("revoked session authenticated")
	}
	audits, _ := st.Audits(context.Background())
	if len(audits) != 2 {
		t.Fatalf("audit count %d", len(audits))
	}
	b, _ := json.Marshal(audits)
	if strings.Contains(string(b), target.ID) || strings.Contains(string(b), target.CSRF) {
		t.Fatal("audit leaked tokens")
	}
	for _, q := range []string{"limit=0", "limit=101", "offset=-1", "offset=10001", "limit=x"} {
		if w := call("GET", "/api/v1/admin/sessions?"+q, "", ""); w.Code != 400 {
			t.Fatalf("bounds %s %d", q, w.Code)
		}
	}
	handle = fmt.Sprintf("%x", sha256.Sum256([]byte(admin.ID)))
	w = call("POST", "/api/v1/admin/sessions/"+handle+"/revoke", `{"confirm":true}`, admin.CSRF)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"current":true`) {
		t.Fatalf("self revoke %s", w.Body.String())
	}
	if w = call("GET", "/api/v1/auth/session", "", ""); w.Code != 401 {
		t.Fatal("self revocation failed")
	}
}
func TestAdminSessionGlobalPermissionsAndDemo(t *testing.T) {
	for _, grant := range []auth.Grant{{Role: "administrator", Cluster: "demo", Action: "*", Pattern: "*"}, {Role: "administrator", Cluster: "*", Action: "read", Pattern: "*"}, {Role: "administrator", Cluster: "*", Action: "manage-sessions", Pattern: "other"}} {
		st, _ := store.New(context.Background(), "")
		a := New(Options{Demo: true, Store: st, Grants: []auth.Grant{grant}})
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/admin/sessions", nil))
		if w.Code != 403 {
			t.Fatalf("grant leaked inventory %+v status %d", grant, w.Code)
		}
	}
	st, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: st})
	r := httptest.NewRequest("POST", "/api/v1/admin/sessions/"+strings.Repeat("a", 64)+"/revoke", strings.NewReader(`{"confirm":true}`))
	r.Header.Set("X-CSRF-Token", a.demo.CSRF)
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)
	if w.Code != 422 {
		t.Fatalf("demo revoke %d", w.Code)
	}
}

func TestSessionAdministrationCapability(t *testing.T) {
	for _, tc := range []struct {
		grant   auth.Grant
		allowed bool
	}{{auth.Grant{Role: "custom", Cluster: "*", Action: "manage-sessions", Pattern: "*"}, true}, {auth.Grant{Role: "custom", Cluster: "demo", Action: "*", Pattern: "*"}, false}} {
		st, _ := store.New(context.Background(), "")
		a := New(Options{Store: st, Demo: true, Grants: []auth.Grant{tc.grant}})
		a.demo.User.Roles = []string{"custom"}
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/auth/session", nil))
		var env struct {
			Data struct{ CanManageSessions bool }
		}
		if e := json.Unmarshal(w.Body.Bytes(), &env); e != nil {
			t.Fatal(e)
		}
		if env.Data.CanManageSessions != tc.allowed {
			t.Fatal("incorrect global session capability")
		}
	}
}

func TestAdminSessionTargetAuthorization(t *testing.T) {
	st, _ := store.New(context.Background(), "")
	actor := auth.NewSession(auth.User{ID: "actor", Roles: []string{"restricted"}})
	target := auth.NewSession(auth.User{ID: "private"})
	for _, s := range []auth.Session{actor, target} {
		if e := st.SaveSession(context.Background(), s); e != nil {
			t.Fatal(e)
		}
	}
	a := New(Options{Store: st, Grants: []auth.Grant{{Role: "restricted", Cluster: "*", Action: "manage-sessions", Pattern: "permitted"}}})
	r := httptest.NewRequest("POST", "/api/v1/admin/sessions/"+fmt.Sprintf("%x", sha256.Sum256([]byte(target.ID)))+"/revoke", strings.NewReader(`{"confirm":true}`))
	r.AddCookie(&http.Cookie{Name: "kaflux_session", Value: actor.ID})
	r.Header.Set("X-CSRF-Token", actor.CSRF)
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("target permission %d %s", w.Code, w.Body.String())
	}
	if _, e := st.Session(context.Background(), target.ID); e != nil {
		t.Fatal("forbidden revoke removed target")
	}
}

// TestRepeatedLoginsListEachSessionSeparatelyWithoutRevokingPriorOnes documents the
// root cause behind "the same user appears repeated many times" on the Active
// sessions page: every successful login creates a new, independently valid
// session row, and nothing revokes a user's earlier sessions when a later one
// is established. That is existing, intended multi-device behavior, not a bug,
// so the inventory is expected to return one entry per login, all carrying the
// same userId, until each is individually revoked or naturally expires. The fix
// for the reported symptom lives in the admin UI (grouping by user), not here;
// this test fixes the server-side contract that UI depends on: distinct
// handles per session, a stable userId to group by, and expired sessions never
// mixed into an otherwise-live user's result set.
func TestRepeatedLoginsListEachSessionSeparatelyWithoutRevokingPriorOnes(t *testing.T) {
	st, _ := store.New(context.Background(), "")
	admin := auth.NewSession(auth.User{ID: "admin", Roles: []string{"administrator"}, Provider: "local"})
	if e := st.SaveSession(context.Background(), admin); e != nil {
		t.Fatal(e)
	}
	a := New(Options{Store: st})
	call := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/api/v1/admin/sessions?limit=100", nil)
		r.AddCookie(&http.Cookie{Name: "kaflux_session", Value: admin.ID})
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		return w
	}
	type repeatedLoginUser struct {
		userID   string
		sessions []auth.Session
	}
	alice := repeatedLoginUser{userID: "alice"}
	for i := 0; i < 3; i++ {
		s := auth.NewSession(auth.User{ID: alice.userID, Roles: []string{"viewer"}, Provider: "local"})
		if e := st.SaveSession(context.Background(), s); e != nil {
			t.Fatal(e)
		}
		alice.sessions = append(alice.sessions, s)
	}
	expired := auth.NewSession(auth.User{ID: alice.userID, Roles: []string{"viewer"}, Provider: "local"})
	expired.Expires = time.Now().Add(-time.Minute)
	if e := st.SaveSession(context.Background(), expired); e != nil {
		t.Fatal(e)
	}
	w := call()
	if w.Code != 200 {
		t.Fatalf("inventory %d %s", w.Code, w.Body.String())
	}
	var env struct {
		Data []struct {
			Handle, UserID string
			Current        bool
		}
	}
	if e := json.Unmarshal(w.Body.Bytes(), &env); e != nil {
		t.Fatal(e)
	}
	aliceRows := map[string]bool{}
	for _, row := range env.Data {
		if row.UserID == alice.userID {
			aliceRows[row.Handle] = true
		}
		if row.Handle == fmt.Sprintf("%x", sha256.Sum256([]byte(expired.ID))) {
			t.Fatal("expired session listed alongside the user's live sessions")
		}
	}
	if len(aliceRows) != len(alice.sessions) {
		t.Fatalf("expected one inventory row per login (%d), got %d: repeated logins were deduplicated or merged unexpectedly", len(alice.sessions), len(aliceRows))
	}
	for _, s := range alice.sessions {
		handle := fmt.Sprintf("%x", sha256.Sum256([]byte(s.ID)))
		if !aliceRows[handle] {
			t.Fatalf("session %s missing from inventory: an earlier login was unexpectedly revoked by a later one", handle)
		}
		if _, e := st.Session(context.Background(), s.ID); e != nil {
			t.Fatalf("session %s no longer valid: a later login revoked an earlier one for the same user", handle)
		}
	}
}
