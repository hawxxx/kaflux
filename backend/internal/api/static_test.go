package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const shell = "<!doctype html><title>Kaflux</title><div id=\"root\"></div>"

func staticAPI(t *testing.T) *API {
	t.Helper()
	a := securityAPI(t)
	a.o.StaticDir = t.TempDir()
	files := map[string]string{
		"index.html":           shell,
		"favicon.svg":          "<svg xmlns=\"http://www.w3.org/2000/svg\"/>",
		"assets/app-1a2b3c.js": "console.log('app')",
	}
	for name, body := range files {
		target := filepath.Join(a.o.StaticDir, name)
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return a
}

// A refresh on a page URL must return the application shell. Topic names,
// consumer group ids and cluster ids contain dots, which must not make the
// server treat the page as a missing file.
func TestClientRoutesServeTheApplicationShellOnRefresh(t *testing.T) {
	a := staticAPI(t)
	for _, path := range []string{
		"/",
		"/login",
		"/clusters",
		"/clusters/cluster-a/overview",
		"/clusters/cluster-a/topics",
		"/clusters/cluster-a/topics/orders",
		"/clusters/cluster-a/topics/orders.payment.captured",
		"/clusters/cluster-a/topics/a.b.c.d",
		"/clusters/cluster-a/topics/events.json",
		"/clusters/cluster-a/topics/some.name.js",
		"/clusters/cluster-a/consumer-groups/billing.events_consumer",
		"/clusters/cluster.a/topics",
		"/clusters/cluster.a/topics/orders.v2",
		"/clusters/cluster-a/topics/orders.v2/extra/segments",
	} {
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", "https://example.com"+path, nil))
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `<div id="root">`) {
			t.Errorf("%s: status %d, body %q; want the application shell", path, w.Code, w.Body.String())
		}
	}
}

func TestFilesAreServedAndMissingFilesStayNotFound(t *testing.T) {
	a := staticAPI(t)
	for _, tc := range []struct {
		path   string
		status int
		body   string
	}{
		{"/assets/app-1a2b3c.js", 200, "console.log('app')"},
		{"/favicon.svg", 200, "<svg"},
		{"/assets/missing-9z9z9z.js", 404, ""},
		{"/favicon.ico", 404, ""},
		{"/robots.txt", 404, ""},
		// The prefix must match whole segments: these are not under /clusters.
		{"/clustersfoo.bar", 404, ""},
		{"/clusters.json", 404, ""},
	} {
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", "https://example.com"+tc.path, nil))
		if w.Code != tc.status {
			t.Errorf("%s: status %d, want %d", tc.path, w.Code, tc.status)
		}
		if tc.body != "" && !strings.Contains(w.Body.String(), tc.body) {
			t.Errorf("%s: body %q does not contain %q", tc.path, w.Body.String(), tc.body)
		}
		if tc.status == 404 && strings.Contains(w.Body.String(), `<div id="root">`) {
			t.Errorf("%s: a missing file must not return the application shell", tc.path)
		}
	}
}

func TestAPIPathsNeverFallBackToTheShell(t *testing.T) {
	a := staticAPI(t)
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "https://example.com/api/v1/clusters/cluster-a/topics/a.b", nil))
	if strings.Contains(w.Body.String(), `<div id="root">`) {
		t.Fatalf("API paths must return API errors, not the application shell: %d", w.Code)
	}
}

func TestIsClientRoute(t *testing.T) {
	for path, want := range map[string]bool{
		"/":                               true,
		"/login":                          true,
		"/clusters":                       true,
		"/clusters/x/topics/a.b":          true,
		"/clusters/cluster.b":             true,
		"/clustersfoo":                    true, // no dot: the generic rule applies
		"/clustersfoo.bar":                false,
		"/assets/app.js":                  false,
		"/favicon.ico":                    false,
		"/some/dir/file.name":             false,
		"/clusters.json":                  false,
		"/not-clusters/x/topics/orders.v": false,
	} {
		if got := isClientRoute(path); got != want {
			t.Errorf("isClientRoute(%q) = %v, want %v", path, got, want)
		}
	}
}
