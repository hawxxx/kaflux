package httpgzip

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func jsonHandler(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	})
}

func get(h http.Handler, path string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func gunzip(t *testing.T, b []byte) string {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("not a gzip stream: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	return string(out)
}

func TestCompressesJSONForClientsThatAcceptGzip(t *testing.T) {
	body := `{"data":[` + strings.Repeat(`{"id":"group","state":"Stable","topics":["a","b","c"]},`, 400) + `{}]}`
	rec := get(Handler(jsonHandler(body)), "/api/v1/clusters/x/consumer-groups", map[string]string{"Accept-Encoding": "gzip, deflate"})
	if rec.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("content-encoding = %q", rec.Header().Get("Content-Encoding"))
	}
	if rec.Header().Get("Content-Length") != "" {
		t.Fatalf("content-length must not be set on a compressed response: %q", rec.Header().Get("Content-Length"))
	}
	if got := gunzip(t, rec.Body.Bytes()); got != body {
		t.Fatal("decompressed body differs from the original")
	}
	if rec.Body.Len() > len(body)/10 {
		t.Fatalf("expected at least 10x smaller, got %d from %d", rec.Body.Len(), len(body))
	}
}

func TestVaryIsSetWhetherOrNotTheResponseIsCompressed(t *testing.T) {
	h := Handler(jsonHandler(`{"data":[]}`))
	for name, headers := range map[string]map[string]string{"accepts": {"Accept-Encoding": "gzip"}, "does not accept": nil} {
		if v := get(h, "/api/v1/clusters", headers).Header().Values("Vary"); strings.Join(v, ",") != "Accept-Encoding" {
			t.Fatalf("%s: vary = %v", name, v)
		}
	}
}

func TestLeavesBodyAloneWhenGzipIsNotAcceptedOrIsRefused(t *testing.T) {
	h := Handler(jsonHandler(`{"data":[]}`))
	for _, value := range []string{"", "identity", "deflate, br", "gzip;q=0", "gzip; q=0.0, br"} {
		rec := get(h, "/api/v1/clusters", map[string]string{"Accept-Encoding": value})
		if rec.Header().Get("Content-Encoding") != "" || rec.Body.String() != `{"data":[]}` {
			t.Fatalf("Accept-Encoding %q: encoding=%q body=%q", value, rec.Header().Get("Content-Encoding"), rec.Body.String())
		}
	}
}

func TestAcceptsGzipParsing(t *testing.T) {
	for value, want := range map[string]bool{"gzip": true, "GZIP": true, "br, gzip": true, "gzip;q=0.8": true, "deflate, gzip ; q=1": true, "gzip;q=0": false, "identity": false, "": false, "gzipped": false} {
		if got := acceptsGzip(value); got != want {
			t.Errorf("acceptsGzip(%q) = %v, want %v", value, got, want)
		}
	}
}

func TestSessionResponsesCarryingTheCSRFTokenAreNeverCompressed(t *testing.T) {
	h := Handler(jsonHandler(`{"data":{"csrfToken":"secret"}}`))
	rec := get(h, "/api/v1/auth/session", map[string]string{"Accept-Encoding": "gzip"})
	if rec.Header().Get("Content-Encoding") != "" || !strings.Contains(rec.Body.String(), "csrfToken") {
		t.Fatalf("auth response was compressed: %q", rec.Header().Get("Content-Encoding"))
	}
}

func TestOnlyGETRequestsUnderTheAPIPrefixAreConsidered(t *testing.T) {
	h := Handler(jsonHandler(`{"a":1}`))
	if rec := get(h, "/assets/index.js", map[string]string{"Accept-Encoding": "gzip"}); rec.Header().Get("Content-Encoding") != "" || rec.Header().Get("Vary") != "" {
		t.Fatalf("static path touched: %v", rec.Header())
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/clusters/x/topics", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("Content-Encoding") != "" {
		t.Fatal("POST response was compressed")
	}
	req = httptest.NewRequest(http.MethodHead, "/api/v1/clusters", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("Content-Encoding") != "" {
		t.Fatal("HEAD response was compressed")
	}
}

func TestRangeRequestsAndAlreadyEncodedOrNonJSONResponsesAreUntouched(t *testing.T) {
	plain := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "hello")
	})
	if rec := get(Handler(plain), "/api/v1/export", map[string]string{"Accept-Encoding": "gzip"}); rec.Header().Get("Content-Encoding") != "" || rec.Body.String() != "hello" {
		t.Fatalf("non JSON response was compressed: %q", rec.Header().Get("Content-Encoding"))
	}
	encoded := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "br")
		_, _ = io.WriteString(w, "already")
	})
	if rec := get(Handler(encoded), "/api/v1/x", map[string]string{"Accept-Encoding": "gzip"}); rec.Header().Get("Content-Encoding") != "br" || rec.Body.String() != "already" {
		t.Fatalf("already encoded response changed: %q", rec.Header().Get("Content-Encoding"))
	}
	if rec := get(Handler(jsonHandler(`{"a":1}`)), "/api/v1/x", map[string]string{"Accept-Encoding": "gzip", "Range": "bytes=0-3"}); rec.Header().Get("Content-Encoding") != "" {
		t.Fatal("range request was compressed")
	}
}

func TestStatusesWithoutABodyGetNoEncodingHeader(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusNotModified} {
		h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
		}))
		rec := get(h, "/api/v1/x", map[string]string{"Accept-Encoding": "gzip"})
		if rec.Code != status || rec.Header().Get("Content-Encoding") != "" || rec.Body.Len() != 0 {
			t.Fatalf("status %d: code=%d encoding=%q body=%d", status, rec.Code, rec.Header().Get("Content-Encoding"), rec.Body.Len())
		}
	}
}

func TestErrorResponsesKeepTheirStatusAndStillDecode(t *testing.T) {
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":{"code":"broker_unavailable"}}`)
	}))
	rec := get(h, "/api/v1/x", map[string]string{"Accept-Encoding": "gzip"})
	if rec.Code != 503 || !strings.Contains(gunzip(t, rec.Body.Bytes()), "broker_unavailable") {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestFlushMidResponseProducesAValidStream(t *testing.T) {
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"a":`)
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, `1}`)
	}))
	rec := get(h, "/api/v1/x", map[string]string{"Accept-Encoding": "gzip"})
	if !rec.Flushed || gunzip(t, rec.Body.Bytes()) != `{"a":1}` {
		t.Fatalf("flushed=%v", rec.Flushed)
	}
}

func TestEmptyBodyDoesNotBreakAndPooledWritersAreReusedSafely(t *testing.T) {
	empty := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	if rec := get(empty, "/api/v1/x", map[string]string{"Accept-Encoding": "gzip"}); rec.Code != 200 || rec.Body.Len() != 0 {
		t.Fatalf("empty response: code=%d body=%d", rec.Code, rec.Body.Len())
	}
	for i := 0; i < 60; i++ {
		body := fmt.Sprintf(`{"n":%d,"pad":"%s"}`, i, strings.Repeat("x", i*37))
		rec := get(Handler(jsonHandler(body)), "/api/v1/x", map[string]string{"Accept-Encoding": "gzip"})
		if got := gunzip(t, rec.Body.Bytes()); got != body {
			t.Fatalf("iteration %d: pooled writer leaked state", i)
		}
	}
}

// A handler that announces the uncompressed length would make the client stop reading early or
// wait for bytes that never come once the body is compressed, so the header has to go.
func TestContentLengthOfTheOriginalBodyIsDroppedWhenCompressing(t *testing.T) {
	body := `{"data":[` + strings.Repeat(`{"id":"group","state":"Stable"},`, 200) + `{}]}`
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		_, _ = io.WriteString(w, body)
	}))
	rec := get(h, "/api/v1/x", map[string]string{"Accept-Encoding": "gzip"})
	if got := rec.Header().Get("Content-Length"); got != "" {
		t.Fatalf("content-length %q describes the uncompressed body", got)
	}
	if gunzip(t, rec.Body.Bytes()) != body {
		t.Fatal("decompressed body differs")
	}
	plain := get(h, "/api/v1/x", nil)
	if plain.Header().Get("Content-Length") != fmt.Sprint(len(body)) {
		t.Fatalf("uncompressed responses must keep their length, got %q", plain.Header().Get("Content-Length"))
	}
}
