// Package httpgzip compresses JSON API responses for clients that accept gzip.
//
// Large list endpoints are mostly repeated field names and values, so they shrink by an order of
// magnitude. Only GET requests under /api/v1/ are considered, and only responses that declare
// application/json and are not already encoded.
package httpgzip

import (
	"compress/gzip"
	"net/http"
	"strings"
	"sync"
)

const apiPrefix = "/api/v1/"

// neverCompress lists API paths that are left as they are. Session responses carry the CSRF
// token, and compressing a secret in the same body as attacker influenced text is the setup for
// BREACH style attacks.
var neverCompress = []string{"/api/v1/auth/"}

var pool = sync.Pool{New: func() any {
	w, _ := gzip.NewWriterLevel(nil, gzip.DefaultCompression)
	return w
}}

// Handler wraps next and compresses eligible responses.
func Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, apiPrefix) {
			next.ServeHTTP(w, r)
			return
		}
		// The body depends on the request header for every API response, including the ones
		// that are not compressed, so shared caches must key on it.
		w.Header().Add("Vary", "Accept-Encoding")
		if !acceptsGzip(r.Header.Get("Accept-Encoding")) || r.Header.Get("Range") != "" || excluded(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		cw := &writer{ResponseWriter: w}
		defer cw.close()
		next.ServeHTTP(cw, r)
	})
}

func excluded(path string) bool {
	for _, prefix := range neverCompress {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// acceptsGzip reports whether an Accept-Encoding value allows gzip (a q value of 0 refuses it).
func acceptsGzip(value string) bool {
	for _, part := range strings.Split(value, ",") {
		fields := strings.Split(part, ";")
		if strings.TrimSpace(strings.ToLower(fields[0])) != "gzip" {
			continue
		}
		for _, param := range fields[1:] {
			name, q, ok := strings.Cut(strings.TrimSpace(param), "=")
			if ok && strings.EqualFold(strings.TrimSpace(name), "q") {
				v := strings.TrimSpace(q)
				if v == "0" || v == "0." || v == "0.0" || v == "0.00" || v == "0.000" {
					return false
				}
			}
		}
		return true
	}
	return false
}

type writer struct {
	http.ResponseWriter
	gz          *gzip.Writer
	wroteHeader bool
}

func compressible(status int, h http.Header) bool {
	if status < 200 || status == http.StatusNoContent || status == http.StatusNotModified {
		return false
	}
	if h.Get("Content-Encoding") != "" {
		return false
	}
	return strings.HasPrefix(strings.ToLower(h.Get("Content-Type")), "application/json")
}

func (w *writer) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	if compressible(status, w.Header()) {
		w.Header().Del("Content-Length")
		w.Header().Set("Content-Encoding", "gzip")
		w.gz = pool.Get().(*gzip.Writer)
		w.gz.Reset(w.ResponseWriter)
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *writer) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if w.gz != nil {
		return w.gz.Write(p)
	}
	return w.ResponseWriter.Write(p)
}

func (w *writer) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if w.gz != nil {
		_ = w.gz.Flush()
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *writer) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *writer) close() {
	if w.gz == nil {
		return
	}
	_ = w.gz.Close()
	pool.Put(w.gz)
	w.gz = nil
}
