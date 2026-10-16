package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthcheckURL(t *testing.T) {
	for listen, want := range map[string]string{
		"":               "http://127.0.0.1:8080/health",
		":8080":          "http://127.0.0.1:8080/health",
		"0.0.0.0:9000":   "http://127.0.0.1:9000/health",
		"[::]:9000":      "http://127.0.0.1:9000/health",
		"127.0.0.1:8081": "http://127.0.0.1:8081/health",
		"localhost:8082": "http://localhost:8082/health",
		"[::1]:8083":     "http://[::1]:8083/health",
	} {
		got, err := healthcheckURL(listen)
		if err != nil || got != want {
			t.Fatalf("healthcheckURL(%q) = %q, %v; want %q", listen, got, err, want)
		}
	}
	if _, err := healthcheckURL("8080"); err == nil {
		t.Fatal("accepted an address without a port")
	}
}

func TestRunHealthcheck(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()
	_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
	if err := runHealthcheck("127.0.0.1:" + port); err != nil {
		t.Fatal(err)
	}
	status = http.StatusServiceUnavailable
	if runHealthcheck("127.0.0.1:"+port) == nil {
		t.Fatal("accepted an unhealthy response")
	}
	srv.Close()
	if runHealthcheck("127.0.0.1:"+port) == nil {
		t.Fatal("accepted a closed port")
	}
}
