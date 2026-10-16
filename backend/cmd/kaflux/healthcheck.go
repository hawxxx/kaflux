package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

// healthcheckURL turns a listen address such as ":8080" or "0.0.0.0:8080" into the loopback
// URL of the health endpoint. The image has no shell or curl, so container healthchecks run
// `kaflux healthcheck` instead.
func healthcheckURL(listen string) (string, error) {
	if listen == "" {
		listen = "127.0.0.1:8080"
	}
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/health", nil
}

// runHealthcheck reports whether the local server answers /health with 200 within three seconds.
func runHealthcheck(listen string) error {
	target, err := healthcheckURL(listen)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health endpoint returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func healthcheckMain() {
	if err := runHealthcheck(os.Getenv("KAFLUX_LISTEN")); err != nil {
		fmt.Fprintln(os.Stderr, "unhealthy:", err)
		os.Exit(1)
	}
}
