package auth

import (
	"testing"
	"time"
)

func TestSessionLifetime(t *testing.T) {
	for _, lifetime := range []time.Duration{0, 5 * time.Minute, 24 * time.Hour} {
		before := time.Now()
		s := NewSession(User{ID: "fixture"}, lifetime)
		want := lifetime
		if want == 0 {
			want = 8 * time.Hour
		}
		if s.Expires.Before(before.Add(want)) || s.Expires.After(time.Now().Add(want)) {
			t.Fatalf("wrong lifetime %v", lifetime)
		}
		if s.ID == "" || s.CSRF == "" {
			t.Fatal("missing tokens")
		}
	}
}

func TestSessionExpiryBoundary(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := Session{Expires: now}
	if s.ValidAt(now) || s.ValidAt(now.Add(time.Nanosecond)) || !s.ValidAt(now.Add(-time.Nanosecond)) {
		t.Fatal("expiry boundary must be exclusive")
	}
}
