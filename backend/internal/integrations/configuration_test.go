package integrations

import (
	"strings"
	"testing"
)

func TestRedactedConnectorCredentialPreservation(t *testing.T) {
	body, e := PreserveRedacted([]byte(`{"password":"[REDACTED]","topics":"updated"}`), []byte(`{"password":"existing-private","topics":"original"}`))
	if e != nil || !strings.Contains(string(body), "existing-private") || !strings.Contains(string(body), "updated") {
		t.Fatal("credentials lost", e)
	}
	if _, e = PreserveRedacted([]byte(`{"token":"[REDACTED]"}`), []byte(`{}`)); e == nil {
		t.Fatal("placeholder invented credential")
	}
}
