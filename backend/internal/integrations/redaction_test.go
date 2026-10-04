package integrations

import (
	"strings"
	"testing"
)

func TestConnectorSecretSettingsAndEmbeddedCredentials(t *testing.T) {
	raw := []byte(`{"schema.registry.basic.auth.user.info":"user:private","ssl.keystore.key":"privatepem","connection.url":"jdbc:postgresql://db/test?password=privatejdbc","service.url":"https://user:privateurl@example.invalid","http.headers":"Authorization: Bearer privateheader","inline":"-----BEGIN PRIVATE KEY----- privateinline","topics":"orders.events"}`)
	safe, e := Redact(raw)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(safe), "private") || !strings.Contains(string(safe), "orders.events") {
		t.Fatalf("redaction failed %s", safe)
	}
}
