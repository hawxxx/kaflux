package awsutil

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/credentials"
)

func TestSignerUsesSessionCredentialsAndPayload(t *testing.T) {
	sign := SignerFromProvider(credentials.NewStaticCredentialsProvider("test-key", "test-secret", "session-token"), "aps", "us-east-1")
	request, _ := http.NewRequest("GET", "https://metrics.example.com/api/v1/query?query=up", nil)
	if err := sign(context.Background(), request, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(request.Header.Get("Authorization"), "/us-east-1/aps/aws4_request") {
		t.Fatal("wrong signing scope")
	}
	if request.Header.Get("X-Amz-Security-Token") != "session-token" {
		t.Fatal("session credential omitted")
	}
	if request.Header.Get("X-Amz-Date") == "" {
		t.Fatal("timestamp missing")
	}
}

func TestSignerRejectsMissingCredentials(t *testing.T) {
	sign := SignerFromProvider(credentials.NewStaticCredentialsProvider("", "", ""), "monitoring", "us-east-1")
	request, _ := http.NewRequest("POST", "https://monitoring.example.com", nil)
	if err := sign(context.Background(), request, []byte("{}")); err == nil {
		t.Fatal("empty credentials accepted")
	}
}
