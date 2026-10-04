// Package awsutil resolves AWS workload credentials only on the server side.
package awsutil

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type RequestSigner func(context.Context, *http.Request, []byte) error

func NewSigner(ctx context.Context, service, region, roleARN string) (RequestSigner, error) {
	if service != "aps" && service != "monitoring" && service != "kafka" {
		return nil, errors.New("unsupported AWS service")
	}
	if region == "" {
		return nil, errors.New("AWS metrics region is required")
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, errors.New("cannot initialize AWS credential provider")
	}
	provider := cfg.Credentials
	if roleARN != "" {
		provider = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), roleARN))
	}
	return SignerFromProvider(provider, service, region), nil
}

func SignerFromProvider(provider aws.CredentialsProvider, service, region string) RequestSigner {
	signer := v4.NewSigner()
	return func(ctx context.Context, request *http.Request, body []byte) error {
		if provider == nil {
			return errors.New("AWS credential provider is missing")
		}
		credentials, err := provider.Retrieve(ctx)
		if err != nil || credentials.AccessKeyID == "" || credentials.SecretAccessKey == "" {
			return errors.New("AWS metrics credentials are unavailable")
		}
		digest := sha256.Sum256(body)
		if err = signer.SignHTTP(ctx, credentials, request, hex.EncodeToString(digest[:]), service, region, time.Now()); err != nil {
			return errors.New("cannot sign AWS metrics request")
		}
		return nil
	}
}
