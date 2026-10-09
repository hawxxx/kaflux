package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

type OIDCConfig struct {
	ID              string              `yaml:"id"`
	Name            string              `yaml:"name"`
	Issuer          string              `yaml:"issuer"`
	ClientID        string              `yaml:"clientId"`
	ClientSecretEnv string              `yaml:"clientSecretEnv"`
	RedirectURL     string              `yaml:"redirectURL"`
	Scopes          []string            `yaml:"scopes"`
	GroupClaim      string              `yaml:"groupClaim"`
	GroupRoles      map[string][]string `yaml:"groupRoles"`
	DefaultRoles    []string            `yaml:"defaultRoles"`
	AllowHTTP       bool                `yaml:"allowHTTP"`
}

type OIDCPending struct {
	Nonce, Verifier string
	Expires         time.Time
}
type OIDCFlowStore interface {
	SaveOIDCState(context.Context, string, OIDCPending) error
	ConsumeOIDCState(context.Context, string) (OIDCPending, error)
}
type OIDC struct {
	cfg       OIDCConfig
	oauth     oauth2.Config
	verifier  *oidc.IDTokenVerifier
	mu        sync.Mutex
	pending   map[string]OIDCPending
	flowStore OIDCFlowStore
}

func NewOIDC(ctx context.Context, cfg OIDCConfig) (*OIDC, error) {
	issuer, err := url.Parse(cfg.Issuer)
	if err != nil || issuer.Host == "" || (issuer.Scheme != "https" && !(cfg.AllowHTTP && issuer.Scheme == "http")) {
		return nil, fmt.Errorf("OIDC requires a valid HTTPS issuer")
	}
	redirect, err := url.Parse(cfg.RedirectURL)
	if err != nil || redirect.Host == "" || (redirect.Scheme != "https" && !(cfg.AllowHTTP && redirect.Scheme == "http")) || cfg.ClientID == "" {
		return nil, fmt.Errorf("OIDC requires client ID and valid HTTPS redirect URL")
	}
	secret := ""
	if cfg.ClientSecretEnv != "" {
		secret = os.Getenv(cfg.ClientSecretEnv)
		if secret == "" {
			return nil, fmt.Errorf("OIDC client secret reference is unset")
		}
	}
	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("OIDC discovery failed")
	}
	if cfg.ID == "" {
		cfg.ID = "oidc"
	}
	if cfg.GroupClaim == "" {
		cfg.GroupClaim = "groups"
	}
	endpoint := provider.Endpoint()
	var discovery struct {
		JWKSURL string `json:"jwks_uri"`
	}
	if provider.Claims(&discovery) != nil {
		return nil, fmt.Errorf("invalid OIDC discovery metadata")
	}
	for _, raw := range []string{endpoint.AuthURL, endpoint.TokenURL, discovery.JWKSURL} {
		u, e := url.Parse(raw)
		if e != nil || u.Host == "" || (u.Scheme != "https" && !(cfg.AllowHTTP && u.Scheme == "http")) {
			return nil, fmt.Errorf("OIDC discovery returned an insecure endpoint")
		}
	}
	return &OIDC{cfg: cfg, oauth: oauth2.Config{ClientID: cfg.ClientID, ClientSecret: secret, Endpoint: endpoint, RedirectURL: cfg.RedirectURL, Scopes: append([]string{oidc.ScopeOpenID, "profile", "email"}, cfg.Scopes...)}, verifier: provider.Verifier(&oidc.Config{ClientID: cfg.ClientID}), pending: map[string]OIDCPending{}}, nil
}

// Name is the label shown on the sign-in button.
func (o *OIDC) Name() string {
	if o.cfg.Name != "" {
		return o.cfg.Name
	}
	return o.cfg.ID
}

// SetFlowStore installs shared flow persistence before the HTTP server starts.
func (o *OIDC) SetFlowStore(store OIDCFlowStore) { o.flowStore = store }

// StartURL creates bounded, expiring state bound to nonce and PKCE verifier.
// Production uses shared flow persistence; local fixtures may use memory.
func (o *OIDC) StartURL() (string, string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for state, p := range o.pending {
		if time.Now().After(p.Expires) {
			delete(o.pending, state)
		}
	}
	if len(o.pending) >= 1000 {
		return "", "", fmt.Errorf("too many pending OIDC logins")
	}
	state, nonce, verifier := Token(), Token(), Token()
	hash := sha256.Sum256([]byte(verifier))
	pending := OIDCPending{Nonce: nonce, Verifier: verifier, Expires: time.Now().Add(5 * time.Minute)}
	if o.flowStore != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := o.flowStore.SaveOIDCState(ctx, o.cfg.ID+":"+state, pending); err != nil {
			return "", "", fmt.Errorf("OIDC login state persistence failed")
		}
	} else {
		o.pending[state] = pending
	}
	return o.oauth.AuthCodeURL(state, oidc.Nonce(nonce), oauth2.SetAuthURLParam("code_challenge", base64.RawURLEncoding.EncodeToString(hash[:])), oauth2.SetAuthURLParam("code_challenge_method", "S256")), state, nil
}

func (o *OIDC) Complete(ctx context.Context, state, code string) (User, error) {
	o.mu.Lock()
	pending, ok := o.pending[state]
	delete(o.pending, state)
	o.mu.Unlock()
	if o.flowStore != nil {
		var err error
		pending, err = o.flowStore.ConsumeOIDCState(ctx, o.cfg.ID+":"+state)
		ok = err == nil
	}
	if !ok || time.Now().After(pending.Expires) || code == "" {
		return User{}, fmt.Errorf("invalid or expired OIDC state")
	}
	token, err := o.oauth.Exchange(ctx, code, oauth2.SetAuthURLParam("code_verifier", pending.Verifier))
	if err != nil {
		return User{}, fmt.Errorf("OIDC token exchange failed")
	}
	raw, ok := token.Extra("id_token").(string)
	if !ok {
		return User{}, fmt.Errorf("OIDC response missing identity token")
	}
	verified, err := o.verifier.Verify(ctx, raw)
	if err != nil {
		return User{}, fmt.Errorf("OIDC identity verification failed")
	}
	if subtle.ConstantTimeCompare([]byte(verified.Nonce), []byte(pending.Nonce)) != 1 {
		return User{}, fmt.Errorf("OIDC nonce mismatch")
	}
	var claims map[string]json.RawMessage
	if err := verified.Claims(&claims); err != nil {
		return User{}, fmt.Errorf("invalid OIDC identity claims")
	}
	username := verified.Subject
	for _, claim := range []string{"preferred_username", "email"} {
		var name string
		if json.Unmarshal(claims[claim], &name) == nil && name != "" {
			username = name
			break
		}
	}
	var groups []string
	if raw, ok := claims[o.cfg.GroupClaim]; ok && json.Unmarshal(raw, &groups) != nil {
		return User{}, fmt.Errorf("invalid OIDC group claim")
	}
	roles := mappedRoles(groups, o.cfg.GroupRoles, o.cfg.DefaultRoles)
	if len(roles) == 0 {
		return User{}, fmt.Errorf("OIDC identity has no assigned roles")
	}
	return User{ID: o.cfg.ID + ":" + verified.Subject, Username: username, Provider: o.cfg.ID, Roles: roles}, nil
}

func mappedRoles(groups []string, mapping map[string][]string, defaults []string) []string {
	found := map[string]bool{}
	for _, role := range defaults {
		if role != "" {
			found[role] = true
		}
	}
	for _, group := range groups {
		for _, role := range mapping[group] {
			if role != "" {
				found[role] = true
			}
		}
	}
	out := make([]string, 0, len(found))
	for role := range found {
		out = append(out, role)
	}
	sort.Strings(out)
	return out
}
