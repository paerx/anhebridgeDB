package httpapi

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/paerx/anhebridgedb/internal/auth"
	"github.com/paerx/anhebridgedb/internal/config"
)

func newAuthServerForTest(t *testing.T) (*Server, string) {
	t.Helper()
	manager := auth.New(config.AuthConfig{
		Enabled:         true,
		Secret:          "test-secret",
		TokenTTLMinutes: 10,
		Users: []config.AuthUser{
			{Username: "admin", Password: "pass"},
		},
	})
	token, _, err := manager.Login("admin", "pass")
	if err != nil {
		t.Fatalf("login token: %v", err)
	}
	return &Server{auth: manager}, token
}

func TestAuthorizeWSHandshakeWithBearerHeader(t *testing.T) {
	s, token := newAuthServerForTest(t)
	req := &http.Request{
		Header: make(http.Header),
		URL:    &url.URL{},
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if _, err := s.authorizeWSHandshake(req); err != nil {
		t.Fatalf("expected bearer auth to pass: %v", err)
	}
}

func TestAuthorizeWSHandshakeWithQueryToken(t *testing.T) {
	s, token := newAuthServerForTest(t)
	req := &http.Request{
		Header: make(http.Header),
		URL:    &url.URL{RawQuery: "token=" + url.QueryEscape(token)},
	}
	if _, err := s.authorizeWSHandshake(req); err != nil {
		t.Fatalf("expected query token auth to pass: %v", err)
	}
}

func TestAuthorizeWSHandshakeRejectsMissingToken(t *testing.T) {
	s, _ := newAuthServerForTest(t)
	req := &http.Request{
		Header: make(http.Header),
		URL:    &url.URL{},
	}
	if _, err := s.authorizeWSHandshake(req); err == nil {
		t.Fatal("expected missing token to fail")
	}
}
