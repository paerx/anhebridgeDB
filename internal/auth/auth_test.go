package auth

import (
	"testing"

	"anhebridgedb/internal/config"
)

func TestLoginAndVerify(t *testing.T) {
	manager := New(config.AuthConfig{
		Enabled:         true,
		Secret:          "test-secret",
		TokenTTLMinutes: 1,
		Users: []config.AuthUser{
			{Username: "admin", Password: "pass"},
		},
	})

	token, claims, err := manager.Login("admin", "pass")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if claims.Username != "admin" {
		t.Fatalf("unexpected username: %s", claims.Username)
	}
	verified, err := manager.Verify(token)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verified.Username != "admin" {
		t.Fatalf("unexpected verified username: %s", verified.Username)
	}
}

func TestVerifyTamperedToken(t *testing.T) {
	manager := New(config.AuthConfig{
		Enabled:         true,
		Secret:          "test-secret",
		TokenTTLMinutes: 1,
		Users:           []config.AuthUser{{Username: "admin", Password: "pass"}},
	})
	token, _, err := manager.Login("admin", "pass")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	token = token + "tampered"
	if _, err := manager.Verify(token); err == nil {
		t.Fatalf("expected verify error for tampered token")
	}
}
