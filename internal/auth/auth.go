package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"anhebridgedb/internal/config"
)

var (
	ErrDisabled           = errors.New("auth disabled")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrInvalidToken       = errors.New("invalid token")
	ErrExpiredToken       = errors.New("expired token")
)

type Claims struct {
	Username  string    `json:"username"`
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type Manager struct {
	enabled bool
	ttl     time.Duration
	secret  []byte
	users   map[string]string
}

func New(cfg config.AuthConfig) *Manager {
	ttl := time.Duration(cfg.TokenTTLMinutes) * time.Minute
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	secret := cfg.Secret
	if secret == "" {
		secret = os.Getenv("ANHEBRIDGE_AUTH_SECRET")
	}
	if secret == "" {
		secret = "anhebridgedb-dev-auth-secret"
	}
	users := map[string]string{}
	for _, user := range cfg.Users {
		if user.Username == "" {
			continue
		}
		users[user.Username] = user.Password
	}
	return &Manager{
		enabled: cfg.Enabled,
		ttl:     ttl,
		secret:  []byte(secret),
		users:   users,
	}
}

func (m *Manager) Enabled() bool {
	return m != nil && m.enabled
}

func (m *Manager) Login(username, password string) (string, Claims, error) {
	if !m.Enabled() {
		return "", Claims{}, ErrDisabled
	}
	if expected, ok := m.users[username]; !ok || expected != password {
		return "", Claims{}, ErrInvalidCredentials
	}
	now := time.Now().UTC()
	claims := Claims{
		Username:  username,
		IssuedAt:  now,
		ExpiresAt: now.Add(m.ttl),
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", Claims{}, err
	}
	tokenPayload := base64.RawURLEncoding.EncodeToString(payload)
	sig := m.sign(tokenPayload)
	return tokenPayload + "." + sig, claims, nil
}

func (m *Manager) Verify(token string) (Claims, error) {
	if !m.Enabled() {
		return Claims{}, nil
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return Claims{}, ErrInvalidToken
	}
	expected := m.sign(parts[0])
	if !hmac.Equal([]byte(expected), []byte(parts[1])) {
		return Claims{}, ErrInvalidToken
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Claims{}, ErrInvalidToken
	}
	var claims Claims
	if err := json.Unmarshal(data, &claims); err != nil {
		return Claims{}, ErrInvalidToken
	}
	if !claims.ExpiresAt.IsZero() && time.Now().UTC().After(claims.ExpiresAt) {
		return Claims{}, ErrExpiredToken
	}
	return claims, nil
}

func (m *Manager) sign(payload string) string {
	mac := hmac.New(sha256.New, m.secret)
	_, _ = mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func BearerToken(header string) (string, error) {
	if header == "" {
		return "", ErrInvalidToken
	}
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return "", fmt.Errorf("%w: missing bearer token", ErrInvalidToken)
	}
	return strings.TrimSpace(parts[1]), nil
}
