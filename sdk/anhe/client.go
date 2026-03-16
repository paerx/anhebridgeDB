package anhe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type DB struct {
	addr       string
	token      string
	httpClient *http.Client
	ws         *wsPool
}

func Open(cfg Config) (*DB, error) {
	addr := strings.TrimRight(strings.TrimSpace(cfg.Addr), "/")
	if addr == "" {
		return nil, errors.New("addr is required")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	client := &http.Client{Timeout: timeout}
	token := strings.TrimSpace(cfg.Token)
	if token == "" && cfg.Username != "" {
		var err error
		token, err = login(client, addr, cfg.Username, cfg.Password)
		if err != nil {
			return nil, err
		}
	}
	wsPoolSize := cfg.WSPoolSize
	if wsPoolSize <= 0 {
		wsPoolSize = 4
	}
	ws, err := newWSPool(addr, token, wsPoolSize)
	if err != nil {
		return nil, err
	}
	return &DB{
		addr:       addr,
		token:      token,
		httpClient: client,
		ws:         ws,
	}, nil
}

func (db *DB) Close() error {
	if db == nil || db.ws == nil {
		return nil
	}
	return db.ws.close()
}

func (db *DB) Key(key string) *KeySession {
	return &KeySession{db: db, key: strings.TrimSpace(key)}
}

func (db *DB) Search() *SearchSession {
	return &SearchSession{db: db, desc: true, limit: 50, page: 1}
}

func (db *DB) Batch() *BatchSession {
	return &BatchSession{db: db}
}

func (db *DB) ExecDSL(ctx context.Context, query string) ([]any, error) {
	_ = ctx
	if db.ws == nil {
		return nil, errors.New("websocket pool is nil")
	}
	return db.ws.exec(strings.TrimSpace(query))
}

func (db *DB) Snapshot(ctx context.Context) (map[string]any, error) {
	return db.postJSONMap(ctx, "/snapshot", nil)
}

func (db *DB) VerifyStorage(ctx context.Context) (map[string]any, error) {
	return db.postJSONMap(ctx, "/admin/verify", nil)
}

func (db *DB) CompactStorage(ctx context.Context) (map[string]any, error) {
	return db.postJSONMap(ctx, "/admin/compact", nil)
}

func (db *DB) Metrics(ctx context.Context) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, db.addr+"/metrics", nil)
	if err != nil {
		return nil, err
	}
	respBody, err := db.do(req)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *DB) postJSONMap(ctx context.Context, path string, payload any) (map[string]any, error) {
	var body io.Reader
	if payload != nil {
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(payloadBytes)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, db.addr+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	respBody, err := db.do(req)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *DB) do(req *http.Request) ([]byte, error) {
	if db.token != "" {
		req.Header.Set("Authorization", "Bearer "+db.token)
	}
	resp, err := db.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("server error (%s): %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func login(client *http.Client, addr, username, password string) (string, error) {
	body, err := json.Marshal(map[string]string{
		"username": username,
		"password": password,
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest(http.MethodPost, addr+"/auth/login", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("login failed (%s): %s", resp.Status, strings.TrimSpace(string(respBody)))
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return "", err
	}
	if strings.TrimSpace(out.Token) == "" {
		return "", errors.New("empty login token")
	}
	return out.Token, nil
}
