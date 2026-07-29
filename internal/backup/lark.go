package backup

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/paerx/anhebridgedb/internal/config"
)

type larkNotifier struct {
	webhook    string
	secret     string
	httpClient *http.Client
}

func newLarkNotifier(cfg config.BackupLarkConfig) (*larkNotifier, error) {
	webhook := strings.TrimSpace(cfg.Webhook)
	if webhook == "" {
		return nil, fmt.Errorf("lark webhook is required")
	}
	return &larkNotifier{
		webhook:    webhook,
		secret:     strings.TrimSpace(cfg.Secret),
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (n *larkNotifier) send(ctx context.Context, message string) error {
	payload := map[string]any{
		"msg_type": "text",
		"content": map[string]string{
			"text": message,
		},
	}
	if n.secret != "" {
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		stringToSign := timestamp + "\n" + n.secret
		hash := hmac.New(sha256.New, []byte(stringToSign))
		_, _ = hash.Write(nil)
		payload["timestamp"] = timestamp
		payload["sign"] = base64.StdEncoding.EncodeToString(hash.Sum(nil))
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.webhook, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := n.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("lark webhook failed: %s: %s", response.Status, strings.TrimSpace(string(data)))
	}
	return nil
}
