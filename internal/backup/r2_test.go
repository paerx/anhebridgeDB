package backup

import (
	"bytes"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestSignS3Request(t *testing.T) {
	target, err := url.Parse("https://example.r2.cloudflarestorage.com/bucket/backups/test.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPut, target.String(), bytes.NewReader([]byte("payload")))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/gzip")
	signS3Request(req, strings.Repeat("a", 64), "access", "secret", time.Date(2026, 7, 29, 1, 2, 3, 0, time.UTC))
	if req.Header.Get("X-Amz-Date") != "20260729T010203Z" {
		t.Fatalf("unexpected x-amz-date: %s", req.Header.Get("X-Amz-Date"))
	}
	authorization := req.Header.Get("Authorization")
	if !strings.Contains(authorization, "Credential=access/20260729/auto/s3/aws4_request") {
		t.Fatalf("unexpected authorization: %s", authorization)
	}
	if !strings.Contains(authorization, "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date") {
		t.Fatalf("unexpected signed headers: %s", authorization)
	}
}
