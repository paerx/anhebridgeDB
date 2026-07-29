package backup

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/paerx/anhebridgedb/internal/config"
)

type r2Uploader struct {
	endpoint   *url.URL
	bucket     string
	prefix     string
	accessKey  string
	secretKey  string
	maxBytes   int64
	httpClient *http.Client
}

func newR2Uploader(cfg config.BackupUploadConfig) (*r2Uploader, error) {
	endpoint, err := url.Parse(strings.TrimRight(cfg.Endpoint, "/"))
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		return nil, fmt.Errorf("backup r2 endpoint is invalid")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("backup r2 bucket is required")
	}
	accessKey := strings.TrimSpace(cfg.AccessKeyID)
	secretKey := strings.TrimSpace(cfg.SecretAccessKey)
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("backup r2 access_key_id and secret_access_key are required")
	}
	return &r2Uploader{
		endpoint:   endpoint,
		bucket:     cfg.Bucket,
		prefix:     strings.Trim(cfg.Prefix, "/"),
		accessKey:  accessKey,
		secretKey:  secretKey,
		maxBytes:   cfg.MaxObjectBytes,
		httpClient: &http.Client{},
	}, nil
}

func (u *r2Uploader) objectKey(filename string, now time.Time) string {
	parts := []string{u.prefix, now.UTC().Format("2006/01/02"), filename}
	filtered := parts[:0]
	for _, part := range parts {
		if part != "" {
			filtered = append(filtered, strings.Trim(part, "/"))
		}
	}
	return strings.Join(filtered, "/")
}

func (u *r2Uploader) upload(ctx context.Context, filename, objectKey string) error {
	return u.putFile(ctx, filename, objectKey, "application/gzip")
}

func (u *r2Uploader) putFile(ctx context.Context, filename, objectKey, contentType string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if u.maxBytes > 0 && info.Size() > u.maxBytes {
		return fmt.Errorf("backup object is %d bytes, exceeds configured single-put limit %d", info.Size(), u.maxBytes)
	}

	payloadHash, err := hashReader(file)
	if err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	response, err := u.do(ctx, http.MethodPut, objectKey, file, info.Size(), payloadHash, contentType)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("r2 upload failed: %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	return nil
}

func (u *r2Uploader) putBytes(ctx context.Context, data []byte, objectKey, contentType string) error {
	if u.maxBytes > 0 && int64(len(data)) > u.maxBytes {
		return fmt.Errorf("backup object is %d bytes, exceeds configured single-put limit %d", len(data), u.maxBytes)
	}
	payloadHash := sha256.Sum256(data)
	response, err := u.do(
		ctx,
		http.MethodPut,
		objectKey,
		bytes.NewReader(data),
		int64(len(data)),
		hex.EncodeToString(payloadHash[:]),
		contentType,
	)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("r2 upload failed: %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	return nil
}

func (u *r2Uploader) getBytes(ctx context.Context, objectKey string, maxBytes int64) ([]byte, error) {
	var buffer bytes.Buffer
	if err := u.download(ctx, objectKey, &buffer, maxBytes); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (u *r2Uploader) exists(ctx context.Context, objectKey string, expectedSize int64) (bool, error) {
	emptyHash := sha256.Sum256(nil)
	response, err := u.do(
		ctx,
		http.MethodHead,
		objectKey,
		nil,
		0,
		hex.EncodeToString(emptyHash[:]),
		"application/octet-stream",
	)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		if expectedSize >= 0 && response.ContentLength >= 0 && response.ContentLength != expectedSize {
			return false, nil
		}
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return false, fmt.Errorf("r2 HEAD failed for %s: %s: %s", objectKey, response.Status, strings.TrimSpace(string(body)))
	}
}

func (u *r2Uploader) download(ctx context.Context, objectKey string, writer io.Writer, maxBytes int64) error {
	emptyHash := sha256.Sum256(nil)
	response, err := u.do(
		ctx,
		http.MethodGet,
		objectKey,
		nil,
		0,
		hex.EncodeToString(emptyHash[:]),
		"application/octet-stream",
	)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("r2 download failed for %s: %s: %s", objectKey, response.Status, strings.TrimSpace(string(body)))
	}
	if maxBytes > 0 && response.ContentLength > maxBytes {
		return fmt.Errorf("r2 object %s is %d bytes, exceeds limit %d", objectKey, response.ContentLength, maxBytes)
	}
	reader := io.Reader(response.Body)
	if maxBytes > 0 {
		reader = io.LimitReader(response.Body, maxBytes+1)
	}
	written, err := io.Copy(writer, reader)
	if err != nil {
		return err
	}
	if maxBytes > 0 && written > maxBytes {
		return fmt.Errorf("r2 object %s exceeds limit %d", objectKey, maxBytes)
	}
	return nil
}

func (u *r2Uploader) prefixedKey(relative string) string {
	if u.prefix == "" {
		return strings.TrimLeft(relative, "/")
	}
	return u.prefix + "/" + strings.TrimLeft(relative, "/")
}

func (u *r2Uploader) do(
	ctx context.Context,
	method string,
	objectKey string,
	body io.Reader,
	contentLength int64,
	payloadHash string,
	contentType string,
) (*http.Response, error) {
	target := *u.endpoint
	target.Path = path.Join(u.endpoint.Path, u.bucket, objectKey)
	request, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return nil, err
	}
	request.ContentLength = contentLength
	request.Header.Set("Content-Type", contentType)
	signS3Request(request, payloadHash, u.accessKey, u.secretKey, time.Now().UTC())
	return u.httpClient.Do(request)
}

func hashReader(reader io.Reader) (string, error) {
	hash := sha256.New()
	if _, err := io.Copy(hash, reader); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func signS3Request(req *http.Request, payloadHash, accessKey, secretKey string, now time.Time) {
	const (
		algorithm = "AWS4-HMAC-SHA256"
		region    = "auto"
		service   = "s3"
	)
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	canonicalURI := req.URL.EscapedPath()
	canonicalHeaders := "content-type:" + strings.TrimSpace(req.Header.Get("Content-Type")) + "\n" +
		"host:" + req.URL.Host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "content-type;host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		req.URL.Query().Encode(),
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
	scope := strings.Join([]string{date, region, service, "aws4_request"}, "/")
	requestHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		algorithm,
		amzDate,
		scope,
		hex.EncodeToString(requestHash[:]),
	}, "\n")

	dateKey := hmacSHA256([]byte("AWS4"+secretKey), date)
	regionKey := hmacSHA256(dateKey, region)
	serviceKey := hmacSHA256(regionKey, service)
	signingKey := hmacSHA256(serviceKey, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	req.Header.Set("Authorization", fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm, accessKey, scope, signedHeaders, signature,
	))
}

func hmacSHA256(key []byte, value string) []byte {
	hash := hmac.New(sha256.New, key)
	_, _ = hash.Write([]byte(value))
	return hash.Sum(nil)
}
