package main

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

type wsExecutor struct {
	addr      string
	token     string
	conn      net.Conn
	rw        *bufio.ReadWriter
	writeMu   sync.Mutex
	requestID uint64
}

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type wsResponseEnvelope struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id,omitempty"`
	Error     string `json:"error,omitempty"`
}

func newWSExecutor(addr string, store *authState) (*wsExecutor, error) {
	token := ""
	if store != nil && strings.TrimRight(store.Addr, "/") == strings.TrimRight(addr, "/") {
		token = strings.TrimSpace(store.Token)
	}
	exec := &wsExecutor{addr: addr, token: token}
	if err := exec.connect(); err != nil {
		return nil, err
	}
	return exec, nil
}

func (e *wsExecutor) close() error {
	if e == nil || e.conn == nil {
		return nil
	}
	return e.conn.Close()
}

func (e *wsExecutor) updateToken(addr string, store *authState) error {
	newToken := ""
	if store != nil && strings.TrimRight(store.Addr, "/") == strings.TrimRight(addr, "/") {
		newToken = strings.TrimSpace(store.Token)
	}
	if e.conn != nil && e.addr == addr && e.token == newToken {
		return nil
	}
	_ = e.close()
	e.addr = addr
	e.token = newToken
	return e.connect()
}

func (e *wsExecutor) execute(query string) ([]any, error) {
	if err := e.ensureConnected(); err != nil {
		return nil, err
	}
	e.requestID++
	reqID := fmt.Sprintf("%d", e.requestID)
	payload, err := json.Marshal(map[string]any{
		"type":       "exec",
		"request_id": reqID,
		"query":      query,
	})
	if err != nil {
		return nil, err
	}
	if err := e.writeFrame(0x1, payload); err != nil {
		_ = e.close()
		e.conn = nil
		return nil, err
	}
	for {
		message, err := e.readJSON()
		if err != nil {
			_ = e.close()
			e.conn = nil
			return nil, err
		}
		var envelope wsResponseEnvelope
		if err := json.Unmarshal(message, &envelope); err != nil {
			return nil, err
		}
		switch envelope.Type {
		case "result":
			if envelope.RequestID != reqID {
				continue
			}
			var response struct {
				Type      string `json:"type"`
				RequestID string `json:"request_id"`
				Results   []any  `json:"results"`
			}
			if err := json.Unmarshal(message, &response); err != nil {
				return nil, err
			}
			return response.Results, nil
		case "error":
			if envelope.RequestID != "" && envelope.RequestID != reqID {
				continue
			}
			if envelope.Error == "" {
				envelope.Error = "websocket error"
			}
			return nil, errors.New(envelope.Error)
		default:
			continue
		}
	}
}

func (e *wsExecutor) connect() error {
	wsURL, err := wsURLForAddr(e.addr, e.token)
	if err != nil {
		return err
	}
	conn, rw, err := dialWebSocket(wsURL, e.token)
	if err != nil {
		return err
	}
	e.conn = conn
	e.rw = rw
	return nil
}

func (e *wsExecutor) ensureConnected() error {
	if e.conn != nil {
		return nil
	}
	return e.connect()
}

func wsURLForAddr(addr, token string) (string, error) {
	base := strings.TrimRight(addr, "/")
	if strings.HasPrefix(base, "http://") {
		base = "ws://" + strings.TrimPrefix(base, "http://")
	} else if strings.HasPrefix(base, "https://") {
		base = "wss://" + strings.TrimPrefix(base, "https://")
	} else if !strings.HasPrefix(base, "ws://") && !strings.HasPrefix(base, "wss://") {
		base = "ws://" + base
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = "/ws"
	if token != "" {
		q := u.Query()
		q.Set("token", token)
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}

func dialWebSocket(rawURL, token string) (net.Conn, *bufio.ReadWriter, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		if u.Scheme == "wss" {
			host += ":443"
		} else {
			host += ":80"
		}
	}
	var conn net.Conn
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	switch u.Scheme {
	case "wss":
		tlsCfg := &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12}
		conn, err = tls.DialWithDialer(dialer, "tcp", host, tlsCfg)
	default:
		conn, err = dialer.Dial("tcp", host)
	}
	if err != nil {
		return nil, nil, err
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)
	path := u.RequestURI()
	if path == "" {
		path = "/ws"
	}
	authHeader := ""
	if strings.TrimSpace(token) != "" {
		authHeader = "Authorization: Bearer " + strings.TrimSpace(token) + "\r\n"
	}
	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n%s\r\n", path, u.Host, key, authHeader)
	if _, err := rw.WriteString(req); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if err := rw.Flush(); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	status, err := rw.ReadString('\n')
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if !strings.Contains(status, "101") {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("websocket handshake failed: %s", strings.TrimSpace(status))
	}
	accepted := ""
	for {
		line, err := rw.ReadString('\n')
		if err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
		if line == "\r\n" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "sec-websocket-accept:") {
			accepted = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		}
	}
	if accepted != wsAccept(key) {
		_ = conn.Close()
		return nil, nil, errors.New("websocket handshake accept mismatch")
	}
	return conn, rw, nil
}

func (e *wsExecutor) readJSON() ([]byte, error) {
	for {
		opcode, payload, err := e.readFrame()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case 0x1:
			return payload, nil
		case 0x8:
			return nil, io.EOF
		case 0x9:
			if err := e.writeFrame(0xA, payload); err != nil {
				return nil, err
			}
		}
	}
}

func (e *wsExecutor) readFrame() (byte, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(e.rw, header); err != nil {
		return 0, nil, err
	}
	if header[0]&0x80 == 0 {
		return 0, nil, errors.New("fragmented websocket frames not supported")
	}
	opcode := header[0] & 0x0F
	payloadLen := int64(header[1] & 0x7F)
	switch payloadLen {
	case 126:
		ext := make([]byte, 2)
		if _, err := io.ReadFull(e.rw, ext); err != nil {
			return 0, nil, err
		}
		payloadLen = int64(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err := io.ReadFull(e.rw, ext); err != nil {
			return 0, nil, err
		}
		payloadLen = int64(binary.BigEndian.Uint64(ext))
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(e.rw, payload); err != nil {
		return 0, nil, err
	}
	return opcode, payload, nil
}

func (e *wsExecutor) writeFrame(opcode byte, payload []byte) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	mask := make([]byte, 4)
	if _, err := rand.Read(mask); err != nil {
		return err
	}
	header := []byte{0x80 | opcode}
	switch {
	case len(payload) < 126:
		header = append(header, 0x80|byte(len(payload)))
	case len(payload) <= 65535:
		header = append(header, 0x80|126, 0, 0)
		binary.BigEndian.PutUint16(header[len(header)-2:], uint16(len(payload)))
	default:
		header = append(header, 0x80|127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(header[len(header)-8:], uint64(len(payload)))
	}
	masked := make([]byte, len(payload))
	copy(masked, payload)
	for i := range masked {
		masked[i] ^= mask[i%4]
	}
	if _, err := e.rw.Write(header); err != nil {
		return err
	}
	if _, err := e.rw.Write(mask); err != nil {
		return err
	}
	if len(masked) > 0 {
		if _, err := e.rw.Write(masked); err != nil {
			return err
		}
	}
	return e.rw.Flush()
}

func wsAccept(key string) string {
	sum := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}
