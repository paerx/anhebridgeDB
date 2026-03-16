package anhe

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
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
	"sync/atomic"
	"time"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type wsConn struct {
	raw  net.Conn
	rw   *bufio.ReadWriter
	addr string
	mu   sync.Mutex
}

type wsEnvelope struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id,omitempty"`
	Error     string `json:"error,omitempty"`
}

func newWSConn(addr, token string) (*wsConn, error) {
	wsURL, err := toWSURL(addr, token)
	if err != nil {
		return nil, err
	}
	conn, rw, err := dialWebSocket(wsURL)
	if err != nil {
		return nil, err
	}
	return &wsConn{raw: conn, rw: rw, addr: wsURL}, nil
}

func (w *wsConn) close() error {
	if w == nil || w.raw == nil {
		return nil
	}
	return w.raw.Close()
}

func (w *wsConn) exec(requestID, query string) ([]any, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	payload, err := json.Marshal(map[string]any{
		"type":       "exec",
		"request_id": requestID,
		"query":      query,
	})
	if err != nil {
		return nil, err
	}
	if err := w.writeFrame(0x1, payload); err != nil {
		return nil, err
	}
	for {
		message, err := w.readJSON()
		if err != nil {
			return nil, err
		}
		var env wsEnvelope
		if err := json.Unmarshal(message, &env); err != nil {
			return nil, err
		}
		switch env.Type {
		case "result":
			if env.RequestID != requestID {
				continue
			}
			var out struct {
				Results []any `json:"results"`
			}
			if err := json.Unmarshal(message, &out); err != nil {
				return nil, err
			}
			return out.Results, nil
		case "error":
			if env.RequestID != "" && env.RequestID != requestID {
				continue
			}
			if env.Error == "" {
				env.Error = "websocket exec error"
			}
			return nil, errors.New(env.Error)
		}
	}
}

func (w *wsConn) readJSON() ([]byte, error) {
	for {
		op, payload, err := w.readFrame()
		if err != nil {
			return nil, err
		}
		switch op {
		case 0x1:
			return payload, nil
		case 0x8:
			return nil, io.EOF
		case 0x9:
			if err := w.writeFrame(0xA, payload); err != nil {
				return nil, err
			}
		}
	}
}

func (w *wsConn) readFrame() (byte, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(w.rw, header); err != nil {
		return 0, nil, err
	}
	fin := header[0]&0x80 != 0
	opcode := header[0] & 0x0F
	if !fin {
		return 0, nil, errors.New("fragmented websocket frames not supported")
	}
	masked := header[1]&0x80 != 0
	size := int64(header[1] & 0x7F)
	switch size {
	case 126:
		ext := make([]byte, 2)
		if _, err := io.ReadFull(w.rw, ext); err != nil {
			return 0, nil, err
		}
		size = int64(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err := io.ReadFull(w.rw, ext); err != nil {
			return 0, nil, err
		}
		size = int64(binary.BigEndian.Uint64(ext))
	}
	var maskKey []byte
	if masked {
		maskKey = make([]byte, 4)
		if _, err := io.ReadFull(w.rw, maskKey); err != nil {
			return 0, nil, err
		}
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(w.rw, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}
	return opcode, payload, nil
}

func (w *wsConn) writeFrame(opcode byte, payload []byte) error {
	header := []byte{0x80 | opcode}
	switch {
	case len(payload) < 126:
		header = append(header, byte(len(payload)))
	case len(payload) <= 65535:
		header = append(header, 126, 0, 0)
		binary.BigEndian.PutUint16(header[len(header)-2:], uint16(len(payload)))
	default:
		header = append(header, 127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(header[len(header)-8:], uint64(len(payload)))
	}
	if _, err := w.rw.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.rw.Write(payload); err != nil {
			return err
		}
	}
	return w.rw.Flush()
}

func toWSURL(addr, token string) (string, error) {
	base := strings.TrimRight(addr, "/")
	switch {
	case strings.HasPrefix(base, "http://"):
		base = "ws://" + strings.TrimPrefix(base, "http://")
	case strings.HasPrefix(base, "https://"):
		base = "wss://" + strings.TrimPrefix(base, "https://")
	case strings.HasPrefix(base, "ws://"), strings.HasPrefix(base, "wss://"):
	default:
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

func dialWebSocket(rawURL string) (net.Conn, *bufio.ReadWriter, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}
	if u.Scheme == "wss" {
		return nil, nil, errors.New("wss is not supported in this sdk build")
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":80"
	}
	conn, err := net.DialTimeout("tcp", host, 10*time.Second)
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

	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n\r\n", u.RequestURI(), u.Host, key)
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
	accept := ""
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
			accept = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		}
	}
	if accept != wsAccept(key) {
		_ = conn.Close()
		return nil, nil, errors.New("websocket accept mismatch")
	}
	return conn, rw, nil
}

func wsAccept(key string) string {
	sum := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

type wsPool struct {
	addr      string
	token     string
	conns     []*wsConn
	rr        uint64
	requestID uint64
}

func newWSPool(addr, token string, size int) (*wsPool, error) {
	if size <= 0 {
		size = 1
	}
	p := &wsPool{addr: addr, token: token, conns: make([]*wsConn, 0, size)}
	for i := 0; i < size; i++ {
		conn, err := newWSConn(addr, token)
		if err != nil {
			for _, created := range p.conns {
				_ = created.close()
			}
			return nil, err
		}
		p.conns = append(p.conns, conn)
	}
	return p, nil
}

func (p *wsPool) close() error {
	var first error
	for _, conn := range p.conns {
		if err := conn.close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (p *wsPool) exec(query string) ([]any, error) {
	if len(p.conns) == 0 {
		return nil, errors.New("websocket pool not initialized")
	}
	index := int(atomic.AddUint64(&p.rr, 1)-1) % len(p.conns)
	id := atomic.AddUint64(&p.requestID, 1)
	return p.conns[index].exec(fmt.Sprintf("%d", id), query)
}
