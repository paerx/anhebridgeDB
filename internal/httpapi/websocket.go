package httpapi

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type wsConn struct {
	conn net.Conn
	rw   *bufio.ReadWriter
	mu   sync.Mutex
}

type wsRequest struct {
	Type            string `json:"type"`
	Token           string `json:"token,omitempty"`
	Query           string `json:"query,omitempty"`
	Topic           string `json:"topic,omitempty"`
	IntervalSeconds int    `json:"interval_seconds,omitempty"`
	RequestID       string `json:"request_id,omitempty"`
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeWebSocket(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close()

	subscriptions := map[string]time.Duration{}
	lastSent := map[string]time.Time{}
	var subscriptionsMu sync.RWMutex
	done := make(chan struct{})
	var closeOnce sync.Once
	closeDone := func() {
		closeOnce.Do(func() { close(done) })
	}
	authRequired := s.auth != nil && s.auth.Enabled()
	authed := !authRequired
	if authRequired {
		if _, err := s.authorizeWSHandshake(r); err == nil {
			authed = true
		}
	}

	requireAuth := func(requestID string) bool {
		if authed {
			return true
		}
		_ = conn.WriteJSON(map[string]any{
			"type":       "error",
			"request_id": requestID,
			"code":       "unauthorized",
			"error":      "authentication required",
		})
		_ = conn.Close()
		closeDone()
		return false
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case now := <-ticker.C:
				subscriptionsMu.RLock()
				current := make(map[string]time.Duration, len(subscriptions))
				sent := make(map[string]time.Time, len(lastSent))
				for topic, interval := range subscriptions {
					current[topic] = interval
				}
				for topic, ts := range lastSent {
					sent[topic] = ts
				}
				subscriptionsMu.RUnlock()
				for topic, interval := range current {
					if interval < 10*time.Second {
						interval = 10 * time.Second
					}
					last := sent[topic]
					if !last.IsZero() && now.Sub(last) < interval {
						continue
					}
					var payload any
					switch topic {
					case "metrics":
						payload = s.engine.Metrics()
					case "stats":
						payload = s.engine.Stats()
					default:
						continue
					}
					_ = conn.WriteJSON(map[string]any{
						"type": topic,
						"ts":   now.UTC(),
						"data": payload,
					})
					subscriptionsMu.Lock()
					lastSent[topic] = now
					subscriptionsMu.Unlock()
				}
			}
		}
	}()

	for {
		message, err := conn.ReadJSON()
		if err != nil {
			closeDone()
			return
		}
		var req wsRequest
		if err := json.Unmarshal(message, &req); err != nil {
			_ = conn.WriteJSON(map[string]any{"type": "error", "error": "invalid json"})
			continue
		}
		switch strings.ToLower(strings.TrimSpace(req.Type)) {
		case "auth":
			if !authRequired {
				authed = true
				_ = conn.WriteJSON(map[string]any{"type": "authed"})
				continue
			}
			token := strings.TrimSpace(req.Token)
			if token == "" {
				_ = conn.WriteJSON(map[string]any{"type": "error", "code": "unauthorized", "error": "missing token"})
				_ = conn.Close()
				closeDone()
				return
			}
			if _, err := s.auth.Verify(token); err != nil {
				_ = conn.WriteJSON(map[string]any{"type": "error", "code": "unauthorized", "error": "invalid token"})
				_ = conn.Close()
				closeDone()
				return
			}
			authed = true
			_ = conn.WriteJSON(map[string]any{"type": "authed"})
		case "exec":
			if !requireAuth(req.RequestID) {
				return
			}
			results, err := s.executeDSL(r.Context(), req.Query)
			if err != nil {
				if errors.Is(err, errAdmissionQueueFull) || errors.Is(err, errAdmissionTimeout) {
					_ = conn.WriteJSON(map[string]any{"type": "overloaded", "request_id": req.RequestID, "error": err.Error()})
					continue
				}
				_ = conn.WriteJSON(map[string]any{"type": "error", "request_id": req.RequestID, "error": err.Error()})
				continue
			}
			_ = conn.WriteJSON(map[string]any{"type": "result", "request_id": req.RequestID, "results": results})
		case "subscribe":
			if !requireAuth(req.RequestID) {
				return
			}
			topic := strings.ToLower(strings.TrimSpace(req.Topic))
			if topic != "metrics" && topic != "stats" {
				_ = conn.WriteJSON(map[string]any{"type": "error", "error": "unsupported topic"})
				continue
			}
			interval := time.Duration(req.IntervalSeconds) * time.Second
			if interval < 10*time.Second {
				interval = 10 * time.Second
			}
			subscriptionsMu.Lock()
			subscriptions[topic] = interval
			delete(lastSent, topic)
			subscriptionsMu.Unlock()
			_ = conn.WriteJSON(map[string]any{"type": "subscribed", "topic": topic, "interval_seconds": int(interval / time.Second)})
		case "unsubscribe":
			if !requireAuth(req.RequestID) {
				return
			}
			subscriptionsMu.Lock()
			topic := strings.ToLower(strings.TrimSpace(req.Topic))
			delete(subscriptions, topic)
			delete(lastSent, topic)
			subscriptionsMu.Unlock()
			_ = conn.WriteJSON(map[string]any{"type": "unsubscribed", "topic": req.Topic})
		case "ping":
			_ = conn.WriteJSON(map[string]any{"type": "pong", "ts": time.Now().UTC()})
		default:
			_ = conn.WriteJSON(map[string]any{"type": "error", "error": "unsupported message type"})
		}
	}
}

func upgradeWebSocket(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return nil, errors.New("missing websocket upgrade")
	}
	if !strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
		return nil, errors.New("missing connection upgrade")
	}
	key := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Key"))
	if key == "" {
		return nil, errors.New("missing websocket key")
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("websocket hijack not supported")
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}
	sum := sha1.Sum([]byte(key + wsGUID))
	accept := base64.StdEncoding.EncodeToString(sum[:])
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := rw.WriteString(response); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &wsConn{conn: conn, rw: rw}, nil
}

func (c *wsConn) Close() error {
	return c.conn.Close()
}

func (c *wsConn) ReadJSON() ([]byte, error) {
	for {
		opcode, payload, err := c.readFrame()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case 0x1:
			return payload, nil
		case 0x8:
			_ = c.writeFrame(0x8, nil)
			return nil, io.EOF
		case 0x9:
			_ = c.writeFrame(0xA, payload)
		}
	}
}

func (c *wsConn) WriteJSON(value any) error {
	bytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeFrame(0x1, bytes)
}

func (c *wsConn) readFrame() (byte, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(c.rw, header); err != nil {
		return 0, nil, err
	}
	fin := header[0]&0x80 != 0
	opcode := header[0] & 0x0F
	if !fin {
		return 0, nil, errors.New("fragmented websocket frames not supported")
	}
	masked := header[1]&0x80 != 0
	payloadLen := int64(header[1] & 0x7F)
	switch payloadLen {
	case 126:
		ext := make([]byte, 2)
		if _, err := io.ReadFull(c.rw, ext); err != nil {
			return 0, nil, err
		}
		payloadLen = int64(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err := io.ReadFull(c.rw, ext); err != nil {
			return 0, nil, err
		}
		payloadLen = int64(binary.BigEndian.Uint64(ext))
	}
	var maskKey []byte
	if masked {
		maskKey = make([]byte, 4)
		if _, err := io.ReadFull(c.rw, maskKey); err != nil {
			return 0, nil, err
		}
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(c.rw, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}
	return opcode, payload, nil
}

func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
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
	if _, err := c.rw.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := c.rw.Write(payload); err != nil {
			return err
		}
	}
	return c.rw.Flush()
}
