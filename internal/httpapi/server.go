package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/paerx/anhebridgedb/internal/auth"
	"github.com/paerx/anhebridgedb/internal/db"
	"github.com/paerx/anhebridgedb/internal/dsl"
	"github.com/paerx/anhebridgedb/internal/storage"
)

type Server struct {
	engine *db.Engine
	dsl    *dsl.Executor
	auth   *auth.Manager
	mux    *http.ServeMux
}

func New(engine *db.Engine, authManager *auth.Manager) *Server {
	server := &Server{
		engine: engine,
		dsl:    dsl.New(engine),
		auth:   authManager,
		mux:    http.NewServeMux(),
	}
	server.routes()
	return server
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/auth/login", s.handleLogin)
	s.mux.Handle("/metrics/history", s.withAuth(http.HandlerFunc(s.handleMetricsHistory)))
	s.mux.Handle("/stats", s.withAuth(http.HandlerFunc(s.handleStats)))
	s.mux.Handle("/metrics", s.withAuth(http.HandlerFunc(s.handleMetrics)))
	s.mux.Handle("/debug/perf", s.withAuth(http.HandlerFunc(s.handleDebugPerf)))
	s.mux.Handle("/ws", http.HandlerFunc(s.handleWS))
	s.mux.Handle("/snapshot", s.withAuth(http.HandlerFunc(s.handleSnapshot)))
	s.mux.Handle("/admin/verify", s.withAuth(http.HandlerFunc(s.handleVerify)))
	s.mux.Handle("/admin/compact", s.withAuth(http.HandlerFunc(s.handleCompact)))
	s.mux.Handle("/admin/export", s.withAuth(http.HandlerFunc(s.handleExport)))
	s.mux.Handle("/admin/import", s.withAuth(http.HandlerFunc(s.handleImport)))
	s.mux.Handle("/dsl", s.withAuth(http.HandlerFunc(s.handleDSL)))
	s.mux.Handle("/rules", s.withAuth(http.HandlerFunc(s.handleRules)))
	s.mux.Handle("/rules/", s.withAuth(http.HandlerFunc(s.handleRule)))
	s.mux.Handle("/tasks", s.withAuth(http.HandlerFunc(s.handleTasks)))
	s.mux.Handle("/kv/", s.withAuth(http.HandlerFunc(s.handleKV)))
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) withAuth(next http.Handler) http.Handler {
	if s.auth == nil || !s.auth.Enabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := s.authorizeRequest(r); err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) authorizeRequest(r *http.Request) (auth.Claims, error) {
	if s.auth == nil || !s.auth.Enabled() {
		return auth.Claims{}, nil
	}
	if token := strings.TrimSpace(r.URL.Query().Get("token")); token != "" {
		return s.auth.Verify(token)
	}
	token, err := auth.BearerToken(r.Header.Get("Authorization"))
	if err != nil {
		return auth.Claims{}, err
	}
	return s.auth.Verify(token)
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.auth == nil || !s.auth.Enabled() {
		http.Error(w, "auth disabled", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	token, claims, err := s.auth.Login(strings.TrimSpace(req.Username), req.Password)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"token":      token,
		"username":   claims.Username,
		"issued_at":  claims.IssuedAt,
		"expires_at": claims.ExpiresAt,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.engine.Stats())
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.engine.Metrics())
}

func (s *Server) handleMetricsHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	metricsRaw := strings.TrimSpace(r.URL.Query().Get("metrics"))
	if metricsRaw == "" {
		http.Error(w, "metrics query is required", http.StatusBadRequest)
		return
	}
	metrics := make([]string, 0)
	for _, part := range strings.Split(metricsRaw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			metrics = append(metrics, part)
		}
	}
	from, to, err := storage.ParseMetricHistoryWindow(r.URL.Query().Get("from"), r.URL.Query().Get("to"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	bucket := 10 * time.Second
	if raw := strings.TrimSpace(r.URL.Query().Get("bucket")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			http.Error(w, "invalid bucket", http.StatusBadRequest)
			return
		}
		if parsed < 10*time.Second {
			http.Error(w, "bucket must be >= 10s", http.StatusBadRequest)
			return
		}
		bucket = parsed
	}
	series, err := s.engine.MetricsHistory(metrics, from, to, bucket)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"from":    from,
		"to":      to,
		"bucket":  bucket.String(),
		"metrics": series,
	})
}

func (s *Server) handleDebugPerf(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.engine.DebugPerf())
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	snapshot, err := s.engine.Snapshot()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, snapshot)
}

func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	report, err := s.engine.VerifyStorage()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	report, err := s.engine.CompactStorage()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	scope := strings.TrimSpace(r.URL.Query().Get("scope"))
	if scope == "" {
		scope = "full"
	}
	var segments []string
	if raw := strings.TrimSpace(r.URL.Query().Get("segments")); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				segments = append(segments, filepath.Base(part))
			}
		}
	}

	filename := fmt.Sprintf("anhebridgedb-%s-%s.tar.gz", scope, time.Now().UTC().Format("20060102T150405Z"))
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	if err := storage.ExportBackup(w, s.engine.DataDir(), scope, segments); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (s *Server) handleImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	scope := strings.TrimSpace(r.URL.Query().Get("scope"))
	if scope == "" {
		scope = "full"
	}
	defer r.Body.Close()
	report, err := storage.ImportBackup(r.Body, s.engine.DataDir(), scope)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusCreated, report)
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	if strings.HasSuffix(key, "/timeline") {
		s.handleTimeline(w, r, strings.TrimSuffix(key, "/timeline"))
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	defer r.Body.Close()

	var payload json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}

	event, err := s.engine.Set(key, payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, event)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	timeParam := r.URL.Query().Get("time")

	var (
		record db.Record
		err    error
	)
	if timeParam == "" {
		record, err = s.engine.Get(key)
	} else {
		at, parseErr := time.Parse(time.RFC3339, timeParam)
		if parseErr != nil {
			http.Error(w, "time must be RFC3339", http.StatusBadRequest)
			return
		}
		record, err = s.engine.GetAt(key, at.UTC())
	}

	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"key":        key,
		"value":      json.RawMessage(record.Value),
		"version":    record.Version,
		"updated_at": record.UpdatedAt,
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	event, err := s.engine.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, event)
}

func (s *Server) handleTimeline(w http.ResponseWriter, r *http.Request, key string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	withDiff := r.URL.Query().Get("diff") == "true" || r.URL.Query().Get("diff") == "1"
	limit, before, after, parseErr := parseWindowQuery(r)
	if parseErr != nil {
		http.Error(w, parseErr.Error(), http.StatusBadRequest)
		return
	}
	timeline, err := s.engine.TimelineWindow(key, withDiff, limit, before, after)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"key":      key,
		"timeline": timeline,
	})
}

func parseWindowQuery(r *http.Request) (int, uint64, uint64, error) {
	q := r.URL.Query()
	limit := 0
	before := uint64(0)
	after := uint64(0)
	if raw := q.Get("limit"); raw != "" {
		if _, err := fmt.Sscanf(raw, "%d", &limit); err != nil {
			return 0, 0, 0, errors.New("invalid limit")
		}
	}
	if raw := q.Get("before_version"); raw != "" {
		if _, err := fmt.Sscanf(raw, "%d", &before); err != nil {
			return 0, 0, 0, errors.New("invalid before_version")
		}
	}
	if raw := q.Get("after_version"); raw != "" {
		if _, err := fmt.Sscanf(raw, "%d", &after); err != nil {
			return 0, 0, 0, errors.New("invalid after_version")
		}
	}
	return limit, before, after, nil
}

func (s *Server) handleRules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"rules": s.engine.ListRules()})
	case http.MethodPost:
		defer r.Body.Close()
		var spec db.RuleSpec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		rule, err := s.engine.CreateRule(spec)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusCreated, rule)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleRule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/rules/")
	rule, err := s.engine.GetRule(id)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, rule)
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]any{"tasks": s.engine.ListTasks()})
		return
	}
	if r.Method == http.MethodPost {
		stats, err := s.engine.ProcessDueTasks(time.Now().UTC())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, stats)
		return
	}
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func (s *Server) handleDSL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var body struct {
		Query string `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Query) == "" {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}

	results, err := s.dsl.Execute(body.Query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
