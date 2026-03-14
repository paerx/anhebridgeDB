package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"anhebridgedb/internal/db"
	"anhebridgedb/internal/dsl"
)

type Server struct {
	engine *db.Engine
	dsl    *dsl.Executor
	mux    *http.ServeMux
}

func New(engine *db.Engine) *Server {
	server := &Server{
		engine: engine,
		dsl:    dsl.New(engine),
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
	s.mux.HandleFunc("/stats", s.handleStats)
	s.mux.HandleFunc("/snapshot", s.handleSnapshot)
	s.mux.HandleFunc("/dsl", s.handleDSL)
	s.mux.HandleFunc("/rules", s.handleRules)
	s.mux.HandleFunc("/rules/", s.handleRule)
	s.mux.HandleFunc("/tasks", s.handleTasks)
	s.mux.HandleFunc("/kv/", s.handleKV)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.engine.Stats())
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
	timeline, err := s.engine.Timeline(key, withDiff)
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
