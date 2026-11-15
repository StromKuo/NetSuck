package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

//go:embed web/index.html
var indexHTML []byte

type server struct {
	cfgMu        sync.RWMutex
	currentCfg   *Config
	configPath   string
	engine       *Engine
	store        *Store
	engineCtx    context.Context
	shuttingDown atomic.Bool
}

func newServer(cfg *Config, configPath string, engine *Engine, store *Store, engineCtx context.Context) *server {
	return &server{
		currentCfg: cfg,
		configPath: configPath,
		engine:     engine,
		store:      store,
		engineCtx:  engineCtx,
	}
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/status", s.handleGetStatus)
	mux.HandleFunc("GET /api/targets", s.handleGetTargets)
	mux.HandleFunc("POST /api/targets", s.handlePostTargets)
	mux.HandleFunc("PUT /api/targets/{id}", s.handlePutTarget)
	mux.HandleFunc("DELETE /api/targets/{id}", s.handleDeleteTarget)
	mux.HandleFunc("POST /api/targets/check", s.handlePostCheck)
	mux.HandleFunc("GET /api/history", s.handleGetHistory)
	mux.HandleFunc("POST /api/control", s.handlePostControl)

	mux.HandleFunc("GET /{$}", s.handleIndex)
	mux.HandleFunc("GET /index.html", s.handleIndex)

	return s.wrapMiddleware(mux)
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(indexHTML)
}

func (s *server) handleGetStatus(w http.ResponseWriter, r *http.Request) {
	// 版本号由构建期注入，不属于引擎状态，因此在此处叠加一层，不污染引擎契约。
	// 匿名嵌入保证 EngineSnapshot 的既有字段名与类型原样输出，仅多出 version。
	writeJSON(w, http.StatusOK, struct {
		EngineSnapshot
		Version string `json:"version"`
	}{
		EngineSnapshot: s.engine.Snapshot(),
		Version:        buildVersion,
	})
}

func (s *server) handleGetTargets(w http.ResponseWriter, r *http.Request) {
	snap := s.engine.Snapshot()
	targets := snap.Targets
	if targets == nil {
		targets = []TargetSnapshot{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"targets": targets})
}

func (s *server) handlePostTargets(w http.ResponseWriter, r *http.Request) {
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&raw); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}

	nameRaw, ok1 := raw["name"]
	urlRaw, ok2 := raw["url"]
	weightRaw, ok3 := raw["weight"]
	enabledRaw, ok4 := raw["enabled"]
	if !ok1 || !ok2 || !ok3 || !ok4 {
		writeJSONError(w, http.StatusBadRequest, "missing required fields: name, url, weight, enabled")
		return
	}

	for k, v := range raw {
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			writeJSONError(w, http.StatusBadRequest, "field "+k+" cannot be null")
			return
		}
		switch k {
		case "name", "url", "weight", "enabled":
		default:
			writeJSONError(w, http.StatusBadRequest, "unknown field: "+k)
			return
		}
	}

	var name string
	if err := json.Unmarshal(nameRaw, &name); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid name")
		return
	}
	var u string
	if err := json.Unmarshal(urlRaw, &u); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid url")
		return
	}
	var weight int
	if err := json.Unmarshal(weightRaw, &weight); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid weight")
		return
	}
	var enabled bool
	if err := json.Unmarshal(enabledRaw, &enabled); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid enabled")
		return
	}

	if err := ValidateTargetInput(name, u, weight); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	patch := TargetPatch{
		Name:    &name,
		URL:     &u,
		Weight:  &weight,
		Enabled: &enabled,
	}

	rec, err := s.engine.CreateTarget(patch)
	if err != nil {
		if errors.Is(err, ErrDuplicateURL) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, ErrValidation) {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{"ok": true, "id": rec.ID})
}

func (s *server) handlePutTarget(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid target id")
		return
	}

	var raw map[string]json.RawMessage
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&raw); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if len(raw) == 0 {
		writeJSONError(w, http.StatusBadRequest, "empty patch object")
		return
	}

	for k, v := range raw {
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			writeJSONError(w, http.StatusBadRequest, "field "+k+" cannot be null")
			return
		}
		switch k {
		case "name", "url", "weight", "enabled":
		default:
			writeJSONError(w, http.StatusBadRequest, "unknown field: "+k)
			return
		}
	}

	var patch TargetPatch
	if v, ok := raw["name"]; ok {
		var name string
		if err := json.Unmarshal(v, &name); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid name")
			return
		}
		if strings.TrimSpace(name) == "" {
			writeJSONError(w, http.StatusBadRequest, "name 不能为空")
			return
		}
		patch.Name = &name
	}
	if v, ok := raw["url"]; ok {
		var u string
		if err := json.Unmarshal(v, &u); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid url")
			return
		}
		if err := ValidateTargetInput("valid", u, 1); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		patch.URL = &u
	}
	if v, ok := raw["weight"]; ok {
		var weight int
		if err := json.Unmarshal(v, &weight); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid weight")
			return
		}
		if weight < 1 {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("weight 必须 >= 1，当前为 %d", weight))
			return
		}
		patch.Weight = &weight
	}
	if v, ok := raw["enabled"]; ok {
		var enabled bool
		if err := json.Unmarshal(v, &enabled); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid enabled")
			return
		}
		patch.Enabled = &enabled
	}

	_, err = s.engine.PatchTarget(id, patch)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, ErrDuplicateURL) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, ErrValidation) {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) handleDeleteTarget(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid target id")
		return
	}

	if err := s.engine.DeleteTarget(id); err != nil {
		if errors.Is(err, ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) handlePostCheck(w http.ResponseWriter, r *http.Request) {
	go func() {
		_ = s.engine.CheckAllNow(s.engineCtx)
	}()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	points, err := s.engine.History24h(time.Now())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if points == nil {
		points = []HistoryPoint{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"history": points})
}

func (s *server) handlePostControl(w http.ResponseWriter, r *http.Request) {
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&raw); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if len(raw) == 0 {
		writeJSONError(w, http.StatusBadRequest, "empty control request")
		return
	}

	for k, v := range raw {
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			writeJSONError(w, http.StatusBadRequest, "field "+k+" cannot be null")
			return
		}
		switch k {
		case "action", "max_rate_mbps", "max_connections", "daily_limit_gb", "storage_mode", "schedule":
		default:
			writeJSONError(w, http.StatusBadRequest, "unknown field: "+k)
			return
		}
	}

	actionRaw, ok := raw["action"]
	if !ok {
		writeJSONError(w, http.StatusBadRequest, "missing action")
		return
	}
	var action string
	if err := json.Unmarshal(actionRaw, &action); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid action")
		return
	}

	switch action {
	case "pause":
		if len(raw) > 1 {
			writeJSONError(w, http.StatusBadRequest, "pause action does not accept update fields")
			return
		}
		if err := s.engine.Pause(); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case "resume":
		if len(raw) > 1 {
			writeJSONError(w, http.StatusBadRequest, "resume action does not accept update fields")
			return
		}
		if err := s.engine.Resume(); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case "update":
		s.handleControlUpdate(w, raw)

	default:
		writeJSONError(w, http.StatusBadRequest, "invalid action: "+action)
	}
}

func (s *server) handleControlUpdate(w http.ResponseWriter, raw map[string]json.RawMessage) {
	if len(raw) == 1 {
		writeJSONError(w, http.StatusBadRequest, "no update fields provided")
		return
	}

	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()

	candidate := deepCopyConfig(s.currentCfg)

	if v, ok := raw["max_rate_mbps"]; ok {
		var val int
		if err := json.Unmarshal(v, &val); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid max_rate_mbps")
			return
		}
		if err := ValidateMaxRateMbps(val); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		candidate.MaxRateMbps = val
	}

	if v, ok := raw["max_connections"]; ok {
		var val int
		if err := json.Unmarshal(v, &val); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid max_connections")
			return
		}
		if err := ValidateMaxConnections(val); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		candidate.MaxConnections = val
	}

	if v, ok := raw["daily_limit_gb"]; ok {
		var val float64
		if err := json.Unmarshal(v, &val); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid daily_limit_gb")
			return
		}
		if err := ValidateDailyLimitGB(val); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		candidate.DailyLimitGB = val
	}

	if v, ok := raw["storage_mode"]; ok {
		var val string
		if err := json.Unmarshal(v, &val); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid storage_mode")
			return
		}
		if err := ValidateStorageMode(val); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		candidate.StorageMode = val
	}

	if v, ok := raw["schedule"]; ok {
		var rawSched map[string]json.RawMessage
		if err := json.Unmarshal(v, &rawSched); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid schedule")
			return
		}
		for sk, sv := range rawSched {
			if bytes.Equal(bytes.TrimSpace(sv), []byte("null")) {
				writeJSONError(w, http.StatusBadRequest, "field "+sk+" in schedule cannot be null")
				return
			}
			switch sk {
			case "start_hour", "end_hour":
			default:
				writeJSONError(w, http.StatusBadRequest, "unknown field in schedule: "+sk)
				return
			}
		}

		type scheduleDTO struct {
			StartHour *int `json:"start_hour"`
			EndHour   *int `json:"end_hour"`
		}
		var dto scheduleDTO
		dec := json.NewDecoder(bytes.NewReader(v))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&dto); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid schedule: "+err.Error())
			return
		}
		if dto.StartHour == nil || dto.EndHour == nil {
			writeJSONError(w, http.StatusBadRequest, "schedule requires both start_hour and end_hour")
			return
		}

		val := ScheduleConfig{StartHour: *dto.StartHour, EndHour: *dto.EndHour}
		if err := ValidateSchedule(val); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		candidate.Schedule = val
	}

	if err := candidate.Validate(); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	var persistErr error
	err := s.engine.ApplyConfigUpdate(candidate, func() error {
		persistErr = candidate.Save(s.configPath)
		return persistErr
	})
	if err != nil {
		if persistErr != nil {
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("persist config failed: %v", persistErr))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.currentCfg = candidate
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type captureWriter struct {
	http.ResponseWriter
	statusCode    int
	buf           bytes.Buffer
	headerWritten bool
}

func (w *captureWriter) WriteHeader(code int) {
	if !w.headerWritten {
		w.statusCode = code
		w.headerWritten = true
	}
}

func (w *captureWriter) Write(b []byte) (int, error) {
	if !w.headerWritten {
		w.WriteHeader(http.StatusOK)
	}
	return w.buf.Write(b)
}

func (s *server) wrapMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.shuttingDown.Load() {
			w.Header().Set("Connection", "close")
			writeJSONError(w, http.StatusServiceUnavailable, "server is shutting down")
			return
		}

		isAPI := strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/api"

		if !isAPI {
			next.ServeHTTP(w, r)
			return
		}

		cw := &captureWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(cw, r)

		if cw.statusCode == http.StatusMethodNotAllowed {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
			return
		}
		if cw.statusCode == http.StatusNotFound && !strings.Contains(cw.buf.String(), `"error"`) {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
			return
		}

		w.WriteHeader(cw.statusCode)
		_, _ = w.Write(cw.buf.Bytes())
	})
}

func writeJSON(w http.ResponseWriter, code int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(data)
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func isValidationError(err error) bool {
	return errors.Is(err, ErrValidation)
}

func deepCopyConfig(src *Config) *Config {
	if src == nil {
		return nil
	}
	cp := *src
	if src.Targets != nil {
		cp.Targets = make([]TargetConfig, len(src.Targets))
		copy(cp.Targets, src.Targets)
	}
	return &cp
}

func ensureConfigFile(targetPath string) error {
	if _, err := os.Stat(targetPath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat config file %q: %w", targetPath, err)
	}

	parentDir := filepath.Dir(targetPath)
	if parentDir != "" && parentDir != "." {
		if err := os.MkdirAll(parentDir, 0755); err != nil {
			return fmt.Errorf("create config parent directory %q: %w", parentDir, err)
		}
	}

	srcPath := "/app/config.example.yaml"
	if _, err := os.Stat(srcPath); os.IsNotExist(err) {
		log.Printf("example config not found at %s; trying local fallback config.example.yaml", srcPath)
		srcPath = "config.example.yaml"
		if _, err := os.Stat(srcPath); os.IsNotExist(err) {
			return fmt.Errorf("neither /app/config.example.yaml nor config.example.yaml exists")
		}
	}

	log.Printf("copying example config from %s to %s", srcPath, targetPath)
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open example config %q: %w", srcPath, err)
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if os.IsExist(err) {
			return nil
		}
		return fmt.Errorf("create destination config file %q: %w", targetPath, err)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return fmt.Errorf("copy config content: %w", err)
	}
	return nil
}

// buildVersion 由构建时通过 -ldflags "-X main.buildVersion=..." 注入（CI 使用 git tag）。
var buildVersion = "dev"

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)

	configFlag := flag.String("config", "/data/config.yaml", "path to config file")
	versionFlag := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *versionFlag {
		fmt.Printf("netsuck %s\n", buildVersion)
		return
	}

	configPath := *configFlag

	log.Printf("NetSuck %s starting, config path: %s", buildVersion, configPath)

	if err := ensureConfigFile(configPath); err != nil {
		log.Fatalf("failed to ensure config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("failed to load config from %s: %v", configPath, err)
	}

	log.Printf("Config loaded successfully: listen=%s, storage=%s, rate=%d Mbps, conns=%d, daily_limit=%.2f GB",
		cfg.ListenAddr, cfg.StorageMode, cfg.MaxRateMbps, cfg.MaxConnections, cfg.DailyLimitGB)

	dbPath := filepath.Join(filepath.Dir(configPath), "stats.db")
	log.Printf("Opening store at %s", dbPath)
	store, err := OpenStore(dbPath)
	if err != nil {
		log.Fatalf("failed to open store at %s: %v", dbPath, err)
	}

	if err := store.InitSchema(); err != nil {
		log.Fatalf("failed to init store schema: %v", err)
	}

	if err := store.EnsureSeeded(cfg); err != nil {
		log.Fatalf("failed to ensure store seeded: %v", err)
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:      10,
			MaxConnsPerHost:   1,
			IdleConnTimeout:   90 * time.Second,
			ForceAttemptHTTP2: false,
		},
	}

	engine, err := NewEngine(cfg, store, httpClient)
	if err != nil {
		log.Fatalf("failed to initialize engine: %v", err)
	}

	engineCtx, engineCancel := context.WithCancel(context.Background())
	defer engineCancel()

	log.Printf("Starting engine...")
	if err := engine.Start(engineCtx); err != nil {
		log.Fatalf("failed to start engine: %v", err)
	}

	srv := newServer(cfg, configPath, engine, store, engineCtx)
	httpServer := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: srv.routes(),
	}

	serverErrCh := make(chan error, 1)
	go func() {
		log.Printf("HTTP server listening on %s", cfg.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	var exitCode int
	select {
	case sig := <-sigCh:
		log.Printf("Received signal %s, initiating graceful shutdown...", sig)
	case err := <-serverErrCh:
		log.Printf("HTTP server exited with error: %v", err)
		exitCode = 1
	}

	srv.shuttingDown.Store(true)

	log.Printf("Step 1/3: shutting down HTTP server...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error shutting down HTTP server: %v", err)
		exitCode = 1
	}

	log.Printf("Step 2/3: stopping engine...")
	if err := engine.Stop(); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("Error stopping engine: %v", err)
		exitCode = 1
	}

	log.Printf("Step 3/3: closing database store...")
	if err := store.Close(); err != nil {
		log.Printf("Error closing database store: %v", err)
		exitCode = 1
	}

	if exitCode != 0 {
		log.Printf("Graceful shutdown finished with errors, exiting with code %d", exitCode)
		os.Exit(exitCode)
	}

	log.Printf("NetSuck stopped cleanly")
}
