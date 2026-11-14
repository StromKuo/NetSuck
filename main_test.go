package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func setupTestServer(t *testing.T) (*server, string) {
	t.Helper()
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:18080"
	cfg.DiskDir = filepath.Join(tempDir, "downloads")
	cfg.MemoryDir = filepath.Join(tempDir, "cache")
	if err := cfg.Save(configPath); err != nil {
		t.Fatalf("save config: %v", err)
	}

	dbPath := filepath.Join(tempDir, "stats.db")
	store, err := OpenStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	if err := store.EnsureSeeded(cfg); err != nil {
		t.Fatalf("ensure seeded: %v", err)
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
		t.Fatalf("new engine: %v", err)
	}

	engineCtx, engineCancel := context.WithCancel(context.Background())
	if err := engine.Start(engineCtx); err != nil {
		engineCancel()
		t.Fatalf("engine start: %v", err)
	}
	t.Cleanup(func() {
		_ = engine.Stop()
		engineCancel()
	})

	srv := newServer(cfg, configPath, engine, store, engineCtx)
	return srv, configPath
}

func TestEmbedIndexHTMLNonEmpty(t *testing.T) {
	if len(indexHTML) == 0 {
		t.Fatal("expected indexHTML to be non-empty")
	}
}

func TestIndexAnd404Routes(t *testing.T) {
	srv, _ := setupTestServer(t)
	handler := srv.routes()

	// GET /
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("expected Content-Type text/html; charset=utf-8, got %q", ct)
	}
	if !bytes.Equal(rec.Body.Bytes(), indexHTML) {
		t.Errorf("body does not match indexHTML")
	}

	// GET /index.html
	req = httptest.NewRequest(http.MethodGet, "/index.html", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /index.html expected 200, got %d", rec.Code)
	}

	// GET /api/nonexistent -> 404 JSON
	req = httptest.NewRequest(http.MethodGet, "/api/nonexistent", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /api/nonexistent expected 404, got %d", rec.Code)
	}
	var errResp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil || errResp["error"] == "" {
		t.Fatalf("expected JSON error response, got %s", rec.Body.String())
	}

	// POST /api/status -> 405 Method Not Allowed with Allow header
	req = httptest.NewRequest(http.MethodPost, "/api/status", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /api/status expected 405, got %d", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); allow == "" {
		t.Fatalf("expected Allow header on 405 response")
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil || errResp["error"] == "" {
		t.Fatalf("expected JSON error response on 405, got %s", rec.Body.String())
	}
}

func TestStatusAndHistoryEndpoints(t *testing.T) {
	srv, _ := setupTestServer(t)
	handler := srv.routes()

	// GET /api/status
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/status expected 200, got %d", rec.Code)
	}
	var statusMap map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &statusMap); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if statusMap["status"] != "running" && statusMap["status"] != "sleeping_schedule" {
		t.Errorf("unexpected status: %v", statusMap["status"])
	}
	if _, ok := statusMap["targets"]; ok {
		t.Errorf("Targets should have json:\"-\" and not appear in status")
	}
	if _, ok := statusMap["snapshot_at"]; ok {
		t.Errorf("SnapshotAt should have json:\"-\" and not appear in status")
	}

	// GET /api/history
	req = httptest.NewRequest(http.MethodGet, "/api/history", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/history expected 200, got %d", rec.Code)
	}
	var historyMap map[string][]HistoryPoint
	if err := json.Unmarshal(rec.Body.Bytes(), &historyMap); err != nil {
		t.Fatalf("unmarshal history: %v", err)
	}
	if len(historyMap["history"]) != 24 {
		t.Errorf("expected 24 history points, got %d", len(historyMap["history"]))
	}
}

func TestTargetsCRUD(t *testing.T) {
	srv, _ := setupTestServer(t)
	handler := srv.routes()

	// GET /api/targets initial
	req := httptest.NewRequest(http.MethodGet, "/api/targets", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/targets expected 200, got %d", rec.Code)
	}

	// POST /api/targets - missing fields
	body := []byte(`{"name":"test"}`)
	req = httptest.NewRequest(http.MethodPost, "/api/targets", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing fields, got %d", rec.Code)
	}

	// POST /api/targets - null field
	body = []byte(`{"name":"test","url":null,"weight":10,"enabled":true}`)
	req = httptest.NewRequest(http.MethodPost, "/api/targets", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for null field, got %d", rec.Code)
	}

	// POST /api/targets - invalid URL
	body = []byte(`{"name":"test","url":"ftp://example.com/file","weight":10,"enabled":true}`)
	req = httptest.NewRequest(http.MethodPost, "/api/targets", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid URL scheme, got %d", rec.Code)
	}

	// POST /api/targets - valid creation
	body = []byte(`{"name":"target1","url":"https://example.com/file1.bin","weight":15,"enabled":true}`)
	req = httptest.NewRequest(http.MethodPost, "/api/targets", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 for valid target creation, got %d (%s)", rec.Code, rec.Body.String())
	}
	var createResp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &createResp); err != nil || createResp["id"] == nil {
		t.Fatalf("expected create response with id: %v", rec.Body.String())
	}
	targetID := int64(createResp["id"].(float64))

	// POST /api/targets - duplicate URL -> 409
	body = []byte(`{"name":"target1_dup","url":"https://example.com/file1.bin","weight":10,"enabled":true}`)
	req = httptest.NewRequest(http.MethodPost, "/api/targets", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 for duplicate URL, got %d (%s)", rec.Code, rec.Body.String())
	}

	// PUT /api/targets/{id} - empty object -> 400
	req = httptest.NewRequest(http.MethodPut, fmt.Sprintf("/api/targets/%d", targetID), bytes.NewReader([]byte(`{}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty patch object, got %d", rec.Code)
	}

	// PUT /api/targets/{id} - explicit null -> 400
	req = httptest.NewRequest(http.MethodPut, fmt.Sprintf("/api/targets/%d", targetID), bytes.NewReader([]byte(`{"name":null}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for explicit null, got %d", rec.Code)
	}

	// PUT /api/targets/{id} - not found id -> 404
	req = httptest.NewRequest(http.MethodPut, "/api/targets/999999", bytes.NewReader([]byte(`{"name":"updated"}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nonexistent target, got %d", rec.Code)
	}

	// PUT /api/targets/{id} - valid patch
	req = httptest.NewRequest(http.MethodPut, fmt.Sprintf("/api/targets/%d", targetID), bytes.NewReader([]byte(`{"name":"updated_name","weight":20}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for valid patch, got %d (%s)", rec.Code, rec.Body.String())
	}

	// POST /api/targets/check -> 200 ok
	req = httptest.NewRequest(http.MethodPost, "/api/targets/check", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for targets check, got %d", rec.Code)
	}

	// DELETE /api/targets/{id} - valid delete -> 200
	req = httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/targets/%d", targetID), nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for delete target, got %d", rec.Code)
	}

	// DELETE /api/targets/{id} - deleted target not found -> 404
	req = httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/targets/%d", targetID), nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for already deleted target, got %d", rec.Code)
	}
}

func TestControlEndpoints(t *testing.T) {
	srv, configPath := setupTestServer(t)
	handler := srv.routes()

	// Pause
	req := httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"pause"}`)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for pause, got %d (%s)", rec.Code, rec.Body.String())
	}

	snap := srv.engine.Snapshot()
	if snap.Status != "paused" {
		t.Fatalf("expected status to be paused, got %s", snap.Status)
	}

	// Resume
	req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"resume"}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for resume, got %d (%s)", rec.Code, rec.Body.String())
	}

	// Update with invalid value -> 400
	req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","max_rate_mbps":-5}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for negative rate, got %d", rec.Code)
	}

	// Update with valid values
	req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","max_rate_mbps":100,"max_connections":5,"daily_limit_gb":50.5}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for valid update, got %d (%s)", rec.Code, rec.Body.String())
	}

	snap = srv.engine.Snapshot()
	if snap.MaxRateMbps != 100 || snap.MaxConnections != 5 || snap.DailyLimitGB != 50.5 {
		t.Errorf("snapshot not updated: rate=%d, conns=%d, limit=%f", snap.MaxRateMbps, snap.MaxConnections, snap.DailyLimitGB)
	}

	// Verify persistence
	loaded, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if loaded.MaxRateMbps != 100 || loaded.MaxConnections != 5 || loaded.DailyLimitGB != 50.5 {
		t.Errorf("persisted config mismatch: rate=%d, conns=%d, limit=%f", loaded.MaxRateMbps, loaded.MaxConnections, loaded.DailyLimitGB)
	}

	// Update with persist failure (read-only dir / invalid configPath)
	srv.configPath = "/dev/null/cannot/save/here.yaml"
	req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","max_rate_mbps":200}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when persist fails, got %d (%s)", rec.Code, rec.Body.String())
	}

	// Verify running state unchanged after persist failure
	snap = srv.engine.Snapshot()
	if snap.MaxRateMbps != 100 {
		t.Errorf("running rate should not have changed on persist failure, got %d", snap.MaxRateMbps)
	}
}

func TestEnsureConfigFile(t *testing.T) {
	tempDir := t.TempDir()
	targetPath := filepath.Join(tempDir, "sub", "config.yaml")

	// Target doesn't exist -> copies fallback
	if err := ensureConfigFile(targetPath); err != nil {
		t.Fatalf("ensureConfigFile: %v", err)
	}

	content, err := os.ReadFile(targetPath)
	if err != nil || len(content) == 0 {
		t.Fatalf("failed to read created config: %v", err)
	}

	// Target already exists -> does not overwrite
	marker := []byte("# modified content")
	if err := os.WriteFile(targetPath, marker, 0644); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	if err := ensureConfigFile(targetPath); err != nil {
		t.Fatalf("ensureConfigFile existing: %v", err)
	}
	after, err := os.ReadFile(targetPath)
	if err != nil || !bytes.Equal(after, marker) {
		t.Fatalf("ensureConfigFile should not overwrite existing file")
	}
}
