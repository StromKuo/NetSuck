package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
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

	// Update with storage directory precheck failure -> 500
	srv.configPath = configPath // restore valid config path
	srv.engine.cfg.DiskDir = "/dev/null/cannot/create/dir"
	req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","storage_mode":"disk"}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when precheckStorageDir fails, got %d (%s)", rec.Code, rec.Body.String())
	}
}

func TestControlConcurrentUpdates(t *testing.T) {
	srv, configPath := setupTestServer(t)
	handler := srv.routes()

	var wg sync.WaitGroup
	wg.Add(2)

	errCh := make(chan error, 2)

	// Goroutine 1: update max_rate_mbps
	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","max_rate_mbps":250}`)))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			errCh <- fmt.Errorf("update rate failed: code %d, body %s", rec.Code, rec.Body.String())
		}
	}()

	// Goroutine 2: update daily_limit_gb
	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","daily_limit_gb":75.0}`)))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			errCh <- fmt.Errorf("update quota failed: code %d, body %s", rec.Code, rec.Body.String())
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// 断言两者最终都保留
	snap := srv.engine.Snapshot()
	if snap.MaxRateMbps != 250 {
		t.Errorf("expected MaxRateMbps=250, got %d", snap.MaxRateMbps)
	}
	if snap.DailyLimitGB != 75.0 {
		t.Errorf("expected DailyLimitGB=75.0, got %f", snap.DailyLimitGB)
	}

	loaded, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if loaded.MaxRateMbps != 250 {
		t.Errorf("persisted MaxRateMbps mismatch: want 250, got %d", loaded.MaxRateMbps)
	}
	if loaded.DailyLimitGB != 75.0 {
		t.Errorf("persisted DailyLimitGB mismatch: want 75.0, got %f", loaded.DailyLimitGB)
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

func TestValidationErrorClassification(t *testing.T) {
	// 场景 1：包装错误（wrapped error）—— 使用 fmt.Errorf("%w: ...", ErrValidation) 包装的错误必须被识别为校验错误 (400)
	wrappedErr := fmt.Errorf("target validation failed: %w", ErrValidation)
	if !isValidationError(wrappedErr) {
		t.Errorf("expected wrappedErr to be recognized as validation error")
	}
	if !errors.Is(wrappedErr, ErrValidation) {
		t.Errorf("expected errors.Is(wrappedErr, ErrValidation) to be true")
	}

	// 场景 2：文案变更（message change）—— 不包含任何旧中文关键词的英文/自定义提示，但携带 ErrValidation 哨兵，必须被识别为校验错误 (400)
	customMsgErr := fmt.Errorf("%w: invalid parameter format without legacy keywords", ErrValidation)
	if !isValidationError(customMsgErr) {
		t.Errorf("expected customMsgErr to be recognized as validation error")
	}
	if !errors.Is(customMsgErr, ErrValidation) {
		t.Errorf("expected errors.Is(customMsgErr, ErrValidation) to be true")
	}

	// 场景 3：带敏感词的内部错误（internal error with keywords）—— 包含“非法”、“不能为空”、“解析失败”等敏感词但未包装 ErrValidation 的内部数据库或系统错误，绝对不能被判定为校验错误（防止 500 误报成 400）
	internalErrWithKeywords := errors.New("database disk corruption: table targets 列名非法 且字段不能为空 解析失败")
	if isValidationError(internalErrWithKeywords) {
		t.Errorf("expected internal error with keywords to NOT be recognized as validation error")
	}
	if errors.Is(internalErrWithKeywords, ErrValidation) {
		t.Errorf("expected errors.Is(internalErrWithKeywords, ErrValidation) to be false")
	}

	if isValidationError(nil) {
		t.Errorf("nil error should not be recognized as validation error")
	}
}

func TestControlUpdateSchedule(t *testing.T) {
	srv, configPath := setupTestServer(t)
	handler := srv.routes()

	// 1. 成功更新为跨夜时段
	req := httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","schedule":{"start_hour":23,"end_hour":7}}`)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for valid schedule update, got %d (%s)", rec.Code, rec.Body.String())
	}

	snap := srv.engine.Snapshot()
	if snap.Schedule.StartHour != 23 || snap.Schedule.EndHour != 7 {
		t.Errorf("snapshot schedule mismatch: got %+v", snap.Schedule)
	}

	loaded, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if loaded.Schedule.StartHour != 23 || loaded.Schedule.EndHour != 7 {
		t.Errorf("persisted schedule mismatch: got %+v", loaded.Schedule)
	}

	// 2. 成功更新为全天时段
	req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","schedule":{"start_hour":0,"end_hour":24}}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for 0-24 schedule update, got %d (%s)", rec.Code, rec.Body.String())
	}
	snap = srv.engine.Snapshot()
	if snap.Schedule.StartHour != 0 || snap.Schedule.EndHour != 24 {
		t.Errorf("snapshot schedule mismatch: got %+v", snap.Schedule)
	}

	// 3. 非法 schedule：start_hour 超出 [0, 23]
	req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","schedule":{"start_hour":-1,"end_hour":8}}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for start_hour < 0, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","schedule":{"start_hour":24,"end_hour":8}}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for start_hour > 23, got %d", rec.Code)
	}

	// 4. 非法 schedule：end_hour 超出 [0, 24]
	req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","schedule":{"start_hour":8,"end_hour":25}}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for end_hour > 24, got %d", rec.Code)
	}

	// 5. 非法 schedule：start == end
	req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","schedule":{"start_hour":9,"end_hour":9}}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for start == end, got %d", rec.Code)
	}

	// 6. 非法类型与 null
	req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","schedule":"invalid"}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-object schedule, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","schedule":null}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for null schedule, got %d", rec.Code)
	}

	// 7. 子字段为 null、缺失或包含未知字段 -> 均必须返回 400，且运行态与持久化配置文件保持不变
	// 先将 schedule 设置为基准值 23→7
	req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(`{"action":"update","schedule":{"start_hour":23,"end_hour":7}}`)))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("setup baseline schedule failed: %d", rec.Code)
	}

	invalidPayloads := []string{
		`{"action":"update","schedule":{"start_hour":null,"end_hour":7}}`,
		`{"action":"update","schedule":{"start_hour":23,"end_hour":null}}`,
		`{"action":"update","schedule":{"start_hour":null,"end_hour":null}}`,
		`{"action":"update","schedule":{"start_hour":23}}`,
		`{"action":"update","schedule":{"end_hour":7}}`,
		`{"action":"update","schedule":{"start_hour":23,"end_hour":7,"unknown_field":1}}`,
	}

	for _, payload := range invalidPayloads {
		req = httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(payload)))
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("payload %s expected 400, got %d: %s", payload, rec.Code, rec.Body.String())
		}

		// 断言运行态不变
		curSnap := srv.engine.Snapshot()
		if curSnap.Schedule.StartHour != 23 || curSnap.Schedule.EndHour != 7 {
			t.Fatalf("payload %s mutated running schedule: got %+v", payload, curSnap.Schedule)
		}

		// 断言配置文件不变
		curLoaded, err := LoadConfig(configPath)
		if err != nil {
			t.Fatalf("load config: %v", err)
		}
		if curLoaded.Schedule.StartHour != 23 || curLoaded.Schedule.EndHour != 7 {
			t.Fatalf("payload %s mutated persisted schedule: got %+v", payload, curLoaded.Schedule)
		}
	}
}

func TestControlUpdateScheduleValidation(t *testing.T) {
	srv, configPath := setupTestServer(t)
	handler := srv.routes()

	post := func(t *testing.T, payload string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/api/control", bytes.NewReader([]byte(payload)))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	statusSchedule := func(t *testing.T) ScheduleConfig {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /api/status: expected 200, got %d (%s)", rec.Code, rec.Body.String())
		}
		var body struct {
			Schedule ScheduleConfig `json:"schedule"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode /api/status: %v", err)
		}
		return body.Schedule
	}

	persistedSchedule := func(t *testing.T) ScheduleConfig {
		t.Helper()
		loaded, err := LoadConfig(configPath)
		if err != nil {
			t.Fatalf("load config: %v", err)
		}
		return loaded.Schedule
	}

	assertUnchanged := func(t *testing.T, want ScheduleConfig) {
		t.Helper()
		if got := statusSchedule(t); got != want {
			t.Fatalf("running schedule via /api/status changed: want %+v, got %+v", want, got)
		}
		if got := srv.engine.Snapshot().Schedule; got != want {
			t.Fatalf("engine snapshot schedule changed: want %+v, got %+v", want, got)
		}
		if got := persistedSchedule(t); got != want {
			t.Fatalf("persisted schedule changed: want %+v, got %+v", want, got)
		}
	}

	baseline := statusSchedule(t)

	invalid := []struct {
		name    string
		payload string
	}{
		{"start_hour null", `{"action":"update","schedule":{"start_hour":null,"end_hour":7}}`},
		{"end_hour null", `{"action":"update","schedule":{"start_hour":23,"end_hour":null}}`},
		{"unknown sub field", `{"action":"update","schedule":{"start_hour":1,"end_hour":7,"foo":1}}`},
		{"start_hour too big", `{"action":"update","schedule":{"start_hour":25,"end_hour":7}}`},
		{"start_hour negative", `{"action":"update","schedule":{"start_hour":-1,"end_hour":7}}`},
		{"end_hour too big", `{"action":"update","schedule":{"start_hour":1,"end_hour":25}}`},
		{"start equals end", `{"action":"update","schedule":{"start_hour":9,"end_hour":9}}`},
	}

	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			rec := post(t, tc.payload)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d (%s)", rec.Code, rec.Body.String())
			}
			assertUnchanged(t, baseline)
		})
	}

	t.Run("valid cross-night 23 to 7", func(t *testing.T) {
		rec := post(t, `{"action":"update","schedule":{"start_hour":23,"end_hour":7}}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d (%s)", rec.Code, rec.Body.String())
		}

		want := ScheduleConfig{StartHour: 23, EndHour: 7}
		if got := statusSchedule(t); got != want {
			t.Fatalf("/api/status schedule: want %+v, got %+v", want, got)
		}
		if got := srv.engine.Snapshot().Schedule; got != want {
			t.Fatalf("engine snapshot schedule: want %+v, got %+v", want, got)
		}
		if got := persistedSchedule(t); got != want {
			t.Fatalf("LoadConfig schedule: want %+v, got %+v", want, got)
		}

		raw, err := os.ReadFile(configPath)
		if err != nil {
			t.Fatalf("read config file: %v", err)
		}
		for _, needle := range []string{"start_hour: 23", "end_hour: 7"} {
			if !bytes.Contains(raw, []byte(needle)) {
				t.Fatalf("config file missing %q, content:\n%s", needle, raw)
			}
		}
	})
}
