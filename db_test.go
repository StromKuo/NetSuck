package main

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stats.db")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore(%q): %v", path, err)
	}
	if err := store.InitSchema(); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return store
}

func mustCreateTarget(t *testing.T, store *Store, name, url string, weight int, enabled bool) TargetRecord {
	t.Helper()
	rec, err := store.CreateTarget(TargetPatch{
		Name:    &name,
		URL:     &url,
		Weight:  &weight,
		Enabled: &enabled,
	})
	if err != nil {
		t.Fatalf("CreateTarget(%q): %v", url, err)
	}
	return rec
}

func queryInt(t *testing.T, store *Store, query string, args ...any) int64 {
	t.Helper()
	var v int64
	if err := store.db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return v
}

func queryString(t *testing.T, store *Store, query string, args ...any) string {
	t.Helper()
	var v string
	if err := store.db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return v
}

func todayLocal(now time.Time) string {
	return now.In(time.Local).Format("2006-01-02")
}

func TestInitSchemaIdempotent(t *testing.T) {
	store := newTestStore(t)
	if err := store.InitSchema(); err != nil {
		t.Fatalf("second InitSchema: %v", err)
	}

	got := queryInt(t, store,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('targets','daily_stats','history','metadata')`)
	if got != 4 {
		t.Fatalf("tables = %d, want 4", got)
	}
	idx := queryInt(t, store,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_targets_url_live'`)
	if idx != 1 {
		t.Fatalf("partial unique index missing (count=%d)", idx)
	}
	if mode := queryString(t, store, `PRAGMA journal_mode`); mode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
	if timeout := queryInt(t, store, `PRAGMA busy_timeout`); timeout != 5000 {
		t.Fatalf("busy_timeout = %d, want 5000", timeout)
	}

	path := filepath.Join(t.TempDir(), "reopen.db")
	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen OpenStore: %v", err)
	}
	defer reopened.Close()
	if err := reopened.InitSchema(); err != nil {
		t.Fatalf("InitSchema on fresh db: %v", err)
	}
	if err := reopened.InitSchema(); err != nil {
		t.Fatalf("InitSchema twice on fresh db: %v", err)
	}
}

func TestEnsureSeededOnlyFirstTime(t *testing.T) {
	store := newTestStore(t)

	seedCfg := &Config{Targets: []TargetConfig{
		{Name: "A", URL: "https://example.com/a.bin", Weight: 5, Enabled: true},
		{Name: "B", URL: "https://example.com/b.bin", Weight: 0, Enabled: false},
	}}
	if err := store.EnsureSeeded(seedCfg); err != nil {
		t.Fatalf("EnsureSeeded: %v", err)
	}
	recs, err := store.ListTargetRecords()
	if err != nil {
		t.Fatalf("ListTargetRecords: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("seeded targets = %d, want 2", len(recs))
	}
	if recs[0].Name != "A" || recs[0].URL != "https://example.com/a.bin" || recs[0].Weight != 5 || !recs[0].Enabled {
		t.Fatalf("unexpected first seeded record: %+v", recs[0])
	}
	if recs[0].Health != "Unknown" || recs[0].TotalBytes != 0 {
		t.Fatalf("unexpected seeded defaults: %+v", recs[0])
	}
	if recs[1].Weight != defaultTargetWeight {
		t.Fatalf("zero weight not defaulted: %+v", recs[1])
	}
	if recs[1].Enabled {
		t.Fatalf("disabled target seeded as enabled: %+v", recs[1])
	}
	if marker := queryString(t, store, `SELECT value FROM metadata WHERE key = ?`, metaKeyTargetsSeeded); marker == "" {
		t.Fatal("targets_seeded marker not written")
	}

	second := &Config{Targets: []TargetConfig{
		{Name: "C", URL: "https://example.com/c.bin", Weight: 1, Enabled: true},
		{Name: "D", URL: "https://example.com/d.bin", Weight: 1, Enabled: true},
		{Name: "E", URL: "https://example.com/e.bin", Weight: 1, Enabled: true},
	}}
	if err := store.EnsureSeeded(second); err != nil {
		t.Fatalf("second EnsureSeeded: %v", err)
	}
	recs, err = store.ListTargetRecords()
	if err != nil {
		t.Fatalf("ListTargetRecords: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("re-seed changed target count: got %d, want 2", len(recs))
	}
}

func TestEnsureSeededEmptyListMarksSeeded(t *testing.T) {
	store := newTestStore(t)

	if err := store.EnsureSeeded(&Config{Targets: []TargetConfig{}}); err != nil {
		t.Fatalf("EnsureSeeded(empty): %v", err)
	}
	if marker := queryString(t, store, `SELECT value FROM metadata WHERE key = ?`, metaKeyTargetsSeeded); marker == "" {
		t.Fatal("targets_seeded marker not written for empty list")
	}
	if err := store.EnsureSeeded(&Config{Targets: []TargetConfig{
		{Name: "X", URL: "https://example.com/x.bin", Weight: 1, Enabled: true},
	}}); err != nil {
		t.Fatalf("EnsureSeeded after empty: %v", err)
	}
	recs, err := store.ListTargetRecords()
	if err != nil {
		t.Fatalf("ListTargetRecords: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("targets appeared after empty seed: %d", len(recs))
	}

	if err := store.EnsureSeeded(nil); err != nil {
		t.Fatalf("EnsureSeeded(nil): %v", err)
	}
}

func TestEnsureSeededDoesNotResurrectDeletedTargets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stats.db")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	if err := store.InitSchema(); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}

	cfg := &Config{Targets: []TargetConfig{
		{Name: "A", URL: "https://example.com/a.bin", Weight: 10, Enabled: true},
	}}
	if err := store.EnsureSeeded(cfg); err != nil {
		t.Fatalf("EnsureSeeded: %v", err)
	}
	recs, err := store.ListTargetRecords()
	if err != nil {
		t.Fatalf("ListTargetRecords: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("seeded = %d, want 1", len(recs))
	}
	if err := store.SoftDeleteTarget(recs[0].ID); err != nil {
		t.Fatalf("SoftDeleteTarget: %v", err)
	}
	if err := store.EnsureSeeded(cfg); err != nil {
		t.Fatalf("EnsureSeeded after delete: %v", err)
	}
	recs, err = store.ListTargetRecords()
	if err != nil {
		t.Fatalf("ListTargetRecords: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("deleted target resurrected by EnsureSeeded: %d", len(recs))
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 模拟进程重启：重新打开数据库，播种仍不得复活已软删目标。
	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if err := reopened.InitSchema(); err != nil {
		t.Fatalf("reopen InitSchema: %v", err)
	}
	if err := reopened.EnsureSeeded(cfg); err != nil {
		t.Fatalf("reopen EnsureSeeded: %v", err)
	}
	recs, err = reopened.ListTargetRecords()
	if err != nil {
		t.Fatalf("reopen ListTargetRecords: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("deleted target resurrected after restart: %d", len(recs))
	}
}

func TestTargetCRUDAndErrors(t *testing.T) {
	store := newTestStore(t)

	created := mustCreateTarget(t, store, "A", "https://example.com/a.bin", 7, true)
	if created.ID <= 0 {
		t.Fatalf("created id = %d", created.ID)
	}
	if created.Health != "Unknown" || created.TotalBytes != 0 || created.CooldownUntilMS != 0 {
		t.Fatalf("unexpected create defaults: %+v", created)
	}

	if _, err := store.CreateTarget(TargetPatch{Name: strPtr("dup"), URL: strPtr("https://example.com/a.bin")}); !errors.Is(err, ErrDuplicateURL) {
		t.Fatalf("duplicate create err = %v, want ErrDuplicateURL", err)
	}

	// 部分更新：只改 name，其余保持不变。
	updated, err := store.PatchTarget(created.ID, TargetPatch{Name: strPtr("A2")})
	if err != nil {
		t.Fatalf("PatchTarget: %v", err)
	}
	if updated.Name != "A2" || updated.URL != created.URL || updated.Weight != 7 || !updated.Enabled {
		t.Fatalf("partial patch changed unexpected fields: %+v", updated)
	}

	// 空 patch 在已存在目标上应返回当前记录。
	same, err := store.PatchTarget(created.ID, TargetPatch{})
	if err != nil {
		t.Fatalf("PatchTarget(empty): %v", err)
	}
	if same.ID != created.ID || same.Name != "A2" {
		t.Fatalf("empty patch returned unexpected record: %+v", same)
	}

	if _, err := store.PatchTarget(99999, TargetPatch{Name: strPtr("ghost")}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("patch missing err = %v, want ErrNotFound", err)
	}
	if err := store.SoftDeleteTarget(99999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete missing err = %v, want ErrNotFound", err)
	}

	other := mustCreateTarget(t, store, "B", "https://example.com/b.bin", 10, true)
	if _, err := store.PatchTarget(created.ID, TargetPatch{URL: strPtr(other.URL)}); !errors.Is(err, ErrDuplicateURL) {
		t.Fatalf("patch to duplicate url err = %v, want ErrDuplicateURL", err)
	}

	if err := store.SoftDeleteTarget(created.ID); err != nil {
		t.Fatalf("SoftDeleteTarget: %v", err)
	}
	if err := store.SoftDeleteTarget(created.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("double delete err = %v, want ErrNotFound", err)
	}
	if _, err := store.PatchTarget(created.ID, TargetPatch{Name: strPtr("nope")}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("patch deleted err = %v, want ErrNotFound", err)
	}

	recs, err := store.ListTargetRecords()
	if err != nil {
		t.Fatalf("ListTargetRecords: %v", err)
	}
	if len(recs) != 1 || recs[0].ID != other.ID {
		t.Fatalf("live targets after delete = %+v", recs)
	}

	// 缺省字段使用默认值。
	defaults, err := store.CreateTarget(TargetPatch{})
	if err != nil {
		t.Fatalf("CreateTarget(defaults): %v", err)
	}
	if defaults.Weight != defaultTargetWeight || !defaults.Enabled {
		t.Fatalf("create defaults = %+v", defaults)
	}
}

// R2：PatchTargetAtomic 在单个事务内改 URL 并原子重置健康（Unknown/cooldown=0/errors=0）。
func TestPatchTargetAtomicResetsHealth(t *testing.T) {
	store := newTestStore(t)
	created := mustCreateTarget(t, store, "A", "https://example.com/a.bin", 7, true)

	// 写入非默认健康（RateLimited + 冷却 + 错误计数）。
	err := store.PersistBatch(CounterBatch{
		TargetHealth: map[int64]TargetHealthPersist{
			created.ID: {Health: "RateLimited", CooldownUntilMS: 123456, LastStatusCode: 429, LatencyMS: 88, ErrorMsg: "limited", ConsecutiveErrors: 2},
		},
		Seq: 1,
	})
	if err != nil {
		t.Fatalf("seed health: %v", err)
	}
	// url 变更且 resetHealth=true → URL 与健康原子重置。
	rec, err := store.PatchTargetAtomic(created.ID, TargetPatch{URL: strPtr("https://example.com/a2.bin")}, true)
	if err != nil {
		t.Fatalf("PatchTargetAtomic: %v", err)
	}
	if rec.URL != "https://example.com/a2.bin" {
		t.Fatalf("url 应更新: %q", rec.URL)
	}
	if rec.Health != "Unknown" || rec.CooldownUntilMS != 0 || rec.ConsecutiveErrors != 0 || rec.ErrorMsg != "" {
		t.Fatalf("改 URL 后健康未原子重置: %+v", rec)
	}
	// 未重置模式：仅改 URL/名称，保留现有健康（等价原 PatchTarget）。
	err = store.PersistBatch(CounterBatch{
		TargetHealth: map[int64]TargetHealthPersist{
			created.ID: {Health: "Dead", CooldownUntilMS: 0, LastStatusCode: 404, LatencyMS: 5, ErrorMsg: "gone", ConsecutiveErrors: 0},
		},
		Seq: 2,
	})
	if err != nil {
		t.Fatalf("seed health 2: %v", err)
	}
	rec2, err := store.PatchTargetAtomic(created.ID, TargetPatch{Name: strPtr("A2")}, false)
	if err != nil {
		t.Fatalf("PatchTargetAtomic(non-reset): %v", err)
	}
	if rec2.Health != "Dead" || rec2.ErrorMsg != "gone" {
		t.Fatalf("非重置模式不应清健康: %+v", rec2)
	}
	// 重复 url（未软删）→ ErrDuplicateURL。
	other := mustCreateTarget(t, store, "B", "https://example.com/a3.bin", 10, true)
	if _, err := store.PatchTargetAtomic(created.ID, TargetPatch{URL: strPtr(other.URL)}, true); !errors.Is(err, ErrDuplicateURL) {
		t.Fatalf("atomic 到重复 url err = %v, want ErrDuplicateURL", err)
	}
}


func TestPartialUniqueIndexAllowsReAddAfterSoftDelete(t *testing.T) {
	store := newTestStore(t)
	const url = "https://example.com/same.bin"

	first := mustCreateTarget(t, store, "first", url, 10, true)
	now := time.Now()
	today := todayLocal(now)
	hour := localHourStarts(now)[historyWindowHours-1].Unix()

	batch := CounterBatch{
		TargetDays:   map[string]map[int64]int64{today: {first.ID: 1234}},
		GlobalDays:   map[string]int64{today: 1234},
		TargetTotals: map[int64]int64{first.ID: 1234},
		HourBuckets:  map[int64]int64{hour: 1234},
		TargetHealth: map[int64]TargetHealthPersist{
			first.ID: {Health: "Healthy", LastStatusCode: 206, LatencyMS: 42},
		},
		Seq: 1,
	}
	if err := store.PersistBatch(batch); err != nil {
		t.Fatalf("PersistBatch: %v", err)
	}

	if err := store.SoftDeleteTarget(first.ID); err != nil {
		t.Fatalf("SoftDeleteTarget: %v", err)
	}

	second, err := store.CreateTarget(TargetPatch{Name: strPtr("second"), URL: strPtr(url)})
	if err != nil {
		t.Fatalf("re-add after soft delete: %v", err)
	}
	if second.ID == first.ID {
		t.Fatalf("re-added target reused id %d", second.ID)
	}
	if second.TotalBytes != 0 {
		t.Fatalf("re-added target inherited totals: %d", second.TotalBytes)
	}

	recs, err := store.ListTargetRecords()
	if err != nil {
		t.Fatalf("ListTargetRecords: %v", err)
	}
	if len(recs) != 1 || recs[0].ID != second.ID {
		t.Fatalf("live targets = %+v, want only new id", recs)
	}

	// 旧目标的日桶与小时桶必须保留。
	if got := queryInt(t, store, `SELECT bytes FROM daily_stats WHERE day = ? AND target_id = ?`, today, first.ID); got != 1234 {
		t.Fatalf("old daily_stats = %d, want 1234", got)
	}
	if got := queryInt(t, store, `SELECT bytes FROM history WHERE hour_ts = ?`, hour); got != 1234 {
		t.Fatalf("old history = %d, want 1234", got)
	}
	// 软删行仍在表内（仅被过滤）。
	if got := queryInt(t, store, `SELECT COUNT(*) FROM targets WHERE id = ? AND deleted_at IS NOT NULL`, first.ID); got != 1 {
		t.Fatalf("soft-deleted row missing (count=%d)", got)
	}
}

func TestPersistBatchIdempotentAndSeqGuard(t *testing.T) {
	store := newTestStore(t)
	a := mustCreateTarget(t, store, "A", "https://example.com/a.bin", 10, true)
	b := mustCreateTarget(t, store, "B", "https://example.com/b.bin", 10, true)

	now := time.Now()
	today := todayLocal(now)
	hours := localHourStarts(now)
	h0 := hours[historyWindowHours-1].Unix()
	h1 := hours[historyWindowHours-2].Unix()

	seq1 := CounterBatch{
		TargetDays:   map[string]map[int64]int64{today: {a.ID: 100, b.ID: 50}},
		GlobalDays:   map[string]int64{today: 150},
		TargetTotals: map[int64]int64{a.ID: 1000, b.ID: 2000},
		HourBuckets:  map[int64]int64{h0: 70, h1: 80},
		TargetHealth: map[int64]TargetHealthPersist{
			a.ID: {
				Health: "Healthy", CooldownUntilMS: 0, LastStatusCode: 200,
				LatencyMS: 30, ErrorMsg: "", ConsecutiveErrors: 0,
			},
			b.ID: {
				Health: "RateLimited", CooldownUntilMS: 1700000000000, LastStatusCode: 429,
				LatencyMS: 55, ErrorMsg: "too many requests", ConsecutiveErrors: 2,
			},
		},
		Seq: 1,
	}
	if err := store.PersistBatch(seq1); err != nil {
		t.Fatalf("PersistBatch(seq1): %v", err)
	}
	assertBatchState(t, store, today, a.ID, b.ID, h0, h1, 100, 50, 1000, 2000, 70, 80)

	// 同批重放：成功且不重复累加（绝对值 upsert）。
	if err := store.PersistBatch(seq1); err != nil {
		t.Fatalf("PersistBatch(seq1 replay): %v", err)
	}
	assertBatchState(t, store, today, a.ID, b.ID, h0, h1, 100, 50, 1000, 2000, 70, 80)

	// 健康列已按绝对值写入。
	recA, err := store.PatchTarget(a.ID, TargetPatch{})
	if err != nil {
		t.Fatalf("read A: %v", err)
	}
	if recA.Health != "Healthy" || recA.LastStatusCode != 200 || recA.LatencyMS != 30 {
		t.Fatalf("health A = %+v", recA)
	}
	recB, err := store.PatchTarget(b.ID, TargetPatch{})
	if err != nil {
		t.Fatalf("read B: %v", err)
	}
	if recB.Health != "RateLimited" || recB.CooldownUntilMS != 1700000000000 ||
		recB.LastStatusCode != 429 || recB.ErrorMsg != "too many requests" || recB.ConsecutiveErrors != 2 {
		t.Fatalf("health B = %+v", recB)
	}

	seq2 := CounterBatch{
		TargetDays:   map[string]map[int64]int64{today: {a.ID: 160, b.ID: 90}},
		GlobalDays:   map[string]int64{today: 250},
		TargetTotals: map[int64]int64{a.ID: 1500, b.ID: 2600},
		HourBuckets:  map[int64]int64{h0: 130},
		Seq:          2,
	}
	if err := store.PersistBatch(seq2); err != nil {
		t.Fatalf("PersistBatch(seq2): %v", err)
	}
	assertBatchState(t, store, today, a.ID, b.ID, h0, h1, 160, 90, 1500, 2600, 130, 80)

	// 旧批被拒绝，且不得覆盖新值。
	if err := store.PersistBatch(seq1); !errors.Is(err, errStaleSeq) {
		t.Fatalf("stale batch err = %v, want errStaleSeq", err)
	}
	assertBatchState(t, store, today, a.ID, b.ID, h0, h1, 160, 90, 1500, 2600, 130, 80)

	// 已提交批次重试仍成功。
	if err := store.PersistBatch(seq2); err != nil {
		t.Fatalf("PersistBatch(seq2 replay): %v", err)
	}
	if got := queryString(t, store, `SELECT value FROM metadata WHERE key = ?`, metaKeyLastBatchSeq); got != "2" {
		t.Fatalf("last_batch_seq = %q, want 2", got)
	}
}

func assertBatchState(t *testing.T, store *Store, today string, aID, bID, h0, h1, dayA, dayB, totalA, totalB, hour0, hour1 int64) {
	t.Helper()
	if got := queryInt(t, store, `SELECT bytes FROM daily_stats WHERE day = ? AND target_id = ?`, today, aID); got != dayA {
		t.Fatalf("daily_stats[A] = %d, want %d", got, dayA)
	}
	if got := queryInt(t, store, `SELECT bytes FROM daily_stats WHERE day = ? AND target_id = ?`, today, bID); got != dayB {
		t.Fatalf("daily_stats[B] = %d, want %d", got, dayB)
	}
	if got := queryInt(t, store, `SELECT total_bytes FROM targets WHERE id = ?`, aID); got != totalA {
		t.Fatalf("total_bytes[A] = %d, want %d", got, totalA)
	}
	if got := queryInt(t, store, `SELECT total_bytes FROM targets WHERE id = ?`, bID); got != totalB {
		t.Fatalf("total_bytes[B] = %d, want %d", got, totalB)
	}
	if got := queryInt(t, store, `SELECT bytes FROM history WHERE hour_ts = ?`, h0); got != hour0 {
		t.Fatalf("history[h0] = %d, want %d", got, hour0)
	}
	if got := queryInt(t, store, `SELECT bytes FROM history WHERE hour_ts = ?`, h1); got != hour1 {
		t.Fatalf("history[h1] = %d, want %d", got, hour1)
	}
	if got := queryString(t, store, `SELECT value FROM metadata WHERE key = ?`, metaGlobalDayPrefix+today); got == "" {
		t.Fatal("global_day metadata not written")
	}
}

func TestLoadEngineState(t *testing.T) {
	store := newTestStore(t)
	target := mustCreateTarget(t, store, "A", "https://example.com/a.bin", 10, true)

	now := time.Now()
	today := todayLocal(now)
	hours := localHourStarts(now)
	cur := hours[historyWindowHours-1].Unix()
	threeAgo := hours[historyWindowHours-4].Unix()
	stale := hours[0].Add(-24 * time.Hour).Unix() // 24h 窗口之外
	oldDay := "2020-01-01"

	batch := CounterBatch{
		TargetDays: map[string]map[int64]int64{
			today:  {target.ID: 5},
			oldDay: {target.ID: 9},
		},
		GlobalDays:   map[string]int64{today: 5, oldDay: 9},
		TargetTotals: map[int64]int64{target.ID: 14},
		HourBuckets:  map[int64]int64{cur: 11, threeAgo: 7, stale: 999},
		Seq:          3,
	}
	if err := store.PersistBatch(batch); err != nil {
		t.Fatalf("PersistBatch: %v", err)
	}

	state, err := store.LoadEngineState(now)
	if err != nil {
		t.Fatalf("LoadEngineState: %v", err)
	}
	if state.LastSeq != 3 {
		t.Fatalf("LastSeq = %d, want 3", state.LastSeq)
	}
	if len(state.Targets) != 1 || state.Targets[0].ID != target.ID {
		t.Fatalf("Targets = %+v", state.Targets)
	}
	if got := state.TargetDays[today][target.ID]; got != 5 {
		t.Fatalf("TargetDays[today][id] = %d, want 5", got)
	}
	if got := state.TargetDays[oldDay][target.ID]; got != 9 {
		t.Fatalf("TargetDays[oldDay][id] = %d, want 9", got)
	}
	if got := state.GlobalDays[today]; got != 5 {
		t.Fatalf("GlobalDays[today] = %d, want 5", got)
	}
	if got := state.GlobalDays[oldDay]; got != 9 {
		t.Fatalf("GlobalDays[oldDay] = %d, want 9", got)
	}
	if got := state.HourBuckets[cur]; got != 11 {
		t.Fatalf("HourBuckets[current] = %d, want 11", got)
	}
	if got := state.HourBuckets[threeAgo]; got != 7 {
		t.Fatalf("HourBuckets[-3h] = %d, want 7", got)
	}
	if _, ok := state.HourBuckets[stale]; ok {
		t.Fatal("HourBuckets included a bucket outside the 24h window")
	}

	// 空库也要给出今日键，便于引擎直接使用。
	fresh := newTestStore(t)
	freshState, err := fresh.LoadEngineState(now)
	if err != nil {
		t.Fatalf("LoadEngineState(fresh): %v", err)
	}
	if _, ok := freshState.TargetDays[today]; !ok {
		t.Fatal("TargetDays missing today key on empty db")
	}
	if _, ok := freshState.GlobalDays[today]; !ok {
		t.Fatal("GlobalDays missing today key on empty db")
	}
	if freshState.GlobalDays[today] != 0 || freshState.LastSeq != 0 {
		t.Fatalf("fresh state = %+v", freshState)
	}
}

func TestHistory24h(t *testing.T) {
	store := newTestStore(t)
	target := mustCreateTarget(t, store, "A", "https://example.com/a.bin", 10, true)

	now := time.Now()
	hours := localHourStarts(now)
	cur := hours[historyWindowHours-1].Unix()
	fiveAgo := hours[historyWindowHours-6].Unix()
	outside := hours[0].Add(-6 * time.Hour).Unix()

	batch := CounterBatch{
		HourBuckets:  map[int64]int64{cur: 100, fiveAgo: 40, outside: 7777},
		TargetTotals: map[int64]int64{target.ID: 140},
		Seq:          1,
	}
	if err := store.PersistBatch(batch); err != nil {
		t.Fatalf("PersistBatch: %v", err)
	}

	points, err := store.History24h(now)
	if err != nil {
		t.Fatalf("History24h: %v", err)
	}
	if len(points) != historyWindowHours {
		t.Fatalf("points = %d, want %d", len(points), historyWindowHours)
	}

	var total int64
	seen := make(map[int64]bool, len(points))
	for i, p := range points {
		if i > 0 && p.HourUnix <= points[i-1].HourUnix {
			t.Fatalf("points not ascending at %d: %d <= %d", i, p.HourUnix, points[i-1].HourUnix)
		}
		if seen[p.HourUnix] {
			t.Fatalf("duplicate bucket %d", p.HourUnix)
		}
		seen[p.HourUnix] = true

		parsed, err := time.Parse(time.RFC3339, p.Hour)
		if err != nil {
			t.Fatalf("point %d hour %q not RFC3339: %v", i, p.Hour, err)
		}
		if parsed.Unix() != p.HourUnix {
			t.Fatalf("point %d hour %q != unix %d", i, p.Hour, p.HourUnix)
		}
		if parsed.Minute() != 0 || parsed.Second() != 0 {
			t.Fatalf("point %d not on hour boundary: %s", i, p.Hour)
		}
		_, offsetWant := time.Unix(p.HourUnix, 0).In(time.Local).Zone()
		_, offsetGot := parsed.Zone()
		if offsetGot != offsetWant {
			t.Fatalf("point %d offset = %d, want local offset %d", i, offsetGot, offsetWant)
		}
		total += p.Bytes
	}

	if points[len(points)-1].HourUnix != cur {
		t.Fatalf("last bucket = %d, want current hour %d", points[len(points)-1].HourUnix, cur)
	}
	if got := points[len(points)-1].Bytes; got != 100 {
		t.Fatalf("current hour bytes = %d, want 100", got)
	}
	if got := points[historyWindowHours-6].Bytes; got != 40 {
		t.Fatalf("-5h bytes = %d, want 40", got)
	}
	if total != 140 {
		t.Fatalf("sum of buckets = %d, want 140 (zero-filled, no outside bucket)", total)
	}
}

func TestCloseAndReopenKeepsData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stats.db")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	if err := store.InitSchema(); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	rec := mustCreateTarget(t, store, "A", "https://example.com/a.bin", 10, true)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	recs, err := reopened.ListTargetRecords()
	if err != nil {
		t.Fatalf("ListTargetRecords: %v", err)
	}
	if len(recs) != 1 || recs[0].ID != rec.ID || recs[0].URL != rec.URL {
		t.Fatalf("reopened records = %+v", recs)
	}
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	store := newTestStore(t)
	target := mustCreateTarget(t, store, "A", "https://example.com/a.bin", 10, true)

	now := time.Now()
	today := todayLocal(now)
	hour := localHourStarts(now)[historyWindowHours-1].Unix()

	const batches = 30
	errCh := make(chan error, 1+batches)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= batches; i++ {
			b := CounterBatch{
				TargetDays:   map[string]map[int64]int64{today: {target.ID: int64(i * 10)}},
				GlobalDays:   map[string]int64{today: int64(i * 10)},
				TargetTotals: map[int64]int64{target.ID: int64(i * 100)},
				HourBuckets:  map[int64]int64{hour: int64(i * 10)},
				Seq:          uint64(i),
			}
			if err := store.PersistBatch(b); err != nil {
				errCh <- err
				return
			}
		}
	}()

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				if _, err := store.ListTargetRecords(); err != nil {
					errCh <- err
					return
				}
				if _, err := store.LoadEngineState(now); err != nil {
					errCh <- err
					return
				}
				if _, err := store.History24h(now); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent op: %v", err)
	}

	if got := queryString(t, store, `SELECT value FROM metadata WHERE key = ?`, metaKeyLastBatchSeq); got != "30" {
		t.Fatalf("last_batch_seq = %q, want 30", got)
	}
	if got := queryInt(t, store, `SELECT total_bytes FROM targets WHERE id = ?`, target.ID); got != 3000 {
		t.Fatalf("total_bytes = %d, want 3000", got)
	}
}

// withLocalZone 临时把 time.Local 切到指定时区，用于验证 DST 边界行为。
func withLocalZone(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Skipf("timezone %q unavailable: %v", name, err)
	}
	old := time.Local
	time.Local = loc
	t.Cleanup(func() { time.Local = old })
	return loc
}

func TestHistory24hAcrossDSTFallBack(t *testing.T) {
	loc := withLocalZone(t, "America/New_York")
	store := newTestStore(t)

	// 2024-11-03 02:00 EDT 回拨到 01:00 EST，01:00 本地钟点出现两次。
	now := time.Date(2024, 11, 3, 3, 30, 0, 0, loc)
	starts := localHourStarts(now)
	buckets := make(map[int64]int64, len(starts))
	for i, start := range starts {
		buckets[start.Unix()] = int64(i + 1)
	}
	if len(buckets) != historyWindowHours {
		t.Fatalf("distinct bucket unix = %d, want %d", len(buckets), historyWindowHours)
	}
	if err := store.PersistBatch(CounterBatch{HourBuckets: buckets, Seq: 1}); err != nil {
		t.Fatalf("PersistBatch: %v", err)
	}

	points, err := store.History24h(now)
	if err != nil {
		t.Fatalf("History24h: %v", err)
	}
	if len(points) != historyWindowHours {
		t.Fatalf("points = %d, want %d", len(points), historyWindowHours)
	}

	var sum int64
	seen := make(map[int64]bool, len(points))
	oneAM := 0
	for i, p := range points {
		if i > 0 && p.HourUnix <= points[i-1].HourUnix {
			t.Fatalf("points not ascending at %d", i)
		}
		if seen[p.HourUnix] {
			t.Fatalf("duplicate bucket %d", p.HourUnix)
		}
		seen[p.HourUnix] = true
		sum += p.Bytes
		if p.Hour[:16] == "2024-11-03T01:00" {
			oneAM++
		}
	}
	if sum != int64(historyWindowHours*(historyWindowHours+1)/2) {
		t.Fatalf("sum = %d, want %d", sum, historyWindowHours*(historyWindowHours+1)/2)
	}
	if oneAM != 2 {
		t.Fatalf("01:00 buckets = %d, want 2 (EDT and EST)", oneAM)
	}
}

func TestHistory24hAcrossDSTSpringForward(t *testing.T) {
	loc := withLocalZone(t, "America/New_York")
	store := newTestStore(t)

	// 2024-03-10 02:00 EST 前跳到 03:00 EDT，02:00 本地钟点不存在。
	now := time.Date(2024, 3, 10, 4, 30, 0, 0, loc)
	starts := localHourStarts(now)
	buckets := make(map[int64]int64, len(starts))
	for i, start := range starts {
		buckets[start.Unix()] = int64(i + 1)
	}
	if err := store.PersistBatch(CounterBatch{HourBuckets: buckets, Seq: 1}); err != nil {
		t.Fatalf("PersistBatch: %v", err)
	}

	points, err := store.History24h(now)
	if err != nil {
		t.Fatalf("History24h: %v", err)
	}
	if len(points) != historyWindowHours {
		t.Fatalf("points = %d, want %d", len(points), historyWindowHours)
	}
	seen := make(map[int64]bool, len(points))
	for i, p := range points {
		if i > 0 && p.HourUnix <= points[i-1].HourUnix {
			t.Fatalf("points not ascending at %d", i)
		}
		if seen[p.HourUnix] {
			t.Fatalf("duplicate bucket %d", p.HourUnix)
		}
		seen[p.HourUnix] = true
		if p.Hour[:16] == "2024-03-10T02:00" {
			t.Fatalf("non-existent local hour produced a bucket: %s", p.Hour)
		}
	}
}

func strPtr(s string) *string { return &s }

// A1：generation 屏障——改 URL（原子推进代际）后，旧代际健康批不得覆盖/复活旧冷却。
func TestA1GenerationBarrierPreservesState(t *testing.T) {
	store := newTestStore(t)
	created := mustCreateTarget(t, store, "A", "https://example.com/a.bin", 7, true)

	// 写旧代际健康（RateLimited + 冷却，gen1）。
	err := store.PersistBatch(CounterBatch{
		TargetHealth: map[int64]TargetHealthPersist{
			created.ID: {Health: "RateLimited", CooldownUntilMS: 111111, LastStatusCode: 403, LatencyMS: 40, ErrorMsg: "limited", ConsecutiveErrors: 0, Generation: 1},
		},
		Seq: 1,
	})
	if err != nil {
		t.Fatalf("seed gen1 health: %v", err)
	}
	// 改 URL（resetHealth=true）→ 原子推进 generation 至 2，健康置 Unknown。
	rec, err := store.PatchTargetAtomic(created.ID, TargetPatch{URL: strPtr("https://example.com/a2.bin")}, true)
	if err != nil {
		t.Fatalf("PatchTargetAtomic: %v", err)
	}
	if rec.Health != "Unknown" || rec.CooldownUntilMS != 0 || rec.Generation != 2 {
		t.Fatalf("改 URL 后应 Unknown/cooldown0/gen2: %+v", rec)
	}
	// 旧代际（gen1）健康批不得覆盖新代际（gen2）的 Unknown（屏障拦截）。
	err = store.PersistBatch(CounterBatch{
		TargetHealth: map[int64]TargetHealthPersist{
			created.ID: {Health: "RateLimited", CooldownUntilMS: 222222, LastStatusCode: 403, LatencyMS: 40, ErrorMsg: "limited", ConsecutiveErrors: 0, Generation: 1},
		},
		Seq: 2,
	})
	if err != nil {
		t.Fatalf("stale batch: %v", err)
	}
	after, err := store.PatchTarget(created.ID, TargetPatch{})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if after.Health != "Unknown" || after.CooldownUntilMS != 0 {
		t.Fatalf("旧代际批不得复活旧冷却: %+v", after)
	}
	// 正确代际（gen2）批可写（控制组）。
	err = store.PersistBatch(CounterBatch{
		TargetHealth: map[int64]TargetHealthPersist{
			created.ID: {Health: "Healthy", CooldownUntilMS: 0, LastStatusCode: 200, LatencyMS: 10, ErrorMsg: "", ConsecutiveErrors: 0, Generation: 2},
		},
		Seq: 3,
	})
	if err != nil {
		t.Fatalf("gen2 batch: %v", err)
	}
	after2, err := store.PatchTarget(created.ID, TargetPatch{})
	if err != nil {
		t.Fatalf("read2: %v", err)
	}
	if after2.Health != "Healthy" {
		t.Fatalf("正确代际应可写健康: %+v", after2)
	}
}
