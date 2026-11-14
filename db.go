package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"modernc.org/sqlite"
)

// 持久化层错误。契约冻结：仅 ErrDuplicateURL / ErrNotFound 对外导出。
var (
	// ErrDuplicateURL 表示已存在同 URL 且未软删的目标（部分唯一索引冲突）。
	ErrDuplicateURL = errors.New("duplicate url")
	// ErrNotFound 表示目标不存在或已软删。
	ErrNotFound = errors.New("not found")

	// errStaleSeq 表示批次 Seq 小于已持久化的最大 Seq（旧批不得覆盖新批）。
	errStaleSeq = errors.New("stale batch sequence")
)

const (
	metaKeyTargetsSeeded = "targets_seeded"
	metaKeyLastBatchSeq  = "last_batch_seq"
	metaGlobalDayPrefix  = "global_day:"

	// historyWindowHours 是 24h 历史视图的桶数（含当前未完整小时）。
	historyWindowHours = 24
)

// TargetRecord 是一条目标的完整持久化快照。
type TargetRecord struct {
	ID                int64      `json:"id"`
	Name              string     `json:"name"`
	URL               string     `json:"url"`
	Weight            int        `json:"weight"`
	Enabled           bool       `json:"enabled"`
	Health            string     `json:"health"` // Healthy|RateLimited|Dead|Unknown
	CooldownUntilMS   int64      `json:"cooldown_until_ms"`
	LastStatusCode    int        `json:"last_status_code"`
	LatencyMS         int64      `json:"latency_ms"`
	ErrorMsg          string     `json:"error_msg"`
	ConsecutiveErrors int        `json:"consecutive_errors"`
	TotalBytes        int64      `json:"total_bytes"` // 累计字节（含今日，R1）
	DeletedAt         *time.Time `json:"-"`
}

// TargetPatch 是目标的局部更新载荷：指针为 nil 表示不修改该字段。
type TargetPatch struct {
	Name    *string `json:"name"`
	URL     *string `json:"url"`
	Weight  *int    `json:"weight"`
	Enabled *bool   `json:"enabled"`
}

// TargetHealthPersist 是一次批次提交中要持久化的目标健康列（绝对值）。
type TargetHealthPersist struct {
	Health            string
	CooldownUntilMS   int64
	LastStatusCode    int
	LatencyMS         int64
	ErrorMsg          string
	ConsecutiveErrors int
}

// CounterBatch 是不可变的计数快照，一次刷盘提交一批。
type CounterBatch struct {
	TargetDays   map[string]map[int64]int64 // 日期'YYYY-MM-DD' -> target_id -> 该日累计绝对值
	GlobalDays   map[string]int64           // 日期 -> 全局该日累计绝对值
	TargetTotals map[int64]int64            // target_id -> 全量累计绝对值（含今日，R1）
	HourBuckets  map[int64]int64            // hour_unix -> 全局该小时累计绝对值
	TargetHealth map[int64]TargetHealthPersist
	Seq          uint64
}

// EngineRestoreState 是引擎启动恢复所需的持久化状态。
type EngineRestoreState struct {
	Targets     []TargetRecord
	TargetDays  map[string]map[int64]int64 // 今日各 target 绝对值（至少含今日）
	GlobalDays  map[string]int64           // 至少含今日
	HourBuckets map[int64]int64            // 最近 24 个本地小时桶
	LastSeq     uint64                     // metadata.last_batch_seq
}

// HistoryPoint 是 24h 历史视图中的一个钟点桶。
type HistoryPoint struct {
	HourUnix int64  `json:"-"`
	Hour     string `json:"hour"` // RFC3339，带本地偏移
	Bytes    int64  `json:"bytes"`
}

// Store 是 SQLite 持久化句柄。写操作由单把互斥串行化，WAL 下读并发安全。
type Store struct {
	db *sql.DB
	mu sync.Mutex
}

// dbExecutor 抽象 *sql.DB 与 *sql.Tx 的读写能力，供事务内外共用查询。
type dbExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS targets (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		url TEXT NOT NULL,
		weight INTEGER NOT NULL DEFAULT 10,
		enabled INTEGER NOT NULL DEFAULT 1,
		health TEXT NOT NULL DEFAULT 'Unknown',
		cooldown_until_ms INTEGER NOT NULL DEFAULT 0,
		last_status_code INTEGER NOT NULL DEFAULT 0,
		latency_ms INTEGER NOT NULL DEFAULT 0,
		error_msg TEXT NOT NULL DEFAULT '',
		consecutive_errors INTEGER NOT NULL DEFAULT 0,
		total_bytes INTEGER NOT NULL DEFAULT 0,
		deleted_at INTEGER
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_targets_url_live ON targets(url) WHERE deleted_at IS NULL`,
	`CREATE TABLE IF NOT EXISTS daily_stats (
		day TEXT NOT NULL,
		target_id INTEGER NOT NULL,
		bytes INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (day, target_id)
	)`,
	`CREATE TABLE IF NOT EXISTS history (
		hour_ts INTEGER PRIMARY KEY,
		bytes INTEGER NOT NULL DEFAULT 0
	)`,
	`CREATE TABLE IF NOT EXISTS metadata (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	)`,
}

const targetSelectColumns = `id, name, url, weight, enabled, health, cooldown_until_ms,
	last_status_code, latency_ms, error_msg, consecutive_errors, total_bytes, deleted_at`

// sqliteDSN 构造契约指定的 DSN（WAL、busy_timeout、synchronous=NORMAL）。
func sqliteDSN(path string) string {
	escaped := strings.NewReplacer("?", "%3F", "#", "%23").Replace(path)
	return "file:" + escaped +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=synchronous(NORMAL)"
}

// OpenStore 打开（必要时创建）指定路径的 SQLite 数据库。
func OpenStore(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("db: empty database path")
	}
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("db: open %q: %w", path, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("db: ping %q: %w", path, err)
	}
	return &Store{db: db}, nil
}

// InitSchema 幂等地创建四张表与部分唯一索引。
func (s *Store) InitSchema() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx := context.Background()
	for _, stmt := range schemaStatements {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("db: init schema: %w", err)
		}
	}
	return nil
}

// EnsureSeeded 仅在 metadata 缺少 targets_seeded 标记时把 cfg.Targets 导入数据库。
// 单事务完成；空列表同样写入标记。此后数据库是唯一权威来源。
func (s *Store) EnsureSeeded(cfg *Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx := context.Background()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("db: seed begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var marker string
	err = tx.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key = ?`, metaKeyTargetsSeeded).Scan(&marker)
	switch {
	case err == nil:
		return tx.Commit()
	case !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("db: seed read marker: %w", err)
	}

	if cfg != nil {
		for _, t := range cfg.Targets {
			weight := t.Weight
			if weight <= 0 {
				weight = defaultTargetWeight
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT OR IGNORE INTO targets(name, url, weight, enabled) VALUES(?, ?, ?, ?)`,
				t.Name, t.URL, weight, boolToInt(t.Enabled),
			); err != nil {
				return fmt.Errorf("db: seed target %q: %w", t.URL, err)
			}
		}
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO metadata(key, value) VALUES(?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		metaKeyTargetsSeeded, "1",
	); err != nil {
		return fmt.Errorf("db: seed marker: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("db: seed commit: %w", err)
	}
	return nil
}

// ListTargetRecords 返回全部未软删目标，按 id 升序。
func (s *Store) ListTargetRecords() ([]TargetRecord, error) {
	ctx := context.Background()
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+targetSelectColumns+` FROM targets WHERE deleted_at IS NULL ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("db: list targets: %w", err)
	}
	defer rows.Close()

	records := make([]TargetRecord, 0, 16)
	for rows.Next() {
		rec, err := scanTargetRecord(rows)
		if err != nil {
			return nil, fmt.Errorf("db: scan target: %w", err)
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("db: list targets: %w", err)
	}
	return records, nil
}

// CreateTarget 新建一条目标。重复 url（未软删）返回 ErrDuplicateURL。
func (s *Store) CreateTarget(p TargetPatch) (TargetRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx := context.Background()

	name := ""
	if p.Name != nil {
		name = *p.Name
	}
	url := ""
	if p.URL != nil {
		url = *p.URL
	}
	weight := defaultTargetWeight
	if p.Weight != nil {
		weight = *p.Weight
	}
	if weight <= 0 {
		weight = defaultTargetWeight
	}
	enabled := true
	if p.Enabled != nil {
		enabled = *p.Enabled
	}

	res, err := s.db.ExecContext(ctx,
		`INSERT INTO targets(name, url, weight, enabled) VALUES(?, ?, ?, ?)`,
		name, url, weight, boolToInt(enabled),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return TargetRecord{}, ErrDuplicateURL
		}
		return TargetRecord{}, fmt.Errorf("db: create target: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return TargetRecord{}, fmt.Errorf("db: create target id: %w", err)
	}
	return s.getLiveTarget(ctx, s.db, id)
}

// PatchTarget 局部更新一条目标：nil 字段不改。目标不存在或已软删返回 ErrNotFound。
func (s *Store) PatchTarget(id int64, p TargetPatch) (TargetRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx := context.Background()

	sets := make([]string, 0, 4)
	args := make([]any, 0, 5)
	if p.Name != nil {
		sets = append(sets, "name = ?")
		args = append(args, *p.Name)
	}
	if p.URL != nil {
		sets = append(sets, "url = ?")
		args = append(args, *p.URL)
	}
	if p.Weight != nil {
		sets = append(sets, "weight = ?")
		args = append(args, *p.Weight)
	}
	if p.Enabled != nil {
		sets = append(sets, "enabled = ?")
		args = append(args, boolToInt(*p.Enabled))
	}

	if len(sets) == 0 {
		return s.getLiveTarget(ctx, s.db, id)
	}

	args = append(args, id)
	res, err := s.db.ExecContext(ctx,
		`UPDATE targets SET `+strings.Join(sets, ", ")+` WHERE id = ? AND deleted_at IS NULL`, args...)
	if err != nil {
		if isUniqueViolation(err) {
			return TargetRecord{}, ErrDuplicateURL
		}
		return TargetRecord{}, fmt.Errorf("db: patch target %d: %w", id, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return TargetRecord{}, fmt.Errorf("db: patch target %d: %w", id, err)
	}
	if affected == 0 {
		return TargetRecord{}, ErrNotFound
	}
	return s.getLiveTarget(ctx, s.db, id)
}

// SoftDeleteTarget 软删一条目标（保留历史统计）。deleted_at 存 Unix 毫秒。
// 不存在或已软删返回 ErrNotFound。
func (s *Store) SoftDeleteTarget(id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx := context.Background()

	res, err := s.db.ExecContext(ctx,
		`UPDATE targets SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL`,
		time.Now().UnixMilli(), id)
	if err != nil {
		return fmt.Errorf("db: delete target %d: %w", id, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("db: delete target %d: %w", id, err)
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

// PersistBatch 在单事务内按绝对值 upsert 一批计数。同 Seq 重放幂等，旧 Seq 被拒绝。
func (s *Store) PersistBatch(b CounterBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx := context.Background()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("db: batch begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	lastSeq, hasLast, err := readMetaUint64(ctx, tx, metaKeyLastBatchSeq)
	if err != nil {
		return fmt.Errorf("db: batch read seq: %w", err)
	}
	if hasLast {
		if b.Seq < lastSeq {
			return fmt.Errorf("%w: batch seq %d < persisted %d", errStaleSeq, b.Seq, lastSeq)
		}
		if b.Seq == lastSeq && lastSeq > 0 {
			// 同批重试：上批已原子提交，直接成功。
			return nil
		}
	}

	for day, perTarget := range b.TargetDays {
		for targetID, bytes := range perTarget {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO daily_stats(day, target_id, bytes) VALUES(?, ?, ?)
				 ON CONFLICT(day, target_id) DO UPDATE SET bytes = excluded.bytes`,
				day, targetID, bytes,
			); err != nil {
				return fmt.Errorf("db: batch daily_stats %s/%d: %w", day, targetID, err)
			}
		}
	}

	for hourTS, bytes := range b.HourBuckets {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO history(hour_ts, bytes) VALUES(?, ?)
			 ON CONFLICT(hour_ts) DO UPDATE SET bytes = excluded.bytes`,
			hourTS, bytes,
		); err != nil {
			return fmt.Errorf("db: batch history %d: %w", hourTS, err)
		}
	}

	for targetID, total := range b.TargetTotals {
		if _, err := tx.ExecContext(ctx,
			`UPDATE targets SET total_bytes = ? WHERE id = ?`, total, targetID,
		); err != nil {
			return fmt.Errorf("db: batch total_bytes %d: %w", targetID, err)
		}
	}

	for targetID, h := range b.TargetHealth {
		if _, err := tx.ExecContext(ctx,
			`UPDATE targets SET health = ?, cooldown_until_ms = ?, last_status_code = ?,
			 latency_ms = ?, error_msg = ?, consecutive_errors = ? WHERE id = ?`,
			h.Health, h.CooldownUntilMS, h.LastStatusCode, h.LatencyMS, h.ErrorMsg, h.ConsecutiveErrors, targetID,
		); err != nil {
			return fmt.Errorf("db: batch health %d: %w", targetID, err)
		}
	}

	for day, bytes := range b.GlobalDays {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO metadata(key, value) VALUES(?, ?)
			 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
			metaGlobalDayPrefix+day, strconv.FormatInt(bytes, 10),
		); err != nil {
			return fmt.Errorf("db: batch global_day %s: %w", day, err)
		}
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO metadata(key, value) VALUES(?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		metaKeyLastBatchSeq, strconv.FormatUint(b.Seq, 10),
	); err != nil {
		return fmt.Errorf("db: batch seq: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("db: batch commit: %w", err)
	}
	return nil
}

// LoadEngineState 读取引擎启动恢复所需状态：目标、日桶、全局日桶、最近 24 小时桶、LastSeq。
func (s *Store) LoadEngineState(now time.Time) (EngineRestoreState, error) {
	ctx := context.Background()
	today := now.In(time.Local).Format("2006-01-02")

	targets, err := s.ListTargetRecords()
	if err != nil {
		return EngineRestoreState{}, err
	}

	targetDays, err := loadDailyStats(ctx, s.db)
	if err != nil {
		return EngineRestoreState{}, err
	}
	if _, ok := targetDays[today]; !ok {
		targetDays[today] = map[int64]int64{}
	}

	globalDays, err := loadGlobalDays(ctx, s.db)
	if err != nil {
		return EngineRestoreState{}, err
	}
	if _, ok := globalDays[today]; !ok {
		globalDays[today] = 0
	}

	hours, err := s.loadHourBuckets(ctx, now)
	if err != nil {
		return EngineRestoreState{}, err
	}

	lastSeq, _, err := readMetaUint64(ctx, s.db, metaKeyLastBatchSeq)
	if err != nil {
		return EngineRestoreState{}, fmt.Errorf("db: load last seq: %w", err)
	}

	return EngineRestoreState{
		Targets:     targets,
		TargetDays:  targetDays,
		GlobalDays:  globalDays,
		HourBuckets: hours,
		LastSeq:     lastSeq,
	}, nil
}

// History24h 返回本地时区最近 24 个钟点小时桶（含当前未完整小时），补零、最旧在前。
func (s *Store) History24h(now time.Time) ([]HistoryPoint, error) {
	ctx := context.Background()
	starts := localHourStarts(now)

	buckets, err := s.loadHourBucketsBetween(ctx, starts[0].Unix(), starts[len(starts)-1].Unix())
	if err != nil {
		return nil, err
	}

	points := make([]HistoryPoint, 0, len(starts))
	for _, start := range starts {
		ts := start.Unix()
		points = append(points, HistoryPoint{
			HourUnix: ts,
			Hour:     time.Unix(ts, 0).In(time.Local).Format(time.RFC3339),
			Bytes:    buckets[ts],
		})
	}
	return points, nil
}

// Close 关闭底层数据库。
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// loadHourBuckets 读取最近 24 个本地小时窗口内已存在的桶。
func (s *Store) loadHourBuckets(ctx context.Context, now time.Time) (map[int64]int64, error) {
	starts := localHourStarts(now)
	return s.loadHourBucketsBetween(ctx, starts[0].Unix(), starts[len(starts)-1].Unix())
}

func (s *Store) loadHourBucketsBetween(ctx context.Context, lo, hi int64) (map[int64]int64, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT hour_ts, bytes FROM history WHERE hour_ts >= ? AND hour_ts <= ?`, lo, hi)
	if err != nil {
		return nil, fmt.Errorf("db: load hour buckets: %w", err)
	}
	defer rows.Close()

	buckets := make(map[int64]int64, historyWindowHours)
	for rows.Next() {
		var ts, bytes int64
		if err := rows.Scan(&ts, &bytes); err != nil {
			return nil, fmt.Errorf("db: scan hour bucket: %w", err)
		}
		buckets[ts] = bytes
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("db: load hour buckets: %w", err)
	}
	return buckets, nil
}

func loadDailyStats(ctx context.Context, q dbExecutor) (map[string]map[int64]int64, error) {
	rows, err := q.QueryContext(ctx, `SELECT day, target_id, bytes FROM daily_stats`)
	if err != nil {
		return nil, fmt.Errorf("db: load daily_stats: %w", err)
	}
	defer rows.Close()

	days := make(map[string]map[int64]int64)
	for rows.Next() {
		var day string
		var targetID, bytes int64
		if err := rows.Scan(&day, &targetID, &bytes); err != nil {
			return nil, fmt.Errorf("db: scan daily_stats: %w", err)
		}
		perTarget, ok := days[day]
		if !ok {
			perTarget = make(map[int64]int64)
			days[day] = perTarget
		}
		perTarget[targetID] = bytes
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("db: load daily_stats: %w", err)
	}
	return days, nil
}

func loadGlobalDays(ctx context.Context, q dbExecutor) (map[string]int64, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT key, value FROM metadata WHERE key LIKE ?`, metaGlobalDayPrefix+"%")
	if err != nil {
		return nil, fmt.Errorf("db: load global days: %w", err)
	}
	defer rows.Close()

	days := make(map[string]int64)
	for rows.Next() {
		var key, raw string
		if err := rows.Scan(&key, &raw); err != nil {
			return nil, fmt.Errorf("db: scan global day: %w", err)
		}
		day := strings.TrimPrefix(key, metaGlobalDayPrefix)
		bytes, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("db: parse global day %q: %w", key, err)
		}
		days[day] = bytes
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("db: load global days: %w", err)
	}
	return days, nil
}

func readMetaUint64(ctx context.Context, q dbExecutor, key string) (uint64, bool, error) {
	var raw string
	err := q.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key = ?`, key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("metadata %q: %w", key, err)
	}
	return value, true, nil
}

func (s *Store) getLiveTarget(ctx context.Context, q dbExecutor, id int64) (TargetRecord, error) {
	row := q.QueryRowContext(ctx,
		`SELECT `+targetSelectColumns+` FROM targets WHERE id = ? AND deleted_at IS NULL`, id)
	rec, err := scanTargetRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetRecord{}, ErrNotFound
	}
	if err != nil {
		return TargetRecord{}, fmt.Errorf("db: get target %d: %w", id, err)
	}
	return rec, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanTargetRecord(sc rowScanner) (TargetRecord, error) {
	var (
		rec       TargetRecord
		enabled   int
		deletedAt sql.NullInt64
	)
	if err := sc.Scan(
		&rec.ID, &rec.Name, &rec.URL, &rec.Weight, &enabled, &rec.Health,
		&rec.CooldownUntilMS, &rec.LastStatusCode, &rec.LatencyMS, &rec.ErrorMsg,
		&rec.ConsecutiveErrors, &rec.TotalBytes, &deletedAt,
	); err != nil {
		return TargetRecord{}, err
	}
	rec.Enabled = enabled != 0
	if deletedAt.Valid {
		t := time.UnixMilli(deletedAt.Int64)
		rec.DeletedAt = &t
	}
	return rec, nil
}

// localHourStarts 返回最近 24 个本地钟点小时的起始时刻，最旧在前、当前小时后最后。
// 逐小时回退基于绝对时间，DST 重复/缺失小时按实际时间自然排序。
func localHourStarts(now time.Time) []time.Time {
	local := now.In(time.Local)
	current := time.Date(local.Year(), local.Month(), local.Day(), local.Hour(), 0, 0, 0, time.Local)
	starts := make([]time.Time, historyWindowHours)
	for i := 0; i < historyWindowHours; i++ {
		starts[historyWindowHours-1-i] = current.Add(-time.Duration(i) * time.Hour)
	}
	return starts
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		switch sqliteErr.Code() {
		case 19, 1555, 2067: // SQLITE_CONSTRAINT / _PRIMARYKEY / _UNIQUE
			return true
		}
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
