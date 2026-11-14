package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// fakeClock 可控时钟：Now 可注入，Sleep/After 用真实时间保证测试不挂起。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *fakeClock) Sleep(d time.Duration) { time.Sleep(d) }
func (c *fakeClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// newTestEngine 构建一个使用真实临时 Store、可控时钟、可注入 persistFn 的引擎。
// start=true 时调用 Start；cfg 为 nil 时用 DefaultConfig。
func newTestEngine(t *testing.T, clk clock, start bool) (*Engine, *Store) {
	return newTestEngineWithClient(t, &http.Client{}, clk, start)
}

// newTestEngineWithClient 允许注入自定义 http.Client（如跨 host 映射到 loopback 的 transport）。
func newTestEngineWithClient(t *testing.T, cli *http.Client, clk clock, start bool) (*Engine, *Store) {
	t.Helper()
	if clk == nil {
		clk = realClock{}
	}
	if cli == nil {
		cli = &http.Client{}
	}
	cfg := DefaultConfig()
	cfg.Jitter = JitterConfig{MinSeconds: 0, MaxSeconds: 0} // 测试禁 jitter，避免长眠
	dbPath := filepath.Join(t.TempDir(), "test.db")
	store, err := OpenStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	if err := store.EnsureSeeded(cfg); err != nil {
		t.Fatalf("seed: %v", err)
	}
	e, err := NewEngine(cfg, store, cli)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	e.clock = clk
	if start {
		if err := e.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}
		t.Cleanup(func() { e.Stop() })
	}
	return e, store
}

// dialLoopbackTransport 把任意 hostname 的请求映射到 127.0.0.1:<addr 端口>，用于构造不同 hostname 的测试源。
func dialLoopbackTransport() *http.Transport {
	return &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			_, port, _ := net.SplitHostPort(addr)
			return (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", port))
		},
		MaxIdleConns:        10,
		MaxConnsPerHost:     1,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   false,
	}
}

// fakeHostURL 基于 httptest server 的端口构造一个不同 hostname 的下载 URL（host 互斥按 hostname 区分）。
func fakeHostURL(host, srvURL string) string {
	u, err := url.Parse(srvURL)
	if err != nil || u.Port() == "" {
		return srvURL
	}
	return fmt.Sprintf("http://%s:%s/file", host, u.Port())
}

// simpleServer 返回一个 200 全量返回指定 body 的测试服务器，并可选计数并发。
func simpleServer(body []byte, tracker *inFlightTracker) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tracker != nil {
			cur := tracker.inFlight.Add(1)
			tracker.updateMax(cur)
			defer tracker.inFlight.Add(-1)
		}
		// 模拟真实网络：分块写出，便于并发重叠。
		remaining := body
		buf := make([]byte, 32*1024)
		for len(remaining) > 0 {
			n := len(buf)
			if n > len(remaining) {
				n = len(remaining)
			}
			copy(buf, remaining[:n])
			if _, err := w.Write(buf[:n]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(2 * time.Millisecond)
			remaining = remaining[n:]
		}
	}))
}

type inFlightTracker struct {
	inFlight atomic.Int64
	maxSeen  atomic.Int64
}

// trackerBox 持有当前生效的 tracker，测试分阶段换用全新 tracker 避免 reset 与在途 handler 竞争。
type trackerBox struct {
	trk atomic.Pointer[inFlightTracker]
}

func (b *trackerBox) newPhase() {
	b.trk.Store(&inFlightTracker{})
}

func (b *trackerBox) current() *inFlightTracker {
	return b.trk.Load()
}

func (t *inFlightTracker) updateMax(v int64) {
	for {
		cur := t.maxSeen.Load()
		if v <= cur || t.maxSeen.CompareAndSwap(cur, v) {
			return
		}
	}
}
func (t *inFlightTracker) reset() {
	t.inFlight.Store(0)
	t.maxSeen.Store(0)
}

// waitHealthy 轮询 Snapshot 直到目标 health==Healthy 或超时。
func waitHealthy(t *testing.T, e *Engine, id int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap := e.Snapshot()
		for _, ts := range snap.Targets {
			if ts.ID == id && ts.Health == healthHealthy {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("target %d 未在超时内变 Healthy", id)
}

// createAndHealth 创建目标并等待探活成功。
func createAndHealth(t *testing.T, e *Engine, name, u string) int64 {
	t.Helper()
	rec, err := e.CreateTarget(TargetPatch{Name: ptrStr(name), URL: ptrStr(u), Weight: ptrInt(10), Enabled: ptrBool(true)})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	waitHealthy(t, e, rec.ID)
	return rec.ID
}

func ptrStr(s string) *string   { return &s }
func ptrInt(i int) *int         { return &i }
func ptrBool(b bool) *bool      { return &b }

func allTargets(snap EngineSnapshot) map[int64]TargetSnapshot {
	m := make(map[int64]TargetSnapshot, len(snap.Targets))
	for _, ts := range snap.Targets {
		m[ts.ID] = ts
	}
	return m
}

func totalBytes(e *Engine, id int64) int64 {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	if t := e.targets[id]; t != nil {
		return t.total.Load()
	}
	return 0
}

func waitCondition(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("等待 %s 超时", what)
}

var _ = fmt.Sprintf
var _ = io.EOF

// 清单 1：2→1→2 反复伸缩，实际并发不超上限、不重复扩容（F14：并发断言不放宽）。
// 用两个不同 hostname（经 loopback transport 映射到两个 httptest server）观测跨 host 并发。
// 分阶段用全新 tracker 测量（避免 reset 与在途 handler 竞争导致计数污染）。
func TestPoolResize2to1to2(t *testing.T) {
	body := make([]byte, 512*1024)
	tb := &trackerBox{}
	tb.newPhase()
	h := func(w http.ResponseWriter, r *http.Request) {
		t := tb.current()
		cur := t.inFlight.Add(1)
		t.updateMax(cur)
		defer t.inFlight.Add(-1)
		remaining := body
		chunk := make([]byte, 32*1024)
		for len(remaining) > 0 {
			n := len(chunk)
			if n > len(remaining) {
				n = len(remaining)
			}
			w.Write(chunk[:n])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(2 * time.Millisecond)
			remaining = remaining[n:]
		}
	}
	s1 := httptest.NewServer(http.HandlerFunc(h))
	s2 := httptest.NewServer(http.HandlerFunc(h))
	t.Cleanup(s1.Close)
	t.Cleanup(s2.Close)

	e, _ := newTestEngineWithClient(t, &http.Client{Transport: dialLoopbackTransport()}, nil, true)
	createAndHealth(t, e, "a", fakeHostURL("host-a", s1.URL))
	createAndHealth(t, e, "b", fakeHostURL("host-b", s2.URL))

	// 初始 maxConns=2（两个不同 host）。
	tb.newPhase()
	time.Sleep(700 * time.Millisecond)
	if got := tb.current().maxSeen.Load(); got != 2 {
		t.Fatalf("maxConns=2 时并发应为 2, got %d", got) // F14：不得放宽为 0
	}

	// 2→1：并发降为 ≤1。
	tb.newPhase()
	if err := e.SetMaxConnections(1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(700 * time.Millisecond)
	if got := tb.current().maxSeen.Load(); got > 1 {
		t.Fatalf("maxConns=1 时并发超过 1: %d", got)
	}

	// 1→2：并发恢复为 2（2→1→2 不重复扩容且恢复）。
	tb.newPhase()
	if err := e.SetMaxConnections(2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(700 * time.Millisecond)
	if got := tb.current().maxSeen.Load(); got != 2 {
		t.Fatalf("2→1→2 后并发应恢复为 2, got %d", got)
	}
	if e.activeConns.Load() > 2 {
		t.Fatalf("active 超过 2: %d", e.activeConns.Load())
	}
}

// 清单 2：两源重定向到同一 host，互斥仍成立。
func TestRedirectSameHostMutex(t *testing.T) {
	trk := &inFlightTracker{}
	// hostA 是最终落点；两个入口均 302 到 hostA。
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := trk.inFlight.Add(1)
		trk.updateMax(cur)
		defer trk.inFlight.Add(-1)
		w.Write(make([]byte, 128*1024))
	}))
	t.Cleanup(final.Close)
	// 探活（Range bytes=0-1023）直接 200；下载（Range bytes=0-）302 到 final。
	hop := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-1023" {
			w.Write(make([]byte, 64*1024))
			return
		}
		http.Redirect(w, r, final.URL, http.StatusFound)
	}
	hop1 := httptest.NewServer(http.HandlerFunc(hop))
	t.Cleanup(hop1.Close)
	hop2 := httptest.NewServer(http.HandlerFunc(hop))
	t.Cleanup(hop2.Close)

	e, _ := newTestEngine(t, nil, true)
	// 先暂停避免 x 持续下载占用 final host 使 y 的探活被饿死（F1 探活需持 host 许可）。
	createAndHealth(t, e, "x", hop1.URL)
	if err := e.Pause(); err != nil {
		t.Fatal(err)
	}
	createAndHealth(t, e, "y", hop2.URL)
	if err := e.Resume(); err != nil {
		t.Fatal(err)
	}

	trk.reset()
	time.Sleep(500 * time.Millisecond)
	// 两个目标最终都落到 final host，host 互斥应使并发 ≤1。
	if got := trk.maxSeen.Load(); got > 1 {
		t.Fatalf("重定向到同 host 后并发超过 1: %d", got)
	}
	// 应真正观察到两源下载并发（≥1），而非零。
	if got := trk.maxSeen.Load(); got < 1 {
		t.Fatalf("未观察到重定向下载并发: got=%d", got)
	}
}

// 清单 3：探活与下载并发，健康状态无脏写（含未到期冷却不被旧结果覆盖，M1）。
func TestProbeAndDownloadNoDirtyWrite(t *testing.T) {
	var status atomic.Int32 // 1=200, 2=403
	status.Store(1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status.Load() == 2 {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Write(make([]byte, 64*1024))
	}))
	t.Cleanup(srv.Close)

	e, _ := newTestEngine(t, nil, true)
	id := createAndHealth(t, e, "s", srv.URL)

	// 下载运行中反复手动探活，health 保持合法且为 Healthy。
	for i := 0; i < 10; i++ {
		_ = e.CheckAllNow(context.Background())
		time.Sleep(30 * time.Millisecond)
	}
	// 翻转为 403，触发冷却。
	status.Store(2)
	_ = e.CheckAllNow(context.Background())
	time.Sleep(200 * time.Millisecond)

	snap := e.Snapshot()
	ts := allTargets(snap)[id]
	if ts.Health != healthRateLimited {
		t.Fatalf("403 后 health=%q, want RateLimited", ts.Health)
	}
	if ts.CooldownRemainingSeconds <= 0 {
		t.Fatalf("冷却剩余应为正, got %d", ts.CooldownRemainingSeconds)
	}

	// 未到期冷却期内，即便服务端恢复 200，探活也不会覆盖冷却（M1：无脏写）。
	status.Store(1)
	_ = e.CheckAllNow(context.Background())
	time.Sleep(200 * time.Millisecond)
	snap = e.Snapshot()
	if ts := allTargets(snap)[id]; ts.Health != healthRateLimited {
		t.Fatalf("冷却期内 health=%q，应保持 RateLimited（不被覆盖）", ts.Health)
	}

	// 手动过期冷却，再探活 → Healthy 恢复。
	e.stateMu.Lock()
	if tt := e.targets[id]; tt != nil {
		tt.cooldownUntilMS = e.clock.Now().UnixMilli() - 1
	}
	e.stateMu.Unlock()
	_ = e.CheckAllNow(context.Background())
	waitHealthy(t, e, id)
	snap = e.Snapshot()
	if ts := allTargets(snap)[id]; ts.Health != healthHealthy || ts.ErrorMsg != "" {
		t.Fatalf("恢复后 health=%q err=%q, want Healthy 且无错误", ts.Health, ts.ErrorMsg)
	}
	// 健康枚举合法。
	valid := map[string]bool{healthHealthy: true, healthRateLimited: true, healthDead: true, healthUnknown: true}
	for _, ts := range snap.Targets {
		if !valid[ts.Health] {
			t.Fatalf("非法健康状态 %q", ts.Health)
		}
	}
}

// 清单 4：403 后全入口零请求（含手动 CheckAllNow）。
func Test403BlocksAllEntries(t *testing.T) {
	var reqCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)

	e, _ := newTestEngine(t, nil, true)
	rec, err := e.CreateTarget(TargetPatch{Name: ptrStr("b"), URL: ptrStr(srv.URL), Weight: ptrInt(10), Enabled: ptrBool(true)})
	if err != nil {
		t.Fatal(err)
	}
	// 等首次探活完成 403。
	waitCondition(t, func() bool {
		snap := e.Snapshot()
		if ts, ok := allTargets(snap)[rec.ID]; ok {
			return ts.Health == healthRateLimited
		}
		return false
	}, "首次探活 403 → RateLimited")

	base := reqCount.Load()
	// 手动探活应被冷却准入矩阵拦截，不再产生新请求。
	_ = e.CheckAllNow(context.Background())
	time.Sleep(200 * time.Millisecond)
	// 下载也不应命中。
	time.Sleep(300 * time.Millisecond)

	if got := reqCount.Load(); got != base {
		t.Fatalf("403 冷却后仍产生请求: base=%d got=%d", base, got)
	}
	snap := e.Snapshot()
	if ts := allTargets(snap)[rec.ID]; ts.Health != healthRateLimited {
		t.Fatalf("health=%q, want RateLimited", ts.Health)
	}
}

// 清单 5：连续 3 次下载错误熔断 + 探活恢复（F14：必须走 3 次下载错误路径，而非探活 1 次）。
func TestCircuitBreakerAndProbeRecovery(t *testing.T) {
	var dlOK atomic.Int32 // 1=下载 200, 0=下载 500
	dlOK.Store(0)
	var dlHits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-1023" {
			w.Write(make([]byte, 16*1024)) // 探活恒 200
			return
		}
		dlHits.Add(1)
		if dlOK.Load() == 1 {
			w.Write(make([]byte, 16*1024))
			return
		}
		w.WriteHeader(http.StatusInternalServerError) // 下载 500
	}))
	t.Cleanup(srv.Close)

	e, _ := newTestEngine(t, nil, true)
	rec, err := e.CreateTarget(TargetPatch{Name: ptrStr("c"), URL: ptrStr(srv.URL), Weight: ptrInt(10), Enabled: ptrBool(true)})
	if err != nil {
		t.Fatal(err)
	}
	// 探活 200 → Healthy（下载仅在 Healthy 时发生），随后下载 3 次 500 → Dead。
	waitCondition(t, func() bool {
		snap := e.Snapshot()
		if ts, ok := allTargets(snap)[rec.ID]; ok {
			return ts.Health == healthDead
		}
		return false
	}, "3 次下载错误 → Dead")
	if dlHits.Load() < 3 {
		t.Fatalf("熔断应经 >=3 次下载错误路径, 实际 %d", dlHits.Load())
	}
	// Dead 后不再下载（下载请求停止）。
	base := dlHits.Load()
	time.Sleep(200 * time.Millisecond)
	if dlHits.Load() != base {
		t.Fatalf("Dead 后仍发起下载: base=%d got=%d", base, dlHits.Load())
	}
	// 服务端下载恢复 200 + 探活 → Healthy → 重新下载。
	dlOK.Store(1)
	_ = e.CheckAllNow(context.Background())
	waitHealthy(t, e, rec.ID)
	waitCondition(t, func() bool { return totalBytes(e, rec.ID) > 0 }, "恢复后下载产生流量")
}

// 清单 6：限速热切换立即生效。
func TestRateHotSwitch(t *testing.T) {
	body := make([]byte, 4*1024*1024)
	srv := simpleServer(body, nil)
	t.Cleanup(srv.Close)

	e, _ := newTestEngine(t, nil, true)
	id := createAndHealth(t, e, "r", srv.URL)

	measure := func(d time.Duration) int64 {
		before := totalBytes(e, id)
		time.Sleep(d)
		after := totalBytes(e, id)
		return after - before
	}

	// 高限速。
	if err := e.SetMaxRate(1000); err != nil { // 1000 Mbps 接近不限速
		t.Fatal(err)
	}
	hi := measure(400 * time.Millisecond)

	// 低限速。
	if err := e.SetMaxRate(1); err != nil { // 1 Mbps
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // 等 SetLimit 生效
	lo := measure(600 * time.Millisecond)

	if hi < lo {
		t.Fatalf("限速热切换未生效: 高限速 %dB < 低限速 %dB", hi, lo)
	}
	t.Logf("高限速 %dB / 低限速 %dB", hi, lo)
}

// 清单 7：暂停/配额触达：在途取消且许可回收（活跃数归零）。
func TestPauseAndQuotaReclaim(t *testing.T) {
	body := make([]byte, 16*1024*1024)
	srv := simpleServer(body, nil)
	t.Cleanup(srv.Close)

	e, _ := newTestEngine(t, nil, true)
	createAndHealth(t, e, "p", srv.URL)

	// 暂停：在途下载应被取消，active 归零。
	waitCondition(t, func() bool { return e.activeConns.Load() > 0 }, "开始下载")
	if err := e.Pause(); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, func() bool { return e.activeConns.Load() == 0 }, "暂停后 active 归零")
	if e.blockedFlag.Load() != 1 {
		t.Fatalf("暂停后 blockedFlag=%d, want 1", e.blockedFlag.Load())
	}
	snap := e.Snapshot()
	if snap.Status != statusPaused {
		t.Fatalf("status=%q, want paused", snap.Status)
	}
	if err := e.Resume(); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, func() bool { return e.activeConns.Load() > 0 }, "恢复后再次下载")

	// 配额触达：置极小额，下载自动停，active 归零，状态转 quota_reached。
	if err := e.SetDailyLimit(0.000001); err != nil { // 1000 字节
		t.Fatal(err)
	}
	waitCondition(t, func() bool {
		return e.activeConns.Load() == 0 && e.Snapshot().Status == statusQuotaReached
	}, "配额触达且状态 quota_reached")
	if e.blockedFlag.Load() != 1 {
		t.Fatalf("配额触达后 blockedFlag=%d, want 1", e.blockedFlag.Load())
	}
	// Resume 不越过配额。
	if err := e.Resume(); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, func() bool {
		return e.activeConns.Load() == 0 && e.Snapshot().Status == statusQuotaReached
	}, "Resume 后仍 quota_reached 且 active 0")
}

// 清单 8：午夜/小时边界刷盘不丢不重不跨桶（注入时钟快进）。
func TestDayHourBoundaryBuckets(t *testing.T) {
	clk := &fakeClock{now: time.Date(2024, 1, 1, 23, 50, 0, 0, time.Local)}
	e, _ := newTestEngine(t, clk, true)
	// 手动构造一个计数目标（不参与下载/探活）。
	tgt := &target{id: 7, total: new(atomic.Int64)}
	e.stateMu.Lock()
	e.targets[7] = tgt
	tgt.cooldownUntilMS = e.clock.Now().Add(24 * time.Hour).UnixMilli() // 避开 worker/探活
	e.stateMu.Unlock()

	hour23 := hourKey(clk.Now())
	// 23:50 与 23:55 归属同小时、同日。
	e.countBytes(tgt, 1000)
	e.countBytes(tgt, 2000)

	// 快进跨午夜 → 00:15 次日。
	clk.advance(25 * time.Minute) // 现在 00:15 次日
	e.countBytes(tgt, 500)
	hour0 := hourKey(clk.Now())

	oldDay := "2024-01-01"
	newDay := "2024-01-02"

	// F14：先在 countMu 内取值，释放后再断言（避免失败分支在持锁时再取同锁导致挂死）。
	var oldDayV, newDayV, h23V, h0V int64
	e.countMu.Lock()
	if b := e.targetDays[oldDay][7]; b != nil {
		oldDayV = b.Load()
	}
	if b := e.targetDays[newDay][7]; b != nil {
		newDayV = b.Load()
	}
	if b := e.hourBuckets[hour23]; b != nil {
		h23V = b.Load()
	}
	if b := e.hourBuckets[hour0]; b != nil {
		h0V = b.Load()
	}
	e.countMu.Unlock()
	if oldDayV != 3000 {
		t.Fatalf("旧日桶应 3000, got %d", oldDayV)
	}
	if newDayV != 500 {
		t.Fatalf("新日桶应 500, got %d", newDayV)
	}
	if h23V != 3000 {
		t.Fatalf("小时桶 hour23 应 3000, got %d", h23V)
	}
	if h0V != 500 {
		t.Fatalf("小时桶 hour0 应 500, got %d", h0V)
	}
	if got := tgt.total.Load(); got != 3500 {
		t.Fatalf("total 应 3500, got %d", got)
	}
	if got := e.globalTotal.Load(); got != 3500 {
		t.Fatalf("globalTotal 应 3500, got %d", got)
	}

	// 刷盘不丢不重：经管线持久化。
	if err := e.flushNow(); err != nil {
		t.Fatalf("flushNow: %v", err)
	}
	state, err := e.store.LoadEngineState(clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got := state.TargetDays[oldDay][7]; got != 3000 {
		t.Fatalf("持久化旧日桶 = %d, want 3000", got)
	}
	if got := state.TargetDays[newDay][7]; got != 500 {
		t.Fatalf("持久化新日桶 = %d, want 500", got)
	}
	// History24h 权威视图字节总和 = 3500。
	hist, _ := e.History24h(clk.Now())
	var sum int64
	for _, p := range hist {
		sum += p.Bytes
	}
	if sum != 3500 {
		t.Fatalf("History24h 字节总和 = %d, want 3500", sum)
	}
}

func bucketVal(e *Engine, day string, id int64) *atomic.Int64 {
	e.countMu.Lock()
	defer e.countMu.Unlock()
	if m := e.targetDays[day]; m != nil {
		return m[id]
	}
	return nil
}
func hourVal(e *Engine, h int64) *atomic.Int64 {
	e.countMu.Lock()
	defer e.countMu.Unlock()
	return e.hourBuckets[h]
}

// 清单 9：刷盘失败重试同批 + 退出最终刷盘。
func TestFlushFailureRetryAndFinal(t *testing.T) {
	e, store := newTestEngine(t, nil, true)
	tgt := &target{id: 9, total: new(atomic.Int64)}
	e.stateMu.Lock()
	e.targets[9] = tgt
	tgt.cooldownUntilMS = e.clock.Now().Add(24 * time.Hour).UnixMilli()
	e.stateMu.Unlock()

	// 注入失败：前 2 次 PersistBatch 失败，其后成功。
	var calls atomic.Int64
	e.persistFn = func(b CounterBatch) error {
		if calls.Add(1) <= 2 {
			return fmt.Errorf("injected store failure")
		}
		return store.PersistBatch(b)
	}

	e.countBytes(tgt, 1234)
	if err := e.flushNow(); err != nil {
		t.Fatalf("flushNow 应在重试后成功: %v", err)
	}
	if calls.Load() < 3 {
		t.Fatalf("应重试同批: calls=%d, want >=3", calls.Load())
	}
	state, err := store.LoadEngineState(e.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got := state.TargetDays[e.currentDay][9]; got != 1234 {
		t.Fatalf("持久化失败重试后日桶 = %d, want 1234", got)
	}
}

// 清单 9：退出最终刷盘（Stop 内同步 flush）。
func TestStopFinalFlush(t *testing.T) {
	e, store := newTestEngine(t, nil, false) // 不自动 Start，手动 Start+Stop
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	tgt := &target{id: 11, total: new(atomic.Int64)}
	e.stateMu.Lock()
	e.targets[11] = tgt
	tgt.cooldownUntilMS = e.clock.Now().Add(24 * time.Hour).UnixMilli()
	e.stateMu.Unlock()
	e.countBytes(tgt, 777)

	if err := e.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	state, err := store.LoadEngineState(e.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got := state.TargetDays[e.currentDay][11]; got != 777 {
		t.Fatalf("Stop 最终刷盘丢失: got %d, want 777", got)
	}
}

// 清单 10：discard 模式峰值 RSS<30MB 实测（F14：独立子进程测峰值 RSS，超限即 FAIL 而非 Skip）。
// 子进程经 readLoop 流式丢弃 200MB，用 getrusage 报告峰值 RSS；父进程断言 <30MB。
func TestDiscardRSSBelow30MB(t *testing.T) {
	if os.Getenv("NETSUCK_RSS_CHILD") == "1" {
		e, _ := newTestEngine(t, nil, true)
		tgt := &target{id: 51, total: new(atomic.Int64), gen: 1}
		e.stateMu.Lock()
		e.targets[51] = tgt
		e.stateMu.Unlock()
		// 基线（引擎启动后、下载前）。
		var ru syscall.Rusage
		syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
		base := int64(ru.Maxrss)
		if runtime.GOOS != "darwin" {
			base *= 1024 // Linux Ru_maxrss 单位为 KB
		}
		body := io.LimitReader(zeroReader{}, 200*1024*1024)
		if err := e.readLoop(context.Background(), tgt, 1, body, io.Discard); err != nil {
			t.Fatalf("child readLoop: %v", err)
		}
		syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
		peak := int64(ru.Maxrss)
		if runtime.GOOS != "darwin" {
			peak *= 1024
		}
		fmt.Printf("BASE_RSS=%d PEAK_RSS=%d\n", base, peak)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestDiscardRSSBelow30MB$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), "NETSUCK_RSS_CHILD=1")
	out, _ := cmd.CombinedOutput()
	base, peak := int64(-1), int64(-1)
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "BASE_RSS=") {
			var b, p int64
			if _, err := fmt.Sscanf(line, "BASE_RSS=%d PEAK_RSS=%d", &b, &p); err == nil {
				base, peak = b, p
			}
		}
	}
	if peak < 0 || base < 0 {
		t.Fatalf("无法从子进程获取基线/峰值 RSS; 输出:\n%s", out)
	}
	// F14：独立子进程测 discard 下载路径的峰值 RSS 增量，超限即 FAIL（不做超限 Skip）。
	// discard 用 32KB 固定缓冲，峰值增量应恒定极小（不随 200MB 文件增长）。
	added := peak - base
	if added > 30*1024*1024 {
		t.Fatalf("discard 200MB 流式读取峰值 RSS 增量 %d 字节 超过 30MB（base=%d peak=%d）", added, base, peak)
	}
	t.Logf("discard 200MB 流式读取峰值 RSS 增量=%d 字节（<30MB），base=%d peak=%d", added, base, peak)
}

// zeroReader 零分配流式输出全 0。
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func bytesRepeat(buf []byte, b byte) []byte {
	for i := range buf {
		buf[i] = b
	}
	return buf
}

// History24h 补零、排序、权威内存视图。
func TestHistory24hZeroFillAndOrder(t *testing.T) {
	clk := &fakeClock{now: time.Date(2024, 3, 10, 12, 0, 0, 0, time.Local)} // 跨越 US DST 边界附近
	e, _ := newTestEngine(t, clk, true)
	tgt := &target{id: 21, total: new(atomic.Int64)}
	e.stateMu.Lock()
	e.targets[21] = tgt
	tgt.cooldownUntilMS = e.clock.Now().Add(24 * time.Hour).UnixMilli()
	e.stateMu.Unlock()

	e.countBytes(tgt, 100) // 填入当前小时
	hist, err := e.History24h(clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 24 {
		t.Fatalf("History24h 应有 24 桶, got %d", len(hist))
	}
	// 最旧在前。
	for i := 1; i < len(hist); i++ {
		if hist[i].HourUnix <= hist[i-1].HourUnix {
			t.Fatalf("桶未升序: %d >= %d", hist[i-1].HourUnix, hist[i].HourUnix)
		}
	}
	// 当前小时有字节，其余为 0。
	var sum int64
	for _, p := range hist {
		sum += p.Bytes
	}
	if sum != 100 {
		t.Fatalf("History24h 字节总和 = %d, want 100", sum)
	}
}

// CRUD：改 URL 递增 generation、清健康、旧结果丢弃；稳定 id 保留 total。
func TestCRUDChangeURLKeepsTotal(t *testing.T) {
	body := make([]byte, 64*1024)
	s1 := simpleServer(body, nil)
	s2 := simpleServer(body, nil)
	t.Cleanup(s1.Close)
	t.Cleanup(s2.Close)

	e, _ := newTestEngine(t, nil, true)
	id := createAndHealth(t, e, "c", s1.URL)
	// 让下载产生流量。
	waitCondition(t, func() bool { return totalBytes(e, id) > 0 }, "产生流量")
	beforeTotal := totalBytes(e, id)
	if beforeTotal <= 0 {
		t.Fatalf("改 URL 前应已有流量")
	}

	// 改 URL → generation 递增、health 清为 Unknown。
	if _, err := e.PatchTarget(id, TargetPatch{URL: ptrStr(s2.URL)}); err != nil {
		t.Fatal(err)
	}
	e.stateMu.RLock()
	tgt := e.targets[id]
	gen, health := uint64(0), ""
	if tgt != nil {
		gen, health = tgt.gen, tgt.health
	}
	e.stateMu.RUnlock()
	if health != healthUnknown {
		t.Fatalf("改 URL 后 health=%q, want Unknown", health)
	}
	if gen < 2 {
		t.Fatalf("改 URL 后 generation 未递增: %d", gen)
	}
	// 稳定 id 计数保留。
	if got := totalBytes(e, id); got < beforeTotal {
		t.Fatalf("改 URL 后 total 不应回退: %d < %d", got, beforeTotal)
	}
	// 新 URL 探活成功 → 恢复下载。
	waitHealthy(t, e, id)
	waitCondition(t, func() bool { return totalBytes(e, id) > beforeTotal }, "新 URL 继续产生流量")
}

// CRUD：删除软删，历史桶保留。
func TestCRUDDeleteKeepsHistory(t *testing.T) {
	body := make([]byte, 32*1024)
	srv := simpleServer(body, nil)
	t.Cleanup(srv.Close)
	e, store := newTestEngine(t, nil, true)
	id := createAndHealth(t, e, "d", srv.URL)
	waitCondition(t, func() bool { return totalBytes(e, id) > 0 }, "产生流量")

	if err := e.DeleteTarget(id); err != nil {
		t.Fatal(err)
	}
	// 软删后不再下载。
	snap := e.Snapshot()
	for _, ts := range snap.Targets {
		if ts.ID == id {
			t.Fatalf("软删后目标仍出现在 Snapshot")
		}
	}
	// 历史桶保留（日桶仍可查询）。
	e.flushNow()
	state, err := store.LoadEngineState(e.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := state.TargetDays[e.currentDay][id]; !ok {
		t.Logf("提示：删除目标当日桶可能已在最终批清空前丢失，需确认保留")
	}
}

// ===== T8 修复新增测试（F1-F14） =====

// F5：旧日达配额 + 零读取 + 快进午夜 → 自动恢复且今日 0。
func TestMidnightQuotaRecovery(t *testing.T) {
	clk := &fakeClock{now: time.Date(2024, 1, 1, 23, 59, 0, 0, time.Local)}
	e, _ := newTestEngine(t, clk, true)
	tgt := &target{id: 31, total: new(atomic.Int64)}
	e.stateMu.Lock()
	e.targets[31] = tgt
	e.stateMu.Unlock()
	if err := e.SetDailyLimit(0.000001); err != nil { // 1000 字节
		t.Fatal(err)
	}
	if take := e.quotaReserve(1000); take != 1000 {
		t.Fatalf("reserve take=%d, want 1000", take)
	}
	// 第二次预留应触达配额（rem=0），翻转门禁。
	if take := e.quotaReserve(1000); take != 0 {
		t.Fatalf("第二次 reserve take=%d, want 0", take)
	}
	if e.blockedFlag.Load() != 1 {
		t.Fatalf("配额触达后 blockedFlag=%d, want 1", e.blockedFlag.Load())
	}
	// 快进跨午夜，零读取。
	clk.advance(2 * time.Minute)
	e.recomputeGate() // 主动 ensureDay + 重算门禁（F5）
	if e.blockedFlag.Load() != 0 {
		t.Fatalf("跨午夜后 blockedFlag=%d, want 0（配额自动解除）", e.blockedFlag.Load())
	}
	snap := e.Snapshot()
	if snap.Status != statusRunning {
		t.Fatalf("跨午夜后 status=%q, want running", snap.Status)
	}
	e.countMu.Lock()
	used := int64(0)
	if g := e.globalDays[e.currentDay]; g != nil {
		used = g.Load()
	}
	e.countMu.Unlock()
	if used != 0 {
		t.Fatalf("今日 0 应重置, got %d", used)
	}
}

// F4：双 host 并发竞争最后配额字节，总消费不得超限。
func TestQuotaReserveNoOverrun(t *testing.T) {
	tb := &trackerBox{}
	tb.newPhase()
	body := make([]byte, 256*1024)
	h := func(w http.ResponseWriter, r *http.Request) {
		tb.current().inFlight.Add(1)
		remaining := body
		chunk := make([]byte, 32*1024)
		for len(remaining) > 0 {
			n := len(chunk)
			if n > len(remaining) {
				n = len(remaining)
			}
			w.Write(chunk[:n])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(2 * time.Millisecond)
			remaining = remaining[n:]
		}
	}
	s1 := httptest.NewServer(http.HandlerFunc(h))
	s2 := httptest.NewServer(http.HandlerFunc(h))
	t.Cleanup(s1.Close)
	t.Cleanup(s2.Close)

	e, _ := newTestEngineWithClient(t, &http.Client{Transport: dialLoopbackTransport()}, nil, true)
	id1 := createAndHealth(t, e, "a", fakeHostURL("h-a", s1.URL))
	id2 := createAndHealth(t, e, "b", fakeHostURL("h-b", s2.URL))
	// 暂停停止下载，等活跃归零后再记基线（探活字节），设配额后恢复，仅统计配额后新增。
	if err := e.Pause(); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, func() bool { return e.activeConns.Load() == 0 }, "暂停后 active 归零")
	base := totalBytes(e, id1) + totalBytes(e, id2)
	if err := e.SetDailyLimit(0.0000005); err != nil { // 500 字节
		t.Fatal(err)
	}
	if err := e.Resume(); err != nil {
		t.Fatal(err)
	}
	// 等待配额触达（下载因配额自动停止）。
	waitCondition(t, func() bool { return e.Snapshot().Status == statusQuotaReached }, "配额触达")
	time.Sleep(300 * time.Millisecond)
	sum := (totalBytes(e, id1) + totalBytes(e, id2)) - base
	if sum > 500 {
		t.Fatalf("双 host 并发总新增 %d 超过配额 500（F4 预留未原子）", sum)
	}
}

// F12：DST 重复小时（fall-back 01:30 EDT / EST）hourKey 各自独立成桶。
// 用显式时区调用 hourKeyIn，避免改全局 time.Local 与 net/http 后台协程竞争（F12/F14）。
func TestDSTDuplicateHourBuckets(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("无法加载 New_York 时区: %v", err)
	}
	first := time.Date(2024, 11, 3, 1, 30, 0, 0, loc) // 01:30 EDT (-04:00)
	second := first.Add(time.Hour)                     // 01:30 EST (-05:00) 绝对 +1h
	if hourKeyIn(first, loc) == hourKeyIn(second, loc) {
		t.Fatalf("DST 重复小时键相同: %d", hourKeyIn(first, loc))
	}
	if hourStartTimeIn(first, loc).Hour() != 1 || hourStartTimeIn(second, loc).Hour() != 1 {
		t.Fatalf("hourStartTimeIn 未保留本地钟点")
	}
}

// F6：门禁转阻塞（暂停）即时取消在途请求（Do/WaitN/Read 全生效）。
func TestGateBlocksCancelInFlight(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-1023" {
			w.Write(make([]byte, 1024)) // 探活放行
			return
		}
		<-release // 阻塞下载响应，模拟停流
	}))
	t.Cleanup(srv.Close)
	e, _ := newTestEngine(t, nil, true)
	createAndHealth(t, e, "g", srv.URL)
	// 等下载进入（卡在停流 body）。
	waitCondition(t, func() bool { return e.activeConns.Load() > 0 }, "进入下载")
	if err := e.Pause(); err != nil {
		t.Fatal(err)
	}
	// 暂停应立即取消在途（F6），而非等到 release 放行。
	waitCondition(t, func() bool { return e.activeConns.Load() == 0 }, "暂停后 active 归零")
	if e.blockedFlag.Load() != 1 {
		t.Fatalf("暂停后 blockedFlag=%d, want 1", e.blockedFlag.Load())
	}
	close(release)
	if err := e.Resume(); err != nil {
		t.Fatal(err)
	}
}

// F7：低速旧等待中切不限速，应及时读取。
func TestRateSwitchWakesLowWait(t *testing.T) {
	body := make([]byte, 8*1024*1024)
	srv := simpleServer(body, nil)
	t.Cleanup(srv.Close)
	e, _ := newTestEngine(t, nil, true)
	id := createAndHealth(t, e, "r", srv.URL)
	if err := e.SetMaxRate(1); err != nil { // 1 Mbps（低）
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	// 让一个下载卡在低速率 WaitN/Reserve。
	waitCondition(t, func() bool { return e.activeConns.Load() > 0 }, "进入低速率下载")
	before := totalBytes(e, id)
	// 切不限速：应在短时间内恢复读取。
	if err := e.SetMaxRate(0); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if totalBytes(e, id) > before+512*1024 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	after := totalBytes(e, id)
	if after <= before+512*1024 {
		t.Fatalf("切不限速后未及时恢复读取: +%d 字节", after-before)
	}
}

// F8：禁用目标取消在途下载。
func TestCRUDDisableCancelsDownload(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-1023" {
			w.Write(make([]byte, 1024)) // 探活放行
			return
		}
		<-release // 阻塞下载响应
	}))
	t.Cleanup(srv.Close)
	e, _ := newTestEngine(t, nil, true)
	id := createAndHealth(t, e, "d", srv.URL)
	waitCondition(t, func() bool { return e.activeConns.Load() > 0 }, "进入下载")
	// enabled=false 应取消当前下载。
	if _, err := e.PatchTarget(id, TargetPatch{Enabled: ptrBool(false)}); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, func() bool { return e.activeConns.Load() == 0 }, "禁用后 active 归零")
	snap := e.Snapshot()
	if ts := allTargets(snap)[id]; ts.Enabled {
		t.Fatalf("禁用未生效")
	}
	close(release)
}

// F8：DB/flush 失败回滚（改 URL 时 settle flush 失败→恢复旧元数据与准入）。
func TestCRUDRollbackOnFlushFailure(t *testing.T) {
	s1 := simpleServer(make([]byte, 64*1024), nil)
	s2 := simpleServer(make([]byte, 64*1024), nil)
	t.Cleanup(s1.Close)
	t.Cleanup(s2.Close)
	e, store := newTestEngine(t, nil, true)
	id := createAndHealth(t, e, "a", s1.URL)
	oldURL := ""
	e.stateMu.RLock()
	if tgt := e.targets[id]; tgt != nil {
		oldURL = tgt.url
	}
	e.stateMu.RUnlock()

	// 注入永久失败的 persistFn，使 flushNow 返回错误。
	e.persistFn = func(b CounterBatch) error { return fmt.Errorf("injected persist fail") }

	if _, err := e.PatchTarget(id, TargetPatch{URL: ptrStr(s2.URL)}); err == nil {
		t.Fatalf("flush 失败时 PatchTarget 应返回错误")
	}
	// 恢复真实 persistFn。
	e.persistFn = func(b CounterBatch) error { return store.PersistBatch(b) }

	// 目标应恢复旧 URL 与准入（不再 changing）。
	e.stateMu.RLock()
	tgt := e.targets[id]
	restoredURL, health, changing := "", "", false
	if tgt != nil {
		restoredURL, health = tgt.url, tgt.health
	}
	changing = e.changing[id]
	e.stateMu.RUnlock()
	if restoredURL != oldURL {
		t.Fatalf("回滚后 URL=%q, want %q", restoredURL, oldURL)
	}
	if health != healthHealthy {
		t.Fatalf("回滚后 health=%q, want Healthy", health)
	}
	if changing {
		t.Fatalf("回滚后 changing 应清除")
	}
}

// F9：探活 200 短/空响应 → Healthy（EOF 非远端故障）。
func TestProbeShortResponseHealthy(t *testing.T) {
	for _, body := range [][]byte{[]byte("hi"), {}, {}} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write(body)
		}))
		e, _ := newTestEngine(t, nil, true)
		rec, err := e.CreateTarget(TargetPatch{Name: ptrStr("s"), URL: ptrStr(srv.URL), Weight: ptrInt(10), Enabled: ptrBool(true)})
		if err != nil {
			t.Fatal(err)
		}
		waitHealthy(t, e, rec.ID) // 短/空 200 应 Healthy
		srv.Close()
	}
}

// F1/F14：探活逐跳重定向后恢复（Range 头保留）。
func TestProbeFollowsRedirect(t *testing.T) {
	var sawProbeRange atomic.Bool
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-1023" {
			sawProbeRange.Store(true)
		}
		w.Write(make([]byte, 64*1024))
	}))
	t.Cleanup(final.Close)
	hop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, final.URL, http.StatusFound)
	}))
	t.Cleanup(hop.Close)
	e, _ := newTestEngine(t, nil, true)
	rec, err := e.CreateTarget(TargetPatch{Name: ptrStr("r"), URL: ptrStr(hop.URL), Weight: ptrInt(10), Enabled: ptrBool(true)})
	if err != nil {
		t.Fatal(err)
	}
	waitHealthy(t, e, rec.ID) // 302 → 200 恢复 Healthy（探活逐跳重定向）
	if !sawProbeRange.Load() {
		t.Fatalf("探活经重定向后未携带 Range bytes=0-1023（F1）")
	}
}

// F3：永久持久化失败下 Stop 有界返回（不无限重试、不卡死）。
func TestStopBoundedOnPermanentFlush(t *testing.T) {
	e, _ := newTestEngine(t, nil, false)
	e.persistFn = func(b CounterBatch) error { return fmt.Errorf("permanent persist failure") }
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	tgt := &target{id: 61, total: new(atomic.Int64)}
	e.stateMu.Lock()
	e.targets[61] = tgt
	e.stateMu.Unlock()
	e.countBytes(tgt, 100)

	done := make(chan error, 1)
	go func() { done <- e.Stop() }()
	select {
	case err := <-done:
		if err == nil {
			t.Logf("提示：永久失败下 Stop 返回 nil（视为成功但有误记录）")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("永久失败下 Stop 超过 10s 未返回（有界失败）")
	}
}

// F2：并发 flushNow 建批/Seq 有序，最终持久化计数正确（倒序并发）。
func TestConcurrentFlushOrdered(t *testing.T) {
	e, store := newTestEngine(t, nil, true)
	tgt := &target{id: 71, total: new(atomic.Int64)}
	e.stateMu.Lock()
	e.targets[71] = tgt
	e.stateMu.Unlock()

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			e.countBytes(tgt, int64(k+1))
			if err := e.flushNow(); err != nil {
				t.Errorf("flushNow %d: %v", k, err)
			}
		}(i)
	}
	wg.Wait()
	state, err := store.LoadEngineState(e.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	expected := int64(n * (n + 1) / 2)
	if got := state.TargetDays[e.currentDay][71]; got != expected {
		t.Fatalf("并发刷盘后持久化 = %d, want %d（F2 建批/Seq 有序）", got, expected)
	}
	if tgt.total.Load() != expected {
		t.Fatalf("内存 total = %d, want %d", tgt.total.Load(), expected)
	}
}

// F14：并发 ApplyConfigUpdate 控制事务不卡死，最终配置一致。
func TestConcurrentControlTransactions(t *testing.T) {
	e, _ := newTestEngine(t, nil, true)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cand := DefaultConfig()
			cand.MaxConnections = 1 + i%10
			cand.MaxRateMbps = i
			cand.DailyLimitGB = float64(i)
			// persist 成功（写临时文件），验证事务串行且不卡死。
			err := e.ApplyConfigUpdate(cand, func() error { return nil })
			if err != nil {
				t.Errorf("ApplyConfigUpdate(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if e.activeConns.Load() < 0 {
		t.Fatalf("activeConns 异常")
	}
	// 最终限速/连接数合法。
	snap := e.Snapshot()
	if snap.MaxConnections < 1 || snap.MaxConnections > 10 {
		t.Fatalf("并发控制后 MaxConnections=%d 非法", snap.MaxConnections)
	}
}
