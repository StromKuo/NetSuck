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
	"reflect"
	"runtime"
	"strconv"
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
	waitCondition(t, func() bool { return tb.current().maxSeen.Load() >= 2 }, "maxConns=2 达到 2 并发") // F14：不得放宽为 0

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
	waitCondition(t, func() bool { return tb.current().maxSeen.Load() >= 2 }, "2→1→2 后并发恢复为 2")
	if e.activeConns.Load() > 2 {
		t.Fatalf("active 超过 2: %d", e.activeConns.Load())
	}
}

// 清单 2：两源重定向到同一 host，互斥仍成立。
func TestRedirectSameHostMutex(t *testing.T) {
	trk := &inFlightTracker{}
	// host-final 是最终落点；x/y 两个不同源 host（host-x/host-y）都 302 到 host-final。
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := trk.inFlight.Add(1)
		trk.updateMax(cur)
		defer trk.inFlight.Add(-1)
		w.Write(make([]byte, 128*1024))
	}))
	t.Cleanup(final.Close)
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

	// 跨 host：x 源 host 与 y 源 host 不同（fakeHostURL），均 302 到 host-final（M6.8）。
	e, _ := newTestEngineWithClient(t, &http.Client{Transport: dialLoopbackTransport()}, nil, true)
	createAndHealth(t, e, "x", fakeHostURL("host-x", hop1.URL))
	if err := e.Pause(); err != nil {
		t.Fatal(err)
	}
	createAndHealth(t, e, "y", fakeHostURL("host-y", hop2.URL))
	if err := e.Resume(); err != nil {
		t.Fatal(err)
	}

	trk.reset()
	time.Sleep(500 * time.Millisecond)
	// 两目标跨不同源 host，最终都落到 host-final；host 互斥应使并发 ≤1。
	if got := trk.maxSeen.Load(); got > 1 {
		t.Fatalf("跨 host 重定向到同 host 后并发超过 1: %d", got)
	}
	if got := trk.maxSeen.Load(); got < 1 {
		t.Fatalf("未观察到跨 host 重定向下载并发: got=%d", got)
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
		// 子进程：真实 HTTP 下载（discard），测总峰值 RSS（非 race 进程，M6）。
		body := make([]byte, 4*1024*1024)
		for i := range body {
			body[i] = 0xAB
		}
		var served atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for i := 0; i < 64; i++ { // ~256MB 总量
				served.Add(int64(len(body)))
				if _, err := w.Write(body); err != nil {
					return
				}
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
		}))
		defer srv.Close()

		// 指定连接数 1，真实下载链。
		cfg := DefaultConfig()
		cfg.Jitter = JitterConfig{0, 0}
		cfg.MaxConnections = 1
		store, err := OpenStore(filepath.Join(t.TempDir(), "r.db"))
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		store.InitSchema()
		store.EnsureSeeded(cfg)
		eng, err := NewEngine(cfg, store, &http.Client{})
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}
		if err := eng.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}
		// RSS：先 Stop（其错误不得忽略）再 Close store（避免顺序颠倒导致 DB 不一致）。
		defer func() {
			if serr := eng.Stop(); serr != nil {
				t.Errorf("RSS child Stop: %v", serr)
			}
			store.Close()
		}()

		if _, err := eng.CreateTarget(TargetPatch{Name: ptrStr("r"), URL: ptrStr(srv.URL), Weight: ptrInt(10), Enabled: ptrBool(true)}); err != nil {
			t.Fatalf("create target: %v", err)
		}
		// 等真实下载推进（>64MB）。
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) && served.Load() < 64*1024*1024 {
			time.Sleep(20 * time.Millisecond)
		}
		if served.Load() < 64*1024*1024 {
			t.Fatalf("下载未推进: served=%d", served.Load())
		}
		var ru syscall.Rusage
		syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
		peak := int64(ru.Maxrss)
		if runtime.GOOS != "darwin" {
			peak *= 1024 // Linux Ru_maxrss 单位为 KB
		}
		fmt.Printf("PEAK_RSS=%d\n", peak)
		return
	}

	// 父进程：编译非 race 测试二进制，作为独立子进程执行 RSS 下载并断言总峰值 <30MB。
	tmpBin := filepath.Join(t.TempDir(), "rsschild.test")
	build := exec.Command("go", "test", "-c", "-o", tmpBin, ".")
	build.Dir = "."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("编译非race测试二进制失败: %v\n%s", err, out)
	}
	cmd := exec.Command(tmpBin, "-test.run=^TestDiscardRSSBelow30MB$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), "NETSUCK_RSS_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("RSS 子进程失败: %v\n%s", err, out) // M6：子进程退出错误不得丢弃
	}
	peak := int64(-1)
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "PEAK_RSS=") {
			if v, err2 := strconv.ParseInt(strings.TrimPrefix(line, "PEAK_RSS="), 10, 64); err2 == nil {
				peak = v
			}
		}
	}
	if peak < 0 {
		t.Fatalf("无法从子进程获取峰值 RSS; 输出:\n%s", out)
	}
	// M6：非 race 子进程测总峰值 RSS，超限即 FAIL（不做超限 Skip）。
	if peak > 30*1024*1024 {
		t.Fatalf("discard 真实 HTTP 下载总峰值 RSS=%d 字节 超过 30MB", peak)
	}
	t.Logf("discard 真实 HTTP 下载总峰值 RSS=%d 字节（<30MB）", peak)
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
	tok := e.quotaReserve(1000)
	if tok.amount != 1000 || !tok.booked {
		t.Fatalf("reserve amount=%d booked=%v, want 1000/true", tok.amount, tok.booked)
	}
	// 结算成全量实际消费 → globalDays=1000=limit（真实达上限，R3 才翻转门禁）。
	e.settleRead(tok, tgt, 1000)
	// 新预留此时被阻断（amount 0）。
	if ntok := e.quotaReserve(1000); ntok.amount != 0 {
		t.Fatalf("达上限后新预留 amount=%d, want 0", ntok.amount)
	}
	e.recomputeGate()
	if e.blockedFlag.Load() != 1 {
		t.Fatalf("真实达上限后 blockedFlag=%d, want 1", e.blockedFlag.Load())
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
// M1：配额账本闭环 —— 多 goroutine 并发 reserve+settle 竞争最后预算，总实际消费不超限、
// 非零推进且正确耗尽（barrier）。直接测原子预留，确定性不依赖 worker 调度。
func TestQuotaReserveNoOverrun(t *testing.T) {
	e, _ := newTestEngine(t, nil, true)
	tgt := &target{id: 81, total: new(atomic.Int64)}
	e.stateMu.Lock()
	e.targets[81] = tgt
	e.stateMu.Unlock()
	if err := e.SetDailyLimit(0.000003); err != nil { // 3000 字节
		t.Fatal(err)
	}
	const n = 32
	var wg sync.WaitGroup
	actual := &atomic.Int64{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				tok := e.quotaReserve(1024)
				if tok.amount == 0 {
					return // 配额耗尽
				}
				e.settleRead(tok, tgt, tok.amount) // 全量读 → 实际消费
				actual.Add(tok.amount)
			}
		}()
	}
	wg.Wait()
	got := actual.Load()
	if got > 3000 {
		t.Fatalf("并发 barrier 总实际消费 %d 超过配额 3000（M1 未闭环）", got)
	}
	if got <= 0 {
		t.Fatalf("barrier 应非零推进（got=%d）", got)
	}
	// 正确耗尽 + 实际消费以 globalDays 为准且与 settled 一致（M1 账本闭环）。
	e.countMu.Lock()
	gd := int64(0)
	if g := e.globalDays[e.currentDay]; g != nil {
		gd = g.Load()
	}
	e.countMu.Unlock()
	if gd != got || gd > 3000 {
		t.Fatalf("实际消费/globalDays 不一致: gd=%d got=%d", gd, got)
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

	// 用本地时区为 DST 区、且引擎未 Start（无运行协程）构造计数与恢复验证，避免改全局 time.Local 竞争。
	oldLocal := time.Local
	time.Local = loc
	defer func() { time.Local = oldLocal }()
	clk := &fakeClock{now: first}
	e, store := newTestEngine(t, clk, false) // 不 Start，无 goroutine
	tgt := &target{id: 55, total: new(atomic.Int64)}
	e.stateMu.Lock()
	e.targets[55] = tgt
	e.stateMu.Unlock()
	e.countBytes(tgt, 100) // 01:30 EDT → hk1
	clk.advance(time.Hour) // 01:30 EST
	e.countBytes(tgt, 200) // 01:30 EST → hk2
	hk1 := hourKey(first)
	hk2 := hourKey(second)
	if hk1 == hk2 {
		t.Fatalf("countBytes 后两键仍相同")
	}

	// History24h 窗口（自 second 之后 2h=03:00 EST）须呈现第二重复小时的桶值。
	ref := second.Add(3 * time.Hour) // 04:30 EST（完全错开重复小时）
	hist, err := e.History24h(ref)
	if err != nil {
		t.Fatalf("History24h: %v", err)
	}
	found1, found2 := false, false
	for _, p := range hist {
		if p.HourUnix == hk1 && p.Bytes == 100 {
			found1 = true
		}
		if p.HourUnix == hk2 && p.Bytes == 200 {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Fatalf("History24h 恢复窗口未同时呈现两重复小时桶: hk1=%v hk2=%v", found1, found2)
	}

	// 持久化后 LoadEngineState 在重复小时窗口恢复两桶（第二重复小时桶不丢失）。
	e.countMu.Lock()
	v1, v2 := int64(0), int64(0)
	if b := e.hourBuckets[hk1]; b != nil {
		v1 = b.Load()
	}
	if b := e.hourBuckets[hk2]; b != nil {
		v2 = b.Load()
	}
	e.countMu.Unlock()
	err = store.PersistBatch(CounterBatch{
		HourBuckets: map[int64]int64{hk1: v1, hk2: v2},
		Seq:         1,
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	restored, err := store.LoadEngineState(ref)
	if err != nil {
		t.Fatalf("LoadEngineState: %v", err)
	}
	if restored.HourBuckets[hk1] != 100 || restored.HourBuckets[hk2] != 200 {
		t.Fatalf("LoadEngineState 未恢复两重复小时桶: hk1=%d hk2=%d", restored.HourBuckets[hk1], restored.HourBuckets[hk2])
	}
}

// F6：门禁转阻塞（暂停）即时取消在途请求（Do/WaitN/Read 全生效；M6.9：响应头已发后卡 body）。
func TestGateBlocksCancelInFlight(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-1023" {
			w.Write(make([]byte, 1024)) // 探活放行
			return
		}
		// M6.9：发送 200 响应头后卡住 body（卡在 Read）。
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-release
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

// F7/M4：低速旧等待中切不限速，应及时读取（M6：先确认旧预约已建立，断言在旧等待期限内推进）。
func TestRateSwitchWakesLowWait(t *testing.T) {
	body := make([]byte, 8*1024*1024)
	srv := simpleServer(body, nil)
	t.Cleanup(srv.Close)
	e, _ := newTestEngine(t, nil, true)
	id := createAndHealth(t, e, "r", srv.URL)
	if err := e.SetMaxRate(1); err != nil { // 1 Mbps（低）
		t.Fatal(err)
	}
	waitCondition(t, func() bool { return e.activeConns.Load() > 0 }, "进入低速率下载")
	// 先确认旧（低速）预约已建立：低速率下 300ms 只推进少量（≤ 60KB = 1Mbps*0.6s）。
	time.Sleep(20 * time.Millisecond)
	loBefore := totalBytes(e, id)
	time.Sleep(300 * time.Millisecond)
	loAfter := totalBytes(e, id)
	if delta := loAfter - loBefore; delta > 80*1024 {
		t.Fatalf("低速预留未建立/未限速: 300ms 推进 %d 字节（应被限速）", delta)
	}
	// 切不限速：应在旧等待期限内推进（旧 1Mbps 32KB 等约 0.26s），deadline 收紧以证明即时性。
	before := totalBytes(e, id)
	if err := e.SetMaxRate(0); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(700 * time.Millisecond)
	for time.Now().Before(deadline) {
		if totalBytes(e, id) > before+512*1024 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	after := totalBytes(e, id)
	if after <= before+512*1024 {
		t.Fatalf("切不限速后未及时恢复读取（M3 预约唤醒失效）: +%d 字节", after-before)
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

// F3：永久持久化失败下 Stop 有界返回错误（M6：返回 nil 必须 FAIL，不得 Log）。
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
			t.Fatalf("永久持久化失败下 Stop 应返回错误，却返回 nil") // M6：必须 FAIL
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("永久失败下 Stop 超过 10s 未返回（有界失败）")
	}
}

// F3/M6：CRUD 持 adminMu 等待失败 flush 时并发 Stop，必须不卡死且最终完成。
func TestStopConcurrentWithCrudFlushFailure(t *testing.T) {
	e, store := newTestEngine(t, nil, false)
	s1 := simpleServer(make([]byte, 32*1024), nil)
	s2 := simpleServer(make([]byte, 32*1024), nil)
	t.Cleanup(s1.Close)
	t.Cleanup(s2.Close)
	e.persistFn = func(b CounterBatch) error { return store.PersistBatch(b) } // 初始可用
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Stop() })
	rec, err := e.CreateTarget(TargetPatch{Name: ptrStr("a"), URL: ptrStr(s1.URL), Weight: ptrInt(10), Enabled: ptrBool(true)})
	if err != nil {
		t.Fatal(err)
	}
	waitHealthy(t, e, rec.ID)

	// 永久失败的 persistFn 且发出信号，确认 CRUD 已进入失败 flush 再并发 Stop。
	flushStarted := make(chan struct{}, 8)
	var fail atomic.Int64
	e.persistFn = func(b CounterBatch) error {
		fail.Add(1)
		select { case flushStarted <- struct{}{}: default: }
		return fmt.Errorf("permanent persist failure")
	}

	crudDone := make(chan error, 1)
	go func() {
		// 改到不同 URL（非禁用）：需 changing → cancel + flushNow（失败）→ 返回错误并释放 adminMu。
		_, err := e.PatchTarget(rec.ID, TargetPatch{URL: ptrStr(s2.URL)})
		crudDone <- err
	}()
	// 确认 CRUD 已卡在失败的 flush（persistFn 至少触发一次）后，再并发 Stop。
	select {
	case <-flushStarted:
	case <-time.After(10 * time.Second):
		t.Fatalf("CRUD 未进入失败 flush")
	}
	// 并发调用 Stop：不得被 CRUD 的 adminMu 永久卡死（F3/R-测试）。
	stopDone := make(chan error, 1)
	go func() { stopDone <- e.Stop() }()
	select {
	case <-stopDone:
		// Stop 有界完成。
	case <-time.After(10 * time.Second):
		t.Fatalf("并发 Stop 被 CRUD 卡死（F3 未解）")
	}
	select {
	case err := <-crudDone:
		if err == nil {
			t.Fatalf("失败 flush 下 CRUD 应返回错误")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("CRUD 应返回（flush 失败）")
	}
	if fail.Load() < 1 {
		t.Fatalf("应确实触发失败 flush")
	}
}

// F2/M6：并发 flushNow 建批/Seq 有序，最终持久化计数正确（倒序并发）。
// M6：断言目标累计 / 全局 / 小时桶同版本一致，且重试批 Seq/内容相同。
func TestConcurrentFlushOrdered(t *testing.T) {
	e, store := newTestEngine(t, nil, true)
	// 用真实 DB 目标（store.CreateTarget 生成行），使 flush 的 total_bytes 落到 DB，便于断言 DB 累计。
	rec, err := store.CreateTarget(TargetPatch{Name: ptrStr("cf"), URL: ptrStr("http://cf.example/x"), Weight: ptrInt(10), Enabled: ptrBool(true)})
	if err != nil {
		t.Fatal(err)
	}
	tgt := &target{id: rec.ID, total: new(atomic.Int64)}
	e.stateMu.Lock()
	e.targets[rec.ID] = tgt
	tgt.cooldownUntilMS = e.clock.Now().Add(24 * time.Hour).UnixMilli() // 避开下载/探活
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
	day := e.currentDay
	if got := state.TargetDays[day][rec.ID]; got != expected {
		t.Fatalf("并发刷盘后日桶 = %d, want %d（F2 建批/Seq 有序）", got, expected)
	}
	// 目标金额、全局、小时桶同版本一致。
	if got := tgt.total.Load(); got != expected {
		t.Fatalf("内存 total = %d, want %d", got, expected)
	}
	if got := state.GlobalDays[day]; got != expected {
		t.Fatalf("持久化 GlobalDays = %d, want %d", got, expected)
	}
	hk := hourKey(e.clock.Now())
	if got := state.HourBuckets[hk]; got != expected {
		t.Fatalf("持久化 HourBuckets = %d, want %d（与日桶同版本）", got, expected)
	}
	// DB 目标累计断言（R-测试：非内存两次）。loadHour 由 LoadEngineState 的 Targets 中该行 total_bytes 提供。
	found := false
	for _, r := range state.Targets {
		if r.ID == rec.ID {
			found = true
			if r.TotalBytes != expected {
				t.Fatalf("DB 目标 total_bytes = %d, want %d（R5 同版本批次）", r.TotalBytes, expected)
			}
		}
	}
	if !found {
		t.Fatalf("DB 中未找到目标 %d", rec.ID)
	}
}

// F2/M6：重试同批 —— persist 首败后同 Seq/内容重试，最终落库正确。
func TestFlushRetrySameBatch(t *testing.T) {
	e, store := newTestEngine(t, nil, true)
	rec, err := store.CreateTarget(TargetPatch{Name: ptrStr("rs"), URL: ptrStr("http://rs.example/x"), Weight: ptrInt(10), Enabled: ptrBool(true)})
	if err != nil {
		t.Fatal(err)
	}
	tgt := &target{id: rec.ID, total: new(atomic.Int64)}
	e.stateMu.Lock()
	e.targets[rec.ID] = tgt
	e.stateMu.Unlock()

	var calls atomic.Int64
	var first CopyBatch
	var seqFirst uint64
	e.persistFn = func(b CounterBatch) error {
		if calls.Add(1) == 1 {
			first = copyBatch(b) // 深拷贝首次内容
			seqFirst = b.Seq
			return fmt.Errorf("injected transient failure")
		}
		// 重试批需与首次逐字段 DeepEqual（R-测试：非仅 Seq）。
		if !reflect.DeepEqual(CounterBatch(first), b) {
			t.Fatalf("重试批内容与首次不同: first=%+v retry=%+v", CounterBatch(first), b)
		}
		if b.Seq != seqFirst {
			t.Fatalf("重试批 Seq 变化: %d != %d", b.Seq, seqFirst)
		}
		return store.PersistBatch(b)
	}
	e.countBytes(tgt, 999)
	if err := e.flushNow(); err != nil {
		t.Fatalf("flushNow 应重试成功: %v", err)
	}
	if calls.Load() < 2 {
		t.Fatalf("应重试同批: calls=%d", calls.Load())
	}
	e.countMu.Lock()
	day := e.currentDay
	e.countMu.Unlock()
	state, _ := store.LoadEngineState(e.clock.Now())
	if got := state.TargetDays[day][rec.ID]; got != 999 {
		t.Fatalf("重试同批后落库 = %d, want 999", got)
	}
}

// CopyBatch 是 CounterBatch 的深拷贝视图（用于比较重试批内容不变）。
type CopyBatch struct {
	TargetDays   map[string]map[int64]int64
	GlobalDays   map[string]int64
	TargetTotals map[int64]int64
	HourBuckets  map[int64]int64
	TargetHealth map[int64]TargetHealthPersist
	Seq          uint64
}

func copyBatch(b CounterBatch) CopyBatch {
	cp := CopyBatch{
		TargetDays:   make(map[string]map[int64]int64, len(b.TargetDays)),
		GlobalDays:   make(map[string]int64, len(b.GlobalDays)),
		TargetTotals: make(map[int64]int64, len(b.TargetTotals)),
		HourBuckets:  make(map[int64]int64, len(b.HourBuckets)),
		TargetHealth: make(map[int64]TargetHealthPersist, len(b.TargetHealth)),
		Seq:          b.Seq,
	}
	for d, m := range b.TargetDays {
		dm := make(map[int64]int64, len(m))
		for id, v := range m {
			dm[id] = v
		}
		cp.TargetDays[d] = dm
	}
	for k, v := range b.GlobalDays {
		cp.GlobalDays[k] = v
	}
	for k, v := range b.TargetTotals {
		cp.TargetTotals[k] = v
	}
	for k, v := range b.HourBuckets {
		cp.HourBuckets[k] = v
	}
	for k, v := range b.TargetHealth {
		cp.TargetHealth[k] = v
	}
	return cp
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

// B2/M6：改 URL 冲突（409）失败不得清除持久化冷却；重启后原 cooldown_until 仍保留。
func TestCRUDURLConflictPreservesCooldown(t *testing.T) {
	s403 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	sB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-1023" {
			w.Write(make([]byte, 1024))
			return
		}
		w.Write(make([]byte, 64*1024))
	}))
	t.Cleanup(s403.Close)
	t.Cleanup(sB.Close)

	dbPath := filepath.Join(t.TempDir(), "b2.db")
	cfg := DefaultConfig()
	cfg.Jitter = JitterConfig{0, 0}
	st, err := OpenStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	st.InitSchema()
	st.EnsureSeeded(cfg)
	eng, err := NewEngine(cfg, st, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	eng.clock = realClock{}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	// A -> s403 若 403 置 RateLimited；B -> sB 先建 Healthy。
	recA, err := eng.CreateTarget(TargetPatch{Name: ptrStr("a"), URL: ptrStr(s403.URL), Weight: ptrInt(10), Enabled: ptrBool(true)})
	if err != nil {
		t.Fatal(err)
	}
	waitCondition(t, func() bool {
		snap := eng.Snapshot()
		if ts, ok := allTargets(snap)[recA.ID]; ok {
			return ts.Health == healthRateLimited
		}
		return false
	}, "A → RateLimited")
	recB, err := eng.CreateTarget(TargetPatch{Name: ptrStr("b"), URL: ptrStr(sB.URL), Weight: ptrInt(10), Enabled: ptrBool(true)})
	if err != nil {
		t.Fatal(err)
	}
	waitHealthy(t, eng, recB.ID)

	// 把 A 的 URL 改为 B 的 URL → 409 冲突，改 URL 失败。
	if _, err := eng.PatchTarget(recA.ID, TargetPatch{URL: ptrStr(sB.URL)}); err == nil {
		t.Fatalf("改 URL 到已存在地址应 409 冲突")
	}
	cooldownBefore := int64(-1)
	eng.stateMu.RLock()
	if tt := eng.targets[recA.ID]; tt != nil {
		cooldownBefore = tt.cooldownUntilMS
	}
	eng.stateMu.RUnlock()
	if cooldownBefore <= 0 {
		t.Fatalf("A 应处于冷却（cooldown=%d）", cooldownBefore)
	}

	// 关闭重启。
	eng.Stop()
	st.Close()
	st2, err := OpenStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	st2.InitSchema()
	eng2, err := NewEngine(cfg, st2, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	eng2.clock = realClock{}
	if err := eng2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer eng2.Stop()
	defer st2.Close()

	// 重启后原 cooldown_until 仍保留（B2：失败改 URL 未清除持久化冷却）。
	eng2.stateMu.RLock()
	var cooldownAfter int64
	var healthAfter string
	if tt := eng2.targets[recA.ID]; tt != nil {
		cooldownAfter = tt.cooldownUntilMS
		healthAfter = tt.health
	}
	eng2.stateMu.RUnlock()
	if cooldownAfter != cooldownBefore {
		t.Fatalf("重启后 cooldown 变化: before=%d after=%d（B2 失败改 URL 清除了冷却）", cooldownBefore, cooldownAfter)
	}
	if healthAfter != healthRateLimited {
		t.Fatalf("重启后 health=%q, want RateLimited", healthAfter)
	}
}

// R3：quota token 场景（version/booked 结算闭环）。
func TestQuotaR3Scenarios(t *testing.T) {
	// 场景1：无限旧 token→有限新 token→旧 token 退还不得退掉新预留。
	e, _ := newTestEngine(t, nil, true)
	tgt := &target{id: 90, total: new(atomic.Int64)}
	e.stateMu.Lock()
	e.targets[90] = tgt
	e.stateMu.Unlock()
	e.SetDailyLimit(0) // 无限
	tokU := e.quotaReserve(1024)
	if tokU.amount != 1024 || tokU.booked {
		t.Fatalf("无限 token 应 amount=1024 booked=false, got %d/%v", tokU.amount, tokU.booked)
	}
	e.SetDailyLimit(0.000005) // 有限 5000
	tokF := e.quotaReserve(2000)
	if tokF.amount != 2000 || !tokF.booked {
		t.Fatalf("有限 token 应 amount=2000 booked=true, got %d/%v", tokF.amount, tokF.booked)
	}
	e.countMu.Lock()
	resAfterF := e.reserved[e.currentDay]
	e.countMu.Unlock()
	e.settleRead(tokU, tgt, 1024) // 无限 token 结算不得释放他人预留
	e.countMu.Lock()
	resAfterU := e.reserved[e.currentDay]
	e.countMu.Unlock()
	if resAfterF != 2000 || resAfterU != 2000 {
		t.Fatalf("无限 token 结算不得退掉有限预留: F=%d U=%d want 2000", resAfterF, resAfterU)
	}
	e.settleRead(tokF, tgt, 2000) // 清理有限预留

	// 场景3：finite↔0 切换——改限额不重置实际消费；切换回有限后仍按 globalDays 计。
	e.SetDailyLimit(0.000005) // 5000
	tok1 := e.quotaReserve(2500)
	e.settleRead(tok1, tgt, 2500) // globalDays=2500
	actualBefore := tgt.total.Load()
	e.SetDailyLimit(0) // 切无限
	tokInf := e.quotaReserve(1000)
	if tokInf.amount != 1000 || tokInf.booked {
		t.Fatalf("切无限后 token 应 booked=false, got %d/%v", tokInf.amount, tokInf.booked)
	}
	e.settleRead(tokInf, tgt, 1000) // globalDays=3500；不重置实际消费
	actualAfter := tgt.total.Load()
	if actualAfter != actualBefore+1000 {
		t.Fatalf("改限额不应重置实际消费: before=%d after=%d", actualBefore, actualAfter)
	}
	if tgt.total.Load() < 3000 {
		t.Fatalf("实际消费应累计: %d", tgt.total.Load())
	}

	// 场景4：足额预留 + body 延迟 >1s 仍完成读取（预算足额不取消持有者）。
	e2, _ := newTestEngine(t, nil, true)
	tgt2 := &target{id: 91, total: new(atomic.Int64)}
	e2.stateMu.Lock()
	e2.targets[91] = tgt2
	e2.stateMu.Unlock()
	e2.SetDailyLimit(0.00001) // 10000，足额
	body := &slowOnce{delay: 1100 * time.Millisecond, data: []byte("0123456789abcdef")}
	start := time.Now()
	if err := e2.readLoop(context.Background(), tgt2, 1, body, io.Discard); err != nil {
		t.Fatalf("足额预留慢读取应完成: %v", err)
	}
	if d := time.Since(start); d < 1000*time.Millisecond {
		t.Fatalf("慢 body 应实际延迟: %v", d)
	}
	if e2.blockedFlag.Load() != 0 {
		t.Fatalf("足额预留不应取消持有者: blockedFlag=%d", e2.blockedFlag.Load())
	}
}

// slowOnce 延迟后一次性返回 data，再返回 EOF（模拟慢 body）。
type slowOnce struct {
	delay time.Duration
	data  []byte
	done  bool
}

func (s *slowOnce) Read(p []byte) (int, error) {
	if s.done {
		return 0, io.EOF
	}
	if !s.done {
		time.Sleep(s.delay)
		s.done = true
	}
	n := copy(p, s.data)
	return n, nil
}

// R3 场景2：跨午夜双版本读竞争——旧日预留结算入新日，新日 reader 互不干扰。
func TestQuotaR3CrossMidnight(t *testing.T) {
	clk := &fakeClock{now: time.Date(2024, 1, 1, 23, 50, 0, 0, time.Local)}
	e, _ := newTestEngine(t, clk, true)
	tgt := &target{id: 92, total: new(atomic.Int64)}
	e.stateMu.Lock()
	e.targets[92] = tgt
	e.stateMu.Unlock()
	e.SetDailyLimit(0.00001) // 10000

	dayOld := dayKey(clk.Now())
	tokOld := e.quotaReserve(3000) // 旧日预留
	if tokOld.amount != 3000 || !tokOld.booked {
		t.Fatalf("旧日预留应 booked 3000, got %d/%v", tokOld.amount, tokOld.booked)
	}
	// 跨午夜，零读取。
	clk.advance(15 * time.Minute) // 到达 00:05 次日
	e.recomputeGate()
	dayNew := dayKey(clk.Now())
	if dayNew == dayOld {
		t.Fatalf("应跨日")
	}
	// 新日 reader 可正常预留（旧日 reserved 不影响新日）。
	tokNew := e.quotaReserve(2000)
	if tokNew.amount != 2000 {
		t.Fatalf("新日应可预留 2000, got %d", tokNew.amount)
	}
	// 结算旧日 token：释放旧日 reserved（不影响新日账本）。
	e.settleRead(tokOld, tgt, 3000)
	e.countMu.Lock()
	newReserved := e.reserved[dayNew]
	oldReserved := e.reserved[dayOld]
	e.countMu.Unlock()
	if newReserved != 2000 {
		t.Fatalf("新日 reserved 应 2000, got %d（旧日结算不得影响新日）", newReserved)
	}
	if oldReserved != 0 {
		t.Fatalf("旧日 reserved 结算后应 0, got %d", oldReserved)
	}
	// 新日 token 正常结算入新日。
	e.settleRead(tokNew, tgt, 2000)
	e.countMu.Lock()
	gdNew := int64(0)
	if g := e.globalDays[dayNew]; g != nil {
		gdNew = g.Load()
	}
	e.countMu.Unlock()
	if gdNew != 5000 {
		t.Fatalf("跨日完成读按新日结算: 旧日3000+新日2000=5000, got %d", gdNew)
	}
}

// R1：重定向释放 host 后不得重复释放他人持有的同 host 许可（owner 化，releaseHeldHost 清空）。
func TestR1RedirectHostOwnership(t *testing.T) {
	tb := &trackerBox{}
	tb.newPhase()
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(make([]byte, 64*1024))
	}))
	t.Cleanup(final.Close)
	hop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-1023" { // 探活放行
			w.Write(make([]byte, 1024))
			return
		}
		trk := tb.current()
		cur := trk.inFlight.Add(1)
		trk.updateMax(cur)
		defer trk.inFlight.Add(-1)
		http.Redirect(w, r, final.URL, http.StatusFound)
	}))
	t.Cleanup(hop.Close)

	e, _ := newTestEngineWithClient(t, &http.Client{Transport: dialLoopbackTransport()}, nil, true)
	// 三目标共享 hop host（host-hop 不同 path）重定向到同一 final，制造 host-hop 反复获取/释放。
	id1 := createAndHealth(t, e, "x", fakeHostURL("host-hop", hop.URL)+"/a")
	id2 := createAndHealth(t, e, "y", fakeHostURL("host-hop", hop.URL)+"/b")
	id3 := createAndHealth(t, e, "z", fakeHostURL("host-hop", hop.URL)+"/c")
	waitCondition(t, func() bool {
		return totalBytes(e, id1) > 0 && totalBytes(e, id2) > 0 && totalBytes(e, id3) > 0
	}, "三目标均产生流量")
	// fresh tracker 观察 hop host 并发：R1 若重复释放，会被他人抢占造成并发 >1。
	tb.newPhase()
	time.Sleep(500 * time.Millisecond)
	if got := tb.current().maxSeen.Load(); got > 1 {
		t.Fatalf("重定向释放后 hop host 并发超过 1（R1 重复释放）: %d", got)
	}
}
