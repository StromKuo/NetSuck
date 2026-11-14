package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// 引擎常量（规则 4/8/10/12/14）。
const (
	bufSize         = 32 * 1024        // 有界读取缓冲（规则 8/16）
	burstBytes      = 64 * 1024        // 令牌桶 burst（规则 8）
	probeTimeout    = 10 * time.Second // 单次探活超时（规则 12）
	probeRange      = 1024             // 探活最多读取字节数（规则 12）
	probeInterval   = 15 * time.Minute // 定时探活间隔（规则 12）
	flushInterval   = 10 * time.Second // 刷盘周期（规则 10）
	sampleInterval  = 500 * time.Millisecond
	gateInterval    = time.Second
	maxRedirects    = 10              // 重定向跳数上限（规则 3）
	flushRetryDelay = 500 * time.Millisecond

	healthHealthy     = "Healthy"
	healthRateLimited = "RateLimited"
	healthDead        = "Dead"
	healthUnknown     = "Unknown"

	statusRunning          = "running"
	statusPaused           = "paused"
	statusSleepingSchedule = "sleeping_schedule"
	statusQuotaReached     = "quota_reached"
)

// clock 抽象引擎所用时间，供测试注入。真实实现用标准库时间。
type clock interface {
	Now() time.Time
	Sleep(d time.Duration)
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                     { return time.Now() }
func (realClock) Sleep(d time.Duration)              { time.Sleep(d) }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// notifySig 是一个可广播的唤醒信号（关闭+重建 channel）。
type notifySig struct {
	mu sync.Mutex
	ch chan struct{}
}

func (n *notifySig) init() { n.mu.Lock(); n.ch = make(chan struct{}); n.mu.Unlock() }
func (n *notifySig) signal() {
	n.mu.Lock()
	c := n.ch
	n.ch = make(chan struct{})
	n.mu.Unlock()
	close(c)
}
func (n *notifySig) current() chan struct{} {
	n.mu.Lock()
	c := n.ch
	n.mu.Unlock()
	return c
}
func (n *notifySig) wait(ctx context.Context) bool {
	c := n.current()
	select {
	case <-ctx.Done():
		return false
	case <-c:
		return true
	}
}

// target 是引擎内的下载目标。可变元数据字段由 stateMu 保护；稳定计数用 atomic。
type target struct {
	id    int64
	gen   uint64         // URL 变更时递增（规则 15）
	host  string         // 规范化小写 hostname（规则 2）
	total *atomic.Int64  // 全量累计（永不清零，供采样，规则 14）

	// 以下字段受 stateMu 保护。
	name             string
	url              string
	weight           int
	enabled          bool
	deleted          bool
	health           string
	cooldownUntilMS  int64
	lastStatusCode   int
	latencyMS        int64
	errorMsg         string
	consecutiveErrors int
}

type worker struct {
	baseCancel context.CancelFunc
	done       chan struct{}
	mu         sync.Mutex
	curID      int64
	curCancel  context.CancelFunc
	cycleDone  chan struct{}
}

type batchRequest struct {
	batch *CounterBatch
	done  chan error
}

// Engine 是下载引擎核心。锁序：adminMu > stateMu > countMu > dirtyMu（dirtyMu 为叶）。
type Engine struct {
	cfg        *Config
	store      *Store
	persistFn  func(CounterBatch) error // 持久化函数（默认 store.PersistBatch，测试可注入）
	httpClient *http.Client
	limiter    *rate.Limiter
	clock      clock

	adminMu sync.Mutex   // Stop / CRUD / 控制事务门禁（规则 15/18/19）
	stateMu sync.RWMutex // 目标元数据与运行参数
	countMu sync.Mutex   // 计数协调锁（规则 9/11）
	dirtyMu sync.Mutex   // 脏桶集合（叶锁）
	poolMu  sync.Mutex   // 连接池（规则 5）
	hostMu  sync.Mutex   // host 互斥表（规则 2）

	lifeCtx    context.Context
	lifeCancel context.CancelFunc

	stopped     atomic.Bool
	blockedFlag atomic.Int32 // 1=被暂停/配额/时段阻塞，0=可运行（规则 6）

	targets   map[int64]*target // stateMu
	hostLocks map[string]bool   // hostMu

	workers    []*worker // poolMu
	targetSize int       // poolMu

	// 计数（countMu 保护 map，值用 atomic）
	currentDay  string                       // countMu
	targetDays  map[string]map[int64]*atomic.Int64 // 日->target_id->累计绝对值
	globalDays  map[string]*atomic.Int64          // 日->全局累计绝对值
	hourBuckets map[int64]*atomic.Int64           // 小时unix->累计绝对值
	quotaUsed   atomic.Int64                      // 当前日全局字节镜像（规则 7）
	dailyLimit  atomic.Int64                      // 每日字节上限（0=不限）
	globalTotal atomic.Int64                      // 全局全量累计（永不清零，供采样）
	seq         uint64                            // countMu

	// 脏桶（dirtyMu）
	dirtyDays    map[string]map[int64]struct{}
	dirtyGlobal  map[string]struct{}
	dirtyHours   map[int64]struct{}
	dirtyTargets map[int64]struct{}

	// 运行参数（stateMu）
	paused        bool
	storageMode   string
	maxRateMbps   int
	maxConns      int
	schedule      ScheduleConfig
	jitter        JitterConfig
	cooldownHours int
	userAgent     string

	persistCh chan batchRequest

	probeCh      chan int64
	probeMu      sync.Mutex
	probePending map[int64]struct{}
	probeInFlight map[int64]struct{}

	notify notifySig

	activeConns atomic.Int64 // 活跃下载请求数（规则 5）

	// 速率采样（规则 14）
	sampMu sync.Mutex
	rates  map[int64]float64 // id0=全局，其余为 target id -> bytes/s
	samplerState map[int64]*sampleState

	wg       sync.WaitGroup
	probeWG  sync.WaitGroup
	persistWG sync.WaitGroup

	runErrMu sync.Mutex
	runErr   error
}

type sampleState struct {
	last    int64
	lastTS  time.Time
	entries []sampleEntry // 环形缓冲，≤3s 窗口
	rate    float64
}
type sampleEntry struct {
	dt    float64 // 秒
	delta float64 // 字节
}

// mergeCtx 返回一个在 a 或 b 任一取消时即取消的上下文。
func mergeCtx(a, b context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-a.Done():
			cancel()
		case <-b.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// NewEngine 依据配置构建引擎。Transport/CheckRedirect 在此配置（规则 3/4）。
func NewEngine(cfg *Config, store *Store, httpClient *http.Client) (*Engine, error) {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse // 手动逐跳处理重定向（规则 3）
	}
	if httpClient.Transport == nil {
		httpClient.Transport = &http.Transport{
			MaxIdleConns:        10,
			MaxConnsPerHost:     1,
			IdleConnTimeout:     90 * time.Second,
			ForceAttemptHTTP2:   false,
		}
	}

	ratePerSec := rate.Limit(float64(cfg.MaxRateMbps) * 1e6 / 8) // Mbps→bytes/s
	if cfg.MaxRateMbps == 0 {
		ratePerSec = rate.Inf
	}
	lim := rate.NewLimiter(ratePerSec, burstBytes)

	persistFn := func(b CounterBatch) error { return store.PersistBatch(b) }

	e := &Engine{
		cfg:        cfg,
		store:      store,
		persistFn:  persistFn,
		httpClient: httpClient,
		limiter:    lim,
		clock:      realClock{},

		targets:   make(map[int64]*target),
		hostLocks: make(map[string]bool),

		targetDays:  make(map[string]map[int64]*atomic.Int64),
		globalDays:  make(map[string]*atomic.Int64),
		hourBuckets: make(map[int64]*atomic.Int64),

		dirtyDays:    make(map[string]map[int64]struct{}),
		dirtyGlobal:  make(map[string]struct{}),
		dirtyHours:   make(map[int64]struct{}),
		dirtyTargets: make(map[int64]struct{}),

		storageMode:   cfg.StorageMode,
		maxRateMbps:   cfg.MaxRateMbps,
		maxConns:      cfg.MaxConnections,
		schedule:      cfg.Schedule,
		jitter:        cfg.Jitter,
		cooldownHours: cfg.CooldownHours,
		userAgent:     cfg.UserAgent,

		persistCh:   make(chan batchRequest, 1),
		probeCh:     make(chan int64, 256),
		probePending:   make(map[int64]struct{}),
		probeInFlight:  make(map[int64]struct{}),
		rates:       make(map[int64]float64),
		samplerState: make(map[int64]*sampleState),
	}
	e.dailyLimit.Store(int64(cfg.DailyLimitGB * 1e9))
	e.notify.init()
	return e, nil
}

func dayKey(now time.Time) string { return now.In(time.Local).Format("2006-01-02") }

func hourKey(now time.Time) int64 {
	t := now.In(time.Local)
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.Local).Unix()
}

func inSchedule(now time.Time, s ScheduleConfig) bool {
	h := now.In(time.Local).Hour()
	if s.StartHour < s.EndHour {
		return h >= s.StartHour && h < s.EndHour
	}
	return h >= s.StartHour || h < s.EndHour
}

// Start 启动引擎：恢复持久化状态、构建计数对象、启动各协程（规则 10/12/14/15）。
func (e *Engine) Start(ctx context.Context) error {
	state, err := e.store.LoadEngineState(e.clock.Now())
	if err != nil {
		return err
	}
	e.lifeCtx, e.lifeCancel = context.WithCancel(context.Background())
	e.notify.init()

	// 恢复计数对象与元数据（规则 15：稳定 id 计数对象，重载只改元数据）。
	now := e.clock.Now()
	e.currentDay = dayKey(now)
	e.targetDays[dayKey(now)] = make(map[int64]*atomic.Int64)
	e.globalDays[dayKey(now)] = new(atomic.Int64)
	if state.LastSeq > 0 {
		e.seq = state.LastSeq
	}
	// 合并持久化日桶/全局日桶/小时桶到内存权威视图。
	for day, per := range state.TargetDays {
		m := e.targetDays[day]
		if m == nil {
			m = make(map[int64]*atomic.Int64)
			e.targetDays[day] = m
		}
		for id, b := range per {
			a := new(atomic.Int64)
			a.Store(b)
			m[id] = a
		}
	}
	for day, b := range state.GlobalDays {
		a := new(atomic.Int64)
		a.Store(b)
		e.globalDays[day] = a
	}
	for h, b := range state.HourBuckets {
		a := new(atomic.Int64)
		a.Store(b)
		e.hourBuckets[h] = a
	}
	if g := e.globalDays[e.currentDay]; g != nil {
		e.quotaUsed.Store(g.Load())
	}

	// 构建目标计数对象。
	for _, rec := range state.Targets {
		host := normalizeHost(rec.URL)
		t := &target{
			id:               rec.ID,
			gen:              1,
			host:            host,
			total:           new(atomic.Int64),
			name:            rec.Name,
			url:             rec.URL,
			weight:          rec.Weight,
			enabled:         rec.Enabled,
			health:          rec.Health,
			cooldownUntilMS: rec.CooldownUntilMS,
			lastStatusCode:  rec.LastStatusCode,
			latencyMS:       rec.LatencyMS,
			errorMsg:        rec.ErrorMsg,
			consecutiveErrors: rec.ConsecutiveErrors,
		}
		t.total.Store(rec.TotalBytes)
		if t.health == "" {
			t.health = healthUnknown
		}
		e.targets[t.id] = t
		e.globalTotal.Add(rec.TotalBytes)
	}

	// 启动持久化协程（规则 10：唯一持久化协程，存活至 Stop 关闭 persistCh）。
	e.persistWG.Add(1)
	go e.persistWorker()

	// 启动 10s 周期刷盘。
	e.wg.Add(1)
	go e.flushTicker()

	// 启动门禁状态刷新协程（捕捉时段/配额边界）。
	e.wg.Add(1)
	go e.gateTicker()

	// 启动速率采样协程（规则 14）。
	e.wg.Add(1)
	go e.samplerLoop()

	// 启动定时探活协程（规则 12）。
	e.wg.Add(1)
	go e.probeTicker()

	// 启动探活 worker（并发受控）。
	pc := e.maxConns
	if pc < 1 {
		pc = 1
	}
	for i := 0; i < pc; i++ {
		e.probeWG.Add(1)
		go e.probeWorker()
	}

	// 启动下载 worker（规则 5：初始 maxConns）。
	e.recomputeGate()
	e.resizePool(e.maxConns)

	// 启动立即探活 Unknown（规则 12）。
	e.enqueueEligibleProbe(e.clock.Now(), true)

	return nil
}

func normalizeHost(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return strings.ToLower(rawURL)
	}
	return strings.ToLower(u.Hostname())
}

// Stop 关闭引擎（规则 18：拒新→取消并等待全部→最终 flush→返回错误）。
func (e *Engine) Stop() error {
	e.adminMu.Lock()
	defer e.adminMu.Unlock()

	if e.stopped.Load() {
		return e.runErr
	}
	e.stopped.Store(true)
	e.blockedFlag.Store(1)
	e.notify.signal()

	// 取消全部 worker 与协程。
	if e.lifeCancel != nil {
		e.lifeCancel()
	}
	e.cancelAllWorkers()

	// 等待下载 worker / 探活 / 各协程退出（persistWorker 单独存活以承接最终批）。
	e.wg.Wait()
	e.probeWG.Wait()

	// 最终同步 flush（规则 18），经唯一持久化协程提交并等待。
	if e.persistCh != nil {
		if err := e.flushNow(); err != nil {
			e.setRunErr(err)
		}
		close(e.persistCh) // persistWorker 耗尽队列后退出
		e.persistWG.Wait()
	}

	return e.runErr
}

func (e *Engine) setRunErr(err error) {
	e.runErrMu.Lock()
	if e.runErr == nil {
		e.runErr = err
	}
	e.runErrMu.Unlock()
}

func (e *Engine) isStopped() bool { return e.stopped.Load() }

func (e *Engine) cancelAllWorkers() {
	e.poolMu.Lock()
	for _, w := range e.workers {
		w.baseCancel()
	}
	e.poolMu.Unlock()
}

// resizePool 动态调整下载 worker 数量（规则 5：2→1→2 不重复扩容）。
// 调用方应持有 adminMu（或用内部 guard）。内部自持 poolMu，等待退休 worker 回收。
func (e *Engine) resizePool(n int) {
	e.poolMu.Lock()
	e.targetSize = n
	if len(e.workers) > n {
		retiring := e.workers[n:]
		doneList := make([]chan struct{}, len(retiring))
		for i, w := range retiring {
			w.baseCancel() // 取消在途下载（规则 5）
			doneList[i] = w.done
		}
		e.poolMu.Unlock()
		for _, d := range doneList {
			<-d // 等待退休 worker 回收（规则 5）
		}
		e.poolMu.Lock()
	}
	need := n - len(e.workers)
	for i := 0; i < need; i++ {
		e.spawnWorkerLocked()
	}
	e.poolMu.Unlock()
}

func (e *Engine) spawnWorkerLocked() {
	wctx, wcancel := context.WithCancel(e.lifeCtx)
	w := &worker{baseCancel: wcancel, done: make(chan struct{})}
	e.workers = append(e.workers, w)
	e.wg.Add(1)
	go e.workerLoop(wctx, w, w.done)
}

// workerLoop 是单个下载工作协程（规则 5/6/15）。退出时自移除并回吐 done。
func (e *Engine) workerLoop(wctx context.Context, w *worker, done chan struct{}) {
	defer func() {
		e.poolMu.Lock()
		for i, ww := range e.workers {
			if ww == w {
				e.workers = append(e.workers[:i], e.workers[i+1:]...)
				break
			}
		}
		e.poolMu.Unlock()
		e.wg.Done()
		close(done)
	}()

	for {
		select {
		case <-wctx.Done():
			return
		default:
		}
		t, ok := e.selectTarget(wctx)
		if !ok {
			return
		}
		// 记录当前目标以便按目标取消（规则 15）。host 许可由 doWithRedirects 逐跳管理。
		cyc, cycCancel := context.WithCancel(wctx)
		w.mu.Lock()
		w.curID = t.id
		w.curCancel = cycCancel
		w.cycleDone = make(chan struct{})
		d := w.cycleDone
		w.mu.Unlock()

		e.download(cyc, t)

		w.mu.Lock()
		w.curID = 0
		w.curCancel = nil
		w.cycleDone = nil
		w.mu.Unlock()
		close(d)

		e.jitterSleep(wctx)
	}
}

// tryAcquireHost 对规范化 host 做 try-acquire（规则 2：同域不同 URL 共锁）。
func (e *Engine) tryAcquireHost(host string) bool {
	e.hostMu.Lock()
	defer e.hostMu.Unlock()
	if e.hostLocks[host] {
		return false
	}
	e.hostLocks[host] = true
	return true
}

func (e *Engine) releaseHost(host string) {
	e.hostMu.Lock()
	delete(e.hostLocks, host)
	e.hostMu.Unlock()
}

// selectTarget 选择下载目标；无可选时阻塞等待状态变化（规则 1/6，不忙循环）。
func (e *Engine) selectTarget(ctx context.Context) (*target, bool) {
	for {
		if ctx.Err() != nil {
			return nil, false
		}
		if e.stopped.Load() {
			return nil, false
		}
		if e.blockedFlag.Load() != 0 {
			if !e.awaitRunnable(ctx) {
				return nil, false
			}
			continue
		}
		if t := e.weightedPick(); t != nil {
			return t, true
		}
		if !e.notify.wait(ctx) {
			return nil, false
		}
	}
}

// awaitRunnable 等待引擎进入可运行状态（规则 6：一切等待可取消、不忙循环）。
func (e *Engine) awaitRunnable(ctx context.Context) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		if e.stopped.Load() {
			return false
		}
		if e.blockedFlag.Load() == 0 {
			return true
		}
		if !e.notify.wait(ctx) {
			return false
		}
	}
}

// weightedPick 在所有 enabled+Healthy 且不在冷却的目标中按权重随机选一（规则 1）。
func (e *Engine) weightedPick() *target {
	// 在锁内捕获候选与其权重（规则 1），返回后用捕获值，避免锁外读取可变字段造成数据竞争。
	e.stateMu.RLock()
	now := e.clock.Now().UnixMilli()
	elig := make([]struct {
		t *target
		w int
	}, 0, 8)
	total := 0
	for _, t := range e.targets {
		if t.deleted || !t.enabled || t.health != healthHealthy {
			continue
		}
		if t.cooldownUntilMS > now {
			continue
		}
		elig = append(elig, struct {
			t *target
			w int
		}{t, t.weight})
		total += t.weight
	}
	e.stateMu.RUnlock()
	if total <= 0 {
		return nil
	}
	r := rand.Intn(total)
	for _, c := range elig {
		if r < c.w {
			return c.t
		}
		r -= c.w
	}
	return elig[len(elig)-1].t
}

func (e *Engine) jitterSleep(ctx context.Context) {
	e.stateMu.RLock()
	min := e.jitter.MinSeconds
	max := e.jitter.MaxSeconds
	e.stateMu.RUnlock()
	if max <= 0 {
		return
	}
	if max < min {
		min = max
	}
	d := time.Duration(min+rand.Intn(max-min+1)) * time.Second
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// recomputeGate 依据 paused/配额/时段/stopped 刷新 blockedFlag 并广播（规则 6/19）。
func (e *Engine) recomputeGate() {
	now := e.clock.Now()
	e.stateMu.RLock()
	paused := e.paused
	stopped := e.stopped.Load()
	inS := inSchedule(now, e.schedule)
	e.stateMu.RUnlock()

	e.countMu.Lock()
	used := e.quotaUsed.Load()
	limit := e.dailyLimit.Load()
	e.countMu.Unlock()

	quotaReached := limit > 0 && used >= limit
	blocked := stopped || paused || !inS || quotaReached
	if blocked {
		e.blockedFlag.Store(1)
	} else {
		e.blockedFlag.Store(0)
	}
	e.notify.signal()
}

// gateTicker 周期性刷新门禁状态以捕捉时段/配额边界。
func (e *Engine) gateTicker() {
	defer e.wg.Done()
	t := time.NewTicker(gateInterval)
	defer t.Stop()
	for {
		select {
		case <-e.lifeCtx.Done():
			return
		case <-t.C:
			e.recomputeGate()
		}
	}
}

// download 执行一次完整下载（含重定向逐跳与存储策略，规则 3/16）。
func (e *Engine) download(ctx context.Context, t *target) {
	e.activeConns.Add(1)
	defer e.activeConns.Add(-1)

	gen := e.targetGen(t.id)
	mode := e.storageModeOf()

	resp, heldHost, latency, err := e.doWithRedirects(ctx, t, gen)
	if heldHost != "" {
		defer e.releaseHost(heldHost) // 最终 host 许可在 body 关闭后释放（规则 3）
	}
	if err != nil {
		if e.isCtxCancel(err, ctx) {
			return // 因 Stop/CRUD/门禁取消，不计远端故障（规则 13）
		}
		e.recordDownloadFailure(t, gen, 0, err, latency)
		return
	}

	status := resp.StatusCode
	defer resp.Body.Close()

	if status == http.StatusForbidden || status == http.StatusTooManyRequests {
		e.rateLimitTarget(t, gen, status, latency, "rate limited")
		return
	}

	// 打开存储目标（规则 16）。
	d, derr := e.openDestination(t, mode)
	if derr != nil {
		e.recordDownloadFailure(t, gen, status, derr, latency)
		return
	}
	finalized := false
	defer func() {
		if !finalized {
			d.abort()
		}
	}()

	err = e.readLoop(ctx, t, gen, resp.Body, d.w)
	switch {
	case err == nil:
		// 下载成功读完整 body：状态码决定成功与否（规则 13）。
		if status == http.StatusOK || status == http.StatusPartialContent {
			d.finalize()
			finalized = true
			e.recordDownloadSuccess(t, gen, status, latency)
		} else {
			e.recordDownloadFailure(t, gen, status, nil, latency)
		}
		return
	case errors.Is(err, errQuotaReached):
		// 配额触达：正常停止，非远端故障。
		return
	case e.isCtxCancel(err, ctx):
		return
	}
	e.recordDownloadFailure(t, gen, status, err, latency)
}

// doWithRedirects 逐跳处理重定向并逐跳持有 host 许可，返回最终响应与响应头耗时（规则 3）。
func (e *Engine) doWithRedirects(ctx context.Context, t *target, gen uint64) (*http.Response, string, int, error) {
	e.stateMu.RLock()
	rawURL := t.url
	e.stateMu.RUnlock()
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, "", 0, err
	}
	curURL := u.String()
	host := normalizeHost(curURL)
	latency := 0
	heldHost := ""

	for hop := 0; hop <= maxRedirects; hop++ {
		// 发请求前重查准入（规则 1）。
		if ctx.Err() != nil {
			e.releaseHost(heldHost)
			return nil, "", latency, ctx.Err()
		}
		if e.gateBlocked() || !e.targetEligibleForDownload(t.id, gen) {
			e.releaseHost(heldHost)
			return nil, "", latency, context.Canceled
		}
		if !e.tryAcquireHost(host) {
			e.releaseHost(heldHost)
			return nil, "", latency, context.Canceled // host 忙，本 worker 放弃本轮
		}
		heldHost = host
		released := false
		release := func() {
			if !released && heldHost != "" {
				e.releaseHost(heldHost)
				released = true
				heldHost = ""
			}
		}

		req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, curURL, nil)
		if rerr != nil {
			release()
			return nil, "", latency, rerr
		}
		req.Header.Set("User-Agent", e.userAgentOf())
		req.Header.Set("Range", "bytes=0-") // 流式丢弃，请求完整资源

		start := e.clock.Now()
		resp, derr := e.httpClient.Do(req)
		latency = int(e.clock.Now().Sub(start).Milliseconds())
		if derr != nil {
			release()
			return nil, "", latency, derr
		}

		if resp.StatusCode >= 300 && resp.StatusCode < 400 && resp.StatusCode != 304 {
			loc := resp.Header.Get("Location")
			_ = resp.Body.Close()
			release() // 释放旧 host 许可（规则 3）
			if loc == "" {
				return nil, "", latency, fmt.Errorf("redirect without location (status %d)", resp.StatusCode)
			}
			next, nerr := url.Parse(loc)
			if nerr != nil {
				return nil, "", latency, nerr
			}
			nextURL := resp.Request.URL.ResolveReference(next).String()
			curURL = nextURL
			host = normalizeHost(nextURL) // 下一跳取新 host 许可（规则 3）
			continue
		}
		// 非重定向：返回响应；host 许可保持持有，由 download 在 body 关闭后释放（规则 3）。
		return resp, heldHost, latency, nil
	}
	e.releaseHost(heldHost)
	return nil, "", latency, fmt.Errorf("too many redirects (> %d)", maxRedirects)
}

// targetEligibleForDownload 发请求前重查：目标仍存在、未软删、enabled、Healthy、未冷却、generation 一致（规则 1）。
func (e *Engine) targetEligibleForDownload(id int64, gen uint64) bool {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	t := e.targets[id]
	if t == nil || t.deleted || !t.enabled {
		return false
	}
	if t.gen != gen {
		return false
	}
	if t.health != healthHealthy {
		return false
	}
	if t.cooldownUntilMS > e.clock.Now().UnixMilli() {
		return false
	}
	return true
}

func (e *Engine) targetGen(id int64) uint64 {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	t := e.targets[id]
	if t == nil {
		return 0
	}
	return t.gen
}

func (e *Engine) gateBlocked() bool { return e.blockedFlag.Load() != 0 }

func (e *Engine) isCtxCancel(err error, ctx context.Context) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	return ctx.Err() != nil
}

func (e *Engine) storageModeOf() string {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	return e.storageMode
}

func (e *Engine) userAgentOf() string {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	return e.userAgent
}

func (e *Engine) cooldownHoursOf() time.Duration {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	return time.Duration(e.cooldownHours) * time.Hour
}

// dest 表示存储目标（规则 16）。
type dest struct {
	mode      string
	w         io.Writer
	file      *os.File
	tempPath  string
	finalPath string
}

// openDestination 依存储模式打开目标。
func (e *Engine) openDestination(t *target, mode string) (*dest, error) {
	switch mode {
	case storageModeDiscard:
		return &dest{mode: mode, w: io.Discard}, nil
	case storageModeMemory:
		e.stateMu.RLock()
		dir := e.cfg.MemoryDir
		e.stateMu.RUnlock()
		f, err := os.CreateTemp(dir, "netsuck-*.tmp")
		if err != nil {
			return nil, err
		}
		return &dest{mode: mode, w: f, file: f, tempPath: f.Name()}, nil
	case storageModeDisk:
		e.stateMu.RLock()
		dir := e.cfg.DiskDir
		raw := t.url
		e.stateMu.RUnlock()
		base := safeBasename(raw)
		f, err := os.CreateTemp(dir, base+".part-*")
		if err != nil {
			return nil, err
		}
		return &dest{mode: mode, w: f, file: f, tempPath: f.Name(), finalPath: filepath.Join(dir, base)}, nil
	default:
		return nil, fmt.Errorf("unknown storage mode %q", mode)
	}
}

// finalize 成功时落盘：disk 改名，memory/discard 清除。
func (d *dest) finalize() {
	if d.mode == storageModeDisk && d.file != nil {
		if err := d.file.Close(); err == nil {
			_ = os.Rename(d.tempPath, d.finalPath)
		}
		return
	}
	d.abort()
}

// abort 失败/取消路径：memory 清除，disk 保留 .part（D5），discard 无操作。
func (d *dest) abort() {
	if d.file != nil {
		_ = d.file.Close()
		if d.mode == storageModeMemory {
			_ = os.Remove(d.tempPath)
		}
	}
}

// safeBasename 从 URL 路径提取并清洗文件名（规则 16）。
func safeBasename(rawURL string) string {
	base := "download"
	if u, err := url.Parse(rawURL); err == nil && u.Path != "" {
		if b := path.Base(u.Path); b != "" && b != "." && b != "/" {
			base = b
		}
	}
	base = strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' || r == 0 {
			return -1
		}
		return r
	}, base)
	if base == "" || base == "." {
		return "download"
	}
	return base
}

// readLoop 统一 StatsReader 循环：令牌桶预约-读-归还 + 配额预留-结算 + 字节归属（规则 7/8/9）。
func (e *Engine) readLoop(ctx context.Context, t *target, gen uint64, body io.Reader, w io.Writer) error {
	buf := make([]byte, bufSize)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if e.gateBlocked() {
			return context.Canceled
		}
		remaining := e.quotaRemaining()
		if remaining == 0 {
			return errQuotaReached
		}
		n := int64(len(buf))
		if remaining > 0 && remaining < n {
			n = remaining // 配额预留：min(读长, 剩余额度)（规则 7）
		}
		// 令牌桶预约（规则 8：等待在有界 Read 前，≤32KiB≤burst）。
		if err := e.limiter.WaitN(ctx, int(n)); err != nil {
			return err
		}
		rn, rerr := body.Read(buf[:n])
		if rn > 0 {
			e.countBytes(t, int64(rn)) // 字节归属 + 配额结算（规则 9）
			if _, werr := w.Write(buf[:rn]); werr != nil {
				return werr
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				return nil
			}
			return rerr
		}
	}
}

var errQuotaReached = errors.New("daily quota reached")

// quotaRemaining 返回今日剩余可读字节；0 表示配额触达（0=不限时返回极大值）。
func (e *Engine) quotaRemaining() int64 {
	limit := e.dailyLimit.Load()
	if limit <= 0 {
		return int64(^uint64(0) >> 1) // 不限
	}
	used := e.quotaUsed.Load()
	if used >= limit {
		return 0
	}
	return limit - used
}

// countBytes 在计数协调锁内把读取字节归属到日/小时桶并更新总额与配额（规则 9/11）。
func (e *Engine) countBytes(t *target, n int64) {
	if n <= 0 {
		return
	}
	e.countMu.Lock()
	now := e.clock.Now().In(time.Local)
	day := now.Format("2006-01-02")
	e.ensureDay(day, now)

	m := e.targetDays[day]
	if m == nil {
		m = make(map[int64]*atomic.Int64)
		e.targetDays[day] = m
	}
	b := m[t.id]
	if b == nil {
		b = new(atomic.Int64)
		m[t.id] = b
	}
	b.Add(n)

	g := e.globalDays[day]
	if g == nil {
		g = new(atomic.Int64)
		e.globalDays[day] = g
	}
	g.Add(n)

	hk := hourKey(now)
	h := e.hourBuckets[hk]
	if h == nil {
		h = new(atomic.Int64)
		e.hourBuckets[hk] = h
	}
	h.Add(n)

	t.total.Add(n)
	e.globalTotal.Add(n)
	e.quotaUsed.Add(n)
	e.countMu.Unlock()

	// 标记脏桶（规则 10）。
	e.dirtyMu.Lock()
	if e.dirtyDays[day] == nil {
		e.dirtyDays[day] = make(map[int64]struct{})
	}
	e.dirtyDays[day][t.id] = struct{}{}
	e.dirtyGlobal[day] = struct{}{}
	e.dirtyHours[hk] = struct{}{}
	e.dirtyTargets[t.id] = struct{}{}
	e.dirtyMu.Unlock()

	// 配额触达即时翻转门禁（规则 6）。
	limit := e.dailyLimit.Load()
	if limit > 0 && e.quotaUsed.Load() >= limit && e.blockedFlag.Load() == 0 {
		e.blockedFlag.Store(1)
		e.notify.signal()
	}
}

// ensureDay 在计数锁内处理跨日切换（规则 11：立即切换配额视图，旧日脏桶保留）。
func (e *Engine) ensureDay(day string, now time.Time) {
	if e.currentDay == day {
		return
	}
	e.currentDay = day
	if g := e.globalDays[day]; g != nil {
		e.quotaUsed.Store(g.Load())
	} else {
		e.quotaUsed.Store(0)
	}
	e.recomputeGateLockedQuota()
}

// recomputeGateLockedQuota 在已持 countMu 时只重算配额相关的门禁（避免重复取锁）。
func (e *Engine) recomputeGateLockedQuota() {
	e.stateMu.RLock()
	paused := e.paused
	stopped := e.stopped.Load()
	now := e.clock.Now()
	inS := inSchedule(now, e.schedule)
	e.stateMu.RUnlock()
	limit := e.dailyLimit.Load()
	used := e.quotaUsed.Load()
	quotaReached := limit > 0 && used >= limit
	blocked := stopped || paused || !inS || quotaReached
	if blocked {
		e.blockedFlag.Store(1)
	} else {
		e.blockedFlag.Store(0)
	}
	e.notify.signal()
}

// ---- 刷盘管线（规则 10） ----

// persistWorker 是唯一持久化协程：串行处理批次，失败重试同批（规则 10）。
// 存活至 Stop 关闭 persistCh 并耗尽队列，故最终批可经其提交。
func (e *Engine) persistWorker() {
	defer e.persistWG.Done()
	for req := range e.persistCh {
		e.persistWithRetry(req)
	}
}

func (e *Engine) persistWithRetry(req batchRequest) {
	for {
		err := e.persistFn(*req.batch)
		if err == nil {
			if req.done != nil {
				req.done <- nil
			}
			return
		}
		if e.isStopped() {
			if req.done != nil {
				req.done <- err
			}
			e.setRunErr(err)
			return
		}
		select {
		case <-e.lifeCtx.Done():
			if req.done != nil {
				req.done <- err
			}
			e.setRunErr(err)
			return
		case <-time.After(flushRetryDelay):
			// 重试同批（规则 10）。
		}
	}
}

// buildBatch 在 dirtyMu→countMu→stateMu 非嵌套顺序下构造不可变 CounterBatch（规则 10）。
func (e *Engine) buildBatch() *CounterBatch {
	// 1) 拷贝并清空脏集合。
	e.dirtyMu.Lock()
	days := make(map[string]map[int64]struct{})
	for d, ids := range e.dirtyDays {
		cp := make(map[int64]struct{}, len(ids))
		for id := range ids {
			cp[id] = struct{}{}
		}
		days[d] = cp
	}
	glob := make(map[string]struct{}, len(e.dirtyGlobal))
	for d := range e.dirtyGlobal {
		glob[d] = struct{}{}
	}
	hours := make(map[int64]struct{}, len(e.dirtyHours))
	for h := range e.dirtyHours {
		hours[h] = struct{}{}
	}
	targets := make(map[int64]struct{}, len(e.dirtyTargets))
	for id := range e.dirtyTargets {
		targets[id] = struct{}{}
	}
	e.dirtyDays = make(map[string]map[int64]struct{})
	e.dirtyGlobal = make(map[string]struct{})
	e.dirtyHours = make(map[int64]struct{})
	e.dirtyTargets = make(map[int64]struct{})
	e.dirtyMu.Unlock()

	if len(days) == 0 && len(glob) == 0 && len(hours) == 0 && len(targets) == 0 {
		return nil
	}

	batch := &CounterBatch{
		TargetDays:   make(map[string]map[int64]int64),
		GlobalDays:   make(map[string]int64),
		HourBuckets:  make(map[int64]int64),
		TargetTotals: make(map[int64]int64),
		TargetHealth: make(map[int64]TargetHealthPersist),
	}

	// 2) 读取计数绝对值（countMu）。
	e.countMu.Lock()
	e.seq++
	batch.Seq = e.seq
	for day, ids := range days {
		per := make(map[int64]int64)
		m := e.targetDays[day]
		if m != nil {
			for id := range ids {
				if b := m[id]; b != nil {
					if v := b.Load(); v > 0 {
						per[id] = v
					}
				}
			}
		}
		if len(per) > 0 {
			batch.TargetDays[day] = per
		}
	}
	for day := range glob {
		if g := e.globalDays[day]; g != nil {
			if v := g.Load(); v > 0 {
				batch.GlobalDays[day] = v
			}
		}
	}
	for h := range hours {
		if b := e.hourBuckets[h]; b != nil {
			if v := b.Load(); v > 0 {
				batch.HourBuckets[h] = v
			}
		}
	}
	e.countMu.Unlock()

	// 3) 读取目标 total 与健康快照（stateMu，TargetTotals 为稳定 atomic）。
	e.stateMu.RLock()
	for id := range targets {
		t := e.targets[id]
		if t == nil {
			continue
		}
		batch.TargetTotals[id] = t.total.Load()
		batch.TargetHealth[id] = TargetHealthPersist{
			Health:            t.health,
			CooldownUntilMS:   t.cooldownUntilMS,
			LastStatusCode:    t.lastStatusCode,
			LatencyMS:         t.latencyMS,
			ErrorMsg:          t.errorMsg,
			ConsecutiveErrors: t.consecutiveErrors,
		}
	}
	e.stateMu.RUnlock()

	return batch
}

// flushTicker 每 10s 构建并提交一个批次（fire-and-forget，规则 10）。
func (e *Engine) flushTicker() {
	defer e.wg.Done()
	t := time.NewTicker(flushInterval)
	defer t.Stop()
	for {
		select {
		case <-e.lifeCtx.Done():
			return
		case <-t.C:
			e.flushNow()
		}
	}
}

// flushNow 构建批次并经由唯一持久化协程提交，等待结果（规则 10/13/15/18）。
// persistWorker 在整个引擎生命周期内存活并持续消费，故阻塞提交与等待是安全的。
func (e *Engine) flushNow() error {
	batch := e.buildBatch()
	if batch == nil {
		return nil
	}
	done := make(chan error, 1)
	e.persistCh <- batchRequest{batch: batch, done: done}
	err := <-done
	if err != nil {
		e.setRunErr(err)
	}
	return err
}

// emergencyPersist 立即经管线提交紧急批并等待结果（规则 13/10）。
func (e *Engine) emergencyPersist() error {
	return e.flushNow()
}

// ---- 健康状态机（规则 13） ----

// updateHealth 写回健康前比对 generation 与 cooldown_until，旧结果不得覆盖新冷却。
func (e *Engine) updateHealth(id int64, gen uint64, newHealth string, status, latency int, msg string, untilMS int64) {
	e.stateMu.Lock()
	t := e.targets[id]
	if t == nil || t.gen != gen {
		e.stateMu.Unlock()
		return // 旧 generation 结果丢弃（规则 15）
	}
	now := e.clock.Now().UnixMilli()
	if newHealth != healthRateLimited && t.cooldownUntilMS > now {
		// 存在未到期冷却，非 RateLimited 结果不得覆盖（M1）。
		e.stateMu.Unlock()
		return
	}
	t.lastStatusCode = status
	t.latencyMS = int64(latency)
	t.errorMsg = msg
	switch newHealth {
	case healthRateLimited:
		if untilMS > t.cooldownUntilMS {
			t.cooldownUntilMS = untilMS
		}
		t.health = healthRateLimited
		t.consecutiveErrors = 0
	case healthHealthy:
		t.health = healthHealthy
		t.cooldownUntilMS = 0
		t.consecutiveErrors = 0
	case healthDead:
		t.health = healthDead
		if t.cooldownUntilMS <= now {
			t.cooldownUntilMS = 0
		}
	default:
		t.health = newHealth
	}
	e.stateMu.Unlock()

	e.dirtyMu.Lock()
	e.dirtyTargets[id] = struct{}{}
	e.dirtyMu.Unlock()
	e.notify.signal()
}

// rateLimitTarget 处理 403/429：置 RateLimited+冷却，立即紧急批持久化（规则 13）。
// 当前下载已收到 403 且即将关闭（不 drain），该目标 host 由本 worker 独占，无需 cancelTarget 等待，
// 否则会造成自等死锁。取消该目标在途由 403 自身关闭响应体完成。
func (e *Engine) rateLimitTarget(t *target, gen uint64, status int, latency int, msg string) {
	until := e.clock.Now().Add(e.cooldownHoursOf()).UnixMilli()
	e.updateHealth(t.id, gen, healthRateLimited, status, latency, msg, until)
	e.emergencyPersist() // 立即持久化（规则 13/10），失败保持内存冷却
}

// recordDownloadFailure 下载 404/超时/reset/5xx：consecutiveErrors+1，连续 3 → Dead（规则 13）。
func (e *Engine) recordDownloadFailure(t *target, gen uint64, status int, err error, latency int) {
	msg := ""
	if err != nil {
		msg = err.Error()
	} else {
		msg = fmt.Sprintf("download status %d", status)
	}
	e.stateMu.Lock()
	if t.gen != gen {
		e.stateMu.Unlock()
		return
	}
	now := e.clock.Now().UnixMilli()
	t.lastStatusCode = status
	t.errorMsg = msg
	if t.cooldownUntilMS > now {
		// 未到期冷却期间不叠加熔断计数。
		e.stateMu.Unlock()
		return
	}
	t.consecutiveErrors++
	if t.consecutiveErrors >= 3 {
		t.health = healthDead
		t.errorMsg = fmt.Sprintf("consecutive errors (%d): %s", t.consecutiveErrors, msg)
	}
	e.stateMu.Unlock()

	e.dirtyMu.Lock()
	e.dirtyTargets[t.id] = struct{}{}
	e.dirtyMu.Unlock()
	e.notify.signal()
}

// recordDownloadSuccess 下载成功（200/206）：重置错误计数、置 Healthy。
func (e *Engine) recordDownloadSuccess(t *target, gen uint64, status, latency int) {
	e.updateHealth(t.id, gen, healthHealthy, status, latency, "", 0)
}

// cancelTarget 取消指定目标的在途下载并等待结算（规则 13/15）。
func (e *Engine) cancelTarget(id int64) {
	e.poolMu.Lock()
	var done chan struct{}
	for _, w := range e.workers {
		w.mu.Lock()
		if w.curID == id {
			if w.curCancel != nil {
				w.curCancel()
			}
			if w.cycleDone != nil {
				done = w.cycleDone
			}
			w.mu.Unlock()
			break
		}
		w.mu.Unlock()
	}
	e.poolMu.Unlock()
	if done != nil {
		<-done // 等待结算（规则 13/15）
	}
}

// ---- 探活（规则 12） ----

func (e *Engine) probeTicker() {
	defer e.wg.Done()
	t := time.NewTicker(probeInterval)
	defer t.Stop()
	for {
		select {
		case <-e.lifeCtx.Done():
			return
		case <-t.C:
			e.enqueueEligibleProbe(e.clock.Now(), false)
		}
	}
}

// probeWorker 处理探活队列（并发受控）。
func (e *Engine) probeWorker() {
	defer e.probeWG.Done()
	for {
		select {
		case <-e.lifeCtx.Done():
			return
		case id := <-e.probeCh:
			e.probeMu.Lock()
			delete(e.probePending, id)
			e.probeInFlight[id] = struct{}{}
			e.probeMu.Unlock()

			e.probeOne(id)

			e.probeMu.Lock()
			delete(e.probeInFlight, id)
			e.probeMu.Unlock()
		}
	}
}

// enqueueEligibleProbe 把所有未软删且不在未到期冷却的目标加入探活队列（同轮合并，规则 1/12）。
func (e *Engine) enqueueEligibleProbe(now time.Time, onlyUnknown bool) {
	nowMS := now.UnixMilli()
	var ids []int64
	e.stateMu.RLock()
	for _, t := range e.targets {
		if t.deleted {
			continue
		}
		if t.cooldownUntilMS > nowMS {
			continue
		}
		if onlyUnknown && t.health != healthUnknown {
			continue
		}
		ids = append(ids, t.id)
	}
	e.stateMu.RUnlock()

	for _, id := range ids {
		e.probeMu.Lock()
		_, pending := e.probePending[id]
		_, inflight := e.probeInFlight[id]
		if pending || inflight {
			e.probeMu.Unlock()
			continue // 同轮合并（规则 12）
		}
		e.probePending[id] = struct{}{}
		e.probeMu.Unlock()
		select {
		case e.probeCh <- id:
		case <-e.lifeCtx.Done():
			return
		}
	}
}

// CheckAllNow 手动触发所有未软删且不在冷却目标探活（规则 12）。
func (e *Engine) CheckAllNow(ctx context.Context) error {
	e.adminMu.Lock()
	defer e.adminMu.Unlock()
	if e.stopped.Load() {
		return errors.New("engine stopped")
	}
	e.enqueueEligibleProbe(e.clock.Now(), false)
	return nil
}

// probeOne 执行单个目标探活（单请求超时 10s，Range 0-1023，最多读 1024 字节，规则 12）。
func (e *Engine) probeOne(id int64) {
	e.stateMu.RLock()
	t := e.targets[id]
	if t == nil {
		e.stateMu.RUnlock()
		return
	}
	gen := t.gen
	u := t.url
	e.stateMu.RUnlock()

	pctx, cancel := context.WithTimeout(e.lifeCtx, probeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(pctx, http.MethodGet, u, nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", e.userAgentOf())
	req.Header.Set("Range", "bytes=0-1023")

	start := e.clock.Now()
	resp, derr := e.httpClient.Do(req)
	latency := int(e.clock.Now().Sub(start).Milliseconds())
	if derr != nil {
		if errors.Is(derr, context.Canceled) || errors.Is(derr, context.DeadlineExceeded) {
			// 超时置 Dead（规则 13）。
			e.onProbeTransportError(t, gen, derr, latency)
			return
		}
		if e.lifeCtx.Err() != nil {
			return // 引擎关闭，非远端故障
		}
		e.onProbeTransportError(t, gen, derr, latency)
		return
	}
	defer resp.Body.Close()

	status := resp.StatusCode
	if status == http.StatusForbidden || status == http.StatusTooManyRequests {
		e.rateLimitTarget(t, gen, status, latency, "probe rate limited")
		return
	}

	// 探活字节计入统计与限速（规则 12）。
	if err := e.probeRead(pctx, t, resp.Body); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			e.onProbeTransportError(t, gen, err, latency)
			return
		}
		if e.lifeCtx.Err() != nil {
			return
		}
		e.onProbeTransportError(t, gen, err, latency)
		return
	}

	if status == http.StatusOK || status == http.StatusPartialContent {
		e.onProbeHealthy(t, gen, status, latency)
		return
	}
	// 探活其他非 200/206 状态（非 403/429）→ 探活失败置 Dead（规则 13）。
	e.onProbeDead(t, gen, status, latency, fmt.Sprintf("probe status %d", status))
}

// probeRead 最多读 probeRange 字节，受令牌桶限速并计入统计。
func (e *Engine) probeRead(ctx context.Context, t *target, body io.Reader) error {
	buf := make([]byte, probeRange)
	if err := e.limiter.WaitN(ctx, len(buf)); err != nil {
		return err
	}
	n, rerr := body.Read(buf)
	if n > 0 {
		e.countBytes(t, int64(n))
	}
	if rerr != nil {
		return rerr
	}
	return nil
}

func (e *Engine) getTarget(id int64) *target {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	return e.targets[id]
}

func (e *Engine) onProbeHealthy(t *target, gen uint64, status, latency int) {
	e.updateHealth(t.id, gen, healthHealthy, status, latency, "", 0)
}

func (e *Engine) onProbeDead(t *target, gen uint64, status, latency int, msg string) {
	e.updateHealth(t.id, gen, healthDead, status, latency, msg, 0)
}

func (e *Engine) onProbeTransportError(t *target, gen uint64, err error, latency int) {
	e.updateHealth(t.id, gen, healthDead, 0, latency, err.Error(), 0)
}

// ---- 速率采样（规则 14） ----

func (e *Engine) samplerLoop() {
	defer e.wg.Done()
	t := time.NewTicker(sampleInterval)
	defer t.Stop()
	for {
		select {
		case <-e.lifeCtx.Done():
			return
		case <-t.C:
			now := e.clock.Now()
			rates := make(map[int64]float64)
			// 全局（id 0）。
			rates[0] = e.sampleRate(0, e.globalTotal.Load(), now)
			// 各目标。
			var ids []int64
			e.stateMu.RLock()
			for id := range e.targets {
				ids = append(ids, id)
			}
			e.stateMu.RUnlock()
			for _, id := range ids {
				if t := e.getTarget(id); t != nil {
					rates[id] = e.sampleRate(id, t.total.Load(), now)
				}
			}
			e.sampMu.Lock()
			e.rates = rates
			e.sampMu.Unlock()
		}
	}
}

// sampleRate 按稳定 target id 更新环形缓冲并返回 bytes/s；<2 样本或 3s 无增量=0。
// sampleRate 按稳定 target id 更新环形缓冲并返回 bytes/s；<2 样本或 3s 无增量=0（规则 14）。
func (e *Engine) sampleRate(id int64, cum int64, now time.Time) float64 {
	st := e.samplerState[id]
	if st == nil {
		st = &sampleState{last: cum, lastTS: now}
		e.samplerState[id] = st
		return 0
	}
	dt := now.Sub(st.lastTS).Seconds()
	delta := float64(cum - st.last)
	st.last = cum
	st.lastTS = now

	if dt > 3.0 || dt <= 0 {
		st.entries = st.entries[:0]
		st.rate = 0
		return 0
	}
	st.entries = append(st.entries, sampleEntry{dt: dt, delta: delta})
	var sumDt float64
	keep := len(st.entries)
	for i := len(st.entries) - 1; i >= 0; i-- {
		sumDt += st.entries[i].dt
		if sumDt > 3.0 {
			break
		}
		keep = i
	}
	if keep < len(st.entries) {
		st.entries = append([]sampleEntry{}, st.entries[keep:]...)
	}
	if len(st.entries) < 2 {
		st.rate = 0
		return 0
	}
	var sDt, sDelta float64
	for _, en := range st.entries {
		sDt += en.dt
		sDelta += en.delta
	}
	if sDt <= 0 {
		st.rate = 0
		return 0
	}
	st.rate = sDelta / sDt
	return st.rate
}


// Snapshot 返回引擎与各目标的深拷贝快照（单次统一 now，规则 19）。
func (e *Engine) Snapshot() EngineSnapshot {
	now := e.clock.Now()
	e.stateMu.RLock()
	paused := e.paused
	mode := e.storageMode
	maxRate := e.maxRateMbps
	maxConns := e.maxConns
	sched := e.schedule
	limitGB := float64(e.dailyLimit.Load()) / 1e9
	status := statusRunning
	if e.stopped.Load() {
		status = statusPaused
	} else if paused {
		status = statusPaused
	}
	e.stateMu.RUnlock()

	e.countMu.Lock()
	day := e.currentDay
	used := e.quotaUsed.Load()
	e.countMu.Unlock()
	limit := e.dailyLimit.Load()
	quotaReached := limit > 0 && used >= limit
	if !paused && quotaReached {
		status = statusQuotaReached
	} else if !paused && !inSchedule(now, sched) {
		status = statusSleepingSchedule
	}

	snap := EngineSnapshot{
		Status:           status,
		StorageMode:      mode,
		MaxRateMbps:      maxRate,
		MaxConnections:   maxConns,
		ActiveConnections: e.activeConns.Load(),
		TodayBytes:       used,
		DailyLimitGB:     limitGB,
		Schedule:         sched,
		SnapshotAt:       now,
	}

	// 全局速率（id0）。
	snap.SpeedMBPerSec, snap.SpeedMbps = e.speedFor(0)

	// 先在 countMu 下快照当前日桶（TodayBytes），再在 stateMu 下读元数据，避免嵌套锁。
	e.countMu.Lock()
	day = e.currentDay
	today := make(map[int64]int64)
	if m := e.targetDays[day]; m != nil {
		for id, b := range m {
			today[id] = b.Load()
		}
	}
	e.countMu.Unlock()

	// 目标快照。
	e.stateMu.RLock()
	nowMS := now.UnixMilli()
	for _, t := range e.targets {
		if t.deleted {
			continue
		}
		ts := TargetSnapshot{
			ID:           t.id,
			Name:         t.name,
			URL:          t.url,
			Weight:       t.weight,
			Enabled:      t.enabled,
			Health:       t.health,
			LatencyMS:    t.latencyMS,
			ErrorMsg:     t.errorMsg,
			TotalBytes:   t.total.Load(),
			TodayBytes:   today[t.id],
		}
		if t.cooldownUntilMS > nowMS {
			rem := float64(t.cooldownUntilMS-nowMS) / 1000.0
			ts.CooldownRemainingSeconds = int64(ceilSec(rem))
		}
		ts.SpeedMBPerSec, ts.SpeedMbps = e.speedFor(t.id)
		snap.Targets = append(snap.Targets, ts)
	}
	e.stateMu.RUnlock()
	return snap
}

func ceilSec(sec float64) int64 {
	c := int64(sec)
	if sec > float64(c) {
		c++
	}
	if c < 0 {
		return 0
	}
	return c
}

// speedFor 返回 id 对应的 MB/s 与 Mbps（id0=全局）。
func (e *Engine) speedFor(id int64) (mbPerSec, mbps float64) {
	e.sampMu.Lock()
	r := e.rates[id]
	e.sampMu.Unlock()
	mbPerSec = r / 1e6
	mbps = r * 8 / 1e6
	return
}

// History24h 返回内存权威 24h 历史视图（最近 24 个本地小时，补零、最旧在前，规则 17）。
func (e *Engine) History24h(now time.Time) ([]HistoryPoint, error) {
	starts := localHourStarts(now)
	e.countMu.Lock()
	pts := make([]HistoryPoint, 0, len(starts))
	for _, st := range starts {
		ts := st.Unix()
		b := int64(0)
		if hb := e.hourBuckets[ts]; hb != nil {
			b = hb.Load()
		}
		pts = append(pts, HistoryPoint{
			HourUnix: ts,
			Hour:     time.Unix(ts, 0).In(time.Local).Format(time.RFC3339),
			Bytes:    b,
		})
	}
	e.countMu.Unlock()
	return pts, nil
}

// ---- 运行参数 setter（供 main / ApplyConfigUpdate 使用） ----

func (e *Engine) SetMaxRate(mbps int) error {
	if err := ValidateMaxRateMbps(mbps); err != nil {
		return err
	}
	e.adminMu.Lock()
	defer e.adminMu.Unlock()
	if e.stopped.Load() {
		return errors.New("engine stopped")
	}
	e.applyRate(mbps)
	e.recomputeGate()
	return nil
}

func (e *Engine) applyRate(mbps int) {
	e.stateMu.Lock()
	e.maxRateMbps = mbps
	e.stateMu.Unlock()
	r := rate.Limit(float64(mbps) * 1e6 / 8)
	if mbps == 0 {
		r = rate.Inf
	}
	e.limiter.SetLimit(r) // 规则 8：SetLimit 重算唤醒等待者
}

func (e *Engine) SetMaxConnections(n int) error {
	if err := ValidateMaxConnections(n); err != nil {
		return err
	}
	e.adminMu.Lock()
	defer e.adminMu.Unlock()
	if e.stopped.Load() {
		return errors.New("engine stopped")
	}
	e.stateMu.Lock()
	e.maxConns = n
	e.stateMu.Unlock()
	e.resizePool(n) // 规则 5：动态调整 worker
	e.recomputeGate()
	return nil
}

func (e *Engine) SetStorageMode(mode string) error {
	if err := ValidateStorageMode(mode); err != nil {
		return err
	}
	e.adminMu.Lock()
	defer e.adminMu.Unlock()
	if e.stopped.Load() {
		return errors.New("engine stopped")
	}
	if err := e.precheckStorageDir(mode); err != nil {
		return err
	}
	e.stateMu.Lock()
	e.storageMode = mode
	e.stateMu.Unlock()
	return nil
}

func (e *Engine) SetDailyLimit(gb float64) error {
	if err := ValidateDailyLimitGB(gb); err != nil {
		return err
	}
	e.adminMu.Lock()
	defer e.adminMu.Unlock()
	if e.stopped.Load() {
		return errors.New("engine stopped")
	}
	e.dailyLimit.Store(int64(gb * 1e9))
	e.recomputeGate()
	return nil
}

func (e *Engine) Pause() error {
	e.adminMu.Lock()
	defer e.adminMu.Unlock()
	if e.stopped.Load() {
		return errors.New("engine stopped")
	}
	e.stateMu.Lock()
	e.paused = true
	e.stateMu.Unlock()
	e.recomputeGate()
	return nil
}

func (e *Engine) Resume() error {
	e.adminMu.Lock()
	defer e.adminMu.Unlock()
	if e.stopped.Load() {
		return errors.New("engine stopped")
	}
	e.stateMu.Lock()
	e.paused = false
	e.stateMu.Unlock()
	// 规则 6：Resume 不越过配额/时段。
	e.recomputeGate()
	return nil
}

// ApplyConfigUpdate 是控制事务入口（B2）：验证→存储预检→持久化→发布，失败不改运行态。
func (e *Engine) ApplyConfigUpdate(candidate *Config, persist func() error) error {
	e.adminMu.Lock()
	defer e.adminMu.Unlock()
	if e.stopped.Load() {
		return errors.New("engine stopped")
	}
	if candidate == nil {
		return errors.New("nil config")
	}
	if err := candidate.Validate(); err != nil {
		return err
	}
	if err := e.precheckStorageDir(candidate.StorageMode); err != nil {
		return err
	}
	if persist != nil {
		if err := persist(); err != nil {
			return err // 持久化失败不改运行态（B2）
		}
	}
	// 发布完整配置。
	e.applyRate(candidate.MaxRateMbps)
	e.dailyLimit.Store(int64(candidate.DailyLimitGB * 1e9))
	e.stateMu.Lock()
	e.maxConns = candidate.MaxConnections
	e.storageMode = candidate.StorageMode
	e.schedule = candidate.Schedule
	e.jitter = candidate.Jitter
	e.cooldownHours = candidate.CooldownHours
	e.userAgent = candidate.UserAgent
	e.stateMu.Unlock()
	e.resizePool(candidate.MaxConnections)
	e.recomputeGate()
	return nil
}

func (e *Engine) precheckStorageDir(mode string) error {
	e.stateMu.RLock()
	memDir := e.cfg.MemoryDir
	diskDir := e.cfg.DiskDir
	e.stateMu.RUnlock()
	switch mode {
	case storageModeMemory:
		if err := os.MkdirAll(memDir, 0o755); err != nil {
			return err
		}
	case storageModeDisk:
		if err := os.MkdirAll(diskDir, 0o755); err != nil {
			return err
		}
	}
	return nil
}

// ---- 目标 CRUD（规则 15，由引擎接管） ----

// CreateTarget 新建目标：立即建立 Unknown 计数对象并触发探活（规则 15）。
func (e *Engine) CreateTarget(p TargetPatch) (TargetRecord, error) {
	e.adminMu.Lock()
	defer e.adminMu.Unlock()
	if e.stopped.Load() {
		return TargetRecord{}, errors.New("engine stopped")
	}
	name := ""
	if p.Name != nil {
		name = *p.Name
	}
	urlStr := ""
	if p.URL != nil {
		urlStr = *p.URL
	}
	weight := defaultTargetWeight
	if p.Weight != nil {
		weight = *p.Weight
	}
	if err := ValidateTargetInput(name, urlStr, weight); err != nil {
		return TargetRecord{}, err
	}

	rec, err := e.store.CreateTarget(p)
	if err != nil {
		return TargetRecord{}, err
	}

	host := normalizeHost(rec.URL)
	t := &target{
		id:      rec.ID,
		gen:     1,
		host:    host,
		total:   new(atomic.Int64),
		name:    rec.Name,
		url:     rec.URL,
		weight:  rec.Weight,
		enabled: rec.Enabled,
		health:  healthUnknown,
	}
	t.total.Store(rec.TotalBytes)
	e.stateMu.Lock()
	e.targets[t.id] = t
	e.stateMu.Unlock()
	e.notify.signal()

	// 立即探活新目标（规则 15/12）。
	e.probeMu.Lock()
	if _, pending := e.probePending[t.id]; !pending {
		if _, inflight := e.probeInFlight[t.id]; !inflight {
			e.probePending[t.id] = struct{}{}
			select {
			case e.probeCh <- t.id:
			default:
				delete(e.probePending, t.id)
			}
		}
	}
	e.probeMu.Unlock()
	return rec, nil
}

// PatchTarget 局部更新目标（规则 15）。
func (e *Engine) PatchTarget(id int64, p TargetPatch) (TargetRecord, error) {
	e.adminMu.Lock()
	defer e.adminMu.Unlock()
	if e.stopped.Load() {
		return TargetRecord{}, errors.New("engine stopped")
	}
	// 合并候选进行验证。
	var cand struct {
		name, url string
		weight    int
		oldURL    string
	}
	var cerr error
	cand, cerr = e.mergedCandidate(id, p)
	if cerr != nil {
		return TargetRecord{}, cerr
	}
	if err := ValidateTargetInput(cand.name, cand.url, cand.weight); err != nil {
		return TargetRecord{}, err
	}
	// URL 变更 → generation 递增、清健康、立即探活（规则 15）。
	urlChanged := p.URL != nil && *p.URL != cand.oldURL

	// tombstone：取消在途→结算→刷盘（规则 15）。
	if urlChanged {
		e.cancelTarget(id)
		e.dirtyMu.Lock()
		e.dirtyTargets[id] = struct{}{}
		e.dirtyMu.Unlock()
	}
	if err := e.flushNow(); err != nil {
		return TargetRecord{}, err
	}

	rec, err := e.store.PatchTarget(id, p)
	if err != nil {
		// DB 失败恢复旧目标准入与元数据（已发生流量保留）。
		return TargetRecord{}, err
	}

	e.stateMu.Lock()
	t := e.targets[id]
	if t != nil {
		t.name = rec.Name
		t.url = rec.URL
		t.weight = rec.Weight
		t.enabled = rec.Enabled
		if urlChanged {
			t.gen++
			t.health = healthUnknown
			t.cooldownUntilMS = 0
			t.consecutiveErrors = 0
			t.lastStatusCode = 0
			t.latencyMS = 0
			t.errorMsg = ""
			t.host = normalizeHost(rec.URL)
		}
	}
	e.stateMu.Unlock()
	e.notify.signal()

	if urlChanged {
		// 立即探活（旧 generation 结果丢弃由 updateHealth 保证）。
		e.probeMu.Lock()
		if _, pending := e.probePending[id]; !pending {
			if _, inflight := e.probeInFlight[id]; !inflight {
				e.probePending[id] = struct{}{}
				select {
				case e.probeCh <- id:
				default:
					delete(e.probePending, id)
				}
			}
		}
		e.probeMu.Unlock()
	}
	return rec, nil
}

// mergedCandidate 读取旧目标并与补丁合并（返回候选与旧 URL）。
func (e *Engine) mergedCandidate(id int64, p TargetPatch) (struct {
	name string
	url  string
	weight int
	oldURL string
}, error) {
	var res struct {
		name string
		url  string
		weight int
		oldURL string
	}
	e.stateMu.RLock()
	t := e.targets[id]
	if t == nil {
		e.stateMu.RUnlock()
		return res, ErrNotFound
	}
	res.name, res.url, res.weight, res.oldURL = t.name, t.url, t.weight, t.url
	e.stateMu.RUnlock()
	if p.Name != nil {
		res.name = *p.Name
	}
	if p.URL != nil {
		res.url = *p.URL
	}
	if p.Weight != nil {
		res.weight = *p.Weight
	}
	return res, nil
}

// DeleteTarget 软删目标（规则 15：软删+停准入+结算，历史桶保留）。
func (e *Engine) DeleteTarget(id int64) error {
	e.adminMu.Lock()
	defer e.adminMu.Unlock()
	if e.stopped.Load() {
		return errors.New("engine stopped")
	}
	e.cancelTarget(id)
	e.flushNow()
	if err := e.store.SoftDeleteTarget(id); err != nil {
		return err
	}
	e.stateMu.Lock()
	if t := e.targets[id]; t != nil {
		t.deleted = true
	}
	e.stateMu.Unlock()
	e.notify.signal()
	return nil
}

// ---- 快照 DTO（契约冻结 B1） ----

// TargetSnapshot 单目标快照。
type TargetSnapshot struct {
	ID                       int64   `json:"id"`
	Name                     string  `json:"name"`
	URL                      string  `json:"url"`
	Weight                   int     `json:"weight"`
	Enabled                  bool    `json:"enabled"`
	Health                   string  `json:"health"`
	LatencyMS                int64   `json:"latency_ms"`
	CooldownRemainingSeconds int64   `json:"cooldown_remaining_seconds"`
	TodayBytes               int64   `json:"today_bytes"`
	TotalBytes               int64   `json:"total_bytes"`
	SpeedMbps                float64 `json:"speed_mbps"`
	SpeedMBPerSec            float64 `json:"speed_mb_per_sec"`
	ErrorMsg                 string  `json:"error_msg"`
}

// EngineSnapshot 引擎整体快照。
type EngineSnapshot struct {
	Status            string         `json:"status"`
	StorageMode       string         `json:"storage_mode"`
	MaxRateMbps       int            `json:"max_rate_mbps"`
	MaxConnections    int            `json:"max_connections"`
	ActiveConnections int64          `json:"active_connections"`
	SpeedMbps         float64        `json:"speed_mbps"`
	SpeedMBPerSec     float64        `json:"speed_mb_per_sec"`
	TodayBytes        int64          `json:"today_bytes"`
	DailyLimitGB      float64        `json:"daily_limit_gb"`
	Schedule          ScheduleConfig `json:"schedule"`
	Targets           []TargetSnapshot `json:"-"`
	SnapshotAt        time.Time      `json:"-"`
}
