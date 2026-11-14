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

// ErrValidation 是校验失败的哨兵错误（MINOR-2）。校验失败路径用它包装，供上层识别。
var ErrValidation = errors.New("validation error")

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

// flushReq 是一次刷盘请求。done 非 nil=同步等待结果（紧急批/CRUD/Stop）；nil=异步周期刷盘。
type flushReq struct {
	done chan error
}

// Engine 是下载引擎核心。锁序：adminMu > stateMu > countMu > dirtyMu（dirtyMu 为叶）。
type Engine struct {
	cfg        *Config
	store      *Store
	persistFn  func(CounterBatch) error // 持久化函数（默认 store.PersistBatch，测试可注入）
	httpClient *http.Client
	limiter    *rate.Limiter
	rateSig    notifySig // F7：限速变化广播，唤醒等待中的预留
	clock      clock

	adminMu sync.Mutex   // Stop / CRUD / 控制事务门禁（规则 15/18/19）
	stateMu sync.RWMutex // 目标元数据与运行参数
	countMu sync.Mutex   // 计数协调锁（规则 9/11）
	dirtyMu sync.Mutex   // 脏桶集合（叶锁）
	poolMu  sync.Mutex   // 连接池（规则 5）
	hostMu  sync.Mutex   // host 互斥表（规则 2）
	reqMu   sync.Mutex   // reqCtx/reqCancel 守卫（叶锁，F6）

	lifeCtx    context.Context
	lifeCancel context.CancelFunc

	reqCtx    context.Context // 门禁运行期请求上下文（F6）：转阻塞时取消以中止在途 Do/WaitN/Read
	reqCancel context.CancelFunc

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

	// 脏桶（dirtyMu，值为该条目最近一次标记时的脏纪元，用于成功后才清除 F2）
	dirtyEpoch   uint64
	dirtyDays    map[string]map[int64]uint64
	dirtyGlobal  map[string]uint64
	dirtyHours   map[int64]uint64
	dirtyTargets map[int64]uint64

	// 运行参数（stateMu）
	paused        bool
	storageMode   string
	maxRateMbps   int
	maxConns      int
	schedule      ScheduleConfig
	jitter        JitterConfig
	cooldownHours int
	userAgent     string

	persistCh   chan flushReq // 只含信号，建批/Seq 由唯一持久化协程串行完成（F2）
	persistCtx  context.Context
	persistCancel context.CancelFunc

	probeCh       chan int64
	probeMu       sync.Mutex
	probePending  map[int64]struct{}
	probeInFlight map[int64]struct{}
	probeCancel   map[int64]context.CancelFunc // F8：按目标取消在途探活
	changing      map[int64]bool               // stateMu：CRUD 进行中，拒准入（F8）

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

		dirtyDays:    make(map[string]map[int64]uint64),
		dirtyGlobal:  make(map[string]uint64),
		dirtyHours:   make(map[int64]uint64),
		dirtyTargets: make(map[int64]uint64),

		storageMode:   cfg.StorageMode,
		maxRateMbps:   cfg.MaxRateMbps,
		maxConns:      cfg.MaxConnections,
		schedule:      cfg.Schedule,
		jitter:        cfg.Jitter,
		cooldownHours: cfg.CooldownHours,
		userAgent:     cfg.UserAgent,

		persistCh:      make(chan flushReq, 1),
		persistCtx:     context.Background(),
		probeCh:        make(chan int64, 256),
		probePending:   make(map[int64]struct{}),
		probeInFlight:  make(map[int64]struct{}),
		probeCancel:    make(map[int64]context.CancelFunc),
		changing:       make(map[int64]bool),
		rates:          make(map[int64]float64),
		samplerState:   make(map[int64]*sampleState),
	}
	e.dailyLimit.Store(int64(cfg.DailyLimitGB * 1e9))
	e.notify.init()
	e.rateSig.init()
	return e, nil
}

func dayKey(now time.Time) string { return now.In(time.Local).Format("2006-01-02") }

// hourStartTimeIn 返回 now 在指定时区 loc 下所在钟点小时的起点，保留 now 的实际偏移
// （绝对时刻截断到小时）。跨 DST 重复小时（fall-back 时 01:00 出现两次）各自独立成桶（规则 12 修复）。
func hourStartTimeIn(now time.Time, loc *time.Location) time.Time {
	n := now.In(loc)
	return n.Add(-time.Duration(n.Minute())*time.Minute -
		time.Duration(n.Second())*time.Second -
		time.Duration(n.Nanosecond()))
}

func hourStartTime(now time.Time) time.Time { return hourStartTimeIn(now, time.Local) }

func hourKey(now time.Time) int64 { return hourStartTimeIn(now, time.Local).Unix() }

// hourKeyIn 以显式时区计算小时键（供测试在固定时区验证，避免改全局 time.Local）。
func hourKeyIn(now time.Time, loc *time.Location) int64 {
	return hourStartTimeIn(now, loc).Unix()
}

// engineHourStarts 返回最近 24 个本地钟点小时起点（最旧在前、当前小时后最后），
// 与 hourKey 共用同一偏移保留实现，供 Engine.History24h 权威视图使用（规则 17/F12）。
func engineHourStarts(now time.Time) []time.Time {
	current := hourStartTime(now)
	starts := make([]time.Time, 24)
	for i := 0; i < 24; i++ {
		starts[24-1-i] = current.Add(-time.Duration(i) * time.Hour)
	}
	return starts
}

func inSchedule(now time.Time, s ScheduleConfig) bool {
	h := now.In(time.Local).Hour()
	if s.StartHour < s.EndHour {
		return h >= s.StartHour && h < s.EndHour
	}
	return h >= s.StartHour || h < s.EndHour
}

// wrapValidation 把校验失败错误包装为 ErrValidation 哨兵（MINOR-2）。
func wrapValidation(err error) error {
	return fmt.Errorf("%w: %v", ErrValidation, err)
}

// Start 启动引擎：恢复持久化状态、构建计数对象、启动各协程（规则 10/12/14/15）。
func (e *Engine) Start(ctx context.Context) error {
	state, err := e.store.LoadEngineState(e.clock.Now())
	if err != nil {
		return err
	}
	e.lifeCtx, e.lifeCancel = context.WithCancel(context.Background())
	e.persistCtx, e.persistCancel = context.WithCancel(context.Background())
	e.reqCtx, e.reqCancel = context.WithCancel(context.Background())
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
// F3：先取消 persistCtx（独立于 adminMu），使在途同步 flush 可被取消、管理事务不因重试而卡死。
func (e *Engine) Stop() error {
	if e.persistCancel != nil {
		e.persistCancel()
	}

	e.adminMu.Lock()
	defer e.adminMu.Unlock()

	if e.stopped.Load() {
		return e.runErr
	}
	e.stopped.Store(true)
	e.blockedFlag.Store(1)
	e.setBlocked(true)
	e.notify.signal()

	// 取消全部 worker 与协程。
	if e.lifeCancel != nil {
		e.lifeCancel()
	}
	e.cancelAllWorkers()

	// 等待下载 worker / 探活 / 各协程退出（persistWorker 单独存活以承接最终批）。
	e.wg.Wait()
	e.probeWG.Wait()

	// 最终同步 flush（规则 18），经唯一持久化协程提交并等待；随后关闭通道让其退出。
	// 用不受 persistCtx 取消影响的最终刷盘（此时 persistWorker 处于 drain 阶段，必然消费）。
	if e.persistCh != nil {
		if err := e.flushNowFinal(); err != nil {
			e.setRunErr(err)
		}
		close(e.persistCh)
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
		// F6：请求 ctx 合并 worker ctx 与门禁请求 ctx，转阻塞时即时中止 Do/WaitN/Read。
		cyc, cycCancel := mergeCtx(wctx, e.currentReqCtx())
		w.mu.Lock()
		w.curID = t.id
		w.curCancel = cycCancel
		w.cycleDone = make(chan struct{})
		d := w.cycleDone
		w.mu.Unlock()

		dlErr := e.download(cyc, t)

		// F10：每轮请求结束无条件取消子 ctx，避免 context 泄漏。
		cycCancel()
		w.mu.Lock()
		w.curID = 0
		w.curCancel = nil
		w.cycleDone = nil
		w.mu.Unlock()
		close(d) // F8：cancelTarget 等待到此为止，不因忙等待而拖延

		if errors.Is(dlErr, errHostBusy) {
			// F1：忙目标可取消等待（host 释放会 notify 唤醒），禁止 jitter=0 自旋。
			e.notify.wait(wctx)
		}

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
	e.notify.signal() // F1：host 释放唤醒忙目标等待者
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
	e.ensureDay(dayKey(now), now) // F5：主动跨日切换（午夜即使零读取也解除配额）
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
	e.setBlocked(blocked) // F6：转阻塞取消在途请求
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

// download 执行一次完整下载（含重定向逐跳与存储策略，规则 3/16）。返回 errHostBusy 表示 host 忙（F1）。
func (e *Engine) download(ctx context.Context, t *target) error {
	e.activeConns.Add(1)
	defer e.activeConns.Add(-1)

	gen := e.targetGen(t.id)
	mode := e.storageModeOf()

	resp, heldHost, latency, err := e.doRequest(ctx, t, gen, kindDownload, "bytes=0-")
	if heldHost != "" {
		defer e.releaseHost(heldHost) // 最终 host 许可在 body 关闭后释放（规则 3）
	}
	if err != nil {
		if errors.Is(err, errHostBusy) {
			return errHostBusy
		}
		if e.isCtxCancel(err, ctx) {
			return nil // 因 Stop/CRUD/门禁取消，不计远端故障（规则 13）
		}
		e.recordDownloadFailure(t, gen, 0, err, latency)
		return nil
	}

	status := resp.StatusCode
	defer resp.Body.Close()

	if status == http.StatusForbidden || status == http.StatusTooManyRequests {
		e.rateLimitTarget(t, gen, status, latency, "rate limited")
		return nil
	}

	// 打开存储目标（规则 16）。
	d, derr := e.openDestination(t, mode)
	if derr != nil {
		e.recordDownloadFailure(t, gen, status, derr, latency)
		return nil
	}

	err = e.readLoop(ctx, t, gen, resp.Body, d.w)
	switch {
	case err == nil && (status == http.StatusOK || status == http.StatusPartialContent):
		if ferr := d.finalize(); ferr != nil {
			e.recordDownloadFailure(t, gen, status, ferr, latency)
			return nil
		}
		e.recordDownloadSuccess(t, gen, status, latency)
		return nil
	case err == nil:
		e.abortAndFail(t, gen, status, nil, latency, d)
		return nil
	case errors.Is(err, errQuotaReached):
		e.abortAndMsg(t, gen, d, "daily quota reached")
		return nil
	case e.isCtxCancel(err, ctx):
		e.abortAndMsg(t, gen, d, err.Error())
		return nil
	default:
		e.abortAndFail(t, gen, status, err, latency, d)
		return nil
	}
}

// abortAndFail 清理存储目标并记录下载失败（含清理错误与状态码错误，F11）。
// 无论 err 是否为 nil 都记录（status 决定消息），确保 4xx/5xx 走熔断计数（规则 13）。
func (e *Engine) abortAndFail(t *target, gen uint64, status int, err error, latency int, d *dest) {
	if aerr := d.abort(); aerr != nil {
		e.recordDownloadFailure(t, gen, status, aerr, latency)
		return
	}
	e.recordDownloadFailure(t, gen, status, err, latency)
}

// abortAndMsg 清理存储目标并发布 error_msg（配额/取消等非远端路径，F11）。
func (e *Engine) abortAndMsg(t *target, gen uint64, d *dest, msg string) {
	if aerr := d.abort(); aerr != nil {
		e.publishErrorMsg(t.id, gen, aerr.Error())
		return
	}
}

// publishErrorMsg 只发布错误信息，不改变健康状态（F11 清理错误告警）。
func (e *Engine) publishErrorMsg(id int64, gen uint64, msg string) {
	e.stateMu.Lock()
	t := e.targets[id]
	if t == nil || t.gen != gen {
		e.stateMu.Unlock()
		return
	}
	if msg != "" {
		t.errorMsg = msg
	}
	e.stateMu.Unlock()
	e.markTargetDirty(id)
	e.notify.signal()
}

// eligible 发请求前重查准入（规则 1/F1）。下载仅 enabled+Healthy+未冷却；探活允许未软删且不在
// 未到期冷却的所有目标（含 disabled/Unknown/Dead/已过期 RateLimited）。取得 host 许可后再次调用。
func (e *Engine) eligible(id int64, gen uint64, kind requestKind) bool {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	if e.changing[id] {
		return false // F8：CRUD 进行中，拒绝下载与探活准入
	}
	t := e.targets[id]
	if t == nil || t.deleted {
		return false
	}
	if t.gen != gen {
		return false
	}
	if t.cooldownUntilMS > e.clock.Now().UnixMilli() {
		return false // 一切请求禁止（含已排队探活，F1）
	}
	if kind == kindDownload {
		return t.enabled && t.health == healthHealthy
	}
	return true // 探活
}

// doRequest 统一请求执行器（F1）：逐跳重定向、逐跳 host 许可、逐跳按 kind 重查准入，返回最终响应、
// 当前持有的 host 许可与响应头耗时。取消/失败路径全部释放 host 许可（规则 3）。
func (e *Engine) doRequest(ctx context.Context, t *target, gen uint64, kind requestKind, rangeSpec string) (*http.Response, string, int, error) {
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
		// 发请求前重查准入（规则 1/F1）。
		if ctx.Err() != nil {
			e.releaseHost(heldHost)
			return nil, "", latency, ctx.Err()
		}
		if e.gateBlocked() && kind == kindDownload {
			e.releaseHost(heldHost)
			return nil, "", latency, context.Canceled
		}
		if !e.eligible(t.id, gen, kind) {
			e.releaseHost(heldHost)
			return nil, "", latency, context.Canceled
		}
		if !e.tryAcquireHost(host) {
			// 取得 host 许可前先释放已持有（规则 3）。
			e.releaseHost(heldHost)
			if kind == kindDownload {
				return nil, "", latency, errHostBusy // F1：忙目标交回 worker 可取消等待/改选
			}
			// 探活忙 host：可取消等待短暂后改选（不产生请求、不 spinning）。
			if !e.notify.wait(ctx) {
				return nil, "", latency, ctx.Err()
			}
			continue
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
		if rangeSpec != "" {
			req.Header.Set("Range", rangeSpec)
		}

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
		// 非重定向：返回响应；host 许可保持持有，由调用方在 body 关闭后释放（规则 3）。
		return resp, heldHost, latency, nil
	}
	e.releaseHost(heldHost)
	return nil, "", latency, fmt.Errorf("too many redirects (> %d)", maxRedirects)
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

// setBlocked 依门禁状态维护请求上下文（F6）：转阻塞时取消在途请求 ctx，转运行态重建。
func (e *Engine) setBlocked(blocked bool) {
	e.reqMu.Lock()
	if blocked {
		if e.reqCancel != nil {
			e.reqCancel()
		}
		e.reqCtx, e.reqCancel = nil, nil
	} else if e.reqCtx == nil {
		e.reqCtx, e.reqCancel = context.WithCancel(context.Background())
	}
	e.reqMu.Unlock()
}

func (e *Engine) currentReqCtx() context.Context {
	e.reqMu.Lock()
	defer e.reqMu.Unlock()
	if e.reqCtx == nil {
		return context.Background()
	}
	return e.reqCtx
}

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
		// F11：最终文件名唯一（时间戳+随机后缀），避免同 basename 重复/并发下载互相覆盖。
		uniq := fmt.Sprintf("%s.%d", base, time.Now().UnixNano())
		return &dest{mode: mode, w: f, file: f, tempPath: f.Name(), finalPath: filepath.Join(dir, uniq)}, nil
	default:
		return nil, fmt.Errorf("unknown storage mode %q", mode)
	}
}

// finalize 成功时落盘：disk 改名，memory/discard 清除。任何失败返回错误（F11）。
func (d *dest) finalize() error {
	if d.mode == storageModeDisk && d.file != nil {
		if err := d.file.Close(); err != nil {
			return err
		}
		if err := os.Rename(d.tempPath, d.finalPath); err != nil {
			return err
		}
		return nil
	}
	return d.abort()
}

// abort 失败/取消路径：memory 清除，disk 保留 .part（D5），discard 无操作。错误返回（F11）。
func (d *dest) abort() error {
	if d.file == nil {
		return nil
	}
	cerr := d.file.Close()
	if d.mode == storageModeMemory {
		if rerr := os.Remove(d.tempPath); rerr != nil && !os.IsNotExist(rerr) {
			return rerr
		}
	}
	return cerr
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

var errRateChanged = errors.New("rate limit changed during reservation")

// waitRate 令牌桶可取消预约：等待到有界 Read 前；限速热切换或 ctx 取消时取消本次预约并按需重算（规则 8/F7）。
func (e *Engine) waitRate(ctx context.Context, n int) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r := e.limiter.ReserveN(time.Now(), n)
		if !r.OK() {
			return fmt.Errorf("rate limiter: cannot reserve %d bytes", n)
		}
		delay := r.Delay()
		if delay <= 0 {
			return nil // 已消费（无需等待）
		}
		select {
		case <-time.After(delay):
			return nil // 等待到期，预约已消费
		case <-ctx.Done():
			r.Cancel()
			return ctx.Err()
		case <-e.rateSig.current():
			r.Cancel() // F7：取消未消费预约，按新值重算
			continue
		}
	}
}

// readLoop 统一 StatsReader 循环：配额按日原子预留-结算-退还 + 令牌桶预约-读-归还 + 字节归属（规则 7/8/9/F4）。
func (e *Engine) readLoop(ctx context.Context, t *target, gen uint64, body io.Reader, w io.Writer) error {
	buf := make([]byte, bufSize)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if e.gateBlocked() {
			return context.Canceled
		}
		// F4：Read 前原子预留 min(读长, 剩余额度)；0=配额触达。
		take := e.quotaReserve(int64(len(buf)))
		if take == 0 {
			return errQuotaReached
		}
		// 令牌桶预约（规则 8：等待在有界 Read 前，≤32KiB≤burst；F7 可取消预约）。
		if err := e.waitRate(ctx, int(take)); err != nil {
			e.quotaRefund(take) // F4：取消路径退还全部预留
			return err
		}
		rn, rerr := body.Read(buf[:take])
		e.quotaSettle(take, int64(rn)) // F4：按实际读数结算，短读/错误退还
		if rn > 0 {
			e.countBytes(t, int64(rn)) // 字节归属（规则 9）
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

// errHostBusy 表示目标 host 许可被占（F1：忙目标可取消等待或改选，禁止 jitter=0 自旋）。
var errHostBusy = errors.New("host busy")

// requestKind 区分下载与探活请求的准入资格（规则 1/F1）。
type requestKind int

const (
	kindDownload requestKind = iota
	kindProbe
)

// quotaReserve 在 countMu 内按日预留可读字节（F4）：0 表示配额触达。
// 预留记入 quotaUsed 账本（含在途预留），供并发 reader 原子竞争最后额度。
func (e *Engine) quotaReserve(n int64) int64 {
	e.countMu.Lock()
	defer e.countMu.Unlock()
	now := e.clock.Now().In(time.Local)
	e.ensureDay(dayKey(now), now) // 跨日重绑预算（F5）
	limit := e.dailyLimit.Load()
	if limit <= 0 {
		return n // 不限
	}
	used := e.quotaUsed.Load()
	rem := limit - used
	if rem <= 0 {
		e.blockedFlag.Store(1) // 配额触达：立即翻转门禁
		e.notify.signal()
		return 0
	}
	take := n
	if take > rem {
		take = rem
	}
	e.quotaUsed.Add(take)
	return take
}

// quotaSettle 按实际读数结算预留：短读部分退还（F4）。take>0 才调用。
func (e *Engine) quotaSettle(take, rn int64) {
	if take <= 0 {
		return
	}
	if refund := take - rn; refund > 0 {
		e.quotaRefund(refund)
	}
}

// quotaRefund 退还未消费的预留（取消/短读/错误路径），钳制不低于 0（F4）。
func (e *Engine) quotaRefund(k int64) {
	if k <= 0 || e.dailyLimit.Load() <= 0 {
		return
	}
	for {
		cur := e.quotaUsed.Load()
		if cur <= 0 {
			return
		}
		refund := k
		if refund > cur {
			refund = cur
		}
		if e.quotaUsed.CompareAndSwap(cur, cur-refund) {
			return
		}
	}
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
	e.countMu.Unlock()

	// 标记脏桶（规则 10，F2：脏纪元随标记递增，成功刷盘后才清除）。
	e.markCountDirty(day, t.id, hk)

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
	e.setBlocked(blocked) // F6
	e.notify.signal()
}

// ---- 刷盘管线（规则 10 / F2 / F3） ----

const maxPersistRetries = 4 // 单批持久化最大重试次数（F3：永久/持续失败不得无限重试占住管理事务）

// markCountDirty 记录一次字节计数的脏条目（脏纪元递增，供成功刷盘后才清除）。
func (e *Engine) markCountDirty(day string, id int64, h int64) {
	e.dirtyMu.Lock()
	e.dirtyEpoch++
	if e.dirtyDays[day] == nil {
		e.dirtyDays[day] = make(map[int64]uint64)
	}
	e.dirtyDays[day][id] = e.dirtyEpoch
	e.dirtyGlobal[day] = e.dirtyEpoch
	e.dirtyHours[h] = e.dirtyEpoch
	e.dirtyTargets[id] = e.dirtyEpoch
	e.dirtyMu.Unlock()
}

// markTargetDirty 记录健康/计数变化的目标脏条目。
func (e *Engine) markTargetDirty(id int64) {
	e.dirtyMu.Lock()
	e.dirtyEpoch++
	e.dirtyTargets[id] = e.dirtyEpoch
	e.dirtyMu.Unlock()
}

// dirtySnap 是建批时对脏条目与其脏纪元的快照；仅当批次成功持久化后按快照清除。
type dirtySnap struct {
	days    map[string]map[int64]uint64
	glob    map[string]uint64
	hours   map[int64]uint64
	targets map[int64]uint64
}

// snapshotDirty 在 dirtyMu 下拷贝当前全部脏条目与纪元（不清理）。
func (e *Engine) snapshotDirty() *dirtySnap {
	e.dirtyMu.Lock()
	defer e.dirtyMu.Unlock()
	s := &dirtySnap{
		days:    make(map[string]map[int64]uint64),
		glob:    make(map[string]uint64),
		hours:   make(map[int64]uint64),
		targets: make(map[int64]uint64),
	}
	for d, ids := range e.dirtyDays {
		cp := make(map[int64]uint64, len(ids))
		for id, ep := range ids {
			cp[id] = ep
		}
		s.days[d] = cp
	}
	for d, ep := range e.dirtyGlobal {
		s.glob[d] = ep
	}
	for h, ep := range e.dirtyHours {
		s.hours[h] = ep
	}
	for id, ep := range e.dirtyTargets {
		s.targets[id] = ep
	}
	return s
}

// clearDirty 在 dirtyMu 下按快照清除未被重新标记的脏条目（成功确认后才调用，F2）。
func (e *Engine) clearDirty(s *dirtySnap) {
	e.dirtyMu.Lock()
	defer e.dirtyMu.Unlock()
	for d, ids := range s.days {
		m := e.dirtyDays[d]
		if m == nil {
			continue
		}
		for id, ep := range ids {
			if m[id] == ep {
				delete(m, id)
			}
		}
		if len(m) == 0 {
			delete(e.dirtyDays, d)
		}
	}
	for d, ep := range s.glob {
		if e.dirtyGlobal[d] == ep {
			delete(e.dirtyGlobal, d)
		}
	}
	for h, ep := range s.hours {
		if e.dirtyHours[h] == ep {
			delete(e.dirtyHours, h)
		}
	}
	for id, ep := range s.targets {
		if e.dirtyTargets[id] == ep {
			delete(e.dirtyTargets, id)
		}
	}
}

// isPermanentPersistErr 判断是否为不应无限重试的持久化错误（F2：errStaleSeq 属永久）。
func isPermanentPersistErr(err error) bool {
	return errors.Is(err, errStaleSeq)
}

// buildBatch 由唯一持久化协程串行调用：快照脏条目（不清理）→ countMu 下读绝对值并分配 Seq
// → stateMu 下读健康。返回批次与脏快照（供成功后才清除）。
func (e *Engine) buildBatch() (*CounterBatch, *dirtySnap) {
	snap := e.snapshotDirty()
	if len(snap.days) == 0 && len(snap.glob) == 0 && len(snap.hours) == 0 && len(snap.targets) == 0 {
		return nil, snap
	}

	batch := &CounterBatch{
		TargetDays:   make(map[string]map[int64]int64),
		GlobalDays:   make(map[string]int64),
		HourBuckets:  make(map[int64]int64),
		TargetTotals: make(map[int64]int64),
		TargetHealth: make(map[int64]TargetHealthPersist),
	}

	// countMu 下读计数绝对值 + 分配 Seq（仅持久化协程调用，Seq 串行单调 F2）。
	e.countMu.Lock()
	e.seq++
	batch.Seq = e.seq
	for day, ids := range snap.days {
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
	for day := range snap.glob {
		if g := e.globalDays[day]; g != nil {
			if v := g.Load(); v > 0 {
				batch.GlobalDays[day] = v
			}
		}
	}
	for h := range snap.hours {
		if b := e.hourBuckets[h]; b != nil {
			if v := b.Load(); v > 0 {
				batch.HourBuckets[h] = v
			}
		}
	}
	e.countMu.Unlock()

	// stateMu 下读目标 total 与健康快照。
	e.stateMu.RLock()
	for id := range snap.targets {
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

	return batch, snap
}

// persistWithRetry 持久化单个批次：成功才清除脏快照；永久错误或有界重试耗尽则返回错误。
// 重试等待经 persistCtx 可取消（独立于 adminMu，F3）。
func (e *Engine) persistWithRetry(batch *CounterBatch, snap *dirtySnap, done chan error) {
	var lastErr error
	for attempt := 0; attempt < maxPersistRetries; attempt++ {
		lastErr = e.persistFn(*batch)
		if lastErr == nil {
			e.clearDirty(snap)
			e.replyFlush(done, nil)
			return
		}
		if isPermanentPersistErr(lastErr) {
			e.setRunErr(lastErr)
			e.replyFlush(done, lastErr)
			return
		}
		select {
		case <-e.persistCtx.Done():
			e.setRunErr(lastErr)
			e.replyFlush(done, lastErr)
			return
		case <-time.After(flushRetryDelay):
			// 重试同批（规则 10）。
		}
	}
	e.setRunErr(lastErr)
	e.replyFlush(done, lastErr)
}

func (e *Engine) replyFlush(done chan error, err error) {
	if done != nil {
		done <- err
	}
}

// persistWorker 是唯一持久化协程：串行建批/分配 Seq/持久化。存活至 Stop 关闭 persistCh。
func (e *Engine) persistWorker() {
	defer e.persistWG.Done()
	t := time.NewTicker(flushInterval)
	defer t.Stop()
	for {
		select {
		case req := <-e.persistCh:
			e.handleFlush(req)
		case <-t.C:
			e.handleFlush(flushReq{}) // 异步周期刷盘
		case <-e.persistCtx.Done():
			// 关闭阶段：只处理 persistCh 中剩余同步请求，直到 Stop 关闭 persistCh 后退出（F3）。
			for req := range e.persistCh {
				e.handleFlush(req)
			}
			return
		}
	}
}

// handleFlush 建批并持久化一个请求。
func (e *Engine) handleFlush(req flushReq) {
	batch, snap := e.buildBatch()
	if batch == nil {
		e.replyFlush(req.done, nil)
		return
	}
	e.persistWithRetry(batch, snap, req.done)
}

// flushTicker 每 10s 触发一次异步周期刷盘（fire-and-forget，规则 10）。
func (e *Engine) flushTicker() {
	defer e.wg.Done()
	t := time.NewTicker(flushInterval)
	defer t.Stop()
	for {
		select {
		case <-e.lifeCtx.Done():
			return
		case <-t.C:
			e.flushAsync()
		}
	}
}

// flushAsync 提交一个异步周期刷盘请求（不等待）。
func (e *Engine) flushAsync() {
	select {
	case e.persistCh <- flushReq{}:
	case <-e.persistCtx.Done():
	}
}

// flushNow 提交同步刷盘请求并等待结果（规则 10/13/15/18）。可经 persistCtx 取消（F3）。
func (e *Engine) flushNow() error {
	done := make(chan error, 1)
	select {
	case e.persistCh <- flushReq{done: done}:
	case <-e.persistCtx.Done():
		return context.Canceled
	}
	select {
	case err := <-done:
		if err != nil {
			e.setRunErr(err)
		}
		return err
	case <-e.persistCtx.Done():
		return context.Canceled
	}
}

// flushNowFinal 在关闭阶段提交最终刷盘并阻塞等待结果（不经 persistCtx 取消；F3）。
func (e *Engine) flushNowFinal() error {
	done := make(chan error, 1)
	e.persistCh <- flushReq{done: done}
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

	e.markTargetDirty(id)
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

	e.markTargetDirty(t.id)
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

// setChanging 设置目标的 CRUD 进行中标记（拒准入，F8）。
func (e *Engine) setChanging(id int64, v bool) {
	e.stateMu.Lock()
	e.changing[id] = v
	e.stateMu.Unlock()
	e.notify.signal()
}

// cancelProbe 取消指定目标的在途探活（F8）。取消后探活以 ctx.Canceled 退出且不写健康。
func (e *Engine) cancelProbe(id int64) {
	e.probeMu.Lock()
	c := e.probeCancel[id]
	delete(e.probeCancel, id)
	e.probeMu.Unlock()
	if c != nil {
		c()
	}
	// 有界等待在途探活退出。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		e.probeMu.Lock()
		_, inflight := e.probeInFlight[id]
		e.probeMu.Unlock()
		if !inflight {
			return
		}
		time.Sleep(5 * time.Millisecond)
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
// 经统一执行器 doRequest：逐跳重定向、逐跳 host 许可、逐跳重查准入（F1）。
func (e *Engine) probeOne(id int64) {
	e.stateMu.RLock()
	t := e.targets[id]
	if t == nil {
		e.stateMu.RUnlock()
		return
	}
	gen := t.gen
	e.stateMu.RUnlock()

	pctx, cancel := context.WithTimeout(e.lifeCtx, probeTimeout)
	defer cancel()
	e.probeMu.Lock()
	e.probeCancel[id] = cancel
	e.probeMu.Unlock()
	defer func() {
		e.probeMu.Lock()
		delete(e.probeCancel, id)
		e.probeMu.Unlock()
	}()

	resp, heldHost, latency, err := e.doRequest(pctx, t, gen, kindProbe, "bytes=0-1023")
	if heldHost != "" {
		defer e.releaseHost(heldHost)
	}
	if err != nil {
		// F9：先判引擎/CRUD 取消原因（含冷却准入拦截、host 忙等待取消），再分类远端故障。
		if e.probeNotRemoteFault(err) {
			return
		}
		e.onProbeTransportError(t, gen, err, latency)
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
		if e.probeNotRemoteFault(err) {
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

// probeNotRemoteFault 判断探活错误是否因引擎关闭/CRUD 取消引起（非远端故障，规则 13/F9）。
// 引擎/生命周期取消（lifeCtx）或 context.Canceled 不算远端故障；DeadlineExceeded（10s 自身超时）与
// 传输 reset 等归类为远端故障 → Dead。
func (e *Engine) probeNotRemoteFault(err error) bool {
	if e.lifeCtx.Err() != nil {
		return true // 引擎关闭
	}
	if errors.Is(err, context.Canceled) {
		return true // 引擎/CRUD 取消
	}
	return false
}

// probeRead 最多读 probeRange 字节，受令牌桶限速并计入统计。
func (e *Engine) probeRead(ctx context.Context, t *target, body io.Reader) error {
	buf := make([]byte, probeRange)
	// F4：探活同样按日预留-结算-退还。
	take := e.quotaReserve(int64(len(buf)))
	if take == 0 {
		return nil // 配额触达，本次探活不读字节（不影响健康）
	}
	if err := e.waitRate(ctx, int(take)); err != nil {
		e.quotaRefund(take)
		return err
	}
	n, rerr := body.Read(buf[:take])
	e.quotaSettle(take, int64(n))
	if n > 0 {
		e.countBytes(t, int64(n))
	}
	if rerr != nil {
		if rerr == io.EOF {
			return nil // F9：短响应/空响应读到 EOF 即成功，非远端故障
		}
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
	e.ensureDay(dayKey(now), now)
	day := e.currentDay
	ledger := e.quotaUsed.Load() // 预留账本（与门禁一致），判断配额触达
	used := int64(0)
	if g := e.globalDays[day]; g != nil {
		used = g.Load() // 实际消费（不含在途预留），F4/F5：不报昨日桶
	}
	e.countMu.Unlock()
	limit := e.dailyLimit.Load()
	quotaReached := limit > 0 && ledger >= limit
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
	starts := engineHourStarts(now)
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
		return wrapValidation(err)
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
	e.limiter.SetLimit(r) // 规则 8：SetLimit 重算
	e.rateSig.signal()    // F7：广播限速变化，唤醒等待者按新值重算
}

func (e *Engine) SetMaxConnections(n int) error {
	if err := ValidateMaxConnections(n); err != nil {
		return wrapValidation(err)
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
		return wrapValidation(err)
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
		return wrapValidation(err)
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
		return wrapValidation(err)
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
		return TargetRecord{}, wrapValidation(err)
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

// PatchTarget 局部更新目标（规则 15 / F8：changing→拒准入→取消全部下载+探活并等待→清算→DB→发布）。
func (e *Engine) PatchTarget(id int64, p TargetPatch) (TargetRecord, error) {
	e.adminMu.Lock()
	defer e.adminMu.Unlock()
	if e.stopped.Load() {
		return TargetRecord{}, errors.New("engine stopped")
	}
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
		return TargetRecord{}, wrapValidation(err)
	}
	urlChanged := p.URL != nil && *p.URL != cand.oldURL
	disabling := p.Enabled != nil && !*p.Enabled
	needsChanging := urlChanged || disabling

	// 快照旧元数据以便 DB 失败回滚（F8）。
	var oldSnap target
	if t := e.getTarget(id); t != nil {
		oldSnap = *t
	}

	// F8：仅影响在途工作的变更才需要 changing→取消→清算。
	if needsChanging {
		e.setChanging(id, true)  // 拒准入（下载与探活）
		e.cancelTarget(id)       // 取消下载并等待结算
		e.cancelProbe(id)        // 取消在途探活

		// URL 变更：清空旧健康（改 URL 旧健康记录不得落库，F8）。
		if urlChanged {
			e.stateMu.Lock()
			if t := e.targets[id]; t != nil {
				t.health = healthUnknown
				t.cooldownUntilMS = 0
				t.consecutiveErrors = 0
				t.lastStatusCode = 0
				t.latencyMS = 0
				t.errorMsg = ""
			}
			e.stateMu.Unlock()
			e.markTargetDirty(id)
		}

		// 清算提交（settle）：已发生流量与（清空后的）健康刷盘。
		if err := e.flushNow(); err != nil {
			e.restoreTarget(id, &oldSnap)
			e.setChanging(id, false)
			return TargetRecord{}, err
		}
	}

	rec, err := e.store.PatchTarget(id, p)
	if err != nil {
		// DB 失败：恢复旧目标准入与元数据（已发生流量保留）。
		if needsChanging {
			e.restoreTarget(id, &oldSnap)
			e.setChanging(id, false)
		}
		return TargetRecord{}, err
	}

	e.stateMu.Lock()
	if t := e.targets[id]; t != nil {
		t.name = rec.Name
		t.url = rec.URL
		t.weight = rec.Weight
		t.enabled = rec.Enabled
		if urlChanged {
			t.gen++
			t.host = normalizeHost(rec.URL)
		}
	}
	if needsChanging {
		e.changing[id] = false
	}
	e.stateMu.Unlock()
	e.notify.signal()

	if urlChanged {
		e.enqueueProbe(id)
	}
	return rec, nil
}

// restoreTarget 恢复目标元数据与健康（DB 失败回滚，F8）。changing 由调用方清除。
func (e *Engine) restoreTarget(id int64, snap *target) {
	e.stateMu.Lock()
	if t := e.targets[id]; t != nil && snap != nil {
		t.name = snap.name
		t.url = snap.url
		t.weight = snap.weight
		t.enabled = snap.enabled
		t.gen = snap.gen
		t.host = snap.host
		t.health = snap.health
		t.cooldownUntilMS = snap.cooldownUntilMS
		t.consecutiveErrors = snap.consecutiveErrors
		t.lastStatusCode = snap.lastStatusCode
		t.latencyMS = snap.latencyMS
		t.errorMsg = snap.errorMsg
	}
	e.stateMu.Unlock()
	e.notify.signal()
}

// enqueueProbe 把目标加入探活队列（同轮合并）。
func (e *Engine) enqueueProbe(id int64) {
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

// DeleteTarget 软删目标（规则 15 / F8：changing→拒准入→取消下载+探活→清算→DB 软删→移除）。
func (e *Engine) DeleteTarget(id int64) error {
	e.adminMu.Lock()
	defer e.adminMu.Unlock()
	if e.stopped.Load() {
		return errors.New("engine stopped")
	}
	e.setChanging(id, true)
	e.cancelTarget(id) // 取消下载并等待结算
	e.cancelProbe(id)  // 取消在途探活
	if err := e.flushNow(); err != nil { // 清算：历史桶保留
		e.setChanging(id, false)
		return err
	}
	if err := e.store.SoftDeleteTarget(id); err != nil {
		e.setChanging(id, false)
		return err
	}
	e.stateMu.Lock()
	if t := e.targets[id]; t != nil {
		t.deleted = true
	}
	delete(e.changing, id)
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
