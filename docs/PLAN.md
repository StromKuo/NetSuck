# NetSuck 开发编排计划

> 协调者：Claude Code（常驻）。本文件是所有 worker 的**唯一契约来源**，与 `docs/SPEC.md`（需求规格）配套。
> 任何与 SPEC 冲突之处以 SPEC 为准，除非 PLAN 中标注为"协调者决策"。
> **版本：v2** —— 已按 T1 独立审查（Codex，REJECT：4 BLOCKER + 11 MAJOR + 1 MINOR）全面修订；D1-D6 决策全部获同意。

## 0. 角色分工与额度策略

| 角色 | Agent / 模型 | 职责 | 额度策略 |
|------|-------------|------|---------|
| 协调者 | Claude Code | 任务分解、契约维护、git 集成提交、集成验证、最终验收 | 适度使用 |
| 实现工人 A | opencode (DeepSeek) | 全部核心 Go 代码实现 | 敞开用 |
| 实现工人 B | antigravity / agy (Gemini) | Web 前端、main.go、基础设施文件 | 随便用 |
| 关键审查 | codex (gpt-6.1-sol) | 规划审查（T1/T1b）、engine.go 关键实现独立审查（T8） | 只用于刀刃 |

**铁律**：每个功能块实现完成后，必须由**非实现 Agent** 独立审查通过才算完成；审查者只报告问题、不改代码；修复一律路由回实现者；通过后由协调者统一提交 git。

## 1. 任务 DAG 与状态

| ID | 任务 | 实现 | 独立审查 | 依赖 | 状态 |
|----|------|------|---------|------|------|
| T0 | 引导：SPEC/PLAN/.gitignore/初始提交 | Claude | — | — | ✅ |
| T1 | 规划与契约审查 | — | Codex | T0 | ✅ REJECT→已修订 |
| T1b | 修订版 PLAN 聚焦复审（确认 BLOCKER 已修、无新矛盾） | — | Codex | 契约 v2 | ⬜ |
| T2 | go.mod + config.go + config.example.yaml（已按 v2 增量修正 steering） | opencode | antigravity | T1b 通过后验收 | 🔄 |
| T3 | web/index.html（已按 v2 增量修正 steering） | antigravity | opencode | T1b 通过后验收 | 🔄 |
| T4 | 审查 T2（config 块） | — | antigravity | T2, T3 完工 | ⬜ |
| T5 | db.go（Store，签名见 §4 已冻结） | opencode | antigravity | T2 完工（提交以 T4 通过为准） | ⬜ |
| T6 | 审查 T5（db 块）+ 审查 T3（web 块） | — | antigravity / opencode | T5, T3 | ⬜ |
| T7 | engine.go（按 §5 全部规则 + §8 并发测试） | opencode | **Codex** | T6 通过 | ⬜ |
| T8 | 审查 T7（关键实现独立审查，含 -race 测试核对） | — | Codex | T7 | ⬜ |
| T9 | main.go（API 路由 + 组装 + embed + 关停序列） | antigravity | opencode | T8 通过 | ⬜ |
| T10 | 审查 T9 + 集成冒烟测试（REST 全覆盖清单 §8） | — | opencode | T9 | ⬜ |
| T11 | Dockerfile / docker-compose / CI / README | antigravity | opencode | T10 通过 | ⬜ |
| T12 | 审查 T11 + 最终构建验证（含 -race、双架构交叉编译） | — | opencode | T11 | ⬜ |
| T13 | 最终验收报告 | Claude | — | T12 | ⬜ |

## 2. 全局工程约定

- **module 路径**：`github.com/StromKuo/NetSuck`（D1，T1 同意）；**根目录全部 .go 文件统一 `package main`**，通过文件名划分模块（config.go / db.go / engine.go / main.go），类型互相直接引用，杜绝跨包占位符。
- **Go 版本**：go.mod 声明 `go 1.23`；**T2 必须固定直接依赖版本并生成 go.sum**，全部依赖满足 Go 1.23 且 `GOTOOLCHAIN=local go build` 可构建；若后续决定上调最低 Go 版本，必须同步修改 Docker 构建镜像并重审 SPEC 约束。
- **依赖白名单**：直接依赖仅 `gopkg.in/yaml.v3`、`golang.org/x/time/rate`、`modernc.org/sqlite`；**允许其必要间接依赖**。前端仅 Tailwind CSS (CDN) + Chart.js (CDN) + 原生 JS。
- **静态编译**：`CGO_ENABLED=0`；禁止 cgo。
- **并发纪律**：跨协程共享可变状态必须 `sync.RWMutex`/`sync/atomic` 保护；`go vet` 与 `go test -race`（T7/T12）必须通过。
- **路径约定**：示例配置 `/app/config.example.yaml`；配置文件默认 `/data/config.yaml`，以 `-config` 实参为准；首次初始化目标为 **-config 解析后的路径**（缺失时创建父目录再复制示例，绝不覆盖已有文件）。
- **git 提交**：每个 Task 通过审查后由协调者提交；不带无关变更。

## 3. 配置契约（config.go，必须精确实现）

```go
// 全部位于 package main
type ScheduleConfig struct {
    StartHour int `yaml:"start_hour" json:"start_hour"` // 0-23
    EndHour   int `yaml:"end_hour"   json:"end_hour"`   // 0-24（24=当日午夜）
}
type JitterConfig struct {
    MinSeconds int `yaml:"min_seconds" json:"min_seconds"` // 默认 5
    MaxSeconds int `yaml:"max_seconds" json:"max_seconds"` // 默认 30
}
type TargetConfig struct {
    Name    string `yaml:"name" json:"name"`
    URL     string `yaml:"url"  json:"url"`
    Weight  int    `yaml:"weight" json:"weight"`   // 默认 10，>=1
    Enabled bool   `yaml:"enabled" json:"enabled"` // 默认 true
}
type Config struct {
    ListenAddr     string         `yaml:"listen_addr"`     // 默认 ":8080"
    StorageMode    string         `yaml:"storage_mode"`    // discard|memory|disk，默认 discard
    MemoryDir      string         `yaml:"memory_dir"`      // 默认 "/tmp/dl_cache"
    DiskDir        string         `yaml:"disk_dir"`        // 默认 "/data/downloads"
    MaxRateMbps    int            `yaml:"max_rate_mbps"`   // 默认 0=不限，>=0
    MaxConnections int            `yaml:"max_connections"` // 默认 2，1-10
    DailyLimitGB   float64        `yaml:"daily_limit_gb"`  // 默认 0=不限量；有限非负数
    Schedule       ScheduleConfig `yaml:"schedule"`        // 默认 {0,24}=全天
    Jitter         JitterConfig   `yaml:"jitter_seconds"`
    CooldownHours  int            `yaml:"cooldown_hours"`  // 默认 6，>0
    UserAgent      string         `yaml:"user_agent"`      // 默认桌面 Chrome UA
    Targets        []TargetConfig `yaml:"targets"`
}

func LoadConfig(path string) (*Config, error)
func (c *Config) Save(path string) error   // 原子写：临时文件 + os.Rename
func DefaultConfig() *Config
// 共享验证（config 与 API 复用，必须导出）：
func ValidateStorageMode(v string) error
func ValidateMaxRateMbps(v int) error      // >=0
func ValidateMaxConnections(v int) error   // 1-10
func ValidateDailyLimitGB(v float64) error // 有限、非负
func ValidateSchedule(s ScheduleConfig) error
func ValidateTargetInput(name, url string, weight int) error // 详见 §6 URL 规则
```

**presence 语义（必须）**：YAML **缺字段**才填默认值；显式 `enabled: false`、`max_rate_mbps: 0`、`daily_limit_gb: 0`、`weight: 1`、`jitter min=0` 必须原样保留。实现方式：解码到带指针字段的 raw 结构再装配，或 `yaml.Node` presence 检测。

**schedule 语义（定案）**：活跃窗口 `[start_hour, end_hour)`；`end_hour=24` 仅允许作为 end，表示到午夜；`start<end` 时 `start<=本地小时<end`；`start>end` 跨夜（`小时>=start 或 小时<end`）；`start==end` 非法（拒绝）。按本地日历钟点判断，不用固定 24h 计时。校验：`0<=start<=23`、`0<=end<=24`。

**config.example.yaml**：≥12 个不同厂商大体积公开直链；每条验证命令（**必须**）：
`curl -sL -o /dev/null -w '%{http_code} %{size_download}' -r 0-1023 -A '<桌面Chrome UA>' --max-time 20 --max-filesize 2000000 '<URL>'`
跟随重定向、限时限量；记录最终状态码与是否支持 Range（206 或 size≈1024）；因主动截断退出（curl exit 23/63 等）且已读到数据**不算死链**。只保留可用链接。

## 4. 数据库契约（db.go —— `Store`，签名已冻结，T5 前不得变更）

```go
type Store struct { /* 内部 *sql.DB + 写互斥 */ }
func OpenStore(path string) (*Store, error)  // DSN: file:<path>?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)
func (s *Store) InitSchema() error
func (s *Store) EnsureSeeded(cfg *Config) error   // 见"播种规则"
func (s *Store) ListTargetRecords() ([]TargetRecord, error) // 不含软删
func (s *Store) CreateTarget(p TargetPatch) (TargetRecord, error) // url 重复 → ErrDuplicateURL
func (s *Store) PatchTarget(id int64, p TargetPatch) (TargetRecord, error) // 不存在 → ErrNotFound
func (s *Store) SoftDeleteTarget(id int64) error               // 不存在 → ErrNotFound
func (s *Store) PersistBatch(b CounterBatch) error              // 单事务、幂等
func (s *Store) History24h(now time.Time) ([]HistoryPoint, error) // 已刷盘的最近24个本地小时桶，补零，最旧在前
func (s *Store) Close() error
var ErrDuplicateURL = errors.New("duplicate url")
var ErrNotFound     = errors.New("not found")

// 共享 DTO（package main，engine 与 main 直接使用）
type TargetRecord struct {
    ID    int64  `json:"id"`
    Name  string `json:"name"`
    URL   string `json:"url"`
    Weight int   `json:"weight"`
    Enabled bool `json:"enabled"`
    // 健康与风控持久化（重启恢复）
    Health            string `json:"health"`              // Healthy|RateLimited|Dead|Unknown
    CooldownUntilMS   int64  `json:"cooldown_until_ms"`   // Unix 毫秒，0=无
    LastStatusCode    int    `json:"last_status_code"`
    LatencyMS         int64  `json:"latency_ms"`
    ErrorMsg          string `json:"error_msg"`
    ConsecutiveErrors int    `json:"consecutive_errors"`
    TotalBytes        int64  `json:"total_bytes"`          // 历史累计（不含今日）
    DeletedAt         *time.Time `json:"-"`                // 软删标记
}
type TargetPatch struct { // 全部指针字段；nil=不改（PUT 语义）
    Name    *string `json:"name"`
    URL     *string `json:"url"`
    Weight  *int    `json:"weight"`
    Enabled *bool   `json:"enabled"`
}
type CounterBatch struct { // 不可变快照；含所有脏桶与各 target 累计绝对值
    Day        string                    // 本地 'YYYY-MM-DD'
    TargetDay  map[int64]int64           // target_id -> 当日累计绝对值
    TargetTotals map[int64]int64         // target_id -> 历史累计绝对值（含当日）
    HourBuckets map[int64]int64          // hour_unix -> 全局该小时累计绝对值
    TargetHealth map[int64]TargetHealthPersist // 见 §5 探活持久化
    Seq        uint64                    // 生成顺序号
}
type TargetHealthPersist struct {
    Health string; CooldownUntilMS int64; LastStatusCode int; LatencyMS int64
    ErrorMsg string; ConsecutiveErrors int
}
type HistoryPoint struct {
    HourUnix int64 `json:"-"`          // 排序/叠加用
    Hour     string `json:"hour"`      // RFC3339 带本地偏移，如 2026-11-14T15:00:00+08:00
    Bytes    int64 `json:"bytes"`
}
```

表结构：
```sql
CREATE TABLE IF NOT EXISTS targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL, url TEXT NOT NULL UNIQUE,
    weight INTEGER NOT NULL DEFAULT 10,
    enabled INTEGER NOT NULL DEFAULT 1,
    health TEXT NOT NULL DEFAULT 'Unknown',
    cooldown_until_ms INTEGER NOT NULL DEFAULT 0,
    last_status_code INTEGER NOT NULL DEFAULT 0,
    latency_ms INTEGER NOT NULL DEFAULT 0,
    error_msg TEXT NOT NULL DEFAULT '',
    consecutive_errors INTEGER NOT NULL DEFAULT 0,
    total_bytes INTEGER NOT NULL DEFAULT 0,   -- 历史累计（不含今日当日桶）
    deleted_at INTEGER                        -- Unix 秒，NULL=未删
);
CREATE TABLE IF NOT EXISTS daily_stats (day TEXT NOT NULL, target_id INTEGER NOT NULL, bytes INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(day,target_id));
CREATE TABLE IF NOT EXISTS history (hour_ts INTEGER PRIMARY KEY, bytes INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
```

**播种规则（定案）**：`EnsureSeeded` 在**单事务**内：`metadata` 无 `targets_seeded` 键时才导入 cfg.Targets（含空列表也算已播种）并写入标记；此后 DB 是 URL 列表唯一权威，**永不**依据 config.yaml 恢复/新增/复活；删除至空不触发重播。运行参数（限速/并发等）以 **-config 所指配置文件**为权威。

**持久化语义**：`PersistBatch` 单事务 upsert 绝对值（daily_stats 按 (day,target)、history 按 hour、targets.total_bytes 与健康列按 id）；批按 `Seq` 顺序处理，**禁止旧批覆盖新批**（Seq 回退直接拒绝）；今日 `total_bytes` = DB 累计 + 当日桶（查询时相加，不双写）。已删除 target 的历史桶保留。

## 5. 引擎契约（engine.go）

```go
type Engine struct { /* ... */ }
func NewEngine(cfg *Config, store *Store, httpClient *http.Client) (*Engine, error)
func (e *Engine) Start(ctx context.Context) error   // 启动全部后台循环；ctx 为引擎生命周期
func (e *Engine) Stop() error                        // 见"关停序列"；内部完成最终刷盘
func (e *Engine) SetMaxRate(mbps int) error          // 0 => rate.Inf
func (e *Engine) SetMaxConnections(n int) error
func (e *Engine) SetStorageMode(mode string) error
func (e *Engine) SetDailyLimit(gb float64) error
func (e *Engine) Pause() error / Resume() error      // Resume 仅撤销手动暂停，不越过配额与时段门禁
func (e *Engine) Snapshot() EngineSnapshot           // 深拷贝值；单次快照内统一 now
func (e *Engine) CheckAllNow(ctx context.Context) error // ctx 必须是引擎生命周期派生，而非 handler 请求级
func (e *Engine) ApplyTargetChanges() error          // CRUD 后由 main 调用；见"重载规则"
func (e *Engine) CurrentHourLiveBytes() int64        // 供 /api/history 叠加当前小时未刷增量
```

**EngineSnapshot / TargetSnapshot**（main 直接序列化为 /api/status 与 /api/targets）：
```go
type TargetSnapshot struct {
    ID int64 `json:"id"`; Name string `json:"name"`; URL string `json:"url"`
    Weight int `json:"weight"`; Enabled bool `json:"enabled"`
    Health string `json:"health"`                       // "Healthy"|"RateLimited"|"Dead"|"Unknown"
    LatencyMS int64 `json:"latency_ms"`                 // 无检测=0
    CooldownRemainingSeconds int64 `json:"cooldown_remaining_seconds"` // max(0,ceil((cooldown_until-now)/1s))
    TodayBytes int64 `json:"today_bytes"`; TotalBytes int64 `json:"total_bytes"`
    SpeedMbps float64 `json:"speed_mbps"`; SpeedMBPerSec float64 `json:"speed_mb_per_sec"`
    ErrorMsg string `json:"error_msg"`
}
type EngineSnapshot struct {
    Status string `json:"status"` // running|paused|sleeping_schedule|quota_reached
    StorageMode string `json:"storage_mode"`; MaxRateMbps int `json:"max_rate_mbps"`
    MaxConnections int `json:"max_connections"`; ActiveConnections int `json:"active_connections"`
    SpeedMbps float64 `json:"speed_mbps"`; SpeedMBPerSec float64 `json:"speed_mb_per_sec"`
    TodayBytes int64 `json:"today_bytes"`; DailyLimitGB float64 `json:"daily_limit_gb"`
    Schedule ScheduleConfig `json:"schedule"`
    Targets []TargetSnapshot `json:"-"` // /api/targets 单独序列化
    SnapshotAt time.Time `json:"-"`
}
```

规则（每条必须落实，编号供审查对照）：
1. **统一准入**：下载、自动探活、手动探活统一经过目标/host 请求准入；**开始每个请求前重新检查**目标 tombstone、generation、`now<cooldown_until`（冷却中任何入口禁止发请求）、enabled、Dead 状态。
2. **host 互斥**：host key = 规范化小写 hostname（忽略端口、忽略 scheme）；同域名不同 URL 共享同一把锁；非阻塞 try-acquire，占用改选其他目标。
3. **重定向**：最多 10 跳；每跳先 `Close` 前一响应、释放旧 host 许可，取得**新 host** 许可后再发下一跳；全程保留原目标互斥与计数归属；任何取消/失败路径必须释放全部许可。
4. **HTTP/2 防旁路**：不依赖 `MaxConnsPerHost=1` 单独保证单请求——准入由 §5.2 应用层锁保证；Transport 仍按 SPEC 配置（MaxIdleConns:10, MaxConnsPerHost:1, IdleConnTimeout:90s），并 `ForceAttemptHTTP2=false` 禁用 HTTP/2 多路复用旁路。
5. **动态连接池**：单一池管理器串行处理目标连接数变化，以"实际存活 + 待退出"集合对账，杜绝 2→1→2 重复扩容；**每次下载请求获取全局下载许可**，活跃计数在请求开始前 +1、`Body.Close` 后 -1（jitter 休眠不计活跃）；减容时协作取消多余下载、等待许可回收后才返回；上限收紧立即对**新请求**生效。
6. **暂停/时段/配额门禁**：`paused`、非活跃时段、达到配额 → 取消所有在途下载并阻止新请求；`Resume` 不越过配额与时段门禁；jitter、host 等待、无可选目标等待全部可取消/唤醒，**不忙循环**（用 time.Timer + ctx 或条件变量）。
7. **配额预留结算**：配额有限时，Read 前向协调器预留 `min(读长, 剩余额度)`，限制本次读长，按实际读数结算、归还未用预留；只约束应用层读取量（不承诺消除内核缓冲已收字节）。
8. **令牌桶**：全局共享一个 limiter；**令牌等待置于有界 Read 之前**（预约-读-归还模型）；每次读取固定 ≤32KiB 且 ≤ burst（burst=64KiB）；`SetLimit` 唤醒等待者按新值重算、取消未消费旧预约；无限速↔有限速切换同样适用；等待出错不得漏记已从网卡读到的 n 字节（先计数后处理等待错误）；MB/s = bytes/s ÷ 1e6，Mbps = bytes/s × 8 ÷ 1e6（D3）。
9. **字节归属**：每次 Read 的 n>0 立即按**读取完成时刻**的本地 day 与小时桶归属；计数更新与日/小时桶切换由**同一把计数协调锁**线性化（桶内 atomic 累加）；永不在午夜对计数器 Store(0)（新日=新桶键，天然隔离）。
10. **刷盘管线**：唯一持久化协程每 10 秒接收不可变 `CounterBatch`（含所有脏桶与 target 累计绝对值），单事务提交；失败重试**同一批**，成功前不丢脏桶；按 Seq 顺序处理；`Stop()` 前 flush 最终批覆盖退出前全部 Read。
11. **跨日切换**：配额协程在本地 00:00 触发：①最终 flush 旧日批 → ②持计数协调锁翻日（新日桶键生效、解除 quota_reached）→ ③后续计数入新日。字节与日期标签同锁，绝不跨日记错。
12. **今日全局配额基数**：全局 today 从**独立全局日桶**维护与恢复（`metadata['global_day:<date>']` 或独立表），不依赖现存 targets 求和；删除节点不减少全局累计。
13. **探活生命周期**：启动**立即**探活全部 Unknown；此后每 15 分钟 + 手动触发；同轮未完成时新触发**合并**；并发受控；每请求有限超时（10s）；**最多读取 1024 字节**（服务器忽略 Range 返回 200 时也读 1024 即关，不无限 drain）；探活读到的字节**计入**统计与限速（与"每个读取字节均记录"一致）；冷却到期的 RateLimited 进入待探活队列，**探活成功后**才恢复下载资格。
14. **健康状态机**：下载或探活收到 403/429 → 立即 `RateLimited` + `cooldown_until = now + cooldown_hours`，**立即持久化**（重启仍生效），并取消该目标全部在途活动；下载 404/超时/连接重置/5xx → `consecutive_errors+1`，连续 3 次 → `Dead`；下载成功或探活 200/206 → 重置 consecutive_errors；探活 404/超时/重置 → 立即 `Dead`；**旧探活成功不得覆盖新冷却**（写回健康状态前比对 generation 与已有 cooldown_until）；健康/冷却/错误信息全部持久化（TargetRecord 列）。
15. **冷却恢复**：探活 200/206 时——`Dead` → `Healthy`；`RateLimited` 且 `now>=cooldown_until` → 清冷却恢复 `Healthy`；`RateLimited` 未到期 → 保持冷却仅更新延迟与 error_msg；任何入口不得提前解除未到期冷却。
16. **实时速率采样**：唯一采样协程每 500ms 从**永不清零的累计字节**读取样本；按稳定 target id 维护环形缓冲；速率 = 实际时间差内的 delta ÷ 实际跨度（窗口 ≤3s）；启动不足 3s 用实际跨度、少于 2 个样本返回 0；无新增字节满 3s 返回 0；环更新与速率发布用 mutex/atomic pointer；Snapshot 深拷贝不暴露内部结构；全局速率从独立全局累计采样；删除节点不减少全局累计。
17. **重载与对象身份（CRUD）**：计数对象以**稳定 id**为身份；`ApplyTargetChanges` 只更新元数据（name/url/weight/enabled），**不重置**计数与采样环；禁用/删除/改 URL：先置 tombstone/准入拒绝 → 取消该目标在途请求 → 等待结算 → 提交最终计数批 → 更新 DB；删除 = 软删（deleted_at），列表与调度过滤、历史桶保留；改 URL：递增 generation、保留 id 与历史统计、清空旧健康状态并立即探活，旧 generation 探活结果禁止写回；改名/改权重不重建计数器。
18. **存储模式**：每次请求**开始时捕获**存储策略快照，热修改仅影响新请求；三种模式统一走 StatsReader + 显式 32KiB 固定循环读写（不用 io.CopyBuffer 端点优化路径）；memory：`os.CreateTemp(memory_dir)`，成功/错误/取消均 Close+Remove（清理失败记录 error_msg 可观测）；disk：basename 清洗（filepath.Base + 去除路径穿越）+ CreateTemp 唯一化，错误/取消保留 `.part` 文件，成功后改名。"零磁盘 I/O"指**下载载荷**（统计 10s 刷盘除外）；discard 模式验收：指定连接数下实测 RSS < 30MB（T8 记录测量环境）。
19. **关停序列**：`Stop()`：拒绝新修改/探活/下载 → 取消并等待全部 worker、探活、定时刷盘 → 最终同步 flush 成功 → 返回；失败记录错误并由 main 以非零码退出。
20. **状态优先级**：`paused` > `quota_reached` > `sleeping_schedule` > `running`（D4）。

## 6. API 与前端契约（main.go + web/index.html）

路由（Go 1.22 `ServeMux` pattern；未匹配的 `/api/*` 一律返回 JSON 404，**不得**回落到首页）：

| 方法+路径 | 成功 | 错误 |
|-----------|------|------|
| `GET /api/status` | 200 `{EngineSnapshot 序列化，字段见 §5}` | 500 |
| `GET /api/targets` | 200 `{"targets":[TargetSnapshot...]}` | 500 |
| `POST /api/targets` | 201 `{"ok":true,"id":N}` | 400 非法输入 / 409 url 重复 |
| `PUT /api/targets/{id}` | 200 `{"ok":true}` | 400（空对象/显式 null/非法值）/ 404 / 409 |
| `DELETE /api/targets/{id}` | 200 `{"ok":true}` | 404 |
| `POST /api/targets/check` | 200 `{"ok":true}` 异步执行（ctx=引擎生命周期派生） | 500 |
| `GET /api/history` | 200 `{"history":[24×HistoryPoint]}`，当前小时 = DB 值 + `CurrentHourLiveBytes()` 叠加，不重计 | 500 |
| `POST /api/control` | 200 `{"ok":true}` | 400 / 500 持久化失败 |
| `GET /` 及静态 | 200 embed 的 index.html | — |

**JSON 规则**：全部错误返回 `{"error":"..."}`；405 带 `Allow` 头；`id` 正 int64；计数字段非负 int64（JSON number；前端注意 >2^53 精度，显示层按 GB 取整即可）；`hour` 为带本地偏移的 RFC3339 字符串。

**POST /api/targets 输入**：`{name,url,weight,enabled}` 四字段**必填**；URL 必须 http/https scheme + 非空有效 hostname + 无 userinfo；name 非空字符串；weight ≥1 整数；enabled 布尔。
**PUT**：`TargetPatch` 指针语义，字段**缺失**=不修改，**显式 null**=400，空对象=400；修改后的值复用 POST 验证；改 URL 触发 §5.17 generation 流程。
**POST /api/control**：`{"action":"pause"}` | `{"action":"resume"}` | `{"action":"update", "max_rate_mbps"?, "max_connections"?, "daily_limit_gb"?, "storage_mode"?}`（任意子集，未知字段/非法值 400）。**事务边界**：main 串行化全部控制更新——构造候选完整 Config → 统一验证 + 预检存储目录 → **原子 Save 成功后**才发布配置快照并执行**不会失败**的引擎热更新；Save 失败 → 运行态不变 + 500。整个请求成功后才返回 ok。

**main 组装顺序**：解析 `-config` → 目标配置缺失时创建父目录并复制 `/app/config.example.yaml`（不覆盖）→ LoadConfig → OpenStore+InitSchema+EnsureSeeded → NewEngine+Start → HTTP Serve（优雅关停：先拒绝新修改与探活 → 停止接入 → 取消并等待全部 handler/worker/探活/刷盘 → Engine.Stop()（内含最终 flush）→ Store.Close；任一步失败以非零码退出）。

**web/index.html**（SPEC §4.2 全部布局 + 以下 UI 规则）：
- 2s 轮询 status+targets；60s 或 CRUD 后刷新 history
- `daily_limit_gb=0` → 显示"不限量"、不计算进度百分比
- `Unknown` 健康态单独呈现（灰）——初始加载常见
- 控件修改失败（4xx/5xx）→ **回滚控件显示值**并 toast 报错
- 接口失败离线横幅 + 自动恢复；bytes 自适应格式化；cooldown 剩余人性化（"5h23m"）；error_msg 悬停提示
- `?demo=1` 演示模式（内置数据模拟轮询，顶部 DEMO 徽标）

## 7. 基础设施契约（T11）

- **Dockerfile**：`golang:1.23-alpine` → `CGO_ENABLED=0 go build` → `alpine:3.20` + `ca-certificates` + `tzdata`；example 置 `/app/`；`ENTRYPOINT ["/app/netsuck","-config","/data/config.yaml"]`。若 go.mod 上调最低版本须同步基础镜像。
- **docker-compose.yml**：`ghcr.io/stromkuo/netsuck:latest`（D2 小写）；`8080:8080`；`/data` 卷；**tmpfs 可选段**：`/tmp/dl_cache` 带显式 `size`（memory 模式必须由部署模板提供有上限的 tmpfs）；`TZ`；`restart: unless-stopped`。
- **docker-publish.yml**：push main 或 tag `v*` 触发；`permissions: contents: read, packages: write`；owner 转小写；buildx `linux/amd64,linux/arm64` 推 ghcr（main→latest+sha，v*→semver tags）；登录后推送合并 manifest。
- **README.md**：中文；背景与特性、Compose 快速部署、全部配置项表、网络调优原理（限速/并发/NAT/风控冷却）、本地运行（含如何从 config.example.yaml 起步、`-config` 用法）。

## 8. 验收清单（T8 / T10 / T12 分级执行）

**T7 自带测试（T8 审查核对，`go test -race` 必过）**——本地可控 HTTP 测试服务 + 可注入时钟：
- [ ] 2→1→2 反复伸缩不超发连接、不重复扩容
- [ ] 两个源重定向到同一 host：host 互斥仍成立
- [ ] 探活与下载并发竞争：健康状态无脏写
- [ ] 403 后该目标全入口零请求（含手动探活）
- [ ] 连续 3 次错误熔断 + 探活恢复
- [ ] 限速热切换立即生效（旧预约作废）
- [ ] 暂停/配额触达时在途下载被取消且许可回收
- [ ] 午夜/小时边界刷盘不丢不重不跨桶
- [ ] 刷盘失败重试同批、退出最终刷盘
- [ ] discard 模式 RSS < 30MB 实测记录

**T10 集成冒烟（REST 全覆盖）**：
- [ ] 全部方法+错误码（400/404/405+Allow/409/500 路径）
- [ ] PUT enabled=false 显式生效；max_rate_mbps=0 保留不丢
- [ ] control 持久化失败→运行态不变
- [ ] 删除/改 URL 后重启：不复活、历史保留、generation 隔离
- [ ] 403 冷却后重启：冷却仍生效（持久化验证）
- [ ] 前端页面加载、轮询、demo 模式、控件回滚

**T12 最终**：
- [ ] `go mod tidy` 干净、`go vet ./...`、`go test -race ./...` 通过
- [ ] `CGO_ENABLED=0 go build`（darwin）+ `GOOS=linux GOARCH=amd64/arm64` 交叉编译
- [ ] SPEC §3/§4/§5 逐条对照实现（审查者核对）
- [ ] 交付物与 SPEC §6 目录结构一致；CI/README/compose 齐备

## 9. 协调者决策记录

| # | 决策 | T1 裁决 |
|---|------|--------|
| D1 | module = `github.com/StromKuo/NetSuck` | ✅ 同意 |
| D2 | 镜像名 `ghcr.io/stromkuo/netsuck`（小写） | ✅ 同意 |
| D3 | 1 GB = 1e9；MB/s=bytes/1e6、Mbps=bytes×8/1e6 | ✅ 同意 |
| D4 | 状态优先级 paused > quota_reached > sleeping_schedule > running；Resume 不越过配额/时段 | ✅ 同意 |
| D5 | disk 模式成功保留、失败/取消保留 .part | ✅ 同意（补部分文件语义） |
| D6 | `daily_limit_gb=0`、`max_rate_mbps=0` = 不限；显式 0 必须保留，负数/非有限数拒绝 | ✅ 同意 |
