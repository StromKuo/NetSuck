# NetSuck 开发编排计划

> 协调者：Claude Code（常驻）。本文件是所有 worker 的**唯一契约来源**，与 `docs/SPEC.md`（需求规格）配套。
> 任何与 SPEC 冲突之处以 SPEC 为准，除非 PLAN 中标注为"协调者决策"。
> **版本：v3** —— T1 审查（REJECT：4B+11M+1m）全面修订为 v2；T1b 聚焦复审（10 项落实/6 项部分，R1-R5 阻断）修订为 v3：累计口径统一、CounterBatch/恢复接口闭合、跨日独立于刷盘、探活资格矩阵、引擎接管 CRUD、历史权威视图、验证与错误表补全、软删唯一索引。D1-D6 获两轮同意。

## 0. 角色分工与额度策略

| 角色 | Agent / 模型 | 职责 | 额度策略 |
|------|-------------|------|---------|
| 协调者 | Claude Code | 任务分解、契约维护、git 集成提交、集成验证、最终验收 | 适度使用 |
| 实现工人 A | opencode (DeepSeek) | 全部核心 Go 代码实现 | 敞开用 |
| 实现工人 B | antigravity / agy (Gemini) | Web 前端、main.go、基础设施文件 | 随便用 |
| 关键审查 | codex (gpt-6.1-sol) | 规划审查（T1/T1b/T1c）、engine.go 关键实现独立审查（T8） | 只用于刀刃 |

**铁律**：每个功能块实现完成后，必须由**非实现 Agent** 独立审查通过才算完成；审查者只报告问题、不改代码；修复一律路由回实现者；通过后由协调者统一提交 git。

## 1. 任务 DAG 与状态

| ID | 任务 | 实现 | 独立审查 | 依赖 | 状态 |
|----|------|------|---------|------|------|
| T0 | 引导 | Claude | — | — | ✅ |
| T1 | 规划与契约审查 | — | Codex | T0 | ✅ REJECT→v2 |
| T1b | v2 聚焦复审 | — | Codex | v2 | ✅ REJECT→v3 |
| T1c | v3 聚焦复审（冻结门禁） | — | Codex | v3 | ⬜ |
| T2 | go.mod + config.go + config.example.yaml | opencode | antigravity（T4） | v3 增量已投递 | 🔄 |
| T3 | web/index.html + v2/v3 UI 增补 | antigravity | opencode（T6） | — | 🔄 |
| T4 | 审查 T2 | — | antigravity | T2 完工 | ⬜ |
| T5 | db.go（Store，签名见 §4 冻结） | opencode | antigravity（T6） | T2 完工 | ⬜ |
| T6 | 审查 T5 + 审查 T3 | — | antigravity / opencode | T5, T3 完工 | ⬜ |
| T7 | engine.go（§5 规则 + §8 测试） | opencode | **Codex**（T8） | T6 通过 | ⬜ |
| T8 | 审查 T7（关键实现独立审查） | — | Codex | T7 | ⬜ |
| T9 | main.go（路由/组装/关停） | antigravity | opencode（T10） | T8 通过 | ⬜ |
| T10 | 审查 T9 + 集成冒烟 | — | opencode | T9 | ⬜ |
| T11 | Dockerfile / compose / CI / README | antigravity | opencode（T12） | T10 通过 | ⬜ |
| T12 | 审查 T11 + 最终构建验证 | — | opencode | T11 | ⬜ |
| T13 | 最终验收报告 | Claude | — | T12 | ⬜ |

## 2. 全局工程约定

- **module 路径**：`github.com/StromKuo/NetSuck`（D1）；根目录全部 .go 文件统一 **package main**。
- **Go 版本**：go.mod `go 1.23`；T2 固定直接依赖版本并生成 go.sum；全部依赖满足 Go 1.23 且 `GOTOOLCHAIN=local go build` 可构建；上调最低版本须同步 Docker 镜像。
- **依赖白名单**：直接依赖仅 `gopkg.in/yaml.v3`、`golang.org/x/time/rate`、`modernc.org/sqlite`，允许必要间接依赖；前端仅 Tailwind/Chart.js CDN + 原生 JS。
- **静态编译**：`CGO_ENABLED=0`；禁止 cgo。
- **并发纪律**：跨协程共享状态用 `sync.RWMutex`/`sync/atomic`；`go vet` 与 `go test -race` 必过。
- **路径**：示例 `/app/config.example.yaml`；配置默认 `/data/config.yaml` 以 `-config` 为准；首启按 `-config` 路径复制示例（建父目录、不覆盖）。
- **git**：每个 Task 审查通过后协调者提交。

## 3. 配置契约（config.go）

```go
// package main
type ScheduleConfig struct {
    StartHour int `yaml:"start_hour" json:"start_hour"` // 0-23
    EndHour   int `yaml:"end_hour"   json:"end_hour"`   // 0-24（24=午夜，仅可作 end）
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
    StorageMode    string         `yaml:"storage_mode"`    // discard|memory|disk
    MemoryDir      string         `yaml:"memory_dir"`      // 默认 "/tmp/dl_cache"
    DiskDir        string         `yaml:"disk_dir"`        // 默认 "/data/downloads"
    MaxRateMbps    int            `yaml:"max_rate_mbps"`   // 默认 0=不限
    MaxConnections int            `yaml:"max_connections"` // 默认 2，1-10
    DailyLimitGB   float64        `yaml:"daily_limit_gb"`  // 默认 0=不限量
    Schedule       ScheduleConfig `yaml:"schedule"`        // 默认 {0,24}
    Jitter         JitterConfig   `yaml:"jitter_seconds"`
    CooldownHours  int            `yaml:"cooldown_hours"`  // 默认 6，>0
    UserAgent      string         `yaml:"user_agent"`
    Targets        []TargetConfig `yaml:"targets"`
}

func LoadConfig(path string) (*Config, error)
func (c *Config) Save(path string) error   // 原子写：临时文件 + os.Rename
func DefaultConfig() *Config
// 共享验证（config 与 API 复用，全部导出）：
func ValidateStorageMode(v string) error
func ValidateMaxRateMbps(v int) error      // >=0
func ValidateMaxConnections(v int) error   // 1-10
func ValidateDailyLimitGB(v float64) error // 有限非负
func ValidateSchedule(s ScheduleConfig) error // start==end 非法；end 仅可 24
func ValidateJitter(j JitterConfig) error  // 0<=Min<=Max
func ValidateTargetInput(name, url string, weight int) error
```

**presence 语义**：YAML 缺字段才填默认；显式 `false`/`0`/`1` 原样保留（指针 raw 结构或 yaml.Node presence）。LoadConfig 必须跑全部验证（含 jitter 与 cooldown_hours）。

**schedule 语义**：`[start_hour, end_hour)`；`start<end`：`start<=h<end`；`start>end` 跨夜：`h>=start || h<end`；本地钟点判断。

**config.example.yaml**：≥12 个不同厂商大体积直链；验证配方：
`curl -sL -o /dev/null -w '%{http_code} %{size_download}' -r 0-1023 -A '<桌面Chrome UA>' --max-time 20 --max-filesize 2000000 '<URL>'`（截断退出且已读到数据不算死链）。

## 4. 数据库契约（db.go —— `Store`，v3 冻结）

```go
type Store struct { /* *sql.DB + 写互斥 */ }
func OpenStore(path string) (*Store, error)  // DSN: file:<path>?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)
func (s *Store) InitSchema() error
func (s *Store) EnsureSeeded(cfg *Config) error        // 单事务；metadata 无 targets_seeded 才导入（空列表也标记）
func (s *Store) ListTargetRecords() ([]TargetRecord, error) // 过滤软删
func (s *Store) CreateTarget(p TargetPatch) (TargetRecord, error) // 重复 url（未删）→ ErrDuplicateURL
func (s *Store) PatchTarget(id int64, p TargetPatch) (TargetRecord, error) // ErrNotFound
func (s *Store) SoftDeleteTarget(id int64) error                   // ErrNotFound
func (s *Store) PersistBatch(b CounterBatch) error                 // 单事务、幂等
func (s *Store) LoadEngineState(now time.Time) (EngineRestoreState, error) // 启动恢复
func (s *Store) Close() error
var ErrDuplicateURL = errors.New("duplicate url")
var ErrNotFound     = errors.New("not found")

type TargetRecord struct {
    ID int64 `json:"id"`; Name string `json:"name"`; URL string `json:"url"`
    Weight int `json:"weight"`; Enabled bool `json:"enabled"`
    Health string `json:"health"`                    // Healthy|RateLimited|Dead|Unknown
    CooldownUntilMS int64 `json:"cooldown_until_ms"`
    LastStatusCode int `json:"last_status_code"`; LatencyMS int64 `json:"latency_ms"`
    ErrorMsg string `json:"error_msg"`; ConsecutiveErrors int `json:"consecutive_errors"`
    TotalBytes int64 `json:"total_bytes"`             // 累计字节（**含今日**，R1）
    DeletedAt *time.Time `json:"-"`
}
type TargetPatch struct { // 指针语义：nil=不改
    Name *string `json:"name"`; URL *string `json:"url"`
    Weight *int `json:"weight"`; Enabled *bool `json:"enabled"`
}
type TargetHealthPersist struct {
    Health string; CooldownUntilMS int64; LastStatusCode int; LatencyMS int64
    ErrorMsg string; ConsecutiveErrors int
}
type CounterBatch struct { // 不可变快照（R2）
    TargetDays  map[string]map[int64]int64 // 日期'YYYY-MM-DD' -> target_id -> 该日累计绝对值（含未刷旧日脏桶）
    GlobalDays  map[string]int64           // 日期 -> 全局该日累计绝对值
    TargetTotals map[int64]int64           // target_id -> 全量累计绝对值（含今日，R1）
    HourBuckets map[int64]int64            // hour_unix -> 全局该小时累计绝对值
    TargetHealth map[int64]TargetHealthPersist
    Seq         uint64
}
type EngineRestoreState struct { // LoadEngineState 返回（R2）
    Targets      []TargetRecord
    TargetDays   map[string]map[int64]int64 // 今日各 target 绝对值（至少含今日）
    GlobalDays   map[string]int64           // 至少含今日
    HourBuckets  map[int64]int64            // 最近 24 个本地小时桶
    LastSeq      uint64                     // metadata.last_batch_seq
}
type HistoryPoint struct {
    HourUnix int64 `json:"-"`; Hour string `json:"hour"`; Bytes int64 `json:"bytes"`
}
```

表结构（R8：url 改**部分唯一索引**）：
```sql
CREATE TABLE IF NOT EXISTS targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL, url TEXT NOT NULL,
    weight INTEGER NOT NULL DEFAULT 10,
    enabled INTEGER NOT NULL DEFAULT 1,
    health TEXT NOT NULL DEFAULT 'Unknown',
    cooldown_until_ms INTEGER NOT NULL DEFAULT 0,
    last_status_code INTEGER NOT NULL DEFAULT 0,
    latency_ms INTEGER NOT NULL DEFAULT 0,
    error_msg TEXT NOT NULL DEFAULT '',
    consecutive_errors INTEGER NOT NULL DEFAULT 0,
    total_bytes INTEGER NOT NULL DEFAULT 0,  -- 全量累计（含今日，R1）
    deleted_at INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_targets_url_live ON targets(url) WHERE deleted_at IS NULL;
CREATE TABLE IF NOT EXISTS daily_stats (day TEXT NOT NULL, target_id INTEGER NOT NULL, bytes INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(day,target_id));
CREATE TABLE IF NOT EXISTS history (hour_ts INTEGER PRIMARY KEY, bytes INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL); -- targets_seeded / last_batch_seq / global_day:<date>
```

**持久化语义（R1/R2）**：`PersistBatch` 单事务：daily_stats 按 (day,target) upsert 绝对值；history 按 hour upsert；targets.total_bytes 与健康列按 id upsert 绝对值；metadata 写 `global_day:<date>` 绝对值与 `last_batch_seq`。查询 TotalBytes **禁止**再加 daily_stats（已是全量）。`Seq` 语义：引擎首批 = `LastSeq+1`；`Seq==已提交值` 视为同批幂等重试（成功返回）；`Seq<已提交值` 拒绝。旧批完成不得清除新日脏桶（批次只追加绝对值，天然满足）。403/429 的"立即持久化"= 经**同一串行管线**提交紧急批并等待结果，禁止旁路直写健康字段；失败时内存冷却保持、错误可观测，旧快照不得覆盖新冷却。

## 5. 引擎契约（engine.go）

```go
func NewEngine(cfg *Config, store *Store, httpClient *http.Client) (*Engine, error)
func (e *Engine) Start(ctx context.Context) error
func (e *Engine) Stop() error                       // 与 CRUD/控制共用管理门禁
func (e *Engine) SetMaxRate(mbps int) error         // 引擎须主动唤醒等待者重算（不假设标准库 SetLimit 自醒）
func (e *Engine) SetMaxConnections(n int) error
func (e *Engine) SetStorageMode(mode string) error
func (e *Engine) SetDailyLimit(gb float64) error
func (e *Engine) Pause() error / Resume() error
func (e *Engine) Snapshot() EngineSnapshot          // 深拷贝；单次统一 now
func (e *Engine) CheckAllNow(ctx context.Context) error // ctx 引擎生命周期派生
func (e *Engine) History24h(now time.Time) ([]HistoryPoint, error) // R6 内存权威视图
// 目标 CRUD 由引擎接管（R5）：main 不得直接调 Store 改 targets
func (e *Engine) CreateTarget(p TargetPatch) (TargetRecord, error)
func (e *Engine) PatchTarget(id int64, p TargetPatch) (TargetRecord, error)
func (e *Engine) DeleteTarget(id int64) error
```

EngineSnapshot / TargetSnapshot 同 v2（json 标签不变；`TotalBytes`=全量累计含今日 R1；`CooldownRemainingSeconds=max(0,ceil((cooldown_until-snapshot_now)/1s))`）。

规则（逐条落实，编号供审查对照）：
1. **准入矩阵（R4）**：共用目标互斥/host 锁/冷却检查，但按请求种类区分资格——**下载**：仅 `enabled 且 Healthy`；**探活**（定时/手动）：允许所有未软删且**不在未到期冷却**的目标（含 disabled、Unknown、Dead、已过期 RateLimited）。`now<cooldown_until` 一切请求禁止。每个请求发起前重查 tombstone/generation/冷却/资格。
2. **host 互斥**：小写 hostname 规范化、忽略端口；同域不同 URL 共锁；try-acquire。
3. **重定向**：≤10 跳；逐跳 Close 前响应→释放旧 host 许可→取新 host 许可→下一跳；全程保留原目标互斥与计数归属；取消/失败路径全部释放。
4. **Transport**：MaxIdleConns:10, MaxConnsPerHost:1, IdleConnTimeout:90s, **ForceAttemptHTTP2=false**；单请求互斥由应用层锁保证。
5. **动态连接池**：单一池管理器串行处理目标连接数变化，以"实际存活+待退出"对账（2→1→2 不重复扩容）；下载许可：请求开始前 +1、Body.Close 后 -1（jitter 不计活跃）；减容协作取消+等待回收。
6. **门禁**：paused/时段外/配额触达 → 取消在途+阻止新请求；Resume 不越过配额/时段；一切等待可取消、不忙循环。
7. **配额预留结算**：Read 前 `min(读长, 剩余额度)` 预留、限读长、按实际读数结算归还；预留记录所属日期，跨日结算与读取完成日期一致（R3）。
8. **令牌桶**：全局单例；预约-读-归还（等待在有界 Read 前）；固定 ≤32KiB、≤burst(64KiB)；SetLimit 主动唤醒等待者按新值重算；先计数后处理等待错误；MB/s=bytes/1e6、Mbps=bytes×8/1e6。
9. **字节归属（R3）**：读取完成时在**计数协调锁**内按时钟决定日桶/小时桶——**完全独立于 DB 刷盘**；桶内 atomic 累加；永不 Store(0)。
10. **刷盘管线**：唯一持久化协程，10s 一批不可变 CounterBatch（含所有脏桶+绝对值）；失败重试同批；Seq 顺序；Stop 最终批；403/429 紧急批走同一管线并等待结果。
11. **跨日切换（R3）**：午夜在计数协调锁内**立即**切换配额视图（新日桶键生效、解除 quota_reached），**不等刷盘**；旧日脏桶保留在 TargetDays/GlobalDays 异步重试，不阻塞新日；全局 today 从独立全局日桶维护恢复（GlobalDays），删节点不减全局。
12. **探活（R4）**：启动立即探 Unknown；15min+手动（同轮合并、并发受控、单请求超时 10s）；请求 `GET` + `Range: bytes=0-1023` + 配置 UA；**最多读 1024 字节**（200 也如此，不 drain）；探活字节计入统计与限速；已过期 RateLimited 进入待探活队列，**成功才恢复下载**。
13. **健康状态机（R4）**：403/429（下载或探活）→ RateLimited + cooldown_until=now+cooldown_hours，立即经管线提交紧急批，取消该目标在途；探活 200/206 → Unknown/Dead/Healthy/已过期 RateLimited 全部置 Healthy、清零错误计数；探活其他非 200/206 状态（除 403/429）→ 记录状态码、**探活失败置 Dead**；下载 404/超时/reset/5xx → consecutive_errors+1，连续 3 → Dead；下载成功 → 重置计数。写回健康前比对 generation 与既有 cooldown_until（旧结果不得覆盖新冷却）。
14. **速率采样**：唯一协程 500ms 采样永不清零累计值；按稳定 target id 环形缓冲；实际时间差 ≤3s 窗口；<2 样本=0；3s 无增量=0；发布 mutex/atomic pointer；Snapshot 深拷贝；全局速率独立累计；删节点不减全局。
15. **目标 CRUD（R5）**：由 Engine 串行管理（与 Stop/控制共用管理门禁）：先验证/合并候选 → 需要时停准入、取消在途、等待结算并提交计数批 → 调 Store 提交 CRUD → 发布元数据；**DB 失败恢复旧目标准入与元数据（已发生流量保留）**；创建立即建立 Unknown 计数对象并触发探活；改 URL 递增 generation、保留 id 与历史统计、清空旧健康状态并立即探活（旧 generation 结果丢弃）；改名/权重不重建计数器；禁用=停准入+取消在途+结算（计数保留）；删除=软删+停准入+结算，历史桶保留；稳定 id 计数对象，重载只改元数据不重置计数与采样环。
16. **存储模式**：请求开始捕获策略；统一 StatsReader+32KiB 显式循环（不用 io.CopyBuffer）；memory：CreateTemp+成功/出错/取消均 Close+Remove；disk：basename 清洗+CreateTemp 唯一+`.part`→成功改名；"零磁盘 I/O"指下载载荷；discard 验收 RSS<30MB。
17. **历史视图（R6）**：Engine.History24h 返回**内存权威视图**：启动由 LoadEngineState 恢复最近 24 小时桶，此后与统计共用锁维护全部小时桶；含当前未完成小时+前 23 个实际存在小时（本地钟表小时边界的实际 Unix 时间为键；DST 重复小时保留不同偏移各自成桶）；缺流量补零；最旧在前；不拼接 DB+增量两个时点。
18. **关停序列**：Stop 经管理门禁：拒绝新 CRUD/控制/探活/下载 → 取消并等待全部 worker/探活/刷盘 → 最终同步 flush → 返回；失败由 main 非零退出。
19. **状态优先级**：paused > quota_reached > sleeping_schedule > running（D4）。

## 6. API 与前端契约（main.go + web/index.html）

| 方法+路径 | 成功 | 错误 |
|-----------|------|------|
| `GET /api/status` | 200 EngineSnapshot 序列化 | 500 |
| `GET /api/targets` | 200 `{"targets":[...]}` | 500 |
| `POST /api/targets` | 201 `{"ok":true,"id":N}` | 400 / **409** url 重复 / **500** 引擎或 DB 失败 |
| `PUT /api/targets/{id}` | 200 `{"ok":true}` | 400（空对象/显式 null/非法值）/ 404 / **409** / **500** |
| `DELETE /api/targets/{id}` | 200 `{"ok":true}` | 404 / **500** |
| `POST /api/targets/check` | 200 `{"ok":true}` 异步（ctx=引擎生命周期派生） | 500 |
| `GET /api/history` | 200 `{"history":[24×HistoryPoint]}`（**调用 Engine.History24h(now)**） | 500 |
| `POST /api/control` | 200 `{"ok":true}` | 400 / 500 持久化失败 |
| `GET /` 等 | 200 embed index.html | — |

- JSON 规则：错误一律 `{"error":"..."}`；405 带 `Allow`；未匹配 `/api/*` 返回 JSON 404 不回落首页；`id` 正 int64；计数非负 int64；`hour` RFC3339 带本地偏移。
- **targets CRUD**：handler 调 **Engine** 的 CreateTarget/PatchTarget/DeleteTarget（不直接碰 Store），映射 ErrDuplicateURL→409、ErrNotFound→404、验证失败→400、其他→500。
- POST 输入四字段必填；PUT 指针语义（缺失=不改、显式 null=400、空对象=400）；URL 须 http/https+有效 hostname+无 userinfo；修改值复用 POST 验证。
- **POST /api/control**：`{"action":"pause"|"resume"}` 或 `{"action":"update", ...参数任意子集}`（未知字段/非法值 400）。**事务边界**：main 串行化控制更新并与 Engine.Stop 共用管理门禁（Save 后的已验证更新不会因并发 Stop 失效）——构造候选 Config → 统一验证+预检存储目录 → 原子 Save 成功 → 发布快照+执行不会失败的引擎热更新；Save 失败运行态不变+500。
- **main 组装**：`-config` 解析 → 配置缺失建父目录复制示例（不覆盖）→ LoadConfig → OpenStore+InitSchema+EnsureSeeded → NewEngine+Start（引擎经 LoadEngineState 恢复计数/健康/冷却/24h 桶）→ HTTP Serve → SIGTERM/SIGINT：停接入 → 等待 handler → Engine.Stop()（内含最终 flush）→ Store.Close；失败非零退出。
- **web/index.html**：SPEC §4.2 布局 + UI 规则：`daily_limit_gb=0`→"不限量"无进度百分比；Unknown 灰灯单独呈现；控件失败回滚+toast；409 表单内联提示；2s 轮询；history 60s/CRUD 后刷新；离线横幅+自动恢复；字节自适应格式化；冷却剩余人性化；error_msg 悬停；`?demo=1` 演示模式。

## 7. 基础设施契约（T11）

- **Dockerfile**：golang:1.23-alpine → CGO_ENABLED=0 build → alpine:3.20 + ca-certificates + tzdata；示例置 /app/；`ENTRYPOINT ["/app/netsuck","-config","/data/config.yaml"]`。
- **docker-compose.yml**：`ghcr.io/stromkuo/netsuck:latest`；8080:8080；/data 卷；可选 tmpfs `/tmp/dl_cache` 显式 size；TZ；restart: unless-stopped。
- **docker-publish.yml**：push main 或 tag v*；`permissions: contents:read, packages:write`；owner 小写化；buildx linux/amd64+linux/arm64 推 ghcr（main→latest+sha，v*→semver）。
- **README.md**：中文；背景特性、Compose 部署、配置项全表、网络调优原理、本地起步（config.example.yaml 与 -config 用法、软删 URL 重加语义）。

## 8. 验收清单

**T5 自带测试**：schema 幂等；播种仅首次+软删不复活；CRUD 错误值；部分唯一索引（软删后同 URL 可重加、新 id、旧历史保留）；PersistBatch 幂等重放+Seq 拒绝旧批/同批重试成功；LoadEngineState 恢复（含 last_seq、今日桶、全局日桶、24h 桶）；History24h 补零边界。`go test -race` 必过。

**T7 自带测试**（§8 清单 10 项，可注入时钟+httptest，`-race` 必过）：2→1→2 伸缩对账；重定向同 host 互斥；探活/下载并发无脏写；403 后全入口零请求（含探活——准入矩阵）；3 次熔断+探活恢复（含 Dead 探活资格）；限速热切换即时生效；暂停/配额在途取消+许可回收；午夜/小时边界桶归属正确（刷盘故障不阻塞跨日）；刷盘失败重试同批+紧急批（403 冷却持久化）+退出最终批；discard RSS<30MB 实测。另：CRUD 引擎接管路径（停准入→结算→DB 失败回滚→流量保留）；History24h 权威视图与 DST。

**T10 集成冒烟**：全部方法+错误码（400/404/405+Allow/409/500）；PUT enabled=false 显式生效；rate=0 保留；control 持久化失败运行态不变；删除/改 URL 重启不复活、历史保留、generation 隔离；403 冷却重启仍生效；/api/history 引擎权威视图；前端加载/轮询/demo/回滚。

**T12 最终**：`go mod tidy` 干净；`go vet`；`go test -race ./...`；darwin 构建 + linux/amd64+arm64 交叉编译；SPEC §3/§4/§5 逐条对照；交付物与 SPEC §6 一致。

## 9. 协调者决策记录

| # | 决策 | 裁决 |
|---|------|------|
| D1 | module = github.com/StromKuo/NetSuck | ✅✅ |
| D2 | 镜像名小写 ghcr.io/stromkuo/netsuck | ✅✅ |
| D3 | 1GB=1e9；MB/s=bytes/1e6、Mbps=bytes×8/1e6 | ✅✅ |
| D4 | paused>quota_reached>sleeping_schedule>running | ✅✅ |
| D5 | disk 成功保留、失败/取消 .part | ✅✅ |
| D6 | 0=不限（显式保留，负数/非有限拒绝） | ✅✅ |
| D7 | (v3 采纳 R8) 软删 URL 可重加：部分唯一索引，新 id，历史保留 | 协调者定 |
