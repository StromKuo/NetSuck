# NetSuck 开发编排计划

> 协调者：Claude Code（常驻）。本文件是所有 worker 的**唯一契约来源**，与 `docs/SPEC.md`（需求规格）配套。
> 任何与 SPEC 冲突之处以 SPEC 为准，除非 PLAN 中标注为"协调者决策"。

## 0. 角色分工与额度策略

| 角色 | Agent / 模型 | 职责 | 额度策略 |
|------|-------------|------|---------|
| 协调者 | Claude Code | 任务分解、契约维护、git 集成提交、集成验证、最终验收 | 适度使用 |
| 实现工人 A | opencode (DeepSeek) | 全部核心 Go 代码实现 | 敞开用 |
| 实现工人 B | antigravity / agy (Gemini) | Web 前端、main.go、基础设施文件 | 随便用 |
| 关键审查 | codex (gpt-6.1-sol) | 规划审查、engine.go 关键实现独立审查 | 只用于刀刃 |

**铁律**：每个功能块实现完成后，必须由**非实现 Agent** 独立审查通过；审查者只报告问题、不改代码；修复一律路由回实现者；通过后由协调者统一提交 git。

## 1. 任务 DAG 与状态

| ID | 任务 | 实现 | 独立审查 | 依赖 | 状态 |
|----|------|------|---------|------|------|
| T0 | 引导：SPEC/PLAN/.gitignore/初始提交 | Claude | — | — | ✅ |
| T1 | 规划与契约审查（决策把关） | — | Codex | T0 | ⬜ |
| T2 | go.mod + config.go + config.example.yaml | opencode | antigravity | T1 通过 | ⬜ |
| T3 | web/index.html（可与 T2 并行） | antigravity | opencode | T1 通过 | ⬜ |
| T4 | 审查 T2（config 块） | — | antigravity | T2 | ⬜ |
| T5 | db.go | opencode | antigravity | T4 通过 | ⬜ |
| T6 | 审查 T5（db 块）+ 审查 T3（web 块） | — | antigravity / opencode | T5, T3 | ⬜ |
| T7 | engine.go | opencode | **Codex** | T6 通过 | ⬜ |
| T8 | 审查 T7（关键实现独立审查） | — | Codex | T7 | ⬜ |
| T9 | main.go（API 路由 + 组装 + embed） | antigravity | opencode | T8 通过 | ⬜ |
| T10 | 审查 T9 + 集成冒烟测试 | — | opencode | T9 | ⬜ |
| T11 | Dockerfile / docker-compose / CI / README | antigravity | opencode | T10 通过 | ⬜ |
| T12 | 审查 T11 + 最终构建验证（交叉编译） | — | opencode | T11 | ⬜ |
| T13 | 最终验收报告 | Claude | — | T12 | ⬜ |

## 2. 全局工程约定

- **module 路径**：`github.com/StromKuo/NetSuck`（协调者决策：SPEC 写 placeholder，实际仓库为 github.com/StromKuo/NetSuck；Go module path 大小写敏感，须与 GitHub 一致）。
- **Go 版本**：go.mod 声明 `go 1.23`（SPEC 要求 1.23+；本机工具链 1.27.4，若依赖要求可上调）。
- **静态编译**：`CGO_ENABLED=0`，禁止任何 cgo 依赖。
- **三方依赖白名单**：`gopkg.in/yaml.v3`、`golang.org/x/time/rate`、`modernc.org/sqlite`。其余 Go 依赖一律不得引入；前端仅允许 Tailwind CSS (CDN) + Chart.js (CDN) + 原生 JS，禁止 Node 构建链。
- **并发纪律**：所有跨协程共享可变状态必须用 `sync.RWMutex` 或 `sync/atomic` 保护，代码必须 `go vet` 通过；不允许 panic 蔓延（recover 或错误返回）。
- **路径约定**：配置默认 `/data/config.yaml`；示例配置 `/app/config.example.yaml`；SQLite `/data/stats.db`。
- **git 提交**：每个 Task 通过审查后由协调者提交，格式如 `T2: config 模块实现`；不带无关变更。
- **代码注释**：Go 代码用简洁中文注释关键逻辑；config.example.yaml 注释必须清晰完整。

## 3. 配置契约（config.go，必须精确实现）

```go
type ScheduleConfig struct {
    StartHour int `yaml:"start_hour"` // 0-23
    EndHour   int `yaml:"end_hour"`   // 0-24（24 = 当日午夜结束）；支持跨夜（如 23 → 07）
}
type JitterConfig struct {
    MinSeconds int `yaml:"min_seconds"` // 默认 5
    MaxSeconds int `yaml:"max_seconds"` // 默认 30
}
type TargetConfig struct {
    Name    string `yaml:"name"`
    URL     string `yaml:"url"`
    Weight  int    `yaml:"weight"`  // >=1
    Enabled bool   `yaml:"enabled"`
}
type Config struct {
    ListenAddr     string         `yaml:"listen_addr"`     // 默认 ":8080"
    StorageMode    string         `yaml:"storage_mode"`    // discard|memory|disk，默认 discard
    MemoryDir      string         `yaml:"memory_dir"`      // 默认 "/tmp/dl_cache"
    DiskDir        string         `yaml:"disk_dir"`        // 默认 "/data/downloads"
    MaxRateMbps    int            `yaml:"max_rate_mbps"`   // 0 = 不限速
    MaxConnections int            `yaml:"max_connections"` // 默认 2，范围 1-10
    DailyLimitGB   float64        `yaml:"daily_limit_gb"`  // 0 = 不限量
    Schedule       ScheduleConfig `yaml:"schedule"`        // 默认 0-24（全天）
    Jitter         JitterConfig   `yaml:"jitter_seconds"`
    CooldownHours  int            `yaml:"cooldown_hours"`  // 默认 6
    UserAgent      string         `yaml:"user_agent"`      // 默认桌面 Chrome UA
    Targets        []TargetConfig `yaml:"targets"`
}

func LoadConfig(path string) (*Config, error)      // 缺文件报错；缺字段填默认值；越界/非法值报错
func (c *Config) Save(path string) error           // 原子写：临时文件 + rename；保留 yaml 注析不强求
```

**schedule 语义（定案）**：活跃窗口为 `[start_hour, end_hour)`；`end_hour=24` 表示到当日午夜；当 `end_hour <= start_hour` 时表示跨夜窗口（如 23→07 = 23:00 至次日 07:00）；默认 `{0, 24}` 即全天。校验：`0<=start<=23`、`0<=end<=24`。

**config.example.yaml 要求**：内置 ≥12 个分散厂商的大体积公开直链（云厂商测速包、镜像站 ISO、大厂安装包等），每条含 name/中文注释；T2 实现时必须用 `curl -s -o /dev/null -w '%{http_code}' -r 0-1023 -A '<UA>' <URL>` 逐一验证返回 200/206，验证结果写进任务报告。

## 4. 数据库契约（db.go）

- 驱动：`modernc.org/sqlite`；DSN：`file:<path>?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)`；全部写入走同一个 `*sql.DB`（写操作加互斥或单写协程）。
- 表结构：

```sql
CREATE TABLE IF NOT EXISTS targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,
    weight INTEGER NOT NULL DEFAULT 10,
    enabled INTEGER NOT NULL DEFAULT 1,
    total_bytes INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS daily_stats (
    day TEXT NOT NULL,            -- 本地日期 'YYYY-MM-DD'（TZ）
    target_id INTEGER NOT NULL,
    bytes INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, target_id)
);
CREATE TABLE IF NOT EXISTS history (
    hour_ts INTEGER PRIMARY KEY,  -- unix 秒，截断到整点
    bytes INTEGER NOT NULL DEFAULT 0
);
```

- 接口：`Open(path) / InitSchema / SyncFromConfig(cfg)`（**仅首次播种**：targets 表为空时才插入 config 的 targets；此后 DB 是 URL 列表的唯一权威，config.yaml 的 targets 变更不再回灌，避免复活用户已删除的 URL）/ `ListTargets / AddTarget / UpdateTarget(部分字段) / DeleteTarget / FlushCounters(per-target 今日与累计绝对值 upsert + 当小时全局流量 upsert) / History24h()（补零返回 24 个点，最旧在前）/ TodayGlobalBytes()`。
- 单位约定：1 GB = 1,000,000,000 字节（十进制，协调者决策）。

## 5. 引擎契约（engine.go）

```go
type Health uint8 // Healthy / RateLimited / Dead / Unknown(初始)
type Engine struct { /* ... */ }
func NewEngine(cfg *config.Config, store DB接口, /* 需要的热更新回调 */) (*Engine, error)
func (e *Engine) Start(ctx context.Context)          // 启动 worker 池/探活/配额/调度各 goroutine
func (e *Engine) Stop() error                        // 取消 ctx、等 worker 收尾、把计数交给调用方刷盘
func (e *Engine) SetMaxRate(mbps int)                // 0 => rate.Inf，立即生效
func (e *Engine) SetMaxConnections(n int)            // 平滑增减：新增立即 spawn；多余者完成当前下载后退出
func (e *Engine) Pause() / Resume()
func (e *Engine) SetStorageMode(mode string) error
func (e *Engine) SetDailyLimit(gb float64)
func (e *Engine) Status() string                     // running|paused|sleeping_schedule|quota_reached
func (e *Engine) Snapshot() /* 供 /api/status 与 /api/targets 的全部数据 */
func (e *Engine) CheckAllNow(ctx)                    // 手动探活（同时重置已过期冷却）
func (e *Engine) ReloadTargets()                     // CRUD 后从 DB 重载
```

关键机制（SPEC §3.2/§3.3 的完整落地）：
1. **令牌桶**：`rate.NewLimiter`，字节层面限速；`StatsReader`（自定义 `io.Reader` 包装 `resp.Body`）读到 n 字节后 `WaitN(ctx, n)`；burst ≥ 64KB；Mbps→bytes/s = `mbps*1_000_000/8`。
2. **单 Host 互斥**：`map[host]chan struct{}`（容量 1）+ 非阻塞 try-acquire；占用时改选其他目标。
3. **工作协程池**：目标并发数 `desired`（atomic）；每 worker 循环：越界则退出 → 按权重在「enabled 且非冷却且健康」集合中加权随机选目标 → 抢 host 锁 → 下载 → jitter 随机休眠。
4. **原子计数**：`StatsReader` 内 `atomic.Int64` 精确累加到 target 的 today/total 与全局 today/total。
5. **探活**：每 15 分钟 + 手动；`Range: bytes=0-1023` GET；记录状态码/延迟 ms/健康状态/error_msg；403/429 → RateLimited + 冷却 6h（`cooldown_until`）；404/超时/连接重置 → Dead；200/206 → Healthy 并清除过期冷却、复活 Dead。
6. **熔断**：下载中连续 3 次（超时/连接重置/404/5xx）→ Dead，直到探活恢复。
7. **每日配额**：全局 today ≥ `daily_limit_gb*1e9` → `quota_reached` 休眠；本地时区（`TZ` 环境变量 → `time.Local`）每日 00:00 清零 today 计数并解除配额状态。
8. **活跃时段**：`[start_hour, end_hour)` 区间外 → `sleeping_schedule`；兼容跨夜。状态优先级：`paused`（手动）> `quota_reached` > `sleeping_schedule` > `running`（协调者决策）。
9. **实时速率**：采样协程每 500ms 读取各 atomic 累计值入环形缓冲，滑动 3 秒窗口计算 per-target 与全局速率（同时给出 MB/s 与 Mbps）。
10. **HTTP 客户端**：全局单例；`Transport{MaxIdleConns:10, MaxConnsPerHost:1, IdleConnTimeout:90s}`；桌面 UA；自动跟随重定向。
11. **存储模式**：discard → `io.CopyBuffer(io.Discard, 32KB)`；memory → 写 `memory_dir` 临时文件、完成或出错即 `os.Remove`；disk → 写 `disk_dir`（文件保留，协调者决策：供缓存需求），文件名取 URL basename + 时间戳防冲突。
12. **退出信号**：`Stop()` 由 main 在收到 SIGTERM/SIGINT 后调用；Engine 内部完成「取消 ctx → 等 worker 收尾 → **最终 FlushCounters**」整个序列（main 不另行刷盘，避免双重写）。
13. **刷盘一致性规则**：Engine 内**单一** flush 协程每 10 秒做一次快照刷盘；任何时刻只有该协程调用 `db.FlushCounters`。计数为**绝对值 upsert**（today 按 (day,target) 键、total 按 target 键、history 按 hour 键），天然幂等，崩溃最多丢最近 10 秒。
14. **跨日切换规则**：由配额协程在本地 00:00 触发，顺序固定：①先做一次旧日最终 flush → ②持同一把日切锁把日期标签翻到新日并清零全部内存 today 计数、解除 `quota_reached` → ③此后计数入新日。字节累加与日期标签的读取由该锁保证不会跨日记错。
15. **冷却恢复规则**：探活（定时或手动）返回 200/206 时——若 health 为 `Dead` → 恢复 `Healthy`；若为 `RateLimited` 且 `now >= cooldown_until` → 清除冷却恢复 `Healthy`；若 `RateLimited` 且未到期 → 保持冷却，仅更新延迟与 error_msg。任何情况下探活不得提前解除未到期冷却。

## 6. API 与前端契约（main.go + web/index.html）

路由（`net/http` 标准库 `ServeMux`，Go 1.22 pattern 语法）：

- `GET /api/status` →
```json
{"status":"running","storage_mode":"discard","max_rate_mbps":100,"max_connections":2,
 "active_connections":1,"speed_mbps":12.3,"speed_mb_per_sec":1.54,"today_bytes":12345678901,
 "daily_limit_gb":300,"schedule":{"start_hour":1,"end_hour":18}}
```
- `GET /api/targets` →
```json
{"targets":[{"id":1,"name":"...","url":"https://...","weight":10,"enabled":true,
 "health":"Healthy","latency_ms":120,"cooldown_remaining_seconds":0,
 "today_bytes":0,"total_bytes":0,"speed_mbps":0.0,"speed_mb_per_sec":0.0,"error_msg":""}]}
```
- `POST /api/targets` body `{name,url,weight,enabled}` → `201 {"ok":true,"id":N}`；校验 url 为 http/https、weight≥1。
- `PUT /api/targets/{id}` body 任意子集 `{name?,url?,weight?,enabled?}` → `{"ok":true}`。
- `DELETE /api/targets/{id}` → `{"ok":true}`。
- `POST /api/targets/check` → `{"ok":true}`（异步执行，前端随后轮询 targets）。
- `GET /api/history` → `{"history":[{"hour":"2026-11-14T15:00:00+08:00","bytes":123456}, ...24 项，最旧在前]}`
- `POST /api/control`：
  - `{"action":"pause"}` / `{"action":"resume"}` → `{"ok":true}`
  - `{"action":"update","max_rate_mbps":200,"max_connections":4,"daily_limit_gb":500,"storage_mode":"memory"}`（参数任意子集）→ 校验 + 热更新 + **持久化回 config.yaml** → `{"ok":true}`
- 错误统一 `400/404/405` + `{"error":"..."}`；`/` 与静态资源由 `//go:embed web/index.html` 提供。

main.go 组装顺序：解析 `-config` → 首次启动复制 `/app/config.example.yaml` → `/data/config.yaml`（仅当目标不存在）→ LoadConfig → Open DB + SyncFromConfig → NewEngine + Start → HTTP Serve → 监听 SIGTERM/SIGINT：Engine.Stop → FlushCounters → Server.Shutdown。

web/index.html：暗/亮自适应（`prefers-color-scheme`）、Tailwind CDN + Chart.js CDN、2 秒轮询；布局按 SPEC §4.2（品牌栏+3 卡片+URL 表格+环形图/折线图）；全部交互（增删改查、启停、热改参数、测试连通性、点击进度条改配额）。

## 7. 基础设施契约（T11）

- **Dockerfile**：`golang:1.23-alpine` → `CGO_ENABLED=0 go build` → `alpine:3.20` + `ca-certificates` + `tzdata`；config.example.yaml 置于 `/app/`；`ENTRYPOINT ["/app/netsuck","-config","/data/config.yaml"]`。
- **docker-compose.yml**：`image: ghcr.io/stromkuo/netsuck:latest`（**镜像名全小写**，ghcr 要求）；`ports 8080:8080`；`/data` 卷；可选 `tmpfs: /tmp/dl_cache (size)`；`TZ` 环境变量；`restart: unless-stopped`。
- **docker-publish.yml**：push 到 main 或打 `v*` tag 触发；buildx 多架构 `linux/amd64,linux/arm64`；推 `ghcr.io`；owner 需转小写（`${GITHUB_REPOSITORY_OWNER,,}`）；`permissions: packages:write`。
- **README.md**：中文；项目背景与特性、Docker Compose 快速部署、全部配置项说明表、网络调优建议（限速/并发/NAT 保护原理）。

## 8. 最终验收清单（T12/T13）

- [ ] `go vet ./...` 无告警
- [ ] `CGO_ENABLED=0 go build`（darwin/arm64 本机）成功
- [ ] `GOOS=linux GOARCH=amd64` 与 `GOOS=linux GOARCH=arm64` 交叉编译成功
- [ ] 本地冒烟：`./netsuck -config <临时配置>` 启动 → `/api/status`、`/api/targets`、`/api/control`、`/api/history` 行为符合契约 → 前端页面可加载、轮询正常（无 JS 报错）
- [ ] SPEC §3 每一条功能点在代码中有对应实现（审查者逐条核对）
- [ ] 交付物清单与 SPEC §6 完全一致

## 9. 协调者决策记录

| # | 决策 | 理由 |
|---|------|------|
| D1 | module = `github.com/StromKuo/NetSuck` | SPEC 的 placeholder 替换为实际仓库路径 |
| D2 | 镜像名 `ghcr.io/stromkuo/netsuck`（小写） | ghcr 强制小写 |
| D3 | 1 GB = 1e9 字节 | 网络流量行业惯例（十进制） |
| D4 | 状态优先级 paused > quota_reached > sleeping_schedule > running | 手动操作优先级最高 |
| D5 | disk 模式文件保留不删除 | SPEC 定位为"有缓存文件需求的用户" |
| D6 | `daily_limit_gb=0` 与 `max_rate_mbps=0` 均表示不限 | 与限速 0=不限保持一致 |
