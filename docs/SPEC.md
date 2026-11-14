# 项目开发规格说明书：NetSuck

> **项目标语**：*Because asymmetrical throttling sucks.* — 专为 NAS 打造的轻量级、零硬盘损耗 HTTP/HTTPS 下行流量生成与上下行带宽平衡工具。

## 1. 项目概述
开发一个名为 **NetSuck** (`netsuck`) 的开源自托管 Go 语言应用程序，打包为支持多架构（`linux/amd64`、`linux/arm64`）的 Docker 镜像，适用于 Unraid、群晖等 NAS 环境。

**NetSuck** 通过可配置的公共 CDN 或大型软件/开源镜像下载直链列表，在严格限制总带宽与并发连接数的前提下，持续产生可控的 HTTP/HTTPS 下行网络流量，用于平衡家庭宽带的上下行流量比例。项目坚持“无状态公开直链流式丢弃”设计（不引入账号登录、Cookie 维护及 BT/Aria2 复杂协议），内置单文件 Web 监控面板，支持实时速率与连接数调整、URL 连通性探活，以及精确到单个 URL 维度的流量统计。

## 2. 技术栈与架构约束
- **开发语言**：Go 1.23+
- **二进制打包**：静态编译单文件（`CGO_ENABLED=0`），前端页面通过 Go 标准库 `//go:embed` 直接嵌入二进制文件中。
- **数据持久化**：使用纯 Go 实现的 SQLite 驱动（`modernc.org/sqlite`），避免 CGO 依赖以便顺利进行多架构交叉编译，用于持久化历史流量数据及动态修改的 URL 列表。
- **HTTP 流式下载引擎**：基于 Go 原生 `net/http` 实现流式读取（**严禁**引入 Aria2 等外部下载器，也**严禁**引入模拟登录/Cookie 维护模块）。通过自定义包装 `io.Reader`，在读取网卡数据块的同时，以原子操作按 URL 精确累加字节数，并将数据路由到指定的存储/丢弃后端。
- **前端面板**：单文件响应式仪表盘（`web/index.html`），采用原生 HTML/JS + Tailwind CSS (CDN) + Chart.js (CDN) 实现，无需引入 Node.js 前端构建链路。
- **CI/CD 自动化构建**：配置 GitHub Actions 工作流，在代码推送时自动构建多架构 Docker 镜像并发布至 GitHub Container Registry (`ghcr.io`)。

## 3. 核心功能需求

### 3.1 存储与丢弃模式 (`storage_mode`)
支持三种可配置的 HTTP 响应体处理模式：
1. `discard`（**默认推荐**）：通过固定大小的极小缓冲区（32KB）循环读取网络响应流，并直接写入 `io.Discard` 丢弃。实现**零磁盘 I/O**，且物理内存占用恒定极低（< 30MB）。
2. `memory`：将下载的数据写入指定临时目录（在 Docker 中挂载为限制大小的 `tmpfs` 内存盘，例如 `/tmp/dl_cache`），单文件下载完成或发生错误后立即调用 `os.Remove` 删除。
3. `disk`：将下载的文件写入指定的持久化硬盘目录（作为可选模式，供有缓存文件需求的用户使用）。

### 3.2 带宽限速、连接数保护与防异常调度
为确保刷流过程**完全不影响家庭日常上网体验**（防止占满带宽及拖垮光猫 NAT 会话表），同时规避 CDN 厂商风控，必须严格实现以下控制机制：

- **全局最大带宽限速 (`max_rate_mbps`，支持 Web 面板实时热修改)**：
  - 使用令牌桶限速器（`golang.org/x/time/rate`）在 `StatsReader` 读取字节层面对所有工作协程的总下行速率进行精准限速（例如 1000M 宽带可设为 `100` Mbps，留出 900M 保障日常使用；设为 `0` 表示不限速）。
  - 在 Web 面板修改 `max_rate_mbps` 时，动态调用 `limiter.SetLimit(...)` 立即生效，无需重启容器。
- **全局与单域名并发连接数限制（保护光猫 NAT 表与防风控，支持 Web 面板动态调整）**：
  - **`max_connections`（全局最大并发连接数，默认 `2`）**：严格限制整个进程同一时刻最多维持的活跃下载连接数。通过 2~3 个高速大厂 CDN 连接即可轻松跑满 100~300 Mbps 带宽，避免大量并发连接打满家用光猫的 NAT 连接跟踪表（Conntrack）导致网页卡顿或丢包。支持在 Web 面板动态调整（调整时平滑增减活跃 worker 数量）。
  - **`per_host_connections`（单域名/单 URL 最大并发数，硬限制为 `1`）**：同一时刻对同一个域名/URL 最多只允许建立 1 条活跃下载连接（通过主机级互斥锁或信号量控制），防止对同一 CDN 发起多线程并发触发单 IP 风控，也避免单源拥塞。
- **底层 HTTP 连接池复用 (`http.Transport`)**：
  - 全局复用同一个配置调优的 `http.Client`，显式配置 `http.Transport`：`MaxIdleConns: 10`、`MaxConnsPerHost: 1`、`IdleConnTimeout: 90 * time.Second`，避免频繁建立和断开短连接产生大量 `TIME_WAIT` 残留连接占用光猫会话表。
- **每日流量配额上限 (`daily_limit_gb`，支持热修改)**：
  - 当今日累计刷取流量达到设定的 GB 阈值（例如 `300` GB 或 `1024` GB）后，自动暂停下载并进入休眠状态；每日当地时间 `00:00`（遵循环境变量 `TZ`）自动清零重置配额。
- **活跃时间段调度 (`schedule`)**：
  - 可配置每日运行的起止小时（`start_hour` 和 `end_hour`，例如 `01` 至 `18` 点以避开晚高峰；需同时兼容跨夜时间段，如 `23` 至 `07` 点）。
- **随机休眠间隔 (`jitter_seconds`)**：
  - 每次单个文件下载完成后，在 `[min, max]` 秒区间内随机休眠一段时间后再发起下一次请求，使下行带宽曲线呈现自然的波动形态。
- **按权重轮询与大文件偏好**：
  - 根据每个 URL 配置的 `weight`（权重），在所有已启用、未处于冷却期且状态健康的 URL 中加权选择下载目标。内置默认配置模板应优先选用各大云厂商公开测速包、高校/云厂商开源镜像站 `.iso` 文件、大厂高频更新客户端安装包等大体积公开直链，以压低 HTTP 请求频次（QPS）。
- **标准请求头**：
  - 请求时附带标准的桌面浏览器 `User-Agent`，正常处理 HTTP 301/302 重定向。

### 3.3 细化到 URL 的流量统计、探活与风控冷却机制
- **精确到 URL 的字节级统计**：
  - 从 `resp.Body` 读取的每一个字节，都必须通过原子操作（`sync/atomic`）精确累加到对应的 `TargetURL` 实例（区分 `today_bytes` 今日流量与 `total_bytes` 历史总流量）以及全局总计中。内存中的计数器需定期（每 10 秒）及在收到进程退出信号（`SIGTERM`/`SIGINT`）时异步刷盘写入 SQLite。
- **实时速率计算**：
  - 基于最近 3 秒的滑动时间窗口，分别计算每个 URL 的实时下载速率以及全局总速率（同时输出 `MB/s` 和 `Mbps`）。
- **定时与手动连通性探活（Health Check）**：
  - 后台每隔 15 分钟自动执行一次探活任务（同时提供网页端手动立即检测接口），向所有 URL 发送携带 `Range: bytes=0-1023` 请求头的轻量 HTTP `GET` 请求（相比 `HEAD` 请求，前 1KB 的 `Range` 请求在各大 CDN 上的兼容性与准确性更高）。
  - 记录每个 URL 的 HTTP 状态码、响应延迟（`ms`）、健康状态（`Healthy` 正常、`RateLimited` 遇到 403/429 限流冷却中、`Dead` 遇到 404/超时/连接重置）以及最近一次报错信息（`error_msg`）。
- **防风控自动冷却与熔断机制（Backoff & Circuit Breaker）**：
  - **403 / 429 限流长冷却**：一旦在下载或探活中收到 `403 Forbidden` 或 `429 Too Many Requests` 状态码，立即将该 URL 标记为 `RateLimited` 并进入**自动冷却期（默认 6 小时，记录 `cooldown_until` 时间戳）**，冷却期间调度器绝对不再请求该链接，避免持续重试导致公网 IP 被 CDN 加入黑名单。
  - **连续错误熔断**：若某个 URL 连续 3 次出现超时、网络重置（`connection reset`）或 `404/5xx` 错误，自动将其标记为 `Dead` 并暂时跳过调度，直到下一次定时或手动探活恢复正常后再重新加入下载队列。

## 4. Web 监控面板与 REST API 设计

### 4.1 REST API 接口规范
- `GET /api/status`：返回引擎当前运行状态（`running` 运行中、`paused` 手动暂停、`sleeping_schedule` 非活跃时段休眠、`quota_reached` 已达今日配额）、当前存储模式、当前全局限速值（`max_rate_mbps`）、当前最大并发连接数（`max_connections`）及当前活跃连接数、全局实时速率（`Mbps` 与 `MB/s`）、今日已刷流量与每日上限、活跃时间段配置。
- `GET /api/targets`：返回所有已配置的 URL 列表及其实时速度、今日流量（`today_bytes`）、历史总流量（`total_bytes`）、健康状态、冷却剩余时间（如有）、响应延迟（`ms`）、权重（`weight`）、启用状态（`enabled`）及错误信息（`error_msg`）。
- `POST /api/targets`：新增一条下载目标 URL（包含 `name`、`url`、`weight`、`enabled`）。
- `PUT /api/targets/{id}`：修改指定 URL 的启用状态、权重、名称或链接地址。
- `DELETE /api/targets/{id}`：删除指定的 URL。
- `POST /api/targets/check`：手动触发对所有 URL 的即时连通性与延迟检测（同时可重置已过期的冷却状态）。
- `GET /api/history`：返回过去 24 小时按小时聚合的历史流量数据，用于前端绘制趋势折线图。
- `POST /api/control`：手动暂停/恢复下载引擎，或动态热更新运行时参数（`max_rate_mbps`、`max_connections`、`daily_limit_gb`、`storage_mode`），更新后同步持久化到配置文件/数据库。

### 4.2 Web 面板界面布局 (`web/index.html`)
1. **顶部品牌栏与核心概览卡片**：
   - 品牌标题：**NetSuck**，副标题标语：*"Because asymmetrical throttling sucks."*
   - 卡片 1（全局速率与控制）：实时全局下行速率（`Mbps` & `MB/s`）、引擎当前状态徽标、一键暂停/恢复按钮。
   - 卡片 2（今日流量配额）：今日已刷总流量（`GB`）与每日上限进度条（支持点击快捷修改每日上限）。
   - 卡片 3（网络与存储策略）：支持在页面上直接调整**全局限速（Mbps）**、**最大并发连接数（1~10）**、**存储/丢弃模式（`纯内存丢弃 / 内存盘 / 硬盘落盘`）**，并展示当前活跃时间段。
2. **URL 节点明细与管理表格（核心区块）**：
   - 表格列：节点名称与 URL、连通状态指示灯（区分正常/冷却中/失效）+ 响应延迟（`ms`）、实时下载速度、**今日已刷流量**、**历史累计流量**、调度权重、启用/禁用开关、删除按钮。
   - 顶部工具栏：提供“添加 URL”弹窗/表单，以及“测试全部连通性”按钮。
3. **可视化统计图表区**：
   - 环形图/柱状图：展示各 URL 的流量贡献占比（直观对比不同 CDN 或软件源分别刷了多少流量）。
   - 折线图：展示过去 24 小时每小时的下行流量变化趋势。

## 5. 配置文件与持久化规则
- 支持通过命令行参数 `-config` 指定配置文件路径，默认值为 `/data/config.yaml`。
- 首次启动时，若 `/data/config.yaml` 不存在，程序自动将镜像内预置的 `/app/config.example.yaml` 复制到 `/data/config.yaml`。
- 启动时将 `config.yaml` 中的初始 `targets` 同步至 SQLite 数据库（`/data/stats.db`），此后用户在 Web 面板中动态增删改的 URL 列表、运行时限速/连接数修改及流量统计数据均持久化保存。

## 6. 仓库目录结构与交付物要求
请按以下目录结构生成完整、可直接编译运行的项目文件：
```text
netsuck/
├── .github/
│   └── workflows/
│       └── docker-publish.yml   # 推送到 main 分支或打 v* 标签时自动构建 linux/amd64,linux/arm64 镜像并推送至 ghcr.io
├── web/
│   └── index.html               # 通过 go:embed 嵌入的完整单文件 Web 监控面板
├── config.example.yaml          # 注释清晰的默认配置模板，内置 10+ 个分散的不同厂商公共大文件/镜像源直链
├── Dockerfile                   # 多阶段构建（golang:1.23-alpine -> alpine:3.20，包含 ca-certificates & tzdata）
├── docker-compose.yml           # 适用于 NAS 部署的编排模板（含 /data 卷映射与可选的 tmpfs 内存盘配置）
├── go.mod
├── main.go                      # 程序入口、命令行参数解析、HTTP 服务与 REST API 路由
├── config.go                    # YAML 配置文件加载、热保存与默认值处理
├── db.go                        # 纯 Go SQLite 表结构初始化、数据持久化与 24 小时历史聚合查询
├── engine.go                    # 动态大小工作协程池、单域名互斥锁、令牌桶限速器、Range 探活器、403/429 冷却退避及原子计数 StatsReader
└── README.md                    # 开源项目说明文档（项目背景与特性、Docker Compose 快速部署指南、网络调优与配置项说明）

## 7. 开发执行步骤
初始化 go.mod（模块名设为 github.com/placeholder/netsuck）并拉取所需依赖（gopkg.in/yaml.v3、golang.org/x/time/rate、modernc.org/sqlite）。
编写 config.go、db.go、engine.go 和 main.go，重点确保：
令牌桶限速器支持运行时动态修改速率；
工作协程池支持运行时动态调整 max_connections 并发数，且严格保证同一时刻单个 Host 最多 1 个连接；
遇到 403/429 触发 6 小时冷却；
多协程并发读写状态时严格保证线程安全（sync.RWMutex 与 sync/atomic）。
编写 web/index.html，采用清爽现代的暗色/亮色自适应风格，每 2 秒自动轮询更新接口数据，支持在页面上直接修改限速 Mbps 与并发连接数，集成 Chart.js 图表与完整的 URL 增删改查交互。
生成 Dockerfile、docker-compose.yml、config.example.yaml、.github/workflows/docker-publish.yml 以及中文/英文双语或中文 README.md。
在本地执行 go mod tidy 和 CGO_ENABLED=0 go build -o netsuck .，确认无任何编译错误后再结束任务。