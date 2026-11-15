# T14：Web 面板两个体验问题修复（活跃时段可编辑 + 目标列表可排序）

你负责修复用户在 NAS 上实测反馈的两个问题。仓库 `/Users/strom/Repositories/GitHub/NetSuck`。只做这两件事，不要改其他功能。

## 问题一：活跃时段无法设置（功能缺失）

**现状**：`web/index.html` 顶栏只**展示** `活跃时段: HH:00 ~ HH:00`（`#headerScheduleText`，约 line 100/1000），**没有任何编辑入口**。后端 `POST /api/control` 的 `action=update` 也只接受 `max_rate_mbps` / `max_connections` / `daily_limit_gb` / `storage_mode`（main.go:322），**不支持 schedule**。

**要求（前后端都要改）**：
1. **后端**：`POST /api/control` 的 `action=update` 新增对 `schedule` 的支持，接收 `{"action":"update","schedule":{"start_hour":N,"end_hour":N}}`。
   - 校验复用 config.go 已有的 `ValidateSchedule`（`0<=start<=23`、`0<=end<=24`、`start==end` 非法）
   - 走**已有的** `ApplyConfigUpdate(candidate, persist)` 事务路径（与其它参数完全一致：候选 Config → 验证 → 持久化 → 热更新），不得另开旁路
   - 引擎侧需要能热更新 schedule（若 `Engine` 已有 `SetSchedule` 之类方法则复用；没有就新增，语义同其它 setter：立即生效、受管理门禁保护）
   - 错误映射沿用现有规则：非法值 400（`ErrValidation`），持久化失败 500
2. **前端**：把顶栏活跃时段做成**可点击编辑**（与限速输入的交互风格一致）：
   - 点击弹出/内联出现 start / end 两个小时选择（下拉或 number input，0-23 / 0-24）
   - 保存时 `POST /api/control` → 成功 toast + 立即刷新，失败回滚控件值并提示
   - 支持跨夜显示说明（如 `23:00 ~ 07:00（跨夜）`）、`0:00 ~ 24:00` 显示为"全天"
   - 遵循现有 UI 规则：正在输入时不被 2s 轮询打断（现有代码已有此逻辑，保持）
3. **面板卡片3**（网络与存储策略卡片）里若展示活跃时段，同步可编辑（避免出现两个不一致入口——若已有展示则复用同一个编辑弹窗）。

## 问题二：目标列表每秒重排、顺序不稳定，需要点击表头排序

**现状根因**（已确认）：
- 前端 `setInterval` 轮询（web/index.html 约 line 1680/1686），且 `tbody.innerHTML = targets.map(...)`（约 line 1024）按**接口返回顺序**直接渲染
- 后端 `Store.ListTargetRecords` 用 `ORDER BY id`（db.go:286），但引擎 `Snapshot` 构造 targets 时遍历的是 **map**（`for _, t := range e.targets`，engine.go 约 line 2383）——**Go map 遍历顺序随机**，所以每次请求顺序都不同 → 列表每秒乱跳

**要求**：
1. **后端**：`Snapshot` 输出的 targets 必须是**稳定顺序**。最小改动：按 `id` 升序排序后输出（用 `sort.Slice`）。不要依赖 map 遍历顺序。
2. **前端**：表格支持**点击表头排序**：
   - 可排序列：名称、连通状态/延迟、实时速率、今日流量、累计流量、权重
   - 点击切换 升序/降序，当前排序列显示 ▲/▼ 指示
   - **排序状态要跨轮询保持**（用户选了"按实时速率降序"，每秒刷新后仍保持该排序，不能被打回默认）
   - 排序在前端本地完成（数据量小，不新增接口参数）
   - 默认排序：按 id（或名称）稳定升序
   - 数值列按数值排（不是字符串）、名称列按 `localeCompare('zh-CN')` 排
3. **不要**因为排序破坏现有交互：启用开关、删除、编辑仍绑定正确的 target id；demo 模式（`?demo=1`）下排序同样可用。

## 自验证（报告必须附结果）
1. `GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race -count=1 ./...` 全绿（后端改动需补测试：control 接受/拒绝 schedule、Snapshot 输出顺序稳定）
2. `CGO_ENABLED=0 go build` 成功
3. **实测**：起服务，连续请求 `/api/targets` 5 次，断言 id 顺序完全一致（贴输出）；`POST /api/control` 改 schedule 后 `GET /api/status` 的 schedule 已变且重启后仍生效（持久化验证）
4. 前端：`node --check` 校验脚本语法；自查表逐条对照上述功能点

## 报告格式
- 改动文件与行数
- 后端测试输出摘要 + 5 次 /api/targets 顺序一致性实证
- schedule 持久化验证（改→重启→确认）
- 前端自查对照表（每项：已实现/说明）
- 与契约的偏差（应为零）
