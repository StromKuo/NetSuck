# T3：NetSuck Web 监控面板（web/index.html 单文件实现）

你负责实现 NetSuck 项目的 Web 监控面板。仓库位于 `/Users/strom/Repositories/GitHub/NetSuck`。

## 必读输入（动手前先完整阅读）
1. `docs/SPEC.md` 的 §4（Web 监控面板与 REST API 设计）——需求来源
2. `docs/PLAN.md` 的 §6（API 与前端契约）——**JSON 字段名与行为已在此定案，必须逐字段遵守**

## 交付物
**仅创建一个文件**：`web/index.html`。不得创建/修改任何其他文件（后端 Go 代码由别人并行实现中）。

## 技术约束（硬性）
- 单文件 HTML；原生 JS + Tailwind CSS（CDN `<script>`）+ Chart.js（CDN）
- 禁止任何 Node 构建链、禁止额外第三方库、禁止外部字体/图标包（图标用内联 SVG）
- 暗/亮色自适应（`prefers-color-scheme`），清爽现代风格
- 所有接口调用遵守 PLAN §6 的 URL、方法、请求/响应 JSON 字段名（一字不差）

## 功能需求（对应 SPEC §4.2）
1. **顶部品牌栏**：NetSuck + 副标语 *"Because asymmetrical throttling sucks."*
2. **三张概览卡片**：
   - 卡片1：全局实时速率（Mbps 与 MB/s 双单位）+ 引擎状态徽标（running/paused/sleeping_schedule/quota_reached 四态，配色区分）+ 一键暂停/恢复按钮（POST /api/control）
   - 卡片2：今日已刷总流量 GB + 每日上限进度条；**点击进度条可快捷修改每日上限**（弹出输入框 → POST /api/control action=update）
   - 卡片3：全局限速 Mbps 输入（可保存）、最大并发连接数（1~10 步进器）、存储模式下拉（纯内存丢弃/内存盘/硬盘落盘 → discard/memory/disk）、当前活跃时间段展示（schedule.start_hour ~ end_hour）
3. **URL 节点表格**（核心区块）：列 = 名称+URL、连通状态指示灯（Healthy 绿/RateLimited 橙/Dead 红/Unknown 灰）+ 延迟 ms、实时速度、今日流量、历史累计流量、权重、启用开关（PUT）、删除按钮（DELETE，需确认）；顶部工具栏：添加 URL（弹窗表单：name/url/weight/enabled → POST）与"测试全部连通性"按钮（POST /api/targets/check 后短暂禁用并等待轮询刷新）；编辑名称/权重/URL（行内或弹窗，PUT）
4. **图表区**（Chart.js）：
   - 环形图：各 URL 历史累计流量占比（无数据显示空态提示）
   - 折线图：过去 24 小时每小时下行流量（GET /api/history，hour 本地格式化，bytes→GB）
5. **轮询**：每 2 秒 GET /api/status + /api/targets；折线图每 60 秒或添加/删除目标后刷新 /api/history
6. **健壮性**：接口失败显示离线横幅并自动重连恢复；数字格式化（bytes→B/KB/MB/GB 自适应、速率保留 1 位小数）；冷却中目标显示剩余冷却时间（cooldown_remaining_seconds → "冷却 5h23m"）；error_msg 悬停提示（title 或 tooltip）

## 自验证要求（必须做并在报告中说明结果）
1. 实现 `?demo=1` 查询参数演示模式：页面顶部出现醒目 "DEMO" 徽标，所有 fetch 替换为内置示例数据生成器（数据随时间小幅随机变化，模拟真实轮询），用于无后端时验证全部 UI 与交互。
2. 用 `node --check` 或等价方式校验内联 JS 语法（可临时把 script 抽出来检查，检查完删除临时文件）；如本机无 node 则用浏览器/其他方式校验并说明。
3. 逐条自查上述功能清单 1-6 是否全部实现，在报告里给出对照表。

## 报告格式（worker_done 时）
- 交付文件路径、行数
- demo 模式说明与自查对照表（功能点 → 已实现/说明）
- JS 语法校验方式与结果
- 你对契约的任何疑问或你做出的假设（不得擅自偏离契约字段名）
