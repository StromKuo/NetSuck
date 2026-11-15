# T15：T14 修复的独立审查（非实现者审查，Codex）

你是独立审查员（该改动由 antigravity/Gemini 实现，你未参与）。仓库 `/Users/strom/Repositories/GitHub/NetSuck`。

## 审查对象
T14 的改动：`main.go`（control 支持 schedule）、`engine.go`（schedule 热更新 + Snapshot 稳定排序）、`web/index.html`（活跃时段编辑 + 表头排序）及相关测试。用 `git log --oneline -3` 与 `git diff HEAD~1` 定位。

## 用户反馈的原始问题（验收基准）
1. **活跃时段无法设置** —— 面板只能看不能改
2. **目标列表顺序每秒乱跳** —— 既不是按速度也不是按流量排，且无排序功能

## 审查维度

### A. 活跃时段（正确性优先）
1. 后端 `action=update` 对 `schedule` 的处理是否**真正复用** `ApplyConfigUpdate` 事务路径（候选 Config → 验证 → 持久化 → 热更新），有无旁路写入
2. 校验是否完整：`0<=start<=23`、`0<=end<=24`、`start==end` 应被拒；非法值是否 400 且**运行态不变**
3. **持久化**：改 schedule 后是否写回 config.yaml；**重启后是否仍生效**（这是关键，用户会重启）
4. 引擎热更新是否真正生效：改完后状态机（`sleeping_schedule`）判断是否用新窗口；跨夜窗口（start>end）语义是否正确；`end=24` 是否按全天/到午夜处理
5. 前端：编辑控件是否受轮询打断（输入中不能被 2s 刷新覆盖）、失败是否回滚、`全天`/`跨夜` 展示是否正确

### B. 列表排序（正确性优先）
1. **后端 Snapshot 输出的 targets 顺序是否稳定**：是否消除了 Go map 随机遍历（这是列表乱跳的根因）；排序键是什么；并发下是否稳定
2. 前端排序：可排序列是否覆盖名称/状态延迟/实时速率/今日流量/累计流量/权重；升降序切换与 ▲▼ 指示是否正确
3. **排序状态跨轮询保持**（用户选了排序，2s 刷新后不能被打回默认）——这点必须实测/核验代码路径
4. 数值列按数值排序（不是字符串比较）；中文名称用 `localeCompare('zh-CN')`
5. 排序后操作按钮（启停/删除/编辑）是否仍绑定正确的 target id（**不能出现点删除删错行**这类严重错误）

### C. 回归风险
1. 后端改动是否影响既有 REST 契约（`/api/targets` 字段不变、既有 control 字段仍工作）
2. `go test -race` 全绿？`go vet` 无告警？
3. 前端改动是否破坏已有功能（CRUD、demo 模式、离线横幅、控件回滚）

## 实操要求
- **只读审查**（可复跑命令、可起服务实测），**不得修改任何文件**
- 独立复跑：`GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race -count=1 ./...`
- **必做实证**：起服务连续请求 `/api/targets` 5 次确认顺序一致；`POST /api/control` 改 schedule → `GET /api/status` 确认 → **重启进程** → 再确认（验证持久化）；尝试用非法 schedule（如 start=25、start=end=10）确认 400 且状态不变

## 输出格式
```
结论：APPROVE | APPROVE_WITH_FIXES | REJECT
【问题一 活跃时段】逐项：通过/缺陷（文件:行 + 修法）
【问题二 列表排序】逐项：通过/缺陷（文件:行 + 修法）
【实证结果】5次顺序一致性 / 持久化重启验证 / 非法输入拒绝 —— 贴关键输出
【回归检查】vet/race 输出摘要 + 契约影响
【发现清单】（[BLOCKER|MAJOR|MINOR] 分级 + 具体修法；没有写"无"）
```
