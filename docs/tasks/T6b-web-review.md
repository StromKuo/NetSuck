# T6b：web 面板独立审查（非实现者审查）

你是独立审查员（web/index.html 由 antigravity/Gemini 实现，你未参与）。仓库 `/Users/strom/Repositories/GitHub/NetSuck`。

## 审查对象
`web/index.html`（约 1564+ 行，v1+v2 增补后版本）。

## 审查依据
1. `docs/SPEC.md` §4.2（面板布局与功能）、§4.1（API）
2. `docs/PLAN.md` v3.1 **§6（API 与前端契约：路由/JSON 字段/错误码/UI 规则）**——逐字段对照 fetch 调用
3. `docs/PLAN.md` §5 EngineSnapshot/TargetSnapshot 的 json 标签（前端消费的字段名必须一字不差）

## 审查维度
1. **契约符合性**：所有 fetch 的 URL/方法/请求体/响应字段名与 §6 表格逐一对照（status/targets/history/control/check）；speed_mbps/speed_mb_per_sec/today_bytes/daily_limit_gb/cooldown_remaining_seconds 等拼写
2. **功能完整性**：SPEC §4.2 布局（品牌栏/三卡片/表格列/两图表）；v2 增补 5 项（不限量显示/Unknown 灰灯/控件回滚/409 内联/API 404 JSON 处理）；2s 轮询；demo 模式
3. **JS 质量**：内存泄漏（Chart 实例复用/定时器清理）、XSS（error_msg/name 插入 DOM 是否转义）、竞态（快速切换输入时旧响应覆盖新值）、错误处理完备
4. **健壮性**：除零（total=0 时环形图）、空 targets 空态、大数字（>2^53 精度说明）、时区展示

## 实操
- 只读审查；不得修改任何文件
- 静态验证：提取 `<script>` 内容 `node --check`；如可行用 node 模拟 fetch 做 2-3 个纯函数级抽查（可选）
- 重点产出：字段名不匹配清单（若有）——这是前后端对接的最主要风险

## 输出格式
```
结论：APPROVE | APPROVE_WITH_FIXES | REJECT
【发现清单】（[BLOCKER|MAJOR|MINOR] 行号/区域 问题 → 修法；没有写"无"）
【字段对照表】接口 → 前端用的字段 → 契约字段 → 一致/不一致
```