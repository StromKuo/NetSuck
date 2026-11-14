# T10：main 模块独立审查 + 集成冒烟（非实现者审查）

你是独立审查员（main.go 由 antigravity/Gemini 实现，你未参与）。仓库 `/Users/strom/Repositories/GitHub/NetSuck`。注意：engine.go/engine_test.go 可能正被 T7 worker 微调——先做 main.go 审查，冒烟放最后；若冒烟因 engine 在途变更而抖动，等 30s 复跑一次再定论。

## 审查对象
`main.go`、`main_test.go`。

## 审查依据
1. `docs/SPEC.md` §4（REST API）、§5、§7
2. `docs/PLAN.md` v3.1 **§6（API 与前端契约：路由表/错误码/控制事务 ApplyConfigUpdate/组装顺序/关停序列）**、§5（Engine 导出方法）、§3（Validate*）
3. 真实的 config.go/db.go/engine.go 签名

## 审查维度
1. **契约符合性**：路由方法/路径/状态码（含 409/500/405+Allow/JSON 404）；PUT 指针语义（显式 null=400、空对象=400）；control 走 ApplyConfigUpdate 且 persist 失败返回 500 运行态不变；check 异步 ctx；embed 面板；首启复制尊重 -config 路径
2. **正确性**：优雅关停顺序（Shutdown→Engine.Stop→Store.Close、失败非零退出）；并发 handler 的配置读写竞争；日志合理；错误 JSON 一致
3. **冒烟**（最后做）：临时目录+config.example.yaml 副本（listen :18080、rate 0、并发 1、全天、配额 0）启动 `CGO_ENABLED=0 go build` 产物 → curl：GET status/targets/history、POST targets（合法+非法 400+重复 409）、PUT enabled=false、DELETE、POST control pause/resume、POST check → 贴关键输出；最后 kill -TERM 验证干净退出
4. **全量验证**：`GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race -count=1 ./...`（全包，含 engine/db/config 测试）贴输出

## 输出格式
```
结论：APPROVE | APPROVE_WITH_FIXES | REJECT
【发现清单】（[BLOCKER|MAJOR|MINOR] 文件:行 问题 → 修法）
冒烟结果摘要（每接口一行：路径→状态码→关键断言）
全量测试输出摘要
```
---

## 审查报告（opencode 独立审查员，2026-11-14）

结论：**APPROVE**

### 【发现清单】
- `[MINOR] main.go:426-437 + 444-451` — control `update` 分支中，存储目录预检（`os.MkdirAll(MemoryDir/DiskDir)`）失败被映射为 **400（Bad Request）**，而此类文件系统错误语义上更贴近 500（服务端 I/O 失败，与 400 的"客户端输入非法"语义不符）。同时该预检在 main 与 `ApplyConfigUpdate`（`precheckStorageDir`）两处重复执行。→ 修法：删除 main 中冗余的 `MkdirAll` 预检，仅依赖 `ApplyConfigUpdate` 内的预检，并将 precheck 失败返回 500。低风险、不影响契约正确性。
- `[MINOR] main.go:533-545` — `isValidationError` 依赖中文错误消息子串匹配（"不能为空"/"必须使用 http/https" 等）来将 `PatchTarget` 的验证错误映射为 400。由于 `handlePutTarget` 对 name/url/weight 已做内联校验，engine 的合并候选校验错误通常不会到达该分支，属低风险兜底；但消息文案一旦重构即可能静默改变错误码。→ 修法：engine 暴露哨兵错误（如 `ErrValidation`）替代字符串匹配，更稳健。建议后续处理，不阻塞本任务。

### 冒烟结果摘要（listen 127.0.0.1:18080、rate=0、conns=1、全天、quota=0，CGO_ENABLED=0 构建产物）
- `GET /api/status` → 200，`{"status":"running","max_rate_mbps":0,"max_connections":1,...}`（Targets/SnapshotAt 未泄漏，json:"-" 生效）
- `GET /api/targets` → 200，`{"targets":[{id,name,url,weight,enabled,health,latency_ms,today_bytes,total_bytes,speed...}]}`
- `GET /api/history` → 200，24 个点，当前小时含真实 bytes，`hour` 带本地偏移 `+08:00`
- `GET /` → 200，`text/html; charset=utf-8`（embed 生效）
- `GET /api/nonexistent` → 404，`{"error":"not found"}`（JSON，不回落首页）
- `POST /api/status` → 405，`Allow: GET, HEAD` + `{"error":"method not allowed"}`
- `POST /api/targets` 合法 → **201** `{"id":2,"ok":true}`
- `POST /api/targets` 重复 url → **409** `{"error":"duplicate url"}`
- `POST /api/targets` ftp url → **400**；缺字段 → **400**；显式 null → **400**
- `PUT /api/targets/2` enabled=false → **200**，随后 GET 确认 `enabled:false` 显式生效
- `PUT /api/targets/2` 空对象 `{}` → **400**；`{"name":null}` → **400**；id 不存在 → **404**
- `DELETE /api/targets/2` → **200**；重复 DELETE → **404**
- `POST /api/control` pause → **200**（status=paused）；resume → **200**（status=running）
- `POST /api/control` update 负 rate → **400**；update rate=50/conns=3 → **200**，状态即时生效且 config.yaml 持久化；unknown action → **400**
- `POST /api/control` update **持久化失败（目录只读）→ 500**，`{"error":"persist config failed: ... permission denied"}`，且运行态 rate **保持不变**（仍 50）→ 验证 B2 事务边界
- `POST /api/targets/check` → **200**（异步）
- `kill -TERM` → 日志顺序 `Step 1/3 shutdown HTTP → Step 2/3 stop engine → Step 3/3 close store → NetSuck stopped cleanly`，进程干净退出、无错误码

### 全量测试输出摘要
- `GOTOOLCHAIN=local go vet ./...` → **VET OK**（零告警）
- `GOTOOLCHAIN=local go test -race -count=1 ./...` → **ok github.com/StromKuo/NetSuck**（42 个测试全绿，含 config 8 + db 11 + engine 17 + main 6，`-race` 通过）
- `CGO_ENABLED=0 go build` → 通过，16.9MB 静态单文件
