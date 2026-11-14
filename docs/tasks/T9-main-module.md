# T9：main 模块（main.go —— 组装、REST API、优雅关停）

你负责实现 NetSuck 的程序入口与 HTTP 层。仓库 `/Users/strom/Repositories/GitHub/NetSuck`，**package main**。config.go/db.go/engine.go 已由他人实现并通过审查——**直接使用其真实类型与函数，不得修改它们**；发现契约矛盾用 ask 问协调者。

## 必读输入
1. `docs/SPEC.md` §4（REST API 与面板）、§5（配置持久化）、§7
2. `docs/PLAN.md` v3.1 **§6（API 与前端契约——路由表/JSON 规则/控制事务/组装顺序逐条精确落实）**、§5（Engine 导出方法）、§4（Store/DTO）
3. 仓库现有 config.go / db.go / engine.go（以真实签名为准）

## 交付物
`main.go`（+如需 `main_test.go`）。**不得修改其他文件**。

## 实现清单
1. **flag**：`-config`（默认 `/data/config.yaml`）。
2. **首启复制**：目标配置缺失 → 建父目录 → 复制 `/app/config.example.yaml`（不存在则尝试工作目录 `config.example.yaml` 作本地开发回退，记日志）→ 绝不覆盖已有文件。
3. **组装**：LoadConfig → OpenStore+InitSchema+EnsureSeeded → NewEngine(cfg, store, client)+Start（引擎内部经 LoadEngineState 恢复）→ HTTP Serve（ListenAddr）。http.Client 由 main 构造（Transport 参数按 PLAN §5 规则 4）传入。
4. **路由**（Go 1.22 ServeMux pattern；未匹配 `/api/` 前缀返回 JSON 404 不回落首页）：
   - `GET /api/status` → `json(engine.Snapshot())`（Targets/SnapshotAt json:"-" 不出现）
   - `GET /api/targets` → `{"targets":[...]}`（Snapshot 的 Targets 序列化）
   - `POST /api/targets` → 解析四必填字段（非法/缺字段 400+error）→ `engine.CreateTarget(TargetPatch{...全指针...})` → 201 `{"ok":true,"id":N}`；ErrDuplicateURL→409；其他错误→500
   - `PUT /api/targets/{id}` → 解析 TargetPatch（缺失=不改；**显式 null=400**；空对象=400）→ `engine.PatchTarget` → 200 `{"ok":true}`；ErrNotFound→404；ErrDuplicateURL→409；验证失败→400；其他→500
   - `DELETE /api/targets/{id}` → `engine.DeleteTarget` → `{"ok":true}`；ErrNotFound→404；其他→500
   - `POST /api/targets/check` → `go engine.CheckAllNow(engineCtx)` 异步 + 立即 200 `{"ok":true}`（ctx 用引擎生命周期派生，不用请求 ctx）
   - `GET /api/history` → `{"history": engine.History24h(time.Now())}`
   - `POST /api/control` → pause/resume 直接调 engine；update 构造候选 Config（当前配置深拷贝+补丁）→ 校验（复用 config.go 导出的 Validate*）+ 目录预检 → `engine.ApplyConfigUpdate(candidate, func() error { return candidate.Save(configPath) })` → 200 `{"ok":true}`；非法→400；persist 失败→500
   - `GET /` 与 `/{$}` → `//go:embed web/index.html` 提供面板（Content-Type text/html）
5. **错误 JSON 统一**：`{"error":"..."}`；405 自动带 Allow（确认 ServeMux 行为，必要时手写）。
6. **优雅关停**：SIGTERM/SIGINT → http.Server.Shutdown（等待在途 handler）→ engine.Stop()（内含最终 flush）→ store.Close()；任一步失败记日志并以非零码退出。新请求在关停开始后拒绝。
7. **日志**：标准库 `log` 到 stdout（启动配置摘要、监听地址、关停步骤、错误）；不引入日志库。

## 自验证（报告附输出）
- `go vet ./...`；`go test ./... -race`（若加 main_test.go：覆盖路由注册、control 事务失败路径、404 JSON、embed 内容非空——用 httptest+临时库）
- `CGO_ENABLED=0 go build -o /tmp/netsuck .` 成功（此时全链路可编译！）
- 手动冒烟：临时目录放配置（拷贝 config.example.yaml、listen :18080、限速 0、并发 1、schedule 全天、配额 0）→ 启动 → `curl` 六个接口各一次贴输出 → SIGTERM 干净退出

## 报告格式
交付文件与行数；构建/测试输出；冒烟六接口输出摘要；与契约偏差（应为零）。
