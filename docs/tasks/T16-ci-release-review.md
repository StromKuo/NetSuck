# T16：CI / 发版体系独立审查（Codex）

你是独立审查员（本次改动由 Claude Code 协调者实现，你未参与）。仓库 `/Users/strom/Repositories/GitHub/NetSuck`。

## 变更范围（提交见 `git log`，工作树未提交部分也需要审查）
1. **`main.go`**：
   - 新增 `var buildVersion = "dev"`（构建期用 `-ldflags -X main.buildVersion=` 注入）
   - 新增 `-version` flag，打印 `netsuck <version>` 后退出
   - `handleGetStatus` 改为先 Marshal 成 map 再附加 `version` 字段输出
2. **`Dockerfile`**：新增 `ARG VERSION=dev`，编译时 `-X main.buildVersion=${VERSION}`；新增 OCI LABEL（含 `org.opencontainers.image.version`）
3. **`.github/workflows/`**：
   - 删除 `docker-publish.yml`
   - 新增 `ci.yml`：PR + main 的 go 源码路径改动触发，跑 tidy 校验 / vet / test -race / 双架构交叉编译 / `-version` 冒烟
   - 新增 `release.yml`：main 分支代码路径改动触发，读自上条 tag 的 commit 做 Conventional Commits 版本推算（feat→minor，fix/perf/refactor→patch，`!` 或 `BREAKING CHANGE`→major），无可发布改动则跳过；发版后在同一 workflow 的 publish job 构建推送镜像
   - 新增 `tag-release.yml`：`workflow_dispatch` 手动选 patch/minor/major 打 tag
4. **`.dockerignore`**：排除 `.git/`、`.github/`、`docs/`、`*.md`（保留 README）、本地产物、IDE 目录

## 用户诉求（验收基准）
1. **推文档更新不应触发镜像构建**
2. **要能发语义化版本**（自动推算）

## 审查维度（按严重度）

### A. 触发条件正确性（最高优先级）
1. `paths` 过滤是否**真正**覆盖了「不影响二进制」的改动：文档、CI 自身、元文件改动是否都不会触发 `release.yml`？
2. 反过来：**会不会漏掉该触发的改动**？检查 `go:embed` 引用的资源（`web/**`）、`config.example.yaml`（Dockerfile COPY 进镜像）、`go.mod`/`go.sum` 是否都在列表内。有没有其他会改变镜像内容但未列入 `paths` 的文件？
3. `release.yml` 的 `paths` 与 `ci.yml` 的 `paths` 是否应保持一致？现在的差异是否有意为之且合理？
4. **关键风险**：如果某次改动只有 `docs/`，`release.yml` 不触发 → 不会有 tag；但 `ci.yml` 也不触发 → 该次 push 完全没有 CI。这是否可接受？是否应该在 `release.yml` 里也跑一遍测试再发版（当前 publish job 直接构建，**没有依赖 CI 的测试结果**）？这是否是漏洞？

### B. 语义化版本推算逻辑
1. `release.yml` 的 grep 正则是否准确匹配 Conventional Commits？`^feat(\(.*\))?:` 能匹配 `feat: x` 和 `feat(api): x` 吗？`^[a-zA-Z0-9_-]+(\(.*\))?!:` 能匹配 `feat!: x` 吗？`BREAKING CHANGE:` 出现在 commit body 时能否被检出（当前用 `%b` 拼进同一个变量再用 `grep`，多行是否会被正确处理）？
2. **合并提交/多行处理**：`git log --pretty=format:'%s%n%b%n---END---'` 后接 `grep -qE '^...'` —— `grep` 是逐行匹配的，`^` 是否意味着「每个 commit 的 subject 都单独成行」（正确），还是会被 `---END---` 干扰？
3. 首次发版（无任何 tag 时从 `v0.0.0` 起算）逻辑是否正确？`range` 为空时是否遍历全部历史导致误判？
4. 版本号解析 `cut -d. -f3`：若 tag 是 `v1.2`（非法两段）会不会出错？是否需要校验现有 tag 格式？
5. **并发安全**：`concurrency: group: release` 是否足以防止两次 push 同时打 tag？如果中途失败（tag 已推、Release 未建），重跑会怎样？

### C. 发布链路完整性
1. `release.yml` 中 `publish` job 的 `metadata-action` 使用 `value=${{ needs.release.outputs.new_tag }}` 配合 `type=semver` —— `value` 传入带 `v` 前缀的 tag，`pattern={{version}}` 会正确产出 `1.2.3`（去掉 v）吗？
2. `build-args: VERSION=${{ needs.release.outputs.new_tag }}` 与 Dockerfile 的 `ARG VERSION` / LABEL 是否一致？
3. `publish` job 不复用 `ci.yml` 的测试，直接构建 —— 是否应该在 publish 前也跑 test？
4. GHCR 权限、`GITHUB_TOKEN` 是否足够（`packages: write` + `contents: write`）？bot 推送 tag 是否会被 branch protection 阻挡？

### D. 代码改动正确性
1. `-version` flag 与既有 `-config` flag 是否冲突？`flag.Parse()` 调用位置是否正确？
2. `handleGetStatus` 改成 Marshal→Unmarshal→塞字段→再 Marshal：是否有更简洁且安全的写法？错误处理是否完备（`json.Marshal(buildVersion)` 的错误被忽略了）？是否会破坏既有 `/api/status` 的字段契约或性能（每 2 秒轮询一次）？
3. `buildVersion` 是包级变量被 handler 读取 —— 有无数据竞争（只读不写，理论上安全，确认）？
4. `.dockerignore` 排除 `*.md` 但保留 README —— `!README.md` 在 `*.md` 之后是否能正确生效（dockerignore 的否定规则顺序语义）？排除 `docs/` 是否影响 `go:embed`（确认 embed 只引用 `web/index.html`）？

## 实操要求
- **只读审查**，不得修改任何文件
- 独立复跑：`go vet ./...`、`go test -race -count=1 ./...`、`CGO_ENABLED=0 go build -ldflags "-X main.buildVersion=v9.9.9" -o /tmp/t16 . && /tmp/t16 -version`
- 用 `git log` 找出实际 commit message，**验证你的正则是否能正确分类这些真实 commit**（把结果贴出来）
- 可用 `act`/本地脚本模拟 version 推算逻辑，或手写 shell 验证关键分支

## 输出格式
```
结论：APPROVE | APPROVE_WITH_FIXES | REJECT
【发现清单】[BLOCKER|MAJOR|MINOR] 文件:行 问题 → 具体修法
【触发条件核对】列出所有会改变镜像内容的路径 → 是否在 paths 中
【版本推算验证】用真实 commit 跑一遍正则的结果
【落空风险】哪些场景下发版会失败或漏发
```
