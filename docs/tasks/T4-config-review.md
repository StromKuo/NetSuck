# T4：config 模块独立审查（非实现者审查）

你是独立审查员（该模块由 opencode/DeepSeek 实现，你未参与）。仓库 `/Users/strom/Repositories/GitHub/NetSuck`。

## 审查对象
`config.go`、`config_test.go`、`config.example.yaml`、`go.mod`、`go.sum`（提交 7e6e766 及之后对 config 的改动；用 `git log --oneline -- config.go` 与 `git diff` 辅助定位）。

## 审查依据
1. `docs/SPEC.md` §2、§3.2 配置项、§5 配置持久化
2. `docs/PLAN.md` **v3.1 §3（配置契约）**——逐字段对照：结构体/yaml 标签/默认值/presence 语义/全部 Validate* 函数/schedule 语义/Save 原子写
3. SPEC 对 config.example.yaml 的要求（≥12 直链、注释清晰）

## 审查维度（按优先级）
1. **契约符合性**：字段、默认值、验证规则、presence 语义（显式 0/false 必须保留）是否与 §3 完全一致
2. **正确性**：Save 原子性（临时文件+rename+权限）、边界值（schedule 跨夜/24、jitter min=max、weight=1）、错误信息质量；测试是否真覆盖这些边界
3. **example 质量**：直链数量/厂商分散度/注释完整性/推荐值合理性（可用 curl 抽验 3-5 条直链是否仍 206）
4. **依赖**：go.mod 只含 yaml.v3 直接依赖、go.sum 齐全、GOTOOLCHAIN=local 可构建

## 实操要求
- **只读审查 + 独立复跑验证**：`cd /Users/strom/Repositories/GitHub/NetSuck && GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race -count=1 ./...`，贴输出
- 不得修改任何文件

## 输出格式
```
结论：APPROVE | APPROVE_WITH_FIXES | REJECT
复跑验证结果：
【发现清单】（按严重度；没有写"无"）
- [BLOCKER|MAJOR|MINOR] <文件:行> <问题> → <建议修法>
```