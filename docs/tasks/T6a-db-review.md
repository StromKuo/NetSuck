# T6a：db 模块独立审查（非实现者审查）

你是独立审查员（该模块由 opencode/DeepSeek 实现，你未参与）。仓库 `/Users/strom/Repositories/GitHub/NetSuck`。

## 审查对象
`db.go`（706 行）、`db_test.go`（818 行）、go.mod/go.sum 中 sqlite 相关变更（提交见 `git log -- db.go`）。

## 审查依据
1. `docs/SPEC.md` §3.3 统计持久化、§5 持久化规则
2. `docs/PLAN.md` v3.1 **§4（数据库契约——签名/DTO/表结构/持久化语义/播种规则全部冻结）**
3. T1d 已冻结契约，任何签名/字段/语义偏差都是发现

## 审查维度
1. **契约符合性**：Store 方法签名、CounterBatch/EngineRestoreState/TargetRecord 字段与 json/yaml 标签、部分唯一索引、DSN pragma、Seq 幂等/拒绝语义、EnsureSeeded 单事务仅首播
2. **SQL 正确性**：upsert 语句（INSERT ON CONFLICT）、事务边界、并发写互斥、软删过滤、WAL 下读安全
3. **边界**：DST 小时桶、LoadEngineState 恢复完整性（今日+旧日桶、last_seq）、空库/空表行为
4. **测试质量**：13 个测试是否真覆盖上述（找漏测点）

## 实操
- 只读审查 + 独立复跑：`GOTOOLCHAIN=local go test -race -count=1 ./...` 贴输出
- 可用 `sqlite3` 临时库手工验证个别 SQL（可选）
- 不得修改任何文件

## 输出格式
```
结论：APPROVE | APPROVE_WITH_FIXES | REJECT
复跑结果：
【发现清单】（[BLOCKER|MAJOR|MINOR] 文件:行 问题 → 修法；没有写"无"）
```