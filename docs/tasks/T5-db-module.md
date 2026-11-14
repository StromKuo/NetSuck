# T5：db 模块（db.go —— Store 实现）

你负责实现 NetSuck 的 SQLite 持久化层。仓库 `/Users/strom/Repositories/GitHub/NetSuck`，全部根目录 Go 文件 **package main**。

## 必读输入
1. `docs/SPEC.md` §3.3（统计持久化）、§5（持久化规则）
2. `docs/PLAN.md` v2 **§4（数据库契约——Store 签名与 DTO 已冻结，逐字段精确实现，不得改名/改签名/改标签）**、§2 全局约定

## 交付物（只创建 db.go 与 db_test.go，不得动其他文件）
`db.go` 实现契约的全部类型与函数：
- `OpenStore`（指定 DSN 参数）、`InitSchema`（四张表：targets/daily_stats/history/metadata，列定义见 §4）
- `EnsureSeeded`：单事务；metadata 无 `targets_seeded` 键才导入 cfg.Targets（空列表也写标记）；此后 DB 是唯一权威
- `ListTargetRecords`（过滤 deleted_at）、`CreateTarget`（url 重复→`ErrDuplicateURL`）、`PatchTarget`（nil 字段不改；不存在→`ErrNotFound`）、`SoftDeleteTarget`
- `PersistBatch(CounterBatch)`：**单事务**；daily_stats 按 (day,target)、history 按 hour_ts、targets 的 total_bytes/健康列按 id 全部 **upsert 绝对值**；`Seq` 回退（小于已持久化的最大 Seq）直接返回错误，禁止旧批覆盖新批；健康字段持久化见 TargetHealthPersist
- `History24h(now)`：本地时区最近 24 个钟点小时的桶（含当前未完整小时），补零、最旧在前、不重计；hour_ts 为该本地小时的 Unix 秒（DST 重复/缺失小时按实际时间排序取最近 24 桶）；返回 HistoryPoint（Hour 为 RFC3339 带本地偏移）
- `Close`
- **写入串行化**：Store 内单把写互斥；WAL 模式下读并发安全

`db_test.go`：临时目录建库，覆盖：schema 幂等、播种仅首次（含删除后重启不复活——mock：seed 后 SoftDelete 再 EnsureSeeded 不复活）、CRUD 与错误值（ErrDuplicateURL/ErrNotFound）、PersistBatch 幂等（同批重放结果一致）与 Seq 拒绝旧批、History24h 跨小时补零与边界。`go test ./... -race` 必须通过。

## 验证（报告附输出）
```bash
go vet ./... && go test ./... -race -count=1
CGO_ENABLED=0 go build ./... 2>/dev/null || true   # main 包尚未齐，以 go vet + go test 为准
```
注意：此时仓库可能已有 config.go（T2 产出）；若其类型与本文件依赖的 DTO 冲突，以 PLAN §4 为准修复**你自己的文件**，不得改 config.go。

## 报告格式
交付文件与行数；测试输出摘要；与契约偏差（应为零）。
