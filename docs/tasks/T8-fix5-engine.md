# T8-fix5：engine 第五轮修复（T8e 残留 U1/U2 + 非阻断小项）

T8e 认可 A1/A3 核心修复。残留 U1/U2（MAJOR）+ 三个非阻断小项。可改 engine.go/engine_test.go/db.go/db_test.go。行号基于 4e68a18。

## 修复清单

- **U1 [MAJOR] 配额版本校验时点错误 + 在途 Read 未跟踪**（1289-1305、2158-2172、2495-2499、1418-1439）：
  - 版本校验/重预约移到 **waitRate 结束后、紧邻实际 Read**；版本变化后重新计算读长及必要令牌
  - 配额/日期切换跟踪**已进入 Read 的在途许可**：放行新预算前取消并等待旧 Read 结算，或将其最大剩余许可迁移扣入新预算；无限量 token 也纳入跟踪
  - 所有已读字节仍必须记录（禁止 settle 时丢弃/截断统计伪造未超限）
  - **生产路径 barrier 测试**（不得手动重新 quotaReserve）：真实调用 readLoop/probeRead——卡在 waitRate 内部→切限额 100→释放等待→读取总量 ≤100；卡住 Body.Read→跨午夜/变更限额→旧+新 reader 合计不超预算
- **U2 [MAJOR] generation 列无旧库迁移**（db.go 112-147、178-186）：
  - InitSchema 幂等版本迁移：`PRAGMA table_info(targets)` 检测缺列 → `ALTER TABLE targets ADD COLUMN generation INTEGER NOT NULL DEFAULT 1`；与写互斥/事务协调；保留 URL/统计/冷却；重复执行无副作用
  - 升级测试：用**旧版本完整 schema**（无 generation 列）建库+目标+冷却+流量→关闭→新 OpenStore+InitSchema+LoadEngineState 打开→generation=1 且历史/今日/cooldown 完全保留→再次 InitSchema 仍成功
- **非阻断小项**（一并修）：
  1. db.go:510 屏障条件精确匹配 generation（0 仅在明确兼容入口归一化，避免无版本批绕过新链接保护）
  2. probe 轮次预算到期区分本地 cause 与单跳网络 timeout（probeNotRemoteFault 目前只识别 Canceled/errProbeSkipped，Do/Body 中的轮次 DeadlineExceeded 可能误置 Dead）
  3. TestA3ProbeBodyDelayHealthy 补客户端实际接收断言（TargetSnapshot.TotalBytes/TodayBytes，非仅 server 写入计数）

## 验证（报告附输出）
`GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race -count=1 ./...` 全绿。报告逐项 U1/U2/小项：修复位置+测试名（U1 测试必须走生产 readLoop 路径）。
