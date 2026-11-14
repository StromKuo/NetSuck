# T8-fix6：engine 第六轮修复（T8f 残留 P1/P2）

T8f 通过 U2 迁移、U1 等待后重校验与有限预留跨日迁移。残留 P1（MAJOR）+ P2（MINOR）。可改 engine.go/engine_test.go/db.go/db_test.go。行号基于 cfff19e。

## 修复清单

- **P1 [MAJOR] 限额切换未协调已进入 Read 的请求**（1300-1309、quotaReserve 无限分支、settleRead 1431-1442、setDailyLimitBytes 2513-2517）：
  - 实现**真正的限额切换事务**（SetDailyLimit / 有限上限下调路径）：①先停止新读取准入 ②取消并等待全部旧读取（含探活与 booked=false 的无限令牌请求）结算 ③发布新限额并恢复按新预算准入。或等价方案：对所有实际 Read 建立**可迁移的在途许可记录**（无限令牌也纳入），新限额发布前安全收紧超出预算的旧读并完成结算
  - 不允许只递增 qVersion；不允许 settle 时截断已读字节统计
  - **验收语义**：SetDailyLimit 成功返回后，不再有超额旧读返回；今日既有统计保留
  - 测试（真实 HTTP，先发头再卡 Body，用"已进入 Read"信号确认）：无限→有限（100B）、有限下调、probeRead 三场景——调用 SetDailyLimit 后旧请求被取消且结算、新请求按新预算放行；测试不得靠"在途字节未交付而统计为 0"通过
- **P2 [MINOR] 重预约读长增大未补等令牌**（重试协议）：
  - "quota 预约→rate 等待→版本复查"组成完整重试协议：需要重预约且新读长**增大**时，重新等待相应令牌后再复查进入 Read（读长收紧可保守多扣）；避免重复扣费或透支
  - 测试：低速限速下旧小配额→切无限，断言本次读取不超过速率预算

## 验证（报告附输出）
`GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race -count=1 ./...` 全绿。报告逐项 P1/P2：修复位置+测试名。
