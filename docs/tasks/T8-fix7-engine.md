# T8-fix7：engine 第七轮修复（T8g 残留 E1/E2 + 小项）

T8g 认可 P1 正常取消路径与 P2 代码。残留 E1（MAJOR）+ E2（测试）+ 1 小项。可改 engine.go/engine_test.go。行号基于 bef6c3c。

## 修复清单

- **E1 [MAJOR] 排空失败不得发布**（2536-2545、2558-2567、2571-2603）：
  1. `cancelAllProbes`/`drainInFlightReads`/`applyDailyLimit` 返回 error：任何取消错误或排空超时（activeConns/probes 未归零）→ **不发布新限额、不返回 ok**；保留旧配置并按旧状态恢复门禁
  2. `quotaSwitching` 纳入统一 blocked/eligible 判断（recomputeGate:792 及 recomputeGateLockedQuota 的 blocked 计算须含 quotaSwitching）——周期重算不得在排空中 setBlocked(false) 重建 reqCtx；切换期间不启动新下载/探活
  3. ApplyConfigUpdate 重排为「准备/取消/确认排空 → persist → 无失败发布」（当前先 persist 再可能失败的排空会保存新配置却运行旧配置）；保存失败恢复旧运行参数
  4. 测试：可控慢取消 body/未完成探活触发排空期限 → 断言返回错误、限额不发布、无切换后旧读结算；切换中主动 recomputeGate → 断言 reqCtx 仍取消且无新请求
- **E2 [测试重写] TestP2ReReserveLargerReWaitsRate 未命中"读长增大"分支**（engine_test.go 2271-2300）：
  - 当前：daily_limit=0 起步→SetDailyLimit(100)→SetMaxRate(0)（取消带宽限速≠切配额无限）；读长实际大→小；上界 4MiB 大于源文件总量 1MiB——任何实现都能通过
  - 重写：reader 开始前设**较小有限配额**→耗尽 burst、真实预约进入信号确认等待→等待期间提高到更大有限配额（或正确转无限）、带宽维持低速→记录首次 Read 时间/长度→断言新增字节必须经过差额令牌等待；上界基于实际速率×时间+burst（不是源文件大小）；移除差额 waitRate 的实现必须失败
- **小项**：tightening 表达式改为 `b>0 && (oldLimit<=0 || b<oldLimit)`（当前 `oldLimit>0 && b<oldLimit` 把有限→0 也判收紧，与注释相反）

## 验证（报告附输出）
`GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race -count=1 ./...` 全绿。报告逐项 E1（含 4 子项）/E2/小项：修复位置+测试名。
