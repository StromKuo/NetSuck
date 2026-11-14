# T8-fix3：engine 第三轮修复（T8c 终审残留 R1-R5 + 测试）

T8c 终审认可了 B1 target 全链许可、M2/M3 核心、RSS 测法（25.8MB 实测达标）。残留 R1-R5 + 测试缺口。**本轮约束调整（协调者授权）：为 R2 可修改 db.go 与 db_test.go**（其余情况仍只动 engine.go/engine_test.go）。行号基于 4adc072。

## 修复清单

- **R1 [BLOCKER] heldHost 重复释放**（1010-1012 vs 956-987）：统一 `releaseHeld`（转存并置空后释放），每条释放分支调用；成功返回时才转移许可所有权；建议改用一次性/带 owner 的 permit 结构（而非 host 字符串+bool）；补 barrier 测试：第一跳释放 A→他人持 A→原请求下一跳取消/忙，断言 A 仍被第二请求持有且第三请求取不到。
- **R2 [MAJOR] 成功改 URL 的崩溃窗口**（2626、2636-2664；db.go 304-348）：**改 db.go**——Store.PatchTarget 增加重置健康的选项（或新方法），使 URL 变更+健康重置（Unknown/cooldown=0/errors=0）+generation 在**同一 DB 事务**提交；engine 侧改为调用该原子方法，成功后内存发布新 generation；旧 generation 健康批不得覆盖（持久化时按 generation 条件更新或纳入同一持久化管理序列）。测试：URL 提交后、健康批前模拟非优雅中断（直接关库重开）→ 重启后新 URL 为 Unknown 无旧冷却。
- **R3 [MAJOR] quota token version 未参与结算**（1298-1324、1360-1379、2411-2415）：token 记录真正 booked 额度与唯一日/版本身份；无限令牌不得释放他人预留；额度/日期切换迁移或取消并结算旧预留；version 在 waitRate 后、Read 前、结算时生效；**只有"实际已达上限"才取消全局在途，"预算暂满"仅阻止新预留不取消合法持有者**。测试：无限旧 token→有限新 token→旧 token 退还序列（不得退掉新预留）；跨午夜双版本读竞争；finite↔0 切换；足额预留+body 延迟 >1s 仍完成读取。
- **R4 [MAJOR] 探活等待侵占网络超时**（2010、2021、974-1001）：本地等待与网络超时分离（不同取消原因/ctx）；取得许可准备发请求时才启动 10s 网络计时并持续到响应体处理结束；总轮次截止归为本地 Skipped 不误判远端超时。测试：等许可接近截止后提供自身耗时 <10s 的正常响应→Healthy。
- **R5 [MAJOR] dirty 集合与数值非同快照**（1557-1628）：脏集合选择与全部计数值复制纳入同一一致性版本（统一 countMu 临界区或版本重试）；**先消除 countMu→stateMu 与文件头声明相反的问题**（不得反向嵌套引入真死锁）。测试：snapshotDirty 后 barrier 跨小时/跨日写入再建批，校验每批累计与涵盖桶同版（非仅最终值）。
- **测试缺口**（逐条）：TestStopConcurrentWithCrudFlushFailure 改不同 URL/禁用并用 persistFn 信号确认已在失败 flush 再 Stop；TestConcurrentFlushOrdered 补 DB 目标累计断言（非内存两次）；TestFlushRetrySameBatch 深拷贝逐字段 DeepEqual（非仅 Seq）；TestQuotaReserveNoOverrun 补 R3 全部场景；TestRateSwitchWakesLowWait 用 barrier 确认预约窗口（deadline 收紧到能证明即时性）；TestDSTDuplicateHourBuckets 真正调用 LoadEngineState+History24h 验证第二次重复小时恢复；TestRedirectSameHostMutex 构造 R1 所有权竞争；RSS 子进程 defer 顺序修正（先 eng.Stop 成功再 store.Close，Stop 错误不忽略）。
- **F15**：锁序注释与实际一致（ensureDay 在 countMu 内取 stateMu 的真实顺序如实声明，或重构消除嵌套）。

## 验证（报告附输出）
`GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race -count=1 ./...` 全绿。报告逐项 R1-R5+测试：修复位置+测试名+db.go 变更说明。
