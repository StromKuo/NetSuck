# T8b：engine 修复复审（Codex 刀刃复审——最终门禁）

你此前对 engine.go 的 T8 审查结论为 REJECT（F1-F14 + MINOR-2）。实现者已全部修复（见提交 git log 最新 engine 提交，engine.go 2675 行 + engine_test.go 1282 行，-race 连跑 14 次全绿）。请做聚焦复审：

1. **逐项核对 F1-F14 + MINOR-2**：每项修法是否真正消除你指出的问题（不是表面应付）；实现者自报的修复位置——F1 统一请求执行器(doRequest/eligible:876,900)、F2/F3 持久化协程独建批/Seq(1469,1577)+persistCtx+有界重试、F4/F5 quotaReserve(1223)+ensureDay(1330)+gateTicker 翻日、F6 reqCtx/setBlocked(1003)、F7 可取消 Reserve(1145)、F8 changing+DB 回滚(2453,2615)、F9 EOF/取消分类、F10 cycCancel、F11 磁盘唯一名+错误上报、F12 hourKeyIn、F13(main 侧已另行修复+并发测试)、F14 测试加固、MINOR-2 ErrValidation。
2. **回归检查**：修复是否引入新问题（锁顺序、新代码路径的许可释放、执行器重构对重定向/超时语义的影响）。
3. **测试真实性**：新增测试（TestConcurrentFlushOrdered/TestStopBoundedOnPermanentFlush/TestQuotaReserveNoOverrun/TestMidnightQuotaRecovery/TestGateBlocksCancelInFlight/TestRateSwitchWakesLowWait/TestCRUDDisableCancelsDownload/TestCRUDRollbackOnFlushFailure/TestProbeShortResponseHealthy/TestDSTDuplicateHourBuckets/TestProbeFollowsRedirect 等）是否真实断言了原薄弱点。
4. **RSS 分歧裁决**：实现者用子进程测 discard 路径峰值 RSS **增量**（~1MB）而非总进程 RSS（基线 ~52MB 来自 SQLite/transport），超限 FAIL 不 Skip——接受或要求改测法，给出结论。
5. 独立复跑：`GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race -count=1 ./...`

只读。输出：`APPROVE（engine 模块通过，项目可验收）| APPROVE_WITH_MINOR（列剩余小项，不阻塞验收）| REJECT（列未解决问题）` + 逐项核对表 + 分歧裁决。
