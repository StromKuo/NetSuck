# T8-fix2：engine 第二轮修复（T8b 复审残留 B1/B2 + M1-M5 + 测试/RSS 验收）

T8b 复审认可了 Seq 管线/门禁取消/EOF/context 释放/DST 键/main 事务/哨兵错误等修复，但 REJECT：残留 2 BLOCKER + 5 MAJOR + 测试与 RSS 验收缺口。你继续修复（只动 engine.go/engine_test.go；RSS 裁决允许改测试基建文件）。行号基于 ee47df6。

## 修复清单

- **B1 [BLOCKER] 全链 target 许可缺失 + cancelTarget 不完整**（900-987、1756-1775、2453-2507、2615-2624）：
  - 独立 **target 许可**覆盖整个请求链（含重定向换 host 期间与 body 读取直至关闭）；host 许可只管当前跳
  - doRequest 在取得许可后**再次**复查 eligible/generation/冷却（消除 取许可→发请求 之间的窗口）
  - cancelTarget 收集并等待该 target **全部**下载+探活（不得 break 首个）；cancelProbe 超时返回错误并阻止 DB 发布
  - 测试：双入口/落点 host + barrier 验证同一 target 全链最大在途 1；CRUD 返回后无旧请求/旧字节
- **B2 [BLOCKER] 改 URL 失败可持久化清除旧冷却**（2488-2518、2545-2562；db.go 304-348 协同）：
  - URL 修改与健康重置在**同一 DB 事务**完成，提交成功后才发布新 generation/Unknown；失败不改变持久化健康（不得"先清健康 flush 成功→再改 URL"）
  - 清算仅保存旧 URL 原健康与已发生计数；DB 失败恢复内存即可（持久化从未被破坏）
  - 测试：RateLimited → 改为重复 URL 409 → 关闭重启，断言原 cooldown_until 仍保留
- **M1 [MAJOR] 配额账本闭环**（1223-1274、1278-1339、2292-2303、2334-2366）：
  - 实际消费始终以 GlobalDays 为准；单独维护**按日** reserved；Reserve 返回 {day, amount, version} 令牌；结算在 countMu 内一次性完成实际入桶+释放对应日预留
  - 午夜重置账本后旧请求退款退对应旧日；finite↔unlimited 切换按实际 globalDays 重建账本；修改限额不重置实际消费
  - 测试：不限量读 1000→设 limit 500 立即停且零新增；旧日预留→午夜→新日 reader 互不干扰；0↔有限热切换；跨日完成读按新日结算
- **M2 [MAJOR] oldSnap 无锁读**（2477-2480）：stateMu.RLock 覆盖整个元数据复制，保存值 DTO 不在锁外解引用共享 target；补并发健康写回与 Patch 的 race 压力测试
- **M3 [MAJOR] rateSig 丢失广播**（1145-1165、2245-2254）：预约前捕获 signal channel/版本，同一速率版本协议检查预约时是否变更（不能先 Reserve 再订阅）；取消后重新预约；短读/EOF 未用令牌定义处理；测试 barrier 卡在 Reserve 与订阅之间切 0，要求旧等待期限前推进
- **M4 [MAJOR] 本地 host 等待误判**（913-987、1907、1922-1928）：重定向计数仅在真实 3xx 变 URL 时递增；许可等待与远端超时分离（等待取消/忙→重排或跳过，不改远端健康）；网络超时从实际发请求起算；测试：长下载占 host、连续多次 notify 零 3xx 不 Dead
- **M5 [MAJOR] 持久化批不同版本**（1484-1536）：统一锁序取全部计数绝对值（TargetTotals 与日/全局/小时桶同一线性化版本）；健康版本不与已变更 URL 错配；消除 countMu→stateMu 与文件头声明的不一致
- **小项**：F11 最终路径保留 CreateTemp 随机后缀（非仅 UnixNano）；F15 锁序注释与实际一致；F16 Start 不忽略传入 ctx
- **M6 测试加固**（逐条）：
  1. TestQuotaReserveNoOverrun：上限=实际基线+500；barrier 竞争最后预算；断言非零推进+正确耗尽
  2. TestStopBoundedOnPermanentFlush：Stop 返回 nil 必须 Fatal（不得 Log）；补 CRUD 持锁等待失败 flush 时并发 Stop
  3. RSS：**非 race** 独立进程测**总峰值 RSS** 并断言 <30MB（实测你已达 ~20.4MB，可行）；覆盖真实 HTTP 下载与指定连接数（不能只空引擎喂 readLoop）；父测试检查子进程退出错误（CombinedOutput 错误不得丢弃）；race 版与增量版保留为辅助
  4. TestRateSwitchWakesLowWait：先确认旧预约已建立，断言在旧等待期限内推进
  5. TestConcurrentFlushOrdered：断言目标累计/全局/小时同版本、每次重试 Seq/内容相同（不只一个日桶）
  6. TestCRUDRollbackOnFlushFailure：补 URL 冲突时 DB 健康已清→重启恢复、并发探活取消、复数下载清算
  7. TestDSTDuplicateHourBuckets：补 LoadEngineState 在重复小时内的恢复窗口（Store localHourStarts 第二重复小时）
  8. TestRedirectSameHostMutex 改不同源 host 验证跨 host 目标互斥
  9. 停流测试补"发送响应头后卡住 body"场景

## 验证（报告附输出）
`GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race -count=1 ./...` 全绿；报告逐项 B1/B2/M1-M6/小项：修复位置+测试名。
