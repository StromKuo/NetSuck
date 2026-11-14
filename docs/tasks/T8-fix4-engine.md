# T8-fix4：engine 第四轮修复（T8d 残留 A1-A3 + 边界测试）

T8d 确认 R1、R2 单次 DB 原子性、F15 通过。残留 A1（BLOCKER）+ A2/A3（MAJOR）+ 测试边界。可继续修改 engine.go/engine_test.go/db.go/db_test.go。行号基于 9e0474c。

## 修复清单

- **A1 [BLOCKER] 健康快照与 dirty 纪元错位**（1585-1601、1612-1624、clearDirty；db.go 500-505）：
  - stateMu 读锁覆盖期间同时取得 countMu、snapshotDirty 与全部计数/健康值，逆序解锁（锁序 stateMu→countMu→dirtyMu 已可行）；健康脏纪元与快照值一一对应，旧批成功不得清除新纪元
  - PersistBatch 健康写回加 **generation 条件**（db.go：UPDATE ... WHERE id=? AND generation<=快照gen，或等价屏障），旧健康批不得覆盖原子改链后的新 URL 状态；不能只比较内存 gen——批处理路径同样保护
  - 测试：barrier 卡在健康复制后、dirty 选择前 → 触发 403 / PatchTargetAtomic → 检查 **DB 真实健康与 cooldown**（非仅内存）→ 重启后仍正确
- **A2 [MAJOR] quota version 必须真正生效**（1310-1411、readLoop 1256-1288；`tok.version` 目前生产代码零读取）：
  - 限额/日期切换协调所有在途预算：旧版本 token 在 Read 前**重新校验/预约**；已进入 Read 的请求在新预算放行前取消结算或把最大在途预算迁移到新日/新版本扣除；所有已读字节仍必须统计（只挡新读不截断已读）
  - reserved 按令牌身份验证，真正使用版本协议（qVersion 递增时旧 token 失效或迁移）
  - 边界测试：新上限恰好 100 + 旧无限 token100 + 新有限 token100（不得放行两份重叠预算）；午夜旧 3000 预留 + 新日 10000 上限（合计不得超 10000）
- **A3 [MAJOR] 网络超时在响应头返回即取消 Body**（1017-1026、2073-2084）：
  - 网络 ctx/cancel 所有权随 resp.Body 转移（Body.Close 包装器或显式 cleanup），读体完成/关闭后才 cancel；重定向 Close 当前跳结束网络窗口，下一跳新建；保持"实际发请求→body 处理结束"一段网络期限（请求 ctx 与 readCtx 不得独立失效）
  - 轮次预算到期区分 local round cause 与单跳 10s 网络超时（除等待 host 分支外，其他 DeadlineExceeded 也须正确分类，不得误置 Dead）
  - 测试：httptest flush 200 头 → 100ms 后发短 body → 必须 Healthy 且字节正确；真网络超时置 Dead；CRUD/Stop 取消不改远端健康
- **测试小项**：TestR1RedirectHostOwnership 补 barrier 复现特定释放竞争；DST 恢复时刻改第二次 01:30；A1/A2/A3 的边界测试如上。

## 验证（报告附输出）
`GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race -count=1 ./...` 全绿。报告逐项 A1-A3：修复位置+测试名。
