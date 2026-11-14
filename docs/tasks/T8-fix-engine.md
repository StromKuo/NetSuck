# T8-fix：engine.go 关键缺陷修复（Codex T8 审查 REJECT 3B+11M）

你此前实现了 engine.go/engine_test.go（T7）。Codex 独立审查（T8）结论 REJECT：3 BLOCKER + 11 MAJOR + 测试加固。你现在负责修复**全部 engine 侧问题**。main.go 的 F13/MINOR-2 由他人并行修复，你不要动 main.go。仓库 `/Users/strom/Repositories/GitHub/NetSuck`。

## 修复清单（编号与审查报告一致，行号基于 ae0469d）

**BLOCKER：**
- **F1 探活绕过准入/互斥**（probeOne 1520-1582、doWithRedirects 775-845）：实现统一带 requestKind 的准入与请求执行器——整个链持目标许可、逐跳持 host 许可，取得许可后**再次**验证 deleted/cooldown/generation/资格；探活同样逐跳重定向（保留 Range 头）；忙目标进入可取消等待或改选（禁止 jitter=0 自旋）；已排队探活在 403 冷却后不得再发请求。
- **F2 建批/Seq/入队可倒序**（buildBatch 1180-1276、flushNow 1296、persistWithRetry 1150）：快照生成、Seq 分配与入队统一串行（建议只让持久化协程建批/分配 Seq）；计数/累计/健康快照用一致锁序取同一版本；成功确认前保留批与脏版本；`errStaleSeq` 不得作为瞬时错误无限重试；补倒序并发测试。
- **F3 管理锁↔持久化重试死锁**（429、1150-1174、2055/2084、2168/2174）：关闭请求/生命周期取消必须能**独立于 adminMu** 推进；同步 flush 可取消或有界返回；永久错误不能无限重试占住管理事务；管理失败恢复准入；Stop 最后有限尝试并错误退出；补永久失败+并发 Stop 时限测试。

**MAJOR：**
- **F4 配额预留**（1001-1015、1032）：countMu 下按日 reserved，Read 前原子预留、按实际 n 结算退还（取消/短读/错误路径都退还）；探活同机制；跨日重绑预算；barrier 双 host 竞争最后 100 字节测试。
- **F5 配额后午夜永不恢复**（698-708、1106）：gateTicker/独立午夜调度在 countMu 内主动 ensureDay 并重算门禁；Snapshot 不报昨日桶；测试=旧日达配额+零读取+快进午夜→自动恢复且今日 0。
- **F6 门禁只置 flag 不取消在途**（674-708、991）：门禁转阻塞时取消当前请求 ctx（Do/WaitN/Read 全生效），保留 worker 待 Resume；测试用可控停流 body。
- **F7 限速热切换无唤醒**（1010、1847-1855）：独立限速版本广播+可取消 Reservation；SetMaxRate 取消未消费预约按新值重算；明确已消费/未用预算；测试=低速旧等待中切不限速断言及时读取，反向验证合计上界。
- **F8 CRUD 无 tombstone/结算**（1410、2077-2112、2167）：独立 changing 标记→先拒准入→取消该目标**全部**下载+探活并等待→清算提交→DB 提交；flush/DB 失败返回并恢复元数据与准入；enabled=false 取消当前下载；改 URL 旧健康记录不得落库；测试覆盖禁用慢流/删除在途探活/改 URL/DB 失败回滚。
- **F9 探活 EOF/取消误判**（1544-1598）：200 短响应 n>0+io.EOF = 成功；先判引擎/CRUD 取消原因再分类远端超时/reset；测试=空 200、短 200、自身超时、Stop 取消四类分别预期。
- **F10 child context 泄漏**（537-553）：每轮请求结束无条件 cycCancel；补多轮请求后内存/context 有界验证。
- **F11 disk 覆盖+清理错误吞掉**（934-966）：最终文件名唯一（时间戳/序号后缀）；Close/Rename/Remove 失败→finalize/abort 返回错误并发布 error_msg；完成标志仅成功后设置；补同 basename/连续下载/Rename 故障测试。
- **F12 DST 重复小时同键**（294、1073；db.go 677）：小时键保留实际偏移/绝对时刻；计数与历史窗口共用实现；测试=2024-11-03T01:30-04:00 与 -05:00 两独立桶。
- **F14 测试加固**：池并发断言不得放宽（got=0 必须失败）；熔断必须 3 次**下载**错误路径；requestCountOf 修正；RSS 测试改独立子进程测**峰值 RSS** 失败断言（不做超限 Skip）；测试内持 countMu 时失败分支的断言不得再取同锁（527-539 挂死风险）；补：引擎 CRUD DB 失败回滚、并发控制事务、倒序建批、永久失败 Stop 时限、探活 302 恢复。

**MINOR-2（engine 侧）**：定义导出 `var ErrValidation = errors.New(...)`；全部校验失败路径 `fmt.Errorf("%w: %v", ErrValidation, err)` 包装；**不得**把 persist/Store/内部错误包装成 Validation。

## 验证（报告附输出）
`GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race -count=1 ./...` 全绿（F1-F12 每项对应的新测试全部真实断言）。

## 报告格式
逐项 F1-F14+MINOR-2：修复位置（函数/行号）+ 对应测试名；vet/race 输出；与审查发现的任何分歧说明（不得静默不修）。
