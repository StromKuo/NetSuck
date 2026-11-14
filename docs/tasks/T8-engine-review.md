# T8：engine.go 关键实现独立审查（Codex 刀刃审查）

你是本项目的关键实现独立审查员（engine.go 由 opencode/DeepSeek 实现）。仓库 `/Users/strom/Repositories/GitHub/NetSuck`。这是全项目并发正确性最关键的文件，你此前四轮打磨的契约（PLAN v3.1，T1d 冻结）正是为它服务的。

## 审查对象
`engine.go`（2215 行）、`engine_test.go`（798 行）。

## 审查依据
你冻结的 `docs/PLAN.md` v3.1 **§5（规则 1-19 + EngineSnapshot/TargetSnapshot DTO + 导出方法签名）** 与 **§8 T7 测试清单**、`docs/SPEC.md` §3 全部。

## 审查重点（并发正确性优先）
1. **规则 1-19 逐条核对**：每条规则在代码中的落实位置与正确性（准入矩阵、host 互斥与重定向逐跳换锁、池管理器对账、配额预留结算、令牌桶预约-读-归还与热切换唤醒、字节归属与计数协调锁、刷盘管线 Seq/紧急批、跨日独立于刷盘、探活生命周期与状态机、速率采样、CRUD 接管（停准入→结算→DB 失败回滚）、存储模式请求级捕获、History24h 权威视图、Stop 管理门禁、ApplyConfigUpdate 不递归锁）
2. **锁纪律**：锁获取顺序是否全局一致（有无 stateMu↔countMu 类交叉）；permit/许可的每条取消与错误路径是否释放（泄漏=BLOCKER）
3. **测试真实性**：§8 清单 10 项是否真验证了对应行为（不是仅跑通）；可注入时钟/httptest 是否用到位；找漏测场景
4. **原子性与内存模型**：atomic 使用是否正确（有没有 mixed atomic+non-atomic 访问同一字段）
5. **附带裁决**：T10 审查遗留 MINOR-2（main.go isValidationError 用中文子串匹配 engine 校验错误）——是否要求 engine 暴露哨兵错误（如 ErrValidation）修复，给出结论
6. 独立复跑：`GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race -count=1 ./...`

## 实操
- 只读审查（可以复跑命令）；不得修改任何文件
- 发现问题按 [BLOCKER|MAJOR|MINOR] 分级，给出具体修法（文件:行）

## 输出格式
```
结论：APPROVE | APPROVE_WITH_FIXES | REJECT
复跑结果：
【规则核对】1-19 每条：落实位置(函数/行号) + 正确/缺陷
【发现清单】（分级+修法；没有写"无"）
【测试覆盖评估】10 项清单逐项：真实覆盖/薄弱/缺失
【MINOR-2 裁决】修/不修+理由
```