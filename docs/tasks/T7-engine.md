# T7：engine 模块（engine.go —— 下载引擎核心，本项目最关键文件）

你负责实现 NetSuck 的下载引擎。仓库 `/Users/strom/Repositories/GitHub/NetSuck`，**package main**。这是全项目并发正确性要求最高的文件，将被独立关键审查（含 `go test -race` 核对），请严格按契约实现。

## 必读输入
1. `docs/SPEC.md` §3 全部（3.1/3.2/3.3 逐条都是需求）
2. `docs/PLAN.md` v2 **§5（引擎契约：导出方法 + 规则 1-20，逐条对照实现）**、§4（Store/DTO，已由 db.go 实现，直接使用其真实签名）、§8（T7 必须自带的并发测试清单）
3. 仓库中已有的 config.go / db.go（用其真实类型，不得重复定义）

## 交付物
`engine.go` + `engine_test.go`（+如需辅助 `engine_test_http.go` 测试基建，均 package main）。**不得修改其他文件**；若发现契约与既有代码矛盾，报告并停下来问协调者（用 ask）。

## 实现要点（PLAN §5 规则 1-20 的落地清单）
- 统一请求准入（下载/自动探活/手动探活都过同一门禁：tombstone/generation/冷却/enabled/Dead 检查，**发请求前重查**）
- host 互斥：小写 hostname 规范化、忽略端口、同域不同 URL 共锁、try-acquire
- 重定向逐跳：Close 前响应→释放旧 host 许可→取新 host 许可→下一跳；≤10 跳；取消/失败路径全部释放
- Transport：MaxIdleConns:10, MaxConnsPerHost:1, IdleConnTimeout:90s, **ForceAttemptHTTP2=false**
- 单一池管理器对账连接数变化（2→1→2 不重复扩容）；活跃计数=请求许可（开始前+1，Body.Close 后-1）；减容协作取消+等回收
- paused/时段外/配额触达 → 取消在途+阻止新请求；Resume 不越过配额/时段；全部等待可取消、不忙循环
- 配额预留-结算（Read 前 min(读长,剩余额度)，按实际读数结算归还）
- 令牌桶：全局单例；**预约-读-归还**（等待在有界 Read 前）；固定 ≤32KiB、≤burst(64KiB)；SetLimit 重算唤醒等待者；先计数后处理等待错误，不漏记
- 字节归属：按读取完成时刻本地 day/小时桶；计数协调锁线性化（桶内 atomic）
- 刷盘管线：唯一持久化协程，10s 一批不可变 CounterBatch（全部脏桶+累计绝对值），失败重试同批，Seq 顺序，Stop 最终批
- 跨日：flush 旧日→持锁翻日→新桶；独立全局日桶维护全局 today（metadata 或独立键），删节点不减全局
- 探活：启动立即探 Unknown；15min+手动；同轮合并；超时 10s；**最多读 1024B**（200 也只读 1024 即关）；探活字节计入统计与限速；冷却到期进入待探活队列，成功才恢复
- 健康状态机：403/429→RateLimited+6h 冷却（**立即持久化**）+取消该目标在途；下载 404/超时/reset/5xx 连续 3 次→Dead；成功重置计数；探活 404/超时/reset→立即 Dead；旧探活成功不得覆盖新冷却（写回前比对 generation 与 cooldown_until）
- 速率采样：唯一协程 500ms 采样**永不清零累计值**，按稳定 target id 环形缓冲，实际时间差 ≤3s 窗口，<2 样本=0，3s 无增量=0，发布用 mutex/atomic pointer，Snapshot 深拷贝
- CRUD 重载：稳定 id 计数对象，只改元数据不重置计数；禁用/删除/改 URL → tombstone→取消在途→结算→最终批→更新 DB；软删；改 URL 递增 generation、清健康、立即探活、旧 generation 结果丢弃
- 存储模式：请求开始捕获策略；统一 StatsReader+32KiB 显式循环（不用 io.CopyBuffer）；memory 用 CreateTemp+完成/出错/取消均 Close+Remove；disk 用 basename 清洗+CreateTemp+`.part`→成功改名
- Stop 序列：拒新→取消并等待全部→最终 flush→返回错误（如有）
- Snapshot：单次统一 now；cooldown_remaining_seconds=max(0,ceil(...)); status 优先级 paused>quota_reached>sleeping_schedule>running

## engine_test.go（§8 清单全覆盖，`go test -race` 必过）
用本地 `httptest` 可控服务 + 可注入时钟（把 time.Now/time.Sleep 抽为可替换的 clock 接口）覆盖：
1. 2→1→2 反复伸缩：实际并发不超上限、不重复扩容
2. 两源重定向到同一 host：互斥仍成立
3. 探活与下载并发：健康状态无脏写
4. 403 后全入口零请求（含手动 CheckAllNow）
5. 连续 3 次错误熔断 + 探活恢复
6. 限速热切换立即生效
7. 暂停/配额触达：在途取消且许可回收（活跃数归零可观测）
8. 午夜/小时边界刷盘不丢不重不跨桶（注入时钟快进）
9. 刷盘失败重试同批（用故障注入 Store 或 mock）+ 退出最终刷盘
10. discard 模式 RSS<30MB 实测（记录测量方法；t.Skip 大内存环境不可行时说明）
（若个别场景需要 mock Store 接口而非直接用 *Store，允许在测试中定义窄接口，生产代码不得为此引入抽象层）

## 验证（报告附输出）
```bash
go vet ./... && go test ./... -race -count=1
```

## 报告格式
交付文件与行数；规则 1-20 逐条自评（已实现+所在行号/函数）；测试清单 1-10 对照表（场景→测试函数名→结果）；vet/test 输出摘要；任何契约疑问。
