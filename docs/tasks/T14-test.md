# T14-test：补齐 schedule 校验回归测试（小任务）

仓库 `/Users/strom/Repositories/GitHub/NetSuck`。上一个 worker 已修好 `main.go` 的 schedule null 校验，但**没有补对应测试**，你的任务只有一件事：补测试。

## 背景（已确认的现状）
`main.go` 的 `POST /api/control` 在 `action=update` 处理 `schedule` 时已实现（约 line 434-475）：
- 用 `map[string]json.RawMessage` 遍历子字段，`null` 值 → 400 `"field <name> in schedule cannot be null"`
- 未知子字段 → 400 `"unknown field in schedule: <name>"`
- 再用 `*int` 的 scheduleDTO + `DisallowUnknownFields` 解码
- 合法值走候选 Config → ValidateSchedule → ApplyConfigUpdate

我已实测确认以下行为**全部正确**：
| 请求体 | 实际返回 |
|--------|---------|
| `{"action":"update","schedule":{"start_hour":null,"end_hour":7}}` | 400 |
| `{"action":"update","schedule":{"start_hour":23,"end_hour":null}}` | 400 |
| `{"action":"update","schedule":{"start_hour":1,"end_hour":7,"foo":1}}` | 400 |
| `{"action":"update","schedule":{"start_hour":23,"end_hour":7}}` | 200，且状态变 23→7 |

## 任务
在 `main_test.go` 中**新增**（不要改动已有测试）一个测试函数 `TestControlUpdateScheduleValidation`，用 httptest 覆盖：
1. `start_hour: null` → 400，且**运行态 schedule 不变**（用 `/api/status` 或引擎快照断言）
2. `end_hour: null` → 400，且运行态不变
3. 未知子字段 `foo` → 400，且运行态不变
4. 非法范围（`start_hour: 25`、`start_hour: -1`、`end_hour: 25`、`start_hour==end_hour`）→ 400，运行态不变
5. 合法 `23→7` → 200，`/api/status` 的 schedule 变为 `{23,7}`
6. **持久化断言**：合法修改后，配置文件里写入的 schedule 是 `{23,7}`（读文件断言，或复用现有测试的 config 路径方式）

参考 `main_test.go` 里已有的测试写法（如何构造 server/engine/temp config）保持一致风格。

## 验证
```bash
cd /Users/strom/Repositories/GitHub/NetSuck
GOTOOLCHAIN=local go vet ./...
GOTOOLCHAIN=local go test -race -count=1 -run TestControlUpdateScheduleValidation -v .
GOTOOLCHAIN=local go test -race -count=1 ./...
```
全部通过后报告：新增测试的行号范围 + 测试输出。

**约束**：只改 `main_test.go`；如果发现生产代码有问题，报告而不是擅自改。
