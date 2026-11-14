# T12：基础设施独立审查 + 最终构建验证（非实现者审查）

你是独立审查员（Dockerfile/compose/CI/README 由 antigravity 实现，你未参与）。仓库 `/Users/strom/Repositories/GitHub/NetSuck`。注意：engine.go 正在并行接受 T8 Codex 审查，可能有小改动——infra 审查先行，最终构建验证放最后；若期间 engine 变动导致构建抖动，等 60s 复跑。

## 审查对象
`Dockerfile`、`docker-compose.yml`、`.github/workflows/docker-publish.yml`、`README.md`。

## 审查依据
`docs/PLAN.md` v3.1 §7（基础设施契约）、§2（依赖/版本约定）、§3（配置表对齐）；`docs/SPEC.md` §2/§6。

## 审查维度
1. **Dockerfile**：多阶段 golang:1.23-alpine→alpine:3.20、CGO_ENABLED=0、无硬编码 GOARCH、ca-certificates+tzdata、非 root、COPY 路径与 ENTRYPOINT 一致、/app/config.example.yaml 就位
2. **compose**：镜像名小写 ghcr.io/stromkuo/netsuck、端口/卷/TZ/restart、tmpfs 可选段与 size 说明
3. **CI workflow**：触发条件（main+v*）、permissions（contents:read+packages:write）、owner 小写化、qemu+buildx、metadata tags（latest+sha / semver）、双架构推送、YAML 语法
4. **README**：配置表与 config.example.yaml/PLAN §3 逐字段对照；部署说明可跟做；网络调优建议合理（限速比例/并发/NAT/冷却）；本地开发路径（-config 用法）
5. **最终构建验证**（最后执行并贴输出）：
```bash
cd /Users/strom/Repositories/GitHub/NetSuck
go mod tidy && git diff --stat go.mod go.sum   # 应无变化（依赖锁定）
GOTOOLCHAIN=local go vet ./...
GOTOOLCHAIN=local go test -race -count=1 ./...
CGO_ENABLED=0 go build -o /tmp/netsuck-final . && ls -la /tmp/netsuck-final
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/netsuck-amd64 . && file /tmp/netsuck-amd64
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/netsuck-arm64 . && file /tmp/netsuck-arm64
```
6. **交付物完整性**：SPEC §6 目录结构逐项对照（.github/workflows/docker-publish.yml、web/index.html、config.example.yaml、Dockerfile、docker-compose.yml、go.mod、main.go、config.go、db.go、engine.go、README.md）

## 实操
只读审查（可复跑构建命令）；不得修改任何文件。

## 输出格式
```
结论：APPROVE | APPROVE_WITH_FIXES | REJECT
【发现清单】（分级+修法；没有写"无"）
【构建验证】每条命令结果
【交付物清单核对】SPEC §6 逐项：存在/缺失
```
