# T11：基础设施（Dockerfile / docker-compose.yml / CI workflow / README.md）

你负责 NetSuck 的交付物文件。仓库 `/Users/strom/Repositories/GitHub/NetSuck`。Go 源码（config.go/db.go/engine.go/main.go）由他人并行实现中，**不得创建或修改任何 .go 文件、web/、docs/PLAN.md**；你的文件不依赖 Go 源码内容，只依赖已冻结契约。

## 必读输入
1. `docs/SPEC.md` §2（技术栈）、§6（目录结构与交付物）、§7
2. `docs/PLAN.md` v3 §2（module/依赖/版本约定）、§7（基础设施契约——逐条精确落实）
3. 仓库现有 `config.example.yaml`（若已由 T2 交付，README 配置表与其对齐；若尚未交付，按 PLAN §3 字段写，后续审查会核对）

## 交付物（只创建这 4 个文件）
1. **Dockerfile**：多阶段 `golang:1.23-alpine`（CGO_ENABLED=0、GOAMD64/GOARM64 由 buildx platform 处理，不要硬编码 GOARCH）→ `alpine:3.20` + ca-certificates + tzdata；二进制 `/app/netsuck`；`config.example.yaml` 置 `/app/config.example.yaml`；非 root 用户运行（UID 1000 常规做法）；`ENTRYPOINT ["/app/netsuck","-config","/data/config.yaml"]`；`/data` 为卷。
2. **docker-compose.yml**：`image: ghcr.io/stromkuo/netsuck:latest`（全小写）；`ports: "8080:8080"`；`volumes: ./data:/data`；注释掉的可选 tmpfs 段（`/tmp/dl_cache`，`tmpfs_size: "2g"` 示例 + memory 模式必须给 tmpfs 上限的中文说明）；`environment: TZ=Asia/Shanghai`（注释说明作用）；`restart: unless-stopped`。
3. **`.github/workflows/docker-publish.yml`**：触发 push main + tag `v*`；`permissions: contents: read, packages: write`；owner 小写化步骤（`${GITHUB_REPOSITORY_OWNER,,}`）；`docker/setup-qemu-action` + `docker/setup-buildx-action`；`docker/login-action` ghcr（`${{ secrets.GITHUB_TOKEN }}`）；`docker/metadata-action` tags（main→latest+sha；v*→semver）；`docker/build-push-action` platforms `linux/amd64,linux/arm64` 推送。
4. **README.md**：中文为主（项目标语保留英文 *Because asymmetrical throttling sucks.*）：项目背景与原理（上下行不对称限速、为何刷下行、零磁盘丢弃设计）；特性列表；快速部署（compose 段落 + ghcr 拉取）；**全部配置项说明表**（与 PLAN §3 字段一一对应，含默认值与 0=不限语义）；网络调优说明（限速建议=带宽的 10-30%、为何并发默认 2、NAT/conntrack 保护、403/429 冷却与风控规避原理）；Web 面板功能简介；本地开发运行（`go build`、`-config` 用法、从 config.example.yaml 起步、软删 URL 重加语义）；License 段（MIT 占位）。

## 自验证（报告附结果）
- `docker-compose.yml` 与 workflow YAML 语法校验（`python3 -c "import yaml,sys; yaml.safe_load(open(sys.argv[1]))" <file>` 或等价）
- Dockerfile 本地不构建（无 daemon），但逐行自查：多阶段、无硬编码架构、COPY 路径与 ENTRYPOINT 一致
- README 配置表与 PLAN §3 逐字段对照自查表

## 报告格式
交付文件清单；YAML 校验结果；README 配置表对照自查；与契约偏差（应为零）。
