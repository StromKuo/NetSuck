# T2：config 模块（go.mod + config.go + config.example.yaml）

你负责实现 NetSuck 项目的配置模块。仓库位于 `/Users/strom/Repositories/GitHub/NetSuck`（module 路径 `github.com/StromKuo/NetSuck`）。

## 必读输入（动手前先完整阅读）
1. `docs/SPEC.md` 全文（重点 §2 技术栈、§3.2 配置项、§5 配置持久化）
2. `docs/PLAN.md` 的 §2（全局约定）与 §3（**配置契约——结构体/yaml 标签/默认值必须逐字段精确实现**）

## 交付物（只创建这 3 个文件，不得动其他文件）
1. `go.mod` —— module `github.com/StromKuo/NetSuck`，go 1.23；依赖：`gopkg.in/yaml.v3`、`golang.org/x/time/rate`、`modernc.org/sqlite`（后两者本任务不 import 也要先进 go.mod？——**否**：只引入本文件实际 import 的 yaml.v3，其余依赖在后续任务 `go mod tidy` 时自然进入）
2. `config.go` —— 按 PLAN §3 契约实现 `LoadConfig` / `(*Config).Save`：
   - 缺字段填默认值；**非法值返回 error**（storage_mode 枚举、hours 越界、jitter min>max、weight<1、max_connections 不在 1-10、daily_limit_gb<0、URL 非 http/https 等）
   - `Save` 原子写：写 `<path>.tmp` 后 `os.Rename`
   - 首次运行复制逻辑（example→config）**不在本文件**，由 main.go 负责；但提供导出的 `DefaultConfig() *Config` 供使用
3. `config.example.yaml` —— ≥12 个直链的默认模板：
   - 覆盖不同厂商：云厂商测速文件（Cloudflare/Hetzner/OVH 等）、开源镜像站 ISO（清华/中科大/华为云/阿里云镜像的 Ubuntu/Debian ISO）、大厂安装包（Go/Node/Python 官方发布包）等大体积公开直链
   - **每条 URL 必须实际验证**：`curl -s -o /dev/null -w '%{http_code} %{size_download}' -r 0-1023 -A '<桌面Chrome UA>' -L --max-time 15 '<URL>'`，只保留返回 200/206 的；把每条验证结果（状态码）写进任务报告
   - 顶部注释块：项目一句话说明 + 各配置项中文说明；每个配置项行内注释

## 验证（报告必须附命令与输出）
```bash
cd /Users/strom/Repositories/GitHub/NetSuck
go mod tidy
go vet ./...
CGO_ENABLED=0 go build ./...   # 此时仓库暂无 main 包，用 go build ./... 验证编译；如无包可 build 则 go vet 通过即可
```
另写一个 `config_test.go`（**允许的例外文件**）：表驱动测试覆盖 LoadConfig 默认值填充、非法值报错、Save→Load 往返一致。运行 `go test ./...` 并附输出。

## 报告格式（worker_done 时）
- 交付文件清单与行数
- go vet / go build / go test 输出摘要
- config.example.yaml 每条 URL 的 curl 验证结果表
- 与契约的偏差（应为零；任何假设单独列出）
