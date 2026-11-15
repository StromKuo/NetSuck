# Build stage
FROM golang:1.23-alpine AS builder

# 版本号由 CI 通过 --build-arg 注入（如 v1.2.3）；本地构建默认 dev
ARG VERSION=dev

WORKDIR /build

# 预先下载依赖以提升 Docker 缓存命中率
COPY go.mod go.sum ./
RUN go mod download

# 复制源码并执行静态编译
# 注意：CGO_ENABLED=0 禁用 CGO，GOARCH 由 buildx 目标平台环境变量自动注入，避免硬编码
COPY . .
RUN CGO_ENABLED=0 go build \
        -ldflags="-s -w -X main.buildVersion=${VERSION}" \
        -trimpath -o netsuck .

# Runtime stage
FROM alpine:3.20

# ARG 不跨阶段继承，runtime 阶段需重新声明才能用于 LABEL
ARG VERSION=dev

# 安装根证书与时区数据
RUN apk --no-cache add ca-certificates tzdata

# 创建非 root 用户 (UID 1000) 与所需工作/数据目录
RUN addgroup -g 1000 netsuck && \
    adduser -u 1000 -G netsuck -s /bin/sh -D netsuck && \
    mkdir -p /app /data && \
    chown -R netsuck:netsuck /app /data

WORKDIR /app

# 从构建阶段复制二进制产物及默认配置模板
COPY --from=builder --chown=netsuck:netsuck /build/netsuck /app/netsuck
COPY --chown=netsuck:netsuck config.example.yaml /app/config.example.yaml

USER netsuck

VOLUME ["/data"]

EXPOSE 8080

LABEL org.opencontainers.image.title="NetSuck" \
      org.opencontainers.image.description="Because asymmetrical throttling sucks. — 为 NAS 打造的轻量级 HTTP/HTTPS 下行流量生成与上下行带宽平衡工具" \
      org.opencontainers.image.source="https://github.com/StromKuo/NetSuck" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"

ENTRYPOINT ["/app/netsuck", "-config", "/data/config.yaml"]
