# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /build

# 预先下载依赖以提升 Docker 缓存命中率
COPY go.mod go.sum ./
RUN go mod download

# 复制源码并执行静态编译
# 注意：CGO_ENABLED=0 禁用 CGO，GOARCH 由 buildx 目标平台环境变量自动注入，避免硬编码
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o netsuck .

# Runtime stage
FROM alpine:3.20

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

ENTRYPOINT ["/app/netsuck", "-config", "/data/config.yaml"]
