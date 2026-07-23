# syntax=docker/dockerfile:1
#
# 运行镜像：不在此编译，直接装入 CI 里针对目标架构预编译好的 glibc 二进制。
# 基础镜像用 distroless/cc（含 glibc + libgcc_s，无 shell/包管理器），
# 攻击面小、体积小；aws-lc-rs / ring 的加密代码已静态链接进二进制，无需 OpenSSL。
# 默认以 root 运行，便于绑定特权端口 53。
FROM gcr.io/distroless/cc-debian12

# 构建前由 CI 把对应架构的二进制拷到构建上下文根目录（见 .dockerignore）
COPY dnsbuffer /usr/local/bin/dnsbuffer

# 内置一份示例配置作默认值；生产环境请用 -v 挂载自己的 config.toml 覆盖它
COPY config.example.toml /etc/dnsbuffer/config.toml

# DNS 服务默认监听 UDP 53（见 config.example.toml 的 server.listen）
EXPOSE 53/udp

ENTRYPOINT ["/usr/local/bin/dnsbuffer", "--config", "/etc/dnsbuffer/config.toml"]
