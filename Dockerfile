# syntax=docker/dockerfile:1
#
# 运行镜像：不在此编译，直接装入 CI 里针对目标架构预编译好的 musl 静态二进制。
# 基础镜像用 alpine（自带 sh 与 apk，便于进入容器排查问题）；二进制静态链接，
# 不依赖镜像内的 libc；aws-lc-rs 加密代码与 webpki 根证书均已编译进二进制。
# 本 Dockerfile 不覆盖基础镜像的 USER；持久卷须对实际运行用户可写。
FROM alpine:3.23

# 程序目录兼工作目录：二进制放在 /opt/dnsbuffer/dnsbuffer，
# 配置里的相对路径（如 database_path = "dnsbuffer.db"）以此目录为基准。
# 注意：不要把卷直接 bind mount 到 /opt/dnsbuffer——会遮住二进制；
# 持久化请在配置中写绝对路径并挂载对应目录（见 README）。
WORKDIR /opt/dnsbuffer

# 构建前由 CI 把对应架构的二进制拷到构建上下文根目录（见 .dockerignore）
COPY dnsbuffer dnsbuffer

# 内置一份示例配置作默认值；生产环境请用 -v 挂载自己的 config.toml 覆盖它
COPY config.example.toml config.toml

# 预建数据目录：默认配置 database_path = "data/dnsbuffer.db" 指向这里，
# Store 不会自动创建父目录，没有它未挂卷的裸跑会启动失败
RUN mkdir -p /opt/dnsbuffer/data

# DNS 服务默认监听 UDP 53，仪表板默认监听 TCP 8080（见 config.example.toml）
EXPOSE 53/udp 8080/tcp

# exec 形式的裸命令名只查 $PATH、不查工作目录，必须写 ./ 或绝对路径
ENTRYPOINT ["./dnsbuffer", "--config", "config.toml"]
