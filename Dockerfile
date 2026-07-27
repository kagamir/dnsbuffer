FROM alpine:3.23
WORKDIR /opt/dnsbuffer
COPY dnsbuffer dnsbuffer
COPY config.example.toml config.toml
RUN mkdir -p /opt/dnsbuffer/data
EXPOSE 53/udp 8080/tcp
ENTRYPOINT ["./dnsbuffer", "--config", "config.toml"]
