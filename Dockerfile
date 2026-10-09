# R4a MCP runtime image. Build from a pinned repository commit.
FROM golang:1.26.9-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/ouf-mcp ./cmd/ouf-mcp \
    && mkdir -p /runtime-rootfs/tmp \
    && chmod 1777 /runtime-rootfs/tmp

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /runtime-rootfs/ /
COPY --from=build /out/ouf-mcp /usr/local/bin/ouf-mcp
USER 10005:10005
WORKDIR /app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ouf-mcp"]
CMD ["server"]
