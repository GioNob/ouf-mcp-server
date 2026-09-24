# R4a MCP runtime image. Build from a pinned repository commit.
FROM golang:1.25.13-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/ouf-mcp ./cmd/ouf-mcp

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -g 10005 -S ouf \
    && adduser -u 10005 -S -D -H -G ouf ouf
COPY --from=build /out/ouf-mcp /usr/local/bin/ouf-mcp
USER 10005:10005
WORKDIR /app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ouf-mcp"]
CMD ["server"]
