FROM golang:1.22-alpine AS builder

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/anhe-server ./cmd/server

FROM alpine:3.20

RUN addgroup -S anhe && adduser -S anhe -G anhe

WORKDIR /app

COPY --from=builder /out/anhe-server /usr/local/bin/anhe-server
COPY config/config.json /app/config/config.json

RUN mkdir -p /app/data /app/config && chown -R anhe:anhe /app

USER anhe

EXPOSE 8080
VOLUME ["/app/data", "/app/config"]

ENTRYPOINT ["/usr/local/bin/anhe-server"]
CMD ["-addr", ":8080", "-data", "/app/data", "-config", "/app/config/config.json", "-scheduler-interval", "1s"]
