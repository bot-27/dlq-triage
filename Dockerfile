# Build Stage
FROM golang:1.21-alpine AS builder

WORKDIR /app
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /bin/dlq-triage ./cmd/dlq-triage/main.go

# Production Minimal Stage
FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /bin/dlq-triage /app/dlq-triage
USER nobody:nobody
EXPOSE 9090
ENTRYPOINT ["/app/dlq-triage"]
