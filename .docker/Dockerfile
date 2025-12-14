# --- BUILD STAGE ---
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o agent-service ./cmd/main.go

# --- RUN STAGE ---
FROM alpine:latest AS release

COPY --from=builder /app/agent-service /app/agent-service/
COPY config.yaml /etc/agent-service/

WORKDIR /app/agent-service/

EXPOSE 8080

CMD ["./agent-service"]