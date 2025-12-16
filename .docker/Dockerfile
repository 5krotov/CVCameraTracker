# --- BUILD STAGE ---
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o backend ./cmd/main.go

# --- RUN STAGE ---
FROM alpine:latest AS release

COPY --from=builder /app/backend /app/backend/
COPY config.yaml /etc/backend/

WORKDIR /app/backend/

EXPOSE 8081

CMD ["./backend"]