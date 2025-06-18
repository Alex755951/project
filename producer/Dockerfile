FROM golang:1.24.1 as builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o producer .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/producer .

EXPOSE 8080
CMD ["./producer"]