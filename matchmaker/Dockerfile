FROM golang:latest as builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o worker .

FROM alpine:latest
WORKDIR /root/

RUN apk --no-cache add ca-certificates

COPY --from=builder /app/worker .

CMD ["./worker"]
