FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -o cache-store .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/cache-store ./
EXPOSE 8004
CMD ["./cache-store"]
