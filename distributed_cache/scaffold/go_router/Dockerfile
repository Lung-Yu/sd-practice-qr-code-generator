FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o cache-router .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/cache-router ./
EXPOSE 8000
CMD ["./cache-router"]
