FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o cache-node .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/cache-node ./
EXPOSE 8001
CMD ["./cache-node"]
