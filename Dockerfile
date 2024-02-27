FROM golang:1.21 AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN GOARCH=amd64 GOOS=linux go build -o hermes ./cmd/hermes

# Create lightweight container
FROM alpine:latest

RUN adduser -D -H hermes
WORKDIR /home/hermes
RUN chown -R hermes:hermes /home/hermes
USER hermes

COPY --from=builder /build/hermes /usr/local/bin/hermes

CMD hermes