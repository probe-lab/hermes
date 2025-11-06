FROM golang:1.25 AS builder

# Switch to an isolated build directory
WORKDIR /build

# For caching, only copy the dependency-defining files and download depdencies
COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o hermes ./cmd/hermes

# Create lightweight image. Use Debian as base because we have enabled CGO
# above and hence compile against glibc. This means we can't use alpine.
FROM debian:latest

# Create user hermes
RUN apt-get update && apt-get install -y adduser
RUN adduser --system --no-create-home --disabled-login --group hermes
WORKDIR /home/hermes
USER hermes

COPY --from=builder /build/hermes /usr/local/bin/hermes

# Get more CA certificates
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

CMD ["hermes"]
