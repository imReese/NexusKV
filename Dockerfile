# ==============================================================================
# Stage 1: Build Go Controlplane Binary
# ==============================================================================
FROM golang:1.24-alpine AS go-builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o nexuskv-server ./cmd/server/main.go

# ==============================================================================
# Stage 2: Build Rust Native Planner FFI Extension
# ==============================================================================
FROM rust:1.80-slim AS rust-builder
WORKDIR /app
COPY rust/ ./rust/
WORKDIR /app/rust
RUN cargo build --release -p bindings-py

# ==============================================================================
# Stage 3: Production Runtime (Python 3.12 + Compiled Binaries)
# ==============================================================================
FROM python:3.12-slim AS production
WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy Go server binary
COPY --from=go-builder /app/nexuskv-server /usr/local/bin/nexuskv-server

# Copy Rust FFI shared library
COPY --from=rust-builder /app/rust/target/release/libbindings_py.so /usr/local/lib/libbindings_py.so

# Copy Python codebase
COPY python/ /app/python/
COPY pyproject.toml /app/
ENV PYTHONPATH=/app/python

# Expose gRPC Controlplane and Prometheus Metrics ports
EXPOSE 9090 9091

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD curl -f http://localhost:9091/metrics || exit 1

ENTRYPOINT ["/usr/local/bin/nexuskv-server"]
