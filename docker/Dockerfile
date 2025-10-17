# Multi-stage Dockerfile for building Redis and NATS benchmarks
FROM ubuntu:22.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    pkg-config \
    libhiredis-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Build and install NATS C client
WORKDIR /tmp
RUN git clone --depth 1 --branch v3.8.2 https://github.com/nats-io/nats.c.git && \
    cd nats.c && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local -DNATS_BUILD_STREAMING=OFF && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    cd / && \
    rm -rf /tmp/nats.c

# Copy source files from reorganized structure
WORKDIR /app
COPY src/core/benchmark_common.h .
COPY src/core/message_broker.h .
COPY src/brokers/redis_broker.h .
COPY src/brokers/nats_broker.h .
COPY src/config/config.h .
COPY src/apps/publisher.cpp .
COPY src/apps/subscriber.cpp .
COPY src/apps/aggregator.cpp .

# Build unified publisher and subscriber (works with both Redis and NATS)
RUN g++ -std=c++20 -O3 -pthread -o publisher publisher.cpp -lhiredis -lnats
RUN g++ -std=c++20 -O3 -pthread -o subscriber subscriber.cpp -lhiredis -lnats

# Build aggregator (uses nlohmann-json and filesystem)
RUN g++ -std=c++20 -O3 -o aggregator aggregator.cpp

# Runtime stage - minimal image with just the binaries
FROM ubuntu:22.04

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    libhiredis0.14 \
    && rm -rf /var/lib/apt/lists/*

# Copy NATS library from builder
COPY --from=builder /usr/local/lib/libnats.so* /usr/local/lib/
RUN ldconfig

# Copy compiled binaries
WORKDIR /app
COPY --from=builder /app/publisher .
COPY --from=builder /app/subscriber .
COPY --from=builder /app/aggregator .

# Create results directory for collecting subscriber outputs
RUN mkdir -p /app/results

# Make binaries executable
RUN chmod +x publisher subscriber aggregator

# Default command (can be overridden)
CMD ["/bin/bash"]

