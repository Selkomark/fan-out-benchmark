#!/bin/bash

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "╔═══════════════════════════════════════════════╗"
echo "║   Fanout Pattern Benchmark: Redis vs NATS    ║"
echo "╚═══════════════════════════════════════════════╝"
echo ""

# Load configuration from .env
if [ -f "$PROJECT_DIR/.env" ]; then
    export $(grep -v '^#' "$PROJECT_DIR/.env" | xargs)
else
    echo "❌ Error: .env file not found"
    exit 1
fi

NUM_SUBSCRIBERS=${NUM_SUBSCRIBERS:-3}
NUM_PUBLISHERS=${NUM_PUBLISHERS:-3}
PUBLISH_DURATION_SECONDS=${PUBLISH_DURATION_SECONDS:-10}

DATA_DIR="$PROJECT_DIR/bench-data"

export BATCH_ID=$(date +%Y%m%dT%H%M%S)

echo "📋 Configuration:"
echo "  Subscribers:  $NUM_SUBSCRIBERS"
echo "  Publishers:   $NUM_PUBLISHERS"
echo "  Duration:     $PUBLISH_DURATION_SECONDS seconds"
echo "  Batch ID:     $BATCH_ID"
echo "  DATA_DIR:     $DATA_DIR"
echo ""

# Ensure host bench-data directory exists at project root (matches docker-compose mounts)
mkdir -p "$DATA_DIR"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Cleanup function
cleanup() {
    docker-compose -f "$PROJECT_DIR/docker/docker-compose.yml" --profile redis-bench --profile nats-bench down -v 2>/dev/null || true
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Function to run Redis benchmark
run_redis_benchmark() {
    echo "🔴 REDIS BENCHMARK"
    echo "─────────────────────────────────────────────"
    
    # Start Redis and subscribers first
    docker-compose -f "$PROJECT_DIR/docker/docker-compose.yml" --profile redis-bench up -d redis > /dev/null 2>&1
    docker-compose -f "$PROJECT_DIR/docker/docker-compose.yml" --profile redis-bench up -d --scale redis-subscriber=$NUM_SUBSCRIBERS redis-subscriber > /dev/null 2>&1
    
    # Wait for subscribers to be ready (give them time to connect and subscribe)
    echo "⏳ Waiting for subscribers to connect..."
    sleep 3
    
    # Start publisher
    docker-compose -f "$PROJECT_DIR/docker/docker-compose.yml" --profile redis-bench up -d redis-publisher > /dev/null 2>&1
    
    # Show publisher is running
    echo "⏳ Publishing for $PUBLISH_DURATION_SECONDS seconds..."
    
    # Wait for publisher to finish (with timeout of duration + 30 seconds buffer)
    local wait_timeout=$((PUBLISH_DURATION_SECONDS + 30))
    local wait_start=$(date +%s)
    while docker ps --filter "name=redis-publisher" --format "{{.Names}}" 2>/dev/null | grep -q redis-publisher; do
        local elapsed=$(($(date +%s) - wait_start))
        if [ $elapsed -gt $wait_timeout ]; then
            echo "⚠️  Publisher timeout after ${elapsed}s, forcing stop..."
            docker-compose -f "$PROJECT_DIR/docker/docker-compose.yml" --profile redis-bench stop redis-publisher 2>/dev/null || true
            break
        fi
        sleep 1
    done
    
    # Get publisher output
    docker-compose -f "$PROJECT_DIR/docker/docker-compose.yml" --profile redis-bench logs redis-publisher 2>/dev/null | grep -E "Configuration|Results|Throughput|Complete" | head -15
    
    # Give subscribers time to finish processing and write final results
    echo "⏳ Waiting for subscribers to finish..."
    sleep 12
    
    # Collect and aggregate results BEFORE cleanup
    aggregate_results "Redis" "redis-subscriber"
}

# Function to run NATS benchmark
run_nats_benchmark() {
    echo ""
    echo "🟢 NATS BENCHMARK"
    echo "─────────────────────────────────────────────"
    
    # Start NATS and subscribers first
    docker-compose -f "$PROJECT_DIR/docker/docker-compose.yml" --profile nats-bench up -d nats > /dev/null 2>&1
    docker-compose -f "$PROJECT_DIR/docker/docker-compose.yml" --profile nats-bench up -d --scale nats-subscriber=$NUM_SUBSCRIBERS nats-subscriber > /dev/null 2>&1
    
    # Wait for subscribers to be ready (give them time to connect and subscribe)
    echo "⏳ Waiting for subscribers to connect..."
    sleep 3
    
    # Start publisher
    docker-compose -f "$PROJECT_DIR/docker/docker-compose.yml" --profile nats-bench up -d nats-publisher > /dev/null 2>&1
    
    # Show publisher is running
    echo "⏳ Publishing for $PUBLISH_DURATION_SECONDS seconds..."
    
    # Wait for publisher to finish (with timeout of duration + 30 seconds buffer)
    local wait_timeout=$((PUBLISH_DURATION_SECONDS + 30))
    local wait_start=$(date +%s)
    while docker ps --filter "name=nats-publisher" --format "{{.Names}}" 2>/dev/null | grep -q nats-publisher; do
        local elapsed=$(($(date +%s) - wait_start))
        if [ $elapsed -gt $wait_timeout ]; then
            echo "⚠️  Publisher timeout after ${elapsed}s, forcing stop..."
            docker-compose -f "$PROJECT_DIR/docker/docker-compose.yml" --profile nats-bench stop nats-publisher 2>/dev/null || true
            break
        fi
        sleep 1
    done
    
    # Get publisher output
    docker-compose -f "$PROJECT_DIR/docker/docker-compose.yml" --profile nats-bench logs nats-publisher 2>/dev/null | grep -E "Configuration|Results|Throughput|Complete" | head -15
    
    # Give subscribers time to write final results
    sleep 12
    
    # Collect and aggregate results BEFORE cleanup
    aggregate_results "NATS" "nats-subscriber"
}

# Function to aggregate results from scaled subscribers (logs fallback)
aggregate_results() {
    local solution=$1
    local sub_service=$2

    echo ""
    echo "📊 $solution Results"
    echo "─────────────────────────────────────────────"

    # Collect results from all scaled subscriber instances
    local total_messages=0
    local count=0
    
    for container in $(docker ps -a --filter "name=${sub_service}" --format "{{.Names}}" 2>/dev/null | sort); do
        local logs=$(docker logs "$container" 2>&1)
        local messages=$(echo "$logs" | grep "Messages Received:" | tail -1 | sed -n 's/.*Messages Received: *\([0-9]*\).*/\1/p')
        local throughput=$(echo "$logs" | grep "Throughput:" | tail -1 | sed -n 's/.*Throughput: *\([0-9.]*\).*/\1/p')
        
        if [ -n "$messages" ] && [ "$messages" -gt 0 ]; then
            total_messages=$((total_messages + messages))
            count=$((count + 1))
            echo "  Instance: $messages msgs, $throughput msg/sec"
        fi
    done
    
    if [ $count -eq 0 ]; then
        echo "  ⚠️  No results collected"
        return 0
    fi
    
    local avg_messages=$((total_messages / count))
    echo "  Instances: $count | Avg Messages: $avg_messages"
    echo ""
}

# Analyze JSON files in /data using DuckDB and print analytics
analyze_with_duckdb() {
    echo ""
    echo "📈 DuckDB Analytics"
    echo "─────────────────────────────────────────────"
    # Start a one-off DuckDB container with the same bench-data volume
    docker run --rm \
      -v "$DATA_DIR":/data \
      duckdb/duckdb:latest \
      duckdb -c "INSTALL json; LOAD json; \
          CREATE OR REPLACE TABLE results AS SELECT * FROM read_json('/data/$BATCH_ID/*.json'); \
          SELECT broker_type, COUNT(*) AS files, SUM(messages_received) AS total_msgs, \
                 ROUND(AVG(throughput_msg_per_sec),2) AS avg_tps \
          FROM results GROUP BY broker_type;"
}

# Run benchmarks
run_redis_benchmark
run_nats_benchmark

# Run analytics over persisted JSON results
analyze_with_duckdb

echo "╔═══════════════════════════════════════════════╗"
echo "║      ✅ Benchmarks Complete!                ║"
echo "╚═══════════════════════════════════════════════╝"

# Remove trap and cleanup explicitly
trap - EXIT
cleanup

