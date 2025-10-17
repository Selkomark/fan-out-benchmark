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

# Export variables for docker-compose
export NUM_SUBSCRIBERS
export NUM_PUBLISHERS
export PUBLISH_DURATION_SECONDS

DATA_DIR="$PROJECT_DIR/bench-data"

export BATCH_ID=$(date +%Y-%m-%d_%H-%M-%S)

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
    echo "Cleaning up..."
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
    
    # Wait for subscribers to be ready (Redis Pub/Sub requires subscribers to be fully subscribed before publishing)
    echo "⏳ Waiting for subscribers to connect and subscribe..."
    sleep 5
    
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
}

# Analyze JSON files in /data using DuckDB and print analytics
analyze_with_duckdb() {
    echo ""
    echo "📊 Benchmark Results Summary"
    echo "─────────────────────────────────────────────"
    # Start a one-off DuckDB container with the same bench-data volume
    docker run --rm \
      -v "$DATA_DIR":/data \
      duckdb/duckdb:latest \
      duckdb -c "INSTALL json; LOAD json; \
          CREATE OR REPLACE TABLE results AS SELECT * FROM read_json('/data/$BATCH_ID/*.json'); \
          
          -- Display configuration
          SELECT 
              'Test Configuration' AS info_type,
              MAX(config['num_publishers']) || ' Publisher' AS publishers,
              MAX(config['num_subscribers']) || ' Subscribers' AS subscribers,
              MAX(config['publish_duration_seconds']) || ' seconds' AS duration
          FROM results 
          WHERE role = 'publisher'
          LIMIT 1;
          
          -- Display performance metrics
          SELECT 
              UPPER(broker_type) AS \"Message Broker\",
              format('{:,}', COALESCE(MAX(CASE WHEN role = 'publisher' THEN results['messages_published'] END), 0)) AS \"Messages Sent\",
              format('{:,.0f}', COALESCE(MAX(CASE WHEN role = 'publisher' THEN results['throughput_msg_per_sec'] END), 0)) AS \"Send Rate (msg/s)\",
              format('{:,}', COALESCE(SUM(CASE WHEN role IS NULL THEN messages_received END), 0)) AS \"Total Received\",
              format('{:,.0f}', COALESCE(AVG(CASE WHEN role IS NULL THEN throughput_msg_per_sec END), 0)) AS \"Avg Receive Rate (msg/s)\"
          FROM results 
          GROUP BY broker_type 
          ORDER BY broker_type;" 2>&1 | grep -v "varchar\|int64\|BIGINT\|DOUBLE"
}

# Run benchmarks
run_redis_benchmark
run_nats_benchmark

# Run analytics over persisted JSON results
analyze_with_duckdb

echo "╔═══════════════════════════════════════════════╗"
echo "║      ✅ Benchmarks Complete!                ║"
echo "╚═══════════════════════════════════════════════╝"
echo ""
