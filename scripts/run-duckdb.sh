#!/bin/bash

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Load configuration from .env
if [ -f "$PROJECT_DIR/.env" ]; then
    export $(grep -v '^#' "$PROJECT_DIR/.env" | xargs)
else
    echo "❌ Error: .env file not found"
    exit 1
fi

# Analyze JSON files in /data using DuckDB and print analytics
analyze_with_duckdb() {
    echo ""
    echo "📈 DuckDB Analytics"
    echo "─────────────────────────────────────────────"
    # Start a one-off DuckDB container with the same bench-data volume

    echo "BATCH_ID: $BATCH_ID"
    echo "PROJECT_DIR: $PROJECT_DIR"
    echo "DATA_DIR: $PROJECT_DIR/bench-data"
    docker run --rm \
      -v "$PROJECT_DIR/bench-data":/data \
      duckdb/duckdb:latest \
      duckdb -c "INSTALL json; LOAD json; \
          CREATE OR REPLACE TABLE results AS SELECT * FROM read_json('/data/$BATCH_ID/*.json'); \
          SELECT broker_type, COUNT(*) AS files, SUM(messages_received) AS total_msgs, \
                 ROUND(AVG(throughput_msg_per_sec),2) AS avg_tps \
          FROM results GROUP BY broker_type;"
}


# Run analytics over persisted JSON results
analyze_with_duckdb


