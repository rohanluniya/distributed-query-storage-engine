#!/usr/bin/env bash
set -euo pipefail

export DQSE_DATA_DIR="${DQSE_DATA_DIR:-/app/.local_data/warehouse}"
export DQSE_CATALOG_DB="${DQSE_CATALOG_DB:-/app/.local_data/catalog.sqlite3}"
export DQSE_SPILL_DIR="${DQSE_SPILL_DIR:-/app/.local_data/spill}"
export DQSE_COORDINATOR_HOST="${DQSE_COORDINATOR_HOST:-0.0.0.0}"
export DQSE_COORDINATOR_PORT="${DQSE_COORDINATOR_PORT:-8090}"
export DQSE_WORKERS="${DQSE_WORKERS:-worker-1=127.0.0.1:18091,worker-2=127.0.0.1:18092,worker-3=127.0.0.1:18093}"
export DQSE_SCALE="${DQSE_SCALE:-local}"

mkdir -p "$(dirname "$DQSE_CATALOG_DB")" "$DQSE_DATA_DIR" "$DQSE_SPILL_DIR"

if ! uv run python -c "import os, sys; from models.catalog import MetadataCatalog; sys.exit(0 if MetadataCatalog(os.environ['DQSE_CATALOG_DB']).list_tables() else 1)" 2>/dev/null; then
  echo "[entrypoint] Ingesting clean-room synthetic dataset (scale=${DQSE_SCALE})..."
  uv run python -m services.cli ingest --scale "$DQSE_SCALE" --warehouse-dir "$DQSE_DATA_DIR"
fi

PIDS=()
cleanup() {
  echo "[entrypoint] Stopping gRPC workers..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

for spec in "worker-1:18091" "worker-2:18092" "worker-3:18093"; do
  wid="${spec%%:*}"
  port="${spec##*:}"
  uv run python -m services.worker_server \
    --worker-id "$wid" \
    --host 127.0.0.1 \
    --port "$port" \
    --spill-dir "$DQSE_SPILL_DIR" \
    --max-mem-mb "${DQSE_WORKER_MAX_MEM_MB:-128}" &
  PIDS+=("$!")
done

sleep 1.5
echo "[entrypoint] Starting DQSE Coordinator & Control Tower on ${DQSE_COORDINATOR_HOST}:${DQSE_COORDINATOR_PORT}"
exec uv run uvicorn services.api:app --host "$DQSE_COORDINATOR_HOST" --port "$DQSE_COORDINATOR_PORT"
