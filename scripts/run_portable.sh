#!/usr/bin/env bash
# Zero-Docker portable launcher: syncs uv environment, ingests dataset, starts 3 gRPC workers + Coordinator UI on :8090.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export DQSE_DATA_DIR="${DQSE_DATA_DIR:-$REPO_ROOT/.local_data/warehouse}"
export DQSE_CATALOG_DB="${DQSE_CATALOG_DB:-$REPO_ROOT/.local_data/catalog.sqlite3}"
export DQSE_SPILL_DIR="${DQSE_SPILL_DIR:-$REPO_ROOT/.local_data/spill}"
export DQSE_AUDIT_LOG="${DQSE_AUDIT_LOG:-$REPO_ROOT/.local_data/audit.jsonl}"
export DQSE_COORDINATOR_HOST="${DQSE_COORDINATOR_HOST:-127.0.0.1}"
export DQSE_COORDINATOR_PORT="${DQSE_COORDINATOR_PORT:-8090}"
export DQSE_WORKERS="${DQSE_WORKERS:-worker-1=127.0.0.1:18091,worker-2=127.0.0.1:18092,worker-3=127.0.0.1:18093}"
export DQSE_SCALE="${DQSE_SCALE:-local}"

mkdir -p "$REPO_ROOT/.local_data/logs" "$DQSE_DATA_DIR" "$DQSE_SPILL_DIR"

if [ ! -d "$REPO_ROOT/.venv" ]; then
  echo "[dqse] Syncing Python 3.12 environment via uv..."
  uv sync --frozen
fi

if ! uv run python -c "import os, sys; from models.catalog import MetadataCatalog; sys.exit(0 if MetadataCatalog(os.environ['DQSE_CATALOG_DB']).list_tables() else 1)" 2>/dev/null; then
  echo "[dqse] Generating and ingesting partitioned Parquet dataset (scale=${DQSE_SCALE})..."
  uv run python -m services.cli ingest \
    --scale "$DQSE_SCALE" \
    --warehouse-dir "$DQSE_DATA_DIR" \
    --report-out "$REPO_ROOT/results/dq/dq_report.json"
fi

if [ "${1:-}" = "--daemon" ]; then
  for spec in "worker-1:18091" "worker-2:18092" "worker-3:18093"; do
    wid="${spec%%:*}"
    port="${spec##*:}"
    nohup uv run python -m services.worker_server \
      --worker-id "$wid" \
      --host 127.0.0.1 \
      --port "$port" \
      --spill-dir "$DQSE_SPILL_DIR" \
      --max-mem-mb "${DQSE_WORKER_MAX_MEM_MB:-128}" \
      >"$REPO_ROOT/.local_data/logs/${wid}.log" 2>&1 </dev/null &
    echo "$!" >"$REPO_ROOT/.local_data/${wid}.pid"
  done
  sleep 1.5
  nohup uv run uvicorn services.api:app \
    --host "$DQSE_COORDINATOR_HOST" \
    --port "$DQSE_COORDINATOR_PORT" \
    >"$REPO_ROOT/.local_data/logs/coordinator.log" 2>&1 </dev/null &
  echo "$!" >"$REPO_ROOT/.local_data/coordinator.pid"
  for _ in $(seq 1 30); do
    if curl -fsS "http://${DQSE_COORDINATOR_HOST}:${DQSE_COORDINATOR_PORT}/health" >/dev/null 2>&1; then
      echo "[dqse] Live cluster ready at http://${DQSE_COORDINATOR_HOST}:${DQSE_COORDINATOR_PORT}"
      exit 0
    fi
    sleep 0.5
  done
  echo "[dqse] Coordinator failed to become healthy within 15s" >&2
  exit 1
fi

PIDS=()
cleanup() {
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
echo "[dqse] Serving Control Tower at http://${DQSE_COORDINATOR_HOST}:${DQSE_COORDINATOR_PORT}"
uv run uvicorn services.api:app --host "$DQSE_COORDINATOR_HOST" --port "$DQSE_COORDINATOR_PORT"
