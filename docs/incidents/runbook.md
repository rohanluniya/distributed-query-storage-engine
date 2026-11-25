# Operator Runbook

## 1. Alert: `DqseWorkerRingDegraded` (`dqse_workers_healthy < 2`)
1. Check live worker membership:
   ```bash
   curl -fsS -H "X-API-Key: $DQSE_API_KEY_VIEWER" http://127.0.0.1:8090/v1/workers | jq
   ```
2. Inspect local worker logs under `.local_data/logs/worker-*.log` (or `docker compose logs worker-1`).
3. Restart the failed worker process on its assigned port (`18091..18093`) or run `make stop && make daemon`.
4. Confirm `/health` reports `"workers_healthy": 3` and each worker holds `64` vnodes.

## 2. Alert: `DqseSustainedSpillToDisk`
1. Inspect recent query profiles via `GET /v1/queries` to identify queries with `total_spilled_bytes > 0`.
2. Verify `two_phase_agg` and `skew_salting` are enabled on the offending workload.
3. Increase `DQSE_WORKER_MAX_MEM_MB` (default `128`) or add an additional gRPC worker node to split partition state.
4. Confirm `.local_data/spill/` contains zero leftover `.arrow` files after idle periods.

## 3. Corrupt Parquet Fragment or Schema Drift Detected
1. Inspect quarantined items:
   ```bash
   curl -fsS -H "X-API-Key: $DQSE_API_KEY_VIEWER" http://127.0.0.1:8090/v1/dq/quarantine | jq
   ```
2. Inspect `.local_data/warehouse/quarantine/` for rejected file manifests and invalid row JSONL samples.
3. Correct the upstream source batch and re-run deterministic ingestion (`make ingest`) or incremental append; previously committed silver snapshots remain unaffected during investigation.

## 4. Emergency Query Cancellation & Cache Flush
- Cancel a runaway query:
  ```bash
  curl -X POST -H "X-API-Key: $DQSE_API_KEY_ANALYST" http://127.0.0.1:8090/v1/queries/<query_id>/cancel
  ```
- Force global cache invalidation across Coordinator and all Workers:
  ```bash
  curl -X POST -H "X-API-Key: $DQSE_API_KEY_ADMIN" "http://127.0.0.1:8090/v1/cache/invalidate?table_name=*"
  ```
