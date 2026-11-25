# Interactive Live Demo Script (5 Minutes)

## 1. Start the 3-Worker gRPC Cluster & Control Tower (Zero Docker)
```bash
./scripts/run_portable.sh --daemon
curl -fsS http://127.0.0.1:8090/health | jq
```
Expected output: `"status": "healthy"`, `"workers_healthy": 3`, and snapshot versions `v1` for `nation`, `customer`, `orders`, and `lineitem` (`152,221` total rows).

## 2. Inspect EXPLAIN Plan & Partition Pruning via CLI
```bash
uv run python -m services.cli explain --sql "$(cat sql/queries/q2_partition_pruned_point.sql)"
```
Observe `LogicalPlan (Unoptimized)` (`30/30` partitions, `11/11` columns) contrasted with `OptimizedLogicalPlan` (`1/30` partitions, `6/11` columns) and `PhysicalDistributedDAG`.

## 3. Execute Live Distributed SQL with DuckDB & DataFusion Parity Check
```bash
curl -fsS -X POST http://127.0.0.1:8090/v1/query \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-analyst-key-change-me" \
  -d "{\"sql\": $(jq -Rs . < sql/queries/q3_distributed_join_agg.sql), \"verify_reference\": true}" | jq '.row_count, .profile.total_duration_ms, .profile.partitions_pruned, .profile.reference_parity'
```
Observe `20` rows returned, `20/36` partitions pruned, and `"exact_match": true` against both DuckDB `2.0.0` and Apache DataFusion `55.0.0`.

## 4. Demonstrate RBAC Enforcement
```bash
# Unauthenticated -> 401
curl -i http://127.0.0.1:8090/v1/overview
# Viewer attempting admin cache flush -> 403
curl -i -X POST -H "X-API-Key: dev-viewer-key-change-me" http://127.0.0.1:8090/v1/cache/invalidate
```

## 5. Open the Visual Control Tower
Open `http://127.0.0.1:8090/` in a browser:
1. Click **Q2 Pruned Point** → observe `29/30` partitions pruned and `2` Parquet row groups skipped.
2. Click **Q4 Skew Salt** → switch to **Physical DAG** tab to inspect the 3-stage `SALTED_AGG` plan mitigating `hot_customer_id = 1`.
3. Click **Q5 Spill-to-Disk** (`Mem Cap = 0.5 MB`) → observe live `spilled_bytes` (`~816 KiB`, `72` spill files) and `100% Exact` parity with DuckDB & DataFusion.
4. Toggle **Cache** ON and click **Run Distributed Query** twice → observe warm latency drop from `~60 ms` to `< 2 ms` (`Result Cache HIT`).

## 6. Run the 8-Drill Automated Chaos Suite
```bash
uv run pytest tests/chaos -v
cat results/chaos/chaos_results.json | jq '.drills[] | {id: .drill_id, name: .name, passed: .passed}'
```
