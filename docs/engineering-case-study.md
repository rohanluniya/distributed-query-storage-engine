# Engineering Case Study: Designing a Distributed Analytical Query Engine from First Principles

## Problem Statement
Modern analytical query engines (Trino, DuckDB, DataFusion, Spark SQL) hide critical distributed systems mechanics behind abstractions: how SQL becomes a multi-stage DAG, why two-phase aggregation cuts network traffic by an order of magnitude, how consistent hashing preserves worker scan-cache locality across failures, how memory pools spill cleanly without corrupting floats or counts, and how caches avoid stale reads when tables mutate.

DQSE implements each of those mechanisms transparently across real independent OS worker processes communicating via gRPC and Apache Arrow IPC, and validates every single result numerically against both DuckDB `2.0.0` and Apache DataFusion `55.0.0`.

## Key Engineering Results

1. **97.85% Shuffle Traffic Reduction via Two-Phase Hash Aggregation:**
   On `Q1` (`local` scale, 120,209 lineitems), pushing partial aggregation (`SUM`, `COUNT`, `MIN`, `MAX`, plus decomposed `(SUM, COUNT)` for `AVG`) onto scan workers reduced Arrow IPC shuffle volume from `4,346,568 B` (`4,244.7 KiB`) to `93,600 B` (`91.4 KiB`) — invariant even at `stress` scale (`479,147` lineitems still shuffled only `93,600 B`!).

2. **9.23× Latency Reduction via Multi-Layer Pruning:**
   Combining catalog partition pruning (`order_month`, `region_id`) with Parquet row-group min/max statistics pruning (`total_price >= 25000.0` on sorted partitions) and column projection pushdown cut `Q2` scan volume from `30,000` rows to `491` rows (`29/30` partitions + `2` row groups skipped) and lowered latency from `21.38 ms` to `2.32 ms`.

3. **Bounded Memory Execution Under Extreme Pressure:**
   Under a `0.5 MiB` worker cap on `Q5` (`local` scale) and `2.0 MiB` cap (`stress` scale), workers spilled `835,664 B` and `3,159,032 B` across `72` partitioned Arrow IPC spill files, cleaned up `100%` of temporary files (`0` orphans), and matched DuckDB and DataFusion bit-for-bit.

4. **Real Multi-Process Fault Recovery (`SIGKILL`):**
   Killing `worker-1` with `SIGKILL` mid-query caused the Coordinator to detect `gRPC UNAVAILABLE`, evict the worker from the consistent-hash ring, retry affected tasks onto surviving workers with fresh task leases, and produce exact results verified against both reference engines (`CHAOS-01`).

## Key Tradeoffs & Lessons Learned
- **Python + gRPC Overhead vs. Native Single-Process Engines:** Measuring head-to-head against DuckDB 2.0 and DataFusion 55.0 made serialization and scheduling overhead unmistakably visible (`~45–120 ms` on `local` joins/aggs vs `1.5–5.8 ms` in-process). The companion `rust_worker/` crate outlines the path to native zero-copy execution under the exact same `schemas/query_engine.proto` wire contract.
- **Idempotency Beats Assumed Exactly-Once Delivery:** Separating at-least-once gRPC task dispatch from Coordinator-side `(query_id, stage_id, partition_id)` idempotent commit tracking prevented duplicate-task double-counting (`CHAOS-03`) without complex two-phase commit protocols on workers.
