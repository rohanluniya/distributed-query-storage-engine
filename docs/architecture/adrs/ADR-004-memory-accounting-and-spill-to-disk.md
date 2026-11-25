# ADR-004: Explicit Memory Pool Accounting & Arrow IPC Spill-to-Disk

## Status
Accepted

## Context
High-cardinality hash aggregations (`Q5: GROUP BY order_id, order_month`) and large hash joins can exhaust worker RAM when unguarded, causing OS OOM kills and cascading task retries.

## Decision
Equip every worker task with a byte-accurate `MemoryPool(max_bytes)` and `SpillManager` (`processing/memory_pool.py`). Hash aggregation and Grace hash join reserve estimated operator bytes before expanding in-memory state. When `try_reserve()` fails, operators partition partial state into `K = 8` hash buckets, flush Arrow IPC files under `spill_dir/<query_id>/<task_id>/`, release memory back to the pool, merge bucket-by-bucket at finalize, and delete temporary spill files in a `finally` guard.

## Consequences
- **Positive:** Hard memory ceilings protect worker stability, exact numerical equivalence is preserved (verified via Hypothesis property tests and DuckDB/DataFusion parity), and `spilled_bytes`/`spill_files` appear directly in query profiles and Prometheus metrics.
- **Negative / Tradeoffs:** Disk spill adds local SSD I/O latency (~1.6× slower than unconstrained in-memory execution on local Q5: 181.1 ms vs 113.1 ms) in exchange for bounded RAM stability.
