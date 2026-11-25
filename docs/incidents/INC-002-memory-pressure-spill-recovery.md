# Incident Report INC-002: Worker Memory Pressure & Bounded Disk Spill Recovery

## Summary
- **Incident Type:** Controlled memory-exhaustion fault drill (`CHAOS-04` and `Q5` benchmark ablation)
- **Fault Injected:** High-cardinality aggregation (`q5_low_memory_spill_agg.sql`, `GROUP BY order_id, order_month`) executed under strict worker memory caps (`0.04 MiB` on `smoke`, `0.5 MiB` on `local`, `2.0 MiB` on `stress`).
- **Outcome:** Zero OOM crashes, zero orphaned spill files, 100% exact numerical equivalence with DuckDB 2.0.0 and DataFusion 55.0.0.

## Measured Evidence
| Scale | Worker Cap | Spilled Bytes | Spill Files Written | Orphaned Files Post-Query | Exact Parity (DuckDB & DataFusion) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| `smoke` (`CHAOS-04`) | `0.04 MiB` | `69,720 B` | `24` | `0` | Exact Match |
| `local` (`Q5` ablation) | `0.5 MiB` | `835,664 B` | `72` | `0` | Exact Match |
| `stress` (`Q5` bench) | `2.0 MiB` | `3,159,032 B` | `72` | `0` | Exact Match |

## Operator Takeaways
- Spill-to-disk successfully traded modest local SSD latency (`113.1 ms` in-memory → `181.1 ms` spilled on `local` scale) for hard memory-ceiling compliance.
- `DqseSustainedSpillToDisk` Prometheus alert (`> 100 MiB` spilled over 5m) surfaces persistent memory under-provisioning so operators can tune `DQSE_WORKER_MAX_MEM_MB`.
