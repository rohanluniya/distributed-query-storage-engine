# Capacity Model & Resource Budgets

## 1. Host & Cluster Topology Assumptions

| Tier | Workers | Per-Worker Memory Pool | Per-Worker Scan Cache | Coordinator Result Cache | Dataset Size (`lineitem` rows) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| `smoke` | 3 gRPC procs | 64 MiB (0.04 MiB for spill drill) | 64 MiB | 64 MiB | ~6,000 |
| `local` | 3 gRPC procs | 128 MiB (0.5 MiB for Q5 spill) | 64 MiB | 64 MiB | ~120,000 |
| `stress` | 3 gRPC procs | 128 MiB (2.0 MiB for Q5 spill) | 64 MiB | 64 MiB | ~480,000 |

## 2. Memory Accounting Model

Each worker isolates per-task operator allocations using `MemoryPool(max_bytes)`:

- **Hash Aggregation State Footprint:** Modeled conservatively at `bytes_per_group = max(256, 128 + 64 * num_group_cols + 64 * num_agg_slots)`. For a 3,000-group partition with 3 aggregates, peak hash state is `~1.3 MiB`.
- **Spill Threshold & Chunking:** When `memory_pool.try_reserve(delta_bytes)` returns `False`, the operator flushes accumulated partial states into `K = 8` hash-partitioned Arrow IPC files on local SSD (`spill_dir/<query_id>/<task_id>/`), releases 100% of reserved bytes back to the pool, and resumes streaming. Under tight caps (`<= 2 MiB`), streaming batch size automatically shrinks from `2048` rows to `256` rows to cap peak transient memory.
- **Grace Hash Join:** When `min(left, right)` build-side estimate exceeds available pool memory, both sides are hash-partitioned into `K = 8` matching disk buckets and joined bucket-by-bucket.

## 3. Network Shuffle & gRPC Framing Budget

- **gRPC Message Cap:** Both `WorkerService` server and client channels configure `grpc.max_send_message_length` and `grpc.max_receive_message_length` to `128 MiB` (overriding gRPC's 4 MiB default).
- **Two-Phase Aggregation Compression:** Pushing partial aggregation below the shuffle boundary reduces wire bytes from `O(filtered_rows)` to `O(partitions * distinct_groups)`. For low-cardinality analytical queries like `Q1` (15 output groups over ~100K filtered lineitems), two-phase aggregation reduces Arrow IPC shuffle volume by >90%.
- **Consistent-Hash Locality:** `64` virtual nodes per worker balance partition ownership within `±15%` across workers and confine remapping on worker addition/failure to `~1/N` of partitions, preserving worker-local Parquet scan cache warmth.

## 4. Container Resource Limits (`docker-compose.yml`)

- Coordinator: `1 GiB` memory limit.
- Each gRPC Worker (`worker-1..3`): `512 MiB` memory limit (`128 MiB` operator pool + `64 MiB` scan cache + Arrow/gRPC runtime headroom).
- Optional analytics/observability services (`rustfs`, `postgis`, `prometheus`, `grafana`, `loki`, `otel-collector`): `256–512 MiB` each (`< 4.5 GiB` total for full profile).
