# Staff-Level Systems Interview Deep Dive

## 1. How does Two-Phase Distributed `AVG` remain mathematically exact across arbitrary partition splits?
`AVG` is not associative (`avg(avg(A), avg(B)) != avg(A U B)` whenever `|A| != |B|`). In `processing/operators.py`, Stage 1 (`PartialHashAggregate`) decomposes every `AVG(expr)` into two associative partial accumulators: `__p_sum_i` (float64 sum of non-null values) and `__p_cnt_i` (int64 count of non-null values). Stage 2 (`FinalHashAggregate`) sums both columns across all incoming shuffle buckets and computes `total_sum / total_cnt` only at finalization. This same partial schema powers both network shuffle merges and spill-to-disk bucket merges.

## 2. How does Skew-Aware Salted Shuffle prevent reducer hotspots without breaking `GROUP BY` semantics?
In order analytics, `hot_customer_id = 1` owns `~22%` of all orders (`6,591` orders at `local` scale). Standard `hash(customer_id) % P` routes all rows for customer `1` to one single reducer. When `catalog.hot_keys` flags `customer_id`, DQSE appends a synthetic `__dqse_salt` column (`1..salt_factor` for hot keys, `0` for normal keys), runs Stage 1 partial aggregation on `(customer_id, __dqse_salt)`, shuffles across workers so hot keys process across `salt_factor` parallel buckets in Stage 2 (`SALTED_AGG`), then drops `__dqse_salt` and merges the `salt_factor` partial states into one exact final row in Stage 3.

## 3. Why Consistent Hashing instead of `hash(partition_id) % num_workers`?
With modulo hashing, losing 1 of 3 workers remaps `~67%` of all partitions, thrashing every surviving worker's Parquet scan LRU cache simultaneously. With 64 BLAKE2b virtual nodes per worker (`processing/consistent_hash.py`), losing a worker shifts only that worker's `~33%` share onto surviving preference-list owners, preserving `100%` of cached partitions on unaffected workers (verified via Hypothesis property test `test_property_consistent_hash_minimal_key_movement`).

## 4. What happens if a slow worker finishes a retried task after another worker already committed it?
The Coordinator maintains a thread-safe `committed_keys` set keyed by `(query_id, stage_id, partition_id)` plus partition-indexed response deduplication. Any late or duplicate completion for an already-committed partition is discarded and counted in `duplicate_commits_ignored` (`CHAOS-03`), ensuring at-least-once worker retries yield exact, non-duplicated analytical totals.

## 5. How does DQSE prevent stale reads in its two-tier cache?
Every table mutation (overwrite, incremental append, compaction) atomically increments `current_snapshot` inside SQLite WAL (`models/catalog.py`). Cache keys include `table_versions`, every `VersionedLRUCache.get()` compares entry versions against live `current_versions` and `_table_epochs`, and table mutations broadcast `InvalidateCache` RPCs to all workers (`CHAOS-06` and Hypothesis test `test_property_cache_never_serves_stale_snapshot`).

## 6. How would you evolve DQSE from a 3-worker local engine to a 50-node production cluster?
1. Swap the Python `worker_server.py` data plane with compiled `rust_worker/` DataFusion/Tonic workers using the existing `schemas/query_engine.proto` contract.
2. Replace local filesystem Parquet paths with S3/RustFS object-store URIs and SQLite with PostgreSQL (`imresamu/postgis:17-3.6.1-alpine3.23` already wired in `docker-compose.yml`).
3. Enable mTLS between Coordinator and Workers and externalize Coordinator DAG state for active-standby HA.
4. Add cost-based join reordering (broadcast hash join for small dimensions like `nation` instead of repartitioned shuffle).
