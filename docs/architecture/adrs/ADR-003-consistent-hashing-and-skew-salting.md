# ADR-003: Virtual-Node Consistent Hashing & Skew-Aware Salted Shuffle

## Status
Accepted

## Context
Random or modulo (`hash(p) % N`) worker assignment invalidates nearly all worker-local Parquet scan caches whenever a worker joins or crashes. Additionally, analytical order workloads exhibit heavy key skew (`hot_customer_id = 1` represents ~22% of all orders), which overloads a single reducer partition under standard hash partitioning.

## Decision
1. **Consistent Hash Ring (`processing/consistent_hash.py`):** Assign each healthy worker `64` BLAKE2b virtual nodes on a 64-bit ring. Partition scans route to the primary owner in `get_preference_list(partition_key)` and fail over deterministically to the next healthy worker if the owner crashes.
2. **Skew-Aware Salted Shuffle (`processing/logical_plan.py` + `services/worker_server.py`):** Catalog tables publish detected `hot_keys` metadata. When `GROUP BY` targets a hot column and `two_phase_agg` is enabled, the optimizer injects a 3-stage salted aggregation plan: hot-key rows receive salt buckets `1..salt_factor`, distribute across workers in Stage 2 (`SALTED_AGG`), and merge cleanly in Stage 3 (`FINAL_AGG`).

## Consequences
- **Positive:** Adding or losing a worker remaps only `~1/N` of cached partitions; hot-key reduction spreads across multiple workers while preserving exact numerical output.
- **Negative / Tradeoffs:** Salted aggregation introduces an extra lightweight merge stage (~13 ms on local scale), making it most beneficial when skew pressure threatens single-worker memory or tail latency on larger partitions.
