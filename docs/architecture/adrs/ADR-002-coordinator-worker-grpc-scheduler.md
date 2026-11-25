# ADR-002: Coordinator-Worker Stage DAG Scheduler over gRPC with Task Leases

## Status
Accepted

## Context
Distributed queries must coordinate parallel scans, peer-to-peer shuffle exchanges, fault recovery on worker crash, and active query cancellation without requiring an external cluster manager during local development.

## Decision
Implement a centralized `QueryCoordinator` (`services/coordinator.py`) communicating over gRPC (`schemas/query_engine.proto`) with `N` independent `WorkerService` processes (`services/worker_server.py`). Execution proceeds stage-by-stage across a compiled `PhysicalDAG`:
- Each task carries a unique `lease_id`, `lease_ttl_ms`, and `attempt` number.
- Upstream stages store hash-partitioned Arrow IPC buckets locally and return compact `ShufflePartitionMeta` descriptors; downstream workers pull required partitions directly from peers via `FetchShuffle`.
- Coordinator tracks committed outputs by `(query_id, stage_id, partition_id)` so duplicate/late completions are ignored idempotently.

## Consequences
- **Positive:** Clean failure isolation per worker process, deterministic retry failover on `UNAVAILABLE` / timeout RPC errors, peer-to-peer shuffle avoiding Coordinator data-plane bottlenecks, and explicit `CancelQuery` RPC broadcast.
- **Negative / Tradeoffs:** The Coordinator is a single failure domain for in-flight query state (disclosed in `docs/limitations.md`); production HA would replicate coordinator state or persist DAG checkpoints externally.
