# ADR-005: Bounded Versioned LRU Cache with Snapshot Epoch Invalidation

## Status
Accepted

## Context
Repeated analytical queries over immutable Parquet partitions pay redundant disk I/O and gRPC shuffle costs, but naive caching risks serving stale data after incremental appends or table compactions.

## Decision
Implement a thread-safe byte-bounded `VersionedLRUCache` (`processing/cache.py`) at two layers:
1. **Coordinator Query Result Cache:** Keyed by SHA-256 of `(normalized_sql, table_versions, optimizer_options)`.
2. **Worker Parquet Scan Cache:** Keyed by SHA-256 of `(table_name, snapshot_version, file_path, alias, columns, predicates)`.

Every lookup validates `table_versions` against active SQLite catalog snapshot versions and cached epoch watermarks; table mutations also broadcast `InvalidateCache` RPCs across all workers.

## Consequences
- **Positive:** Measured 40.9× speedup on warm repeated `Q3` queries (`59.0 ms` cold → `1.45 ms` warm) with strict prevention of stale reads (verified by Hypothesis property tests and `CHAOS-06`).
- **Negative / Tradeoffs:** Cache keys include exact SQL text and optimizer flags, so semantically equivalent rewritten SQL strings do not share Coordinator result cache entries (though they still benefit from Worker scan cache hits).
