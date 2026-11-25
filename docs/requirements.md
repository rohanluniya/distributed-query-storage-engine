# System Requirements, SQL Grammar & Storage Design

## 1. Supported SQL Grammar

DQSE implements a custom recursive-descent analytical SQL parser (`processing/sql_parser.py`):

```sql
[EXPLAIN [ANALYZE]]
SELECT <expr> [[AS] <alias>], ...
FROM <table> [[AS] <alias>]
[INNER JOIN <table> [[AS] <alias>] ON <left_col> = <right_col>]*
[WHERE <boolean_expr>]
[GROUP BY <col>, ...]
[ORDER BY <expr> [ASC | DESC], ...]
[LIMIT <non_negative_int>]
```

- **Expressions:** Qualified (`o.total_price`) and unqualified column references, integer/float/string/boolean literals, arithmetic (`+`, `-`, `*`, `/`), parentheses.
- **Predicates:** `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IN (...)`, `NOT IN (...)`, `BETWEEN a AND b`, `NOT BETWEEN a AND b`.
- **Aggregates:** `COUNT(*)`, `COUNT(expr)`, `SUM(expr)`, `AVG(expr)`, `MIN(expr)`, `MAX(expr)`.

## 2. Data Models, Grain, Partitioning & Retention

| Table | Layer | Grain | Primary Key | Partition Columns | Sort Order | Retention Policy |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `orders` | Silver | 1 row per `order_id` (deduplicated by latest `source_version`) | `order_id` | `order_month`, `region_id` | `(total_price, order_id)` | Immutable snapshots retained in catalog; bronze raw kept for replay |
| `lineitem` | Silver | 1 row per `(order_id, line_number)` | `(order_id, line_number)` | `order_month`, `region_id` | `(order_id, line_number)` | Immutable snapshots; sorted for row-group min/max pruning |
| `customer` | Silver | 1 row per `customer_id` | `customer_id` | `region_id` | `(customer_id)` | Dimension snapshot with `hot_keys` skew metadata (`customer_id = 1`) |
| `nation` | Silver | 1 row per `nation_id` | `nation_id` | None (small dimension) | `(nation_id)` | Single-partition reference table |

- **Bronze Raw & Replay:** Raw incoming batches (including duplicate events) are stored durably under `warehouse/bronze/` before deduplication and DQ filtering so any snapshot can be replayed deterministically from seed or raw files.
- **Quarantine / DLQ:** Invalid records (negative prices, null foreign keys) and corrupt/schema-drifted Parquet fragments are isolated in `warehouse/quarantine/` and logged in SQLite `dq_quarantine` without blocking healthy partitions.
- **Event Time vs. Processing Time:** `orders` records explicit `event_time` (order date) separately from `ingested_at` (processing time) and `source_version` (deduplication / correction tie-breaker).

## 3. Execution & Consistency Guarantees

- **Snapshot Isolation per Query:** Every query captures `table_versions` at planning time from the SQLite WAL catalog; all scan tasks read immutable Parquet files pinned to that snapshot version.
- **At-Least-Once Task Execution with Idempotent Output Commit:** Workers execute tasks under explicit `lease_id` + `attempt` metadata. Failed worker RPCs trigger Coordinator retries on surviving workers. Duplicate or late completions for an already-committed `(query_id, stage_id, partition_id)` are discarded so aggregations never double-count.
- **Exactness Under Memory Pressure:** Spill-to-disk writes partitioned Arrow IPC chunks and merges per bucket, guaranteeing identical numerical output to in-memory execution.
- **Cache Freshness:** Both Coordinator result cache and Worker scan cache include `table_versions` in their validation check and support explicit `InvalidateCache` RPC broadcasts on table mutation/compaction.

## 4. Service-Level Objectives (Local 3-Worker Profile)

| Metric | Target SLO | Measurement |
| :--- | :--- | :--- |
| Selective pruned point query (`Q2`) p95 | `< 50 ms` | `dqse_query_duration_ms` |
| Analytical aggregation & 3-table join (`Q1`, `Q3`) p95 | `< 500 ms` (local scale) | `dqse_query_duration_ms` |
| Warm versioned cache hit latency | `< 5 ms` | Coordinator result cache |
| Reference engine numerical accuracy | `100% exact parity` (rel tol `1e-5`) | Verified against DuckDB 2.0 & DataFusion 55.0 |
| Single-worker `SIGKILL` recovery | Automatic completion with 0 wrong rows | Automated `CHAOS-01` drill |
