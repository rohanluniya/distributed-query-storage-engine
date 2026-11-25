# ADR-001: Columnar Parquet Storage & Apache Arrow IPC Wire Format

## Status
Accepted

## Context
A distributed analytical query engine transfers intermediate batches across worker processes during scans, repartitioned joins, and hash aggregations. Row-oriented formats (JSON, CSV, pickled Python objects) incur heavy serialization CPU overhead and discard columnar vectorization and Parquet row-group statistics.

## Decision
Use **Apache Parquet** (via `pyarrow==25.0.1`) for durable partitioned on-disk storage and **Apache Arrow IPC streams** (`pyarrow.ipc.new_stream` / `open_stream`) carried inside Protocol Buffer `bytes` fields over gRPC HTTP/2 (`grpcio==1.85.1`) for worker-to-worker shuffle exchange and disk spill files.

## Consequences
- **Positive:** Zero-schema-inference binary framing, native column projection pushdown (`columns=...`), Parquet row-group min/max pruning (`pf.metadata.row_group(i)`), and direct compatibility with Arrow-native spill files and DuckDB/DataFusion reference engines.
- **Negative / Tradeoffs:** On small local datasets that fit entirely in single-process RAM, gRPC + Arrow IPC framing adds ~2–50 ms of network/process-boundary latency compared to embedded single-process engines (DuckDB, DataFusion). This overhead is measured and reported transparently in `docs/benchmarks/benchmark-report.md`.
