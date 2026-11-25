# Known Limitations & Honest Engineering Disclosures

1. **Host Toolchain Availability (Rust & Docker):**
   - The macOS workstation used for development and verification has Python `3.12.8` and `uv` installed, but lacks `rustc`/`cargo`, Docker, Podman, and Node.js.
   - Consequently, the production-verified engine in this repository executes in Python 3.12.8 powered by C++/Rust-backed `pyarrow==25.0.1`, `grpcio==1.85.1`, `datafusion==55.0.0`, and `duckdb==2.0.0`.
   - The companion Rust worker crate (`rust_worker/`) and `infra/docker/Dockerfile.rust` (`rust:1.86.0-slim-bookworm`) are structured to build cleanly inside Docker/CI where `cargo` is present, but were **not** natively compiled or runtime-tested on this host. Similarly, Docker Compose and Kubernetes manifests are validated structurally and pinned to exact image tags, but were not launched inside a local container daemon on this host.

2. **SQL Language Scope:**
   - The custom parser (`processing/sql_parser.py`) intentionally targets analytical SPJG queries (`SELECT`, `FROM`, `INNER JOIN`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, `EXPLAIN`).
   - Not yet supported: `LEFT`/`RIGHT`/`FULL OUTER JOIN`, `HAVING`, `DISTINCT`, `UNION`, subqueries/CTEs, window functions (`OVER (...)`), and `CASE WHEN`.

3. **Coordinator Single Failure Domain:**
   - Worker process crashes (`SIGKILL`) are handled automatically via ring eviction and task retry (`CHAOS-01`), whereas a Coordinator process crash aborts in-flight queries (catalog state remains safe in SQLite WAL).

4. **Single-Node vs. Distributed Performance Envelope:**
   - Because workers run in Python with gRPC HTTP/2 and Arrow IPC serialization between independent OS processes, single-process native engines (DuckDB 2.0, DataFusion 55.0) are faster on datasets that fit comfortably in one machine's RAM. DQSE's distributed architecture is designed to demonstrate real multi-process scheduling, partition pruning, two-phase shuffle reduction, skew salting, memory-bounded spilling, and worker crash recovery.

5. **Security & Production Hardening Gaps:**
   - Local gRPC channels use plaintext (`insecure_channel`) bound to loopback (`127.0.0.1`); production deployments require mTLS certificates.
   - Local API keys come from environment variables rather than OIDC/JWT + a secrets manager.
   - OpenTelemetry traces export to an in-memory span store exposed at `/v1/traces` when `OTEL_EXPORTER_OTLP_ENDPOINT` is unset; external collector export requires running the `observability` Docker Compose profile.

6. **Manual Catalog Lineage Scope:**
   - Lineage edges (`models/catalog.py`) are explicitly recorded during ingestion and compaction; automatic SQL column-level lineage extraction across arbitrary external BI tools is not implemented.
