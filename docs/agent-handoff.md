# Agent Handoff & Operational Resume Guide

## Current Verified State
- **Live Cluster:** Running locally via `./scripts/run_portable.sh --daemon` on `http://127.0.0.1:8090` with 3 independent gRPC workers on `127.0.0.1:18091..18093` and `local` dataset ingested (`152,221` rows across 4 partitioned Parquet tables).
- **Quality Gates Passing:**
  - `uv run ruff check .` → `0` errors
  - `uv run mypy ingestion models processing services` → `0` errors across 22 source files
  - `uv run pytest --junitxml=results/tests/junit.xml` → `26/26` passed (unit, Hypothesis property, gRPC/Avro contracts, integration, E2E RBAC, 8 chaos drills, performance smoke)
  - `uv run python scripts/check_licenses.py` → `61` packages audited, `0` violations
  - `uv run python scripts/generate_sbom.py` → CycloneDX 1.5 SBOM generated
  - `uv run python benchmarks/run_benchmarks.py` → `smoke`, `local`, `stress` + 5 ablations passing `100%` exact parity against DuckDB `2.0.0` and Apache DataFusion `55.0.0`.

## Exact Resume Commands
```bash
# Check live cluster health
curl -fsS http://127.0.0.1:8090/health | jq

# Re-run full verification suite
make lint && make typecheck && make test && make benchmark

# Stop / restart background local cluster
make stop && make daemon
```

## Open Follow-Up Engineering Improvements
1. Compile and integrate `rust_worker/` inside a Docker/Linux runner with `rustc 1.86.0` to benchmark Rust Tonic workers side-by-side with Python gRPC workers.
2. Add broadcast hash join optimization for small dimension tables (`nation`) to avoid shuffling the probe side.
3. Add `LEFT OUTER JOIN`, `HAVING`, and CTE support to `processing/sql_parser.py`.
4. Add mTLS support to gRPC worker channels and PostgreSQL catalog backend wiring (`imresamu/postgis:17-3.6.1-alpine3.23`).
