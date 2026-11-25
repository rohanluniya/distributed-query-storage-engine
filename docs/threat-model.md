# Security Architecture & STRIDE Threat Model

## 1. Access Control & Audit Controls Implemented

- **RBAC API-Key Authentication (`services/security.py`):** Every `/v1/*` endpoint requires a valid `X-API-Key` header mapped to least-privilege roles:
  - `viewer`: read-only cluster status, catalog tables, query profiles, DQ quarantine, lineage, traces, and audit logs.
  - `analyst`: `viewer` permissions plus `/v1/query`, `/v1/explain`, and `/v1/queries/{id}/cancel`.
  - `admin`: `analyst` permissions plus `/v1/cache/invalidate`, `/v1/admin/ingest`, and `/v1/admin/compact/{table}`.
- **Append-Only Audit Trail:** Both allowed actions and `401`/`403` denials are written to `.local_data/audit.jsonl` with SHA-256 key fingerprints (never raw API keys).
- **Log Redaction (`services/observability.py`):** Structured JSON formatter recursively masks `api_key`, `authorization`, `x-api-key`, `password`, `secret`, and `token` fields.
- **No Dynamic Code Execution:** SQL expressions compile to typed AST nodes serialized as JSON (`expr_from_dict`) and evaluated via PyArrow compute kernels — zero `eval()` or `exec()` calls, and zero raw SQL string interpolation against SQLite.

## 2. STRIDE Threat Analysis

| Threat Category | Attack Vector | Implemented Mitigation | Residual Gap (Dev vs. Prod) |
| :--- | :--- | :--- | :--- |
| **Spoofing** | Unauthorized API caller invoking `/v1/query` or `/v1/cache/invalidate` | Strict `X-API-Key` RBAC dependency returning `401`/`403` + audit log | Local dev uses static env keys; production requires OIDC/JWT + secret manager rotation |
| **Tampering** | Malicious or corrupt Parquet fragment injected into warehouse | Schema & row-group reader validation quarantines corrupt files before catalog commit | Local filesystem lacks object-lock immutability; use S3 Object Lock / RustFS IAM in prod |
| **Tampering (RPC)** | Unauthorized process calling gRPC `ExecuteTask` or `FetchShuffle` | Workers bind to `127.0.0.1` locally or private Docker/K8s network | gRPC channels use plaintext (`insecure_channel`) locally; production requires mTLS |
| **Repudiation** | Analyst runs expensive query or admin flushes cache without trace | Append-only JSONL audit log with key fingerprint, role, action, and `query_id`/`trace_id` | Local file audit log should ship to tamper-evident WORM sink (Loki/SIEM) in prod |
| **Information Disclosure** | Secret leakage in logs or error stack traces | Recursive key redaction in `JsonFormatter`; keys hashed in audit events | Raw query literals appear in query profiles; add column-level masking/PII policies in prod |
| **Denial of Service** | High-cardinality query or skewed key exhausting worker RAM | Per-worker `MemoryPool` cap with automatic Arrow IPC spill-to-disk + `CancelQuery` RPC + query length bound (`8000` chars) | Add per-principal rate limiting and concurrent query admission queue in production |
| **Elevation of Privilege** | `viewer` attempting cache invalidation or ingestion | Explicit role hierarchy check (`ROLE_HIERARCHY`) verified in E2E tests | Add fine-grained table/row-level authorization policies in production |

## 3. Supply Chain & Dependency Hygiene

- Exact dependency pinning via `uv.lock` and exact numeric container image tags (zero unpinned tags).
- Automated license compliance check (`scripts/check_licenses.py`) and CycloneDX 1.5 SBOM (`scripts/generate_sbom.py`).
- Non-root container user (`USER dqse`) in both `Dockerfile.standalone` and `Dockerfile.rust`.
