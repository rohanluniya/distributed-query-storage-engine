"""FastAPI Coordinator API and Distributed Query Engine Control Tower served on :8090."""

from __future__ import annotations

import json
import os
import pathlib
import time
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from ingestion.ingestor import compact_table_partitions, ingest_synthetic_dataset
from models.catalog import MetadataCatalog
from models.contracts import ExplainRequest, ExplainResponse, QueryRequest, QueryResponse
from processing.logical_plan import PlanError
from processing.sql_parser import SQLParseError
from services.coordinator import QueryCancelledError, QueryCoordinator
from services.observability import recent_traces, render_prometheus_metrics
from services.security import Principal, SecurityManager, make_role_dependency

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
WORKBENCH_DIR = REPO_ROOT / "apps" / "query-workbench"
SQL_QUERIES_DIR = REPO_ROOT / "sql" / "queries"
RESULTS_DIR = REPO_ROOT / "results"


def _parse_workers_env(raw: str) -> dict[str, str]:
    workers: dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        wid, ep = item.split("=", 1)
        workers[wid.strip()] = ep.strip()
    return workers


def create_app(
    catalog: MetadataCatalog | None = None,
    coordinator: QueryCoordinator | None = None,
    security: SecurityManager | None = None,
    warehouse_dir: str | pathlib.Path | None = None,
) -> FastAPI:
    wh_dir = pathlib.Path(warehouse_dir or os.environ.get("DQSE_DATA_DIR", ".local_data/warehouse"))
    cat_db = pathlib.Path(os.environ.get("DQSE_CATALOG_DB", ".local_data/catalog.sqlite3"))
    audit_path = pathlib.Path(os.environ.get("DQSE_AUDIT_LOG", ".local_data/audit.jsonl"))

    cat = catalog or MetadataCatalog(cat_db)
    sec = security or SecurityManager(audit_path)
    if coordinator is None:
        workers_raw = os.environ.get(
            "DQSE_WORKERS",
            "worker-1=127.0.0.1:18091,worker-2=127.0.0.1:18092,worker-3=127.0.0.1:18093",
        )
        coord = QueryCoordinator(
            catalog=cat,
            worker_endpoints=_parse_workers_env(workers_raw),
            cache_max_bytes=int(os.environ.get("DQSE_CACHE_MAX_MB", "64")) * 1024 * 1024,
            task_lease_ms=int(os.environ.get("DQSE_TASK_LEASE_MS", "15000")),
            max_retries=int(os.environ.get("DQSE_MAX_TASK_RETRIES", "2")),
        )
    else:
        coord = coordinator

    started_at = time.monotonic()

    app = FastAPI(
        title="DQSE — Distributed Query & Storage Engine",
        version="1.0.0",
        description="First-principles distributed analytical SQL engine with gRPC Arrow IPC exchange, consistent hashing, spill-to-disk, and versioned caching.",
    )

    app.state.catalog = cat
    app.state.coordinator = coord
    app.state.security = sec
    app.state.warehouse_dir = wh_dir

    if WORKBENCH_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(WORKBENCH_DIR)), name="static")

    require_viewer = make_role_dependency(sec, "viewer")
    require_analyst = make_role_dependency(sec, "analyst")
    require_admin = make_role_dependency(sec, "admin")

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def index() -> Any:
        idx = WORKBENCH_DIR / "index.html"
        if idx.exists():
            return FileResponse(idx)
        return HTMLResponse("<h1>DQSE Coordinator Running</h1>")

    @app.get("/health")
    def health() -> dict[str, Any]:
        workers = coord.refresh_workers()
        healthy = [w for w in workers if w.status == "HEALTHY"]
        return {
            "status": "healthy" if healthy else "degraded",
            "service": "dqse-coordinator",
            "version": "1.0.0",
            "uptime_seconds": round(time.monotonic() - started_at, 2),
            "workers_healthy": len(healthy),
            "workers_total": len(workers),
            "table_versions": cat.get_table_versions(),
            "freshness_utc": cat.latest_freshness_utc(),
        }

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(
            content=render_prometheus_metrics(),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    @app.get("/v1/overview")
    def overview(principal: Principal = Depends(require_viewer)) -> dict[str, Any]:
        workers = coord.refresh_workers()
        tables = cat.list_tables()
        recent = coord.list_recent_profiles(limit=15)
        presets: dict[str, str] = {}
        if SQL_QUERIES_DIR.exists():
            for qf in sorted(SQL_QUERIES_DIR.glob("*.sql")):
                presets[qf.stem] = qf.read_text(encoding="utf-8").strip()

        benchmarks: dict[str, Any] = {}
        bench_file = RESULTS_DIR / "benchmarks" / "benchmark_results.json"
        if bench_file.exists():
            benchmarks = json.loads(bench_file.read_text(encoding="utf-8"))
        ablations: dict[str, Any] = {}
        abl_file = RESULTS_DIR / "benchmarks" / "ablation_results.json"
        if abl_file.exists():
            ablations = json.loads(abl_file.read_text(encoding="utf-8"))
        chaos: dict[str, Any] = {}
        chaos_file = RESULTS_DIR / "chaos" / "chaos_results.json"
        if chaos_file.exists():
            chaos = json.loads(chaos_file.read_text(encoding="utf-8"))

        return {
            "role": principal.role,
            "freshness_utc": cat.latest_freshness_utc(),
            "workers": [w.model_dump() for w in workers],
            "tables": [t.model_dump() for t in tables],
            "cache": coord.cache_stats(),
            "recent_queries": [q.model_dump() for q in recent],
            "quarantine_count": len(cat.list_quarantine(limit=200)),
            "lineage_count": len(cat.list_lineage(limit=200)),
            "preset_queries": presets,
            "benchmarks": benchmarks,
            "ablations": ablations,
            "chaos": chaos,
        }

    @app.get("/v1/workers")
    def list_workers(_principal: Principal = Depends(require_viewer)) -> dict[str, Any]:
        workers = coord.refresh_workers()
        return {"workers": [w.model_dump() for w in workers], "count": len(workers)}

    @app.get("/v1/tables")
    def list_tables(_principal: Principal = Depends(require_viewer)) -> dict[str, Any]:
        tables = cat.list_tables()
        return {
            "tables": [t.model_dump() for t in tables],
            "freshness_utc": cat.latest_freshness_utc(),
        }

    @app.post("/v1/explain", response_model=ExplainResponse)
    def explain_sql(
        req: ExplainRequest,
        principal: Principal = Depends(require_analyst),
    ) -> ExplainResponse:
        try:
            resp = coord.explain(req.sql, options=req.optimizer)
            sec.record_audit("EXPLAIN", principal, "ALLOWED", "/v1/explain", {"sql_len": len(req.sql)})
            return resp
        except (SQLParseError, PlanError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    @app.post("/v1/query", response_model=QueryResponse)
    def run_query(
        req: QueryRequest,
        principal: Principal = Depends(require_analyst),
    ) -> QueryResponse:
        try:
            resp = coord.execute_query(
                sql=req.sql,
                options=req.optimizer,
                verify_reference=req.verify_reference,
                query_id=req.query_id,
            )
            sec.record_audit(
                "QUERY",
                principal,
                "ALLOWED",
                "/v1/query",
                {
                    "query_id": resp.query_id,
                    "rows": resp.row_count,
                    "duration_ms": resp.profile.total_duration_ms,
                },
            )
            return resp
        except (SQLParseError, PlanError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        except QueryCancelledError as exc:
            raise HTTPException(status_code=499, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)
            ) from exc

    @app.post("/v1/queries/{query_id}/cancel")
    def cancel_query(
        query_id: str,
        principal: Principal = Depends(require_analyst),
    ) -> dict[str, Any]:
        res = coord.cancel_query(query_id, reason=f"api_request_by_{principal.role}")
        sec.record_audit("CANCEL_QUERY", principal, "ALLOWED", f"/v1/queries/{query_id}/cancel")
        return res

    @app.get("/v1/queries/{query_id}/profile")
    def get_query_profile(
        query_id: str,
        _principal: Principal = Depends(require_viewer),
    ) -> dict[str, Any]:
        prof = coord.get_query_profile(query_id)
        if prof is None:
            raise HTTPException(status_code=404, detail=f"Query profile '{query_id}' not found")
        return prof.model_dump()

    @app.get("/v1/queries")
    def list_queries(_principal: Principal = Depends(require_viewer)) -> dict[str, Any]:
        return {"queries": [p.model_dump() for p in coord.list_recent_profiles(limit=25)]}

    @app.get("/v1/cache/stats")
    def cache_stats(_principal: Principal = Depends(require_viewer)) -> dict[str, Any]:
        return coord.cache_stats()

    @app.post("/v1/cache/invalidate")
    def invalidate_cache(
        table_name: str = "*",
        principal: Principal = Depends(require_admin),
    ) -> dict[str, Any]:
        res = coord.invalidate_cache(table_name=table_name)
        sec.record_audit(
            "CACHE_INVALIDATE",
            principal,
            "ALLOWED",
            "/v1/cache/invalidate",
            {"table_name": table_name},
        )
        return res

    @app.get("/v1/dq/quarantine")
    def dq_quarantine(_principal: Principal = Depends(require_viewer)) -> dict[str, Any]:
        records = cat.list_quarantine(limit=100)
        return {"count": len(records), "records": records}

    @app.get("/v1/lineage")
    def lineage(_principal: Principal = Depends(require_viewer)) -> dict[str, Any]:
        edges = cat.list_lineage(limit=100)
        snapshots = cat.list_snapshots()
        return {"edges": edges, "snapshots": snapshots}

    @app.get("/v1/traces")
    def traces(_principal: Principal = Depends(require_viewer)) -> dict[str, Any]:
        return {"traces": recent_traces(limit=30)}

    @app.get("/v1/audit")
    def audit(_principal: Principal = Depends(require_viewer)) -> dict[str, Any]:
        return {"events": sec.recent_audit_events(limit=50)}

    @app.post("/v1/admin/ingest")
    def trigger_ingest(
        scale: str = "local",
        seed: int = 42,
        principal: Principal = Depends(require_admin),
    ) -> dict[str, Any]:
        report = ingest_synthetic_dataset(wh_dir, cat, scale=scale, seed=seed, force_new_snapshot=True)
        coord.invalidate_cache("*")
        sec.record_audit(
            "ADMIN_INGEST",
            principal,
            "ALLOWED",
            "/v1/admin/ingest",
            {"scale": scale, "seed": seed},
        )
        return report

    @app.post("/v1/admin/compact/{table_name}")
    def trigger_compact(
        table_name: str,
        principal: Principal = Depends(require_admin),
    ) -> dict[str, Any]:
        try:
            new_ver = compact_table_partitions(wh_dir, cat, table_name)
            coord.invalidate_cache(table_name, new_version=new_ver)
            sec.record_audit(
                "ADMIN_COMPACT",
                principal,
                "ALLOWED",
                f"/v1/admin/compact/{table_name}",
                {"new_snapshot_version": new_ver},
            )
            return {"table_name": table_name, "new_snapshot_version": new_ver}
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    return app


app = create_app()
