"""End-to-end tests for FastAPI Coordinator API, RBAC enforcement, audit logging, Prometheus metrics, and UI."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services.api import create_app
from services.security import SecurityManager


@pytest.mark.e2e
def test_api_rbac_query_explain_metrics_and_workbench(smoke_cluster: dict) -> None:
    sec = SecurityManager(smoke_cluster["root"] / "audit.jsonl")
    app = create_app(
        catalog=smoke_cluster["catalog"],
        coordinator=smoke_cluster["coordinator"],
        security=sec,
        warehouse_dir=smoke_cluster["warehouse_dir"],
    )
    client = TestClient(app)

    # Public health, UI root, and Prometheus metrics
    health = client.get("/health")
    assert health.status_code == 200
    hjson = health.json()
    assert hjson["status"] == "healthy"
    assert hjson["workers_healthy"] == 3

    ui = client.get("/")
    assert ui.status_code == 200
    assert "DQSE Control Tower" in ui.text

    # RBAC: unauthorized requests rejected with 401
    assert client.get("/v1/overview").status_code == 401
    assert client.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM orders"}).status_code == 401
    assert client.get("/v1/tables", headers={"X-API-Key": "wrong-key"}).status_code == 401

    # RBAC: viewer can read overview/tables/workers but CANNOT run query or flush cache (403)
    viewer_hdr = {"X-API-Key": "dev-viewer-key-change-me"}
    analyst_hdr = {"X-API-Key": "dev-analyst-key-change-me"}
    admin_hdr = {"X-API-Key": "dev-admin-key-change-me"}

    ov = client.get("/v1/overview", headers=viewer_hdr)
    assert ov.status_code == 200
    assert len(ov.json()["tables"]) == 4
    assert len(ov.json()["workers"]) == 3

    assert (
        client.post(
            "/v1/query",
            headers=viewer_hdr,
            json={"sql": "SELECT COUNT(*) FROM orders"},
        ).status_code
        == 403
    )
    assert client.post("/v1/cache/invalidate", headers=analyst_hdr).status_code == 403

    # Analyst executes live distributed query + reference verification
    q_resp = client.post(
        "/v1/query",
        headers=analyst_hdr,
        json={
            "sql": "SELECT region_id, COUNT(*) AS cnt, SUM(total_price) AS rev FROM orders GROUP BY region_id ORDER BY region_id ASC",
            "verify_reference": True,
        },
    )
    assert q_resp.status_code == 200
    qjson = q_resp.json()
    assert qjson["row_count"] == 5
    assert qjson["profile"]["reference_parity"]["duckdb"]["exact_match"] is True
    assert qjson["profile"]["reference_parity"]["datafusion"]["exact_match"] is True

    # Profile retrieval endpoint
    qid = qjson["query_id"]
    prof_resp = client.get(f"/v1/queries/{qid}/profile", headers=viewer_hdr)
    assert prof_resp.status_code == 200
    assert prof_resp.json()["query_id"] == qid

    # Explain endpoint
    exp = client.post(
        "/v1/explain",
        headers=analyst_hdr,
        json={"sql": "SELECT order_id FROM orders WHERE order_month = '2025-03' LIMIT 10"},
    )
    assert exp.status_code == 200
    assert exp.json()["partitions_pruned"] == 25

    # Admin flushes cache cleanly
    inv = client.post("/v1/cache/invalidate?table_name=*", headers=admin_hdr)
    assert inv.status_code == 200
    assert inv.json()["table_name"] == "*"

    # Audit log, traces, DQ quarantine, lineage, and Prometheus metrics all populated
    assert len(client.get("/v1/audit", headers=viewer_hdr).json()["events"]) >= 4
    assert len(client.get("/v1/traces", headers=viewer_hdr).json()["traces"]) >= 1
    assert client.get("/v1/dq/quarantine", headers=viewer_hdr).json()["count"] >= 3
    assert len(client.get("/v1/lineage", headers=viewer_hdr).json()["edges"]) >= 4

    metrics_resp = client.get("/metrics")
    assert metrics_resp.status_code == 200
    assert "dqse_queries_total" in metrics_resp.text
    assert "dqse_workers_healthy" in metrics_resp.text
