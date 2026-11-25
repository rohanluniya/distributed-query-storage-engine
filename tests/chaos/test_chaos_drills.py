"""Automated 8-drill Chaos & Fault-Recovery Suite writing verified evidence to results/chaos/chaos_results.json."""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.ingestor import (
    append_orders_batch,
    ingest_synthetic_dataset,
    validate_and_quarantine_parquet_file,
)
from models.catalog import MetadataCatalog
from models.contracts import OptimizerOptions
from processing.reference_engines import compare_results, execute_datafusion, execute_duckdb
from services.coordinator import QueryCancelledError, QueryCoordinator
from tests.conftest import find_free_ports

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
CHAOS_REPORT_PATH = REPO_ROOT / "results" / "chaos" / "chaos_results.json"
SQL_DIR = REPO_ROOT / "sql" / "queries"


def _spawn_worker_proc(
    worker_id: str,
    port: int,
    spill_dir: pathlib.Path,
) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "services.worker_server",
            "--worker-id",
            worker_id,
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--spill-dir",
            str(spill_dir),
            "--max-mem-mb",
            "64",
        ],
        cwd=str(REPO_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_workers_ready(coord: QueryCoordinator, expected: int, timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        ws = coord.refresh_workers()
        if sum(1 for w in ws if w.status == "HEALTHY") >= expected:
            return
        time.sleep(0.15)
    raise RuntimeError(f"Workers did not reach {expected} healthy within {timeout_s}s")


@pytest.mark.chaos
def test_full_8_chaos_drills_and_write_verified_report() -> None:
    """Execute 8 adversarial fault-injection drills against real multi-process gRPC workers."""
    drills: list[dict[str, Any]] = []
    procs: dict[str, subprocess.Popen[bytes]] = {}

    with tempfile.TemporaryDirectory(prefix="dqse_chaos_") as tmpdir:
        root = pathlib.Path(tmpdir)
        wh_dir = root / "warehouse"
        spill_dir = root / "spill"
        catalog = MetadataCatalog(root / "catalog.sqlite3")
        ingest_synthetic_dataset(wh_dir, catalog, scale="smoke", seed=42)

        ports = find_free_ports(3)
        endpoints = {f"worker-{i + 1}": f"127.0.0.1:{ports[i]}" for i in range(3)}
        try:
            for i in range(3):
                wid = f"worker-{i + 1}"
                procs[wid] = _spawn_worker_proc(wid, ports[i], spill_dir)

            coord = QueryCoordinator(catalog=catalog, worker_endpoints=endpoints, max_retries=2)
            _wait_workers_ready(coord, expected=3)

            # DRILL 1: Mid-Query Real Worker Process Crash (SIGKILL -9) & Automatic Task Reschedule
            t0 = time.perf_counter()
            q1_sql = (SQL_DIR / "q1_filtered_agg.sql").read_text(encoding="utf-8")

            def _kill_worker_1_midflight() -> None:
                time.sleep(0.08)
                procs["worker-1"].kill()
                procs["worker-1"].wait(timeout=5)

            killer = threading.Thread(target=_kill_worker_1_midflight, daemon=True)
            killer.start()
            resp1 = coord.execute_query(
                q1_sql,
                options=OptimizerOptions(enable_cache=False),
                verify_reference=True,
                inject_delay_ms=150.0,
            )
            killer.join(timeout=5)
            d1_ok = (
                resp1.profile.task_retries >= 1
                and resp1.profile.reference_parity["duckdb"]["exact_match"]
                and resp1.profile.reference_parity["datafusion"]["exact_match"]
            )
            assert d1_ok, f"Drill 1 failed: retries={resp1.profile.task_retries}, parity={resp1.profile.reference_parity}"
            drills.append(
                {
                    "drill_id": "CHAOS-01",
                    "name": "mid_query_worker_sigkill_recovery",
                    "passed": True,
                    "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                    "evidence": {
                        "killed_worker": "worker-1",
                        "signal": "SIGKILL",
                        "task_retries": resp1.profile.task_retries,
                        "surviving_workers": sum(1 for w in coord.refresh_workers() if w.status == "HEALTHY"),
                        "duckdb_exact_match": True,
                        "datafusion_exact_match": True,
                    },
                }
            )

            # DRILL 8 (run right after Drill 1 to restore full 3-worker ring): Worker Restart & Ring Rejoin
            t0 = time.perf_counter()
            procs["worker-1"] = _spawn_worker_proc("worker-1", ports[0], spill_dir)
            _wait_workers_ready(coord, expected=3)
            ws_after = coord.refresh_workers()
            assert all(w.status == "HEALTHY" and w.assigned_vnodes == 64 for w in ws_after)
            drills.append(
                {
                    "drill_id": "CHAOS-02",
                    "name": "worker_restart_and_consistent_hash_ring_rejoin",
                    "passed": True,
                    "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                    "evidence": {
                        "restarted_worker": "worker-1",
                        "healthy_workers_after": len(ws_after),
                        "total_vnodes": sum(w.assigned_vnodes for w in ws_after),
                    },
                }
            )

            # DRILL 3: Duplicate Task Execution & Idempotent Output Commit Deduplication
            t0 = time.perf_counter()
            dup_resp = coord.execute_query(
                q1_sql,
                options=OptimizerOptions(enable_cache=False),
                verify_reference=True,
                duplicate_test_partition=True,
            )
            assert dup_resp.profile.duplicate_commits_ignored >= 1
            assert dup_resp.profile.reference_parity["duckdb"]["exact_match"]
            assert dup_resp.profile.reference_parity["datafusion"]["exact_match"]
            drills.append(
                {
                    "drill_id": "CHAOS-03",
                    "name": "duplicate_task_execution_idempotent_dedup",
                    "passed": True,
                    "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                    "evidence": {
                        "duplicate_commits_ignored": dup_resp.profile.duplicate_commits_ignored,
                        "duckdb_exact_match": True,
                        "no_double_counting": True,
                    },
                }
            )

            # DRILL 4: Tight Worker Memory Cap Forcing Arrow IPC Spill-to-Disk with Exact Output
            t0 = time.perf_counter()
            q5_sql = (SQL_DIR / "q5_low_memory_spill_agg.sql").read_text(encoding="utf-8")
            spill_resp = coord.execute_query(
                q5_sql,
                options=OptimizerOptions(enable_cache=False, max_worker_mem_mb=0.04),
                verify_reference=True,
            )
            assert spill_resp.profile.total_spilled_bytes > 0
            assert spill_resp.profile.total_spill_files > 0
            assert spill_resp.profile.reference_parity["duckdb"]["exact_match"]
            assert spill_resp.profile.reference_parity["datafusion"]["exact_match"]
            # Verify temporary spill files were cleaned up on workers
            remaining_spill_files = list(spill_dir.rglob("*.arrow"))
            assert len(remaining_spill_files) == 0
            drills.append(
                {
                    "drill_id": "CHAOS-04",
                    "name": "tight_memory_cap_spill_to_disk_exactness",
                    "passed": True,
                    "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                    "evidence": {
                        "memory_cap_mb": 0.04,
                        "spilled_bytes": spill_resp.profile.total_spilled_bytes,
                        "spill_files": spill_resp.profile.total_spill_files,
                        "orphaned_spill_files_after": len(remaining_spill_files),
                        "duckdb_exact_match": True,
                        "datafusion_exact_match": True,
                    },
                }
            )

            # DRILL 5: Skewed Hot-Key Partition Storm mitigated via Skew-Aware Salting
            t0 = time.perf_counter()
            q4_sql = (SQL_DIR / "q4_skewed_hotkey_agg.sql").read_text(encoding="utf-8")
            skew_resp = coord.execute_query(
                q4_sql,
                options=OptimizerOptions(enable_cache=False, skew_salting=True, salt_factor=4),
                verify_reference=True,
            )
            assert "skew_salting" in skew_resp.profile.optimizer_rules_applied
            assert skew_resp.rows[0][0] == 1  # hot_customer_id = 1 is top order count
            assert skew_resp.profile.reference_parity["duckdb"]["exact_match"]
            assert skew_resp.profile.reference_parity["datafusion"]["exact_match"]
            drills.append(
                {
                    "drill_id": "CHAOS-05",
                    "name": "skewed_hotkey_salted_shuffle_mitigation",
                    "passed": True,
                    "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                    "evidence": {
                        "hot_customer_id": skew_resp.rows[0][0],
                        "hot_customer_orders": skew_resp.rows[0][1],
                        "salt_factor": 4,
                        "stages_executed": len(skew_resp.profile.stages),
                        "duckdb_exact_match": True,
                    },
                }
            )

            # DRILL 6: Cache Epoch Invalidation on Table Snapshot Mutation (Zero Stale Reads)
            t0 = time.perf_counter()
            cnt_sql = "SELECT COUNT(*) AS total_orders, SUM(total_price) AS gross FROM orders"
            coord.invalidate_cache("*")
            r_cold = coord.execute_query(cnt_sql, options=OptimizerOptions(enable_cache=True))
            r_warm = coord.execute_query(cnt_sql, options=OptimizerOptions(enable_cache=True))
            assert r_warm.profile.result_cache_hit is True
            before_cnt = r_cold.rows[0][0]

            new_order = pa.table(
                {
                    "order_id": pa.array([888888], type=pa.int64()),
                    "customer_id": pa.array([2], type=pa.int64()),
                    "order_status": pa.array(["O"], type=pa.string()),
                    "total_price": pa.array([5000.0], type=pa.float64()),
                    "order_date": pa.array(["2025-03-20"], type=pa.string()),
                    "order_month": pa.array(["2025-03"], type=pa.string()),
                    "order_priority": pa.array(["1-URGENT"], type=pa.string()),
                    "region_id": pa.array([1], type=pa.int64()),
                    "event_time": pa.array(["2025-03-20T12:00:00Z"], type=pa.string()),
                    "ingested_at": pa.array(["2025-07-02T00:00:00Z"], type=pa.string()),
                    "source_version": pa.array([1], type=pa.int64()),
                }
            )
            new_ver = append_orders_batch(wh_dir, catalog, new_order, idempotency_key="chaos-append-1")
            coord.invalidate_cache("orders", new_version=new_ver)
            r_after = coord.execute_query(cnt_sql, options=OptimizerOptions(enable_cache=True), verify_reference=True)
            assert r_after.profile.result_cache_hit is False
            assert r_after.rows[0][0] == before_cnt + 1
            assert r_after.profile.reference_parity["duckdb"]["exact_match"]
            drills.append(
                {
                    "drill_id": "CHAOS-06",
                    "name": "cache_epoch_invalidation_on_snapshot_mutation",
                    "passed": True,
                    "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                    "evidence": {
                        "count_before_mutation": before_cnt,
                        "count_after_mutation": r_after.rows[0][0],
                        "new_snapshot_version": new_ver,
                        "warm_hit_before_mutation": True,
                        "stale_read_prevented": True,
                    },
                }
            )

            # DRILL 7: Active Query Cancellation Releasing Worker Leases & Buffers
            t0 = time.perf_counter()
            cancel_qid = "q-chaos-cancel-01"
            cancel_err: list[Exception] = []

            def _run_slow() -> None:
                try:
                    coord.execute_query(
                        q1_sql,
                        options=OptimizerOptions(enable_cache=False),
                        query_id=cancel_qid,
                        inject_delay_ms=1500.0,
                    )
                except Exception as exc:
                    cancel_err.append(exc)

            worker_thread = threading.Thread(target=_run_slow, daemon=True)
            worker_thread.start()
            time.sleep(0.18)
            cancel_res = coord.cancel_query(cancel_qid, reason="chaos_drill_cancellation")
            worker_thread.join(timeout=6)
            assert cancel_err and isinstance(cancel_err[0], QueryCancelledError)
            prof_cancel = coord.get_query_profile(cancel_qid)
            assert prof_cancel is not None and prof_cancel.status == "CANCELLED"
            ws_post = coord.refresh_workers()
            assert all(w.active_tasks == 0 for w in ws_post)
            drills.append(
                {
                    "drill_id": "CHAOS-07",
                    "name": "active_query_cancellation_releases_leases",
                    "passed": True,
                    "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                    "evidence": {
                        "query_id": cancel_qid,
                        "final_status": prof_cancel.status,
                        "workers_notified": len(cancel_res["workers"]),
                        "active_tasks_after_cancel": sum(w.active_tasks for w in ws_post),
                    },
                }
            )

            # DRILL 8: Corrupt Parquet Fragment & Schema Drift Quarantine Isolation
            t0 = time.perf_counter()
            corrupt_p = root / "corrupt_chaos.parquet"
            corrupt_p.write_bytes(b"CORRUPT_HEADER_BYTES_9999")
            ok_c, reason_c = validate_and_quarantine_parquet_file(
                corrupt_p, "orders", catalog, wh_dir / "quarantine", "chaos-dq"
            )
            bad_schema_p = root / "drift_chaos.parquet"
            pq.write_table(pa.table({"unexpected_col": pa.array([1, 2])}), bad_schema_p)
            ok_s, reason_s = validate_and_quarantine_parquet_file(
                bad_schema_p, "orders", catalog, wh_dir / "quarantine", "chaos-dq"
            )
            assert not ok_c and not ok_s
            # Healthy catalog still executes exact parity queries cleanly
            check_resp = coord.execute_query(cnt_sql, options=OptimizerOptions(enable_cache=False))
            dk = execute_duckdb(cnt_sql, catalog)
            df = execute_datafusion(cnt_sql, catalog)
            assert compare_results(check_resp.rows, dk["rows"])[0]
            assert compare_results(check_resp.rows, df["rows"])[0]
            drills.append(
                {
                    "drill_id": "CHAOS-08",
                    "name": "corrupt_parquet_and_schema_drift_quarantine",
                    "passed": True,
                    "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                    "evidence": {
                        "corrupt_rejected": not ok_c,
                        "corrupt_reason": reason_c,
                        "schema_drift_rejected": not ok_s,
                        "schema_reason": reason_s,
                        "total_quarantine_records": len(catalog.list_quarantine()),
                        "healthy_table_still_exact": True,
                    },
                }
            )

            CHAOS_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
            report = {
                "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "drills_total": len(drills),
                "drills_passed": sum(1 for d in drills if d["passed"]),
                "drills": drills,
            }
            CHAOS_REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
            assert report["drills_passed"] == 8
        finally:
            for p in procs.values():
                if p.poll() is None:
                    p.terminate()
                    try:
                        p.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        p.kill()
