"""Reproducible multi-scale benchmark suite comparing DQSE against DuckDB 2.0.0 and Apache DataFusion 55.0.0.

Measures:
  - smoke, local, and stress scales across Q1..Q5
  - Exact numerical equivalence assertions vs both DuckDB 2.0.0 and DataFusion 55.0.0
  - Optimizer ablations (Partition+RowGroup+Projection Pruning, 2-Phase Agg, Skew Salting, Versioned LRU Cache)
  - Real Arrow IPC shuffle bytes and Q5 tight-memory spill-to-disk telemetry
  - Outputs JSON, CSV, and publication Matplotlib chart in docs/assets/benchmark_distributed_query_engine.png
"""

from __future__ import annotations

import csv
import json
import os
import pathlib
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from typing import Any

import datafusion
import duckdb
import grpc
import matplotlib
import numpy as np
import pyarrow as pa

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ingestion.ingestor import ingest_synthetic_dataset  # noqa: E402
from models.catalog import MetadataCatalog  # noqa: E402
from models.contracts import OptimizerOptions  # noqa: E402
from processing.reference_engines import compare_results, execute_datafusion, execute_duckdb  # noqa: E402
from services.coordinator import QueryCoordinator  # noqa: E402
from services.worker_server import create_worker_server  # noqa: E402

SQL_DIR = REPO_ROOT / "sql" / "queries"
RESULTS_DIR = REPO_ROOT / "results" / "benchmarks"
ASSETS_DIR = REPO_ROOT / "docs" / "assets"


def _hardware_info() -> dict[str, Any]:
    mem_gib = 0.0
    if platform.system() == "Darwin":
        try:
            out = subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip()
            mem_gib = round(int(out) / (1024**3), 1)
        except Exception:
            mem_gib = 0.0
    return {
        "os": f"{platform.system()} {platform.release()}",
        "arch": platform.machine(),
        "cpu_cores": os.cpu_count() or 1,
        "ram_gib": mem_gib,
        "python": platform.python_version(),
        "pyarrow": pa.__version__,
        "grpcio": grpc.__version__,
        "duckdb": duckdb.__version__,
        "datafusion": datafusion.__version__,
    }


def _pct(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    ordered = sorted(vals)
    idx = min(len(ordered) - 1, max(0, int(round((p / 100.0) * (len(ordered) - 1)))))
    return round(ordered[idx], 3)


def _start_worker_cluster(
    spill_root: pathlib.Path,
    base_port: int = 18291,
    num_workers: int = 3,
) -> tuple[list[grpc.Server], dict[str, str]]:
    servers: list[grpc.Server] = []
    endpoints: dict[str, str] = {}
    for i in range(num_workers):
        wid = f"worker-{i + 1}"
        port = base_port + i
        srv, _ = create_worker_server(wid, "127.0.0.1", port, spill_root, max_mem_mb=128)
        srv.start()
        servers.append(srv)
        endpoints[wid] = f"127.0.0.1:{port}"
    return servers, endpoints


def run_profile(scale: str, iterations: int, base_port: int) -> tuple[dict[str, Any], dict[str, Any] | None]:
    tmp = pathlib.Path(tempfile.mkdtemp(prefix=f"dqse_bench_{scale}_"))
    servers: list[grpc.Server] = []
    try:
        catalog = MetadataCatalog(tmp / "catalog.sqlite3")
        ingest_report = ingest_synthetic_dataset(
            tmp / "warehouse", catalog, scale=scale, seed=42, inject_corrupt_fragment=True
        )
        servers, endpoints = _start_worker_cluster(tmp / "spill", base_port=base_port)
        coord = QueryCoordinator(catalog, endpoints)
        coord.refresh_workers()

        q5_mem_mb = {"smoke": 0.04, "local": 0.5, "stress": 2.0}[scale]
        query_results: dict[str, Any] = {}

        for qfile in sorted(SQL_DIR.glob("*.sql")):
            qname = qfile.stem
            sql = qfile.read_text(encoding="utf-8").strip()
            dq_latencies: list[float] = []
            dk_latencies: list[float] = []
            df_latencies: list[float] = []
            last_resp = None
            dk_ok = False
            df_ok = False

            for _ in range(iterations):
                coord.invalidate_cache("*")
                opts = OptimizerOptions(
                    enable_cache=False,
                    max_worker_mem_mb=q5_mem_mb if "q5" in qname else 0.0,
                )
                resp = coord.execute_query(sql, options=opts, verify_reference=False)
                last_resp = resp
                dq_latencies.append(resp.profile.total_duration_ms)

                dk = execute_duckdb(sql, catalog)
                df = execute_datafusion(sql, catalog)
                dk_latencies.append(dk["duration_ms"])
                df_latencies.append(df["duration_ms"])

                ordered = "ORDER BY" in sql.upper()
                dk_ok, dk_msg = compare_results(resp.rows, dk["rows"], ordered=ordered)
                df_ok, df_msg = compare_results(resp.rows, df["rows"], ordered=ordered)
                if not dk_ok:
                    raise AssertionError(f"DuckDB mismatch on {scale}/{qname}: {dk_msg}")
                if not df_ok:
                    raise AssertionError(f"DataFusion mismatch on {scale}/{qname}: {df_msg}")

            assert last_resp is not None
            prof = last_resp.profile
            query_results[qname] = {
                "dqse_p50_ms": round(statistics.median(dq_latencies), 3),
                "dqse_p95_ms": _pct(dq_latencies, 95),
                "duckdb_p50_ms": round(statistics.median(dk_latencies), 3),
                "datafusion_p50_ms": round(statistics.median(df_latencies), 3),
                "rows_scanned": prof.total_rows_scanned,
                "rows_returned": last_resp.row_count,
                "partitions_total": prof.partitions_total,
                "partitions_pruned": prof.partitions_pruned,
                "row_groups_scanned": prof.row_groups_scanned,
                "row_groups_pruned": prof.row_groups_pruned,
                "shuffle_bytes": prof.total_shuffle_bytes,
                "spilled_bytes": prof.total_spilled_bytes,
                "spill_files": prof.total_spill_files,
                "peak_worker_mem_bytes": prof.peak_worker_mem_bytes,
                "optimizer_rules": prof.optimizer_rules_applied,
                "duckdb_exact_match": dk_ok,
                "datafusion_exact_match": df_ok,
            }

        ablations: dict[str, Any] | None = None
        if scale == "local":
            ablations = _run_ablations(coord, catalog)

        profile_payload = {
            "scale": scale,
            "iterations": iterations,
            "dataset": {
                "orders_rows": ingest_report["tables"]["orders"]["deduplicated_rows"],
                "lineitem_rows": ingest_report["tables"]["lineitem"]["rows"],
                "customer_rows": ingest_report["tables"]["customer"]["rows"],
                "nation_rows": ingest_report["tables"]["nation"]["rows"],
            },
            "queries": query_results,
        }
        return profile_payload, ablations
    finally:
        for s in servers:
            s.stop(grace=0.5)
        shutil.rmtree(tmp, ignore_errors=True)


def _run_ablations(coord: QueryCoordinator, catalog: MetadataCatalog) -> dict[str, Any]:
    """Measure before/after impact of each major optimization rule on the local scale dataset."""
    q1_sql = (SQL_DIR / "q1_filtered_agg.sql").read_text(encoding="utf-8").strip()
    q2_sql = (SQL_DIR / "q2_partition_pruned_point.sql").read_text(encoding="utf-8").strip()
    q3_sql = (SQL_DIR / "q3_distributed_join_agg.sql").read_text(encoding="utf-8").strip()
    q4_sql = (SQL_DIR / "q4_skewed_hotkey_agg.sql").read_text(encoding="utf-8").strip()
    q5_sql = (SQL_DIR / "q5_low_memory_spill_agg.sql").read_text(encoding="utf-8").strip()

    # Ablation 1: Q2 Pruning + Pushdown vs Unoptimized Full Scan
    coord.invalidate_cache("*")
    q2_unopt = coord.execute_query(
        q2_sql,
        options=OptimizerOptions(
            projection_pushdown=False,
            predicate_pushdown=False,
            partition_pruning=False,
            two_phase_agg=False,
            skew_salting=False,
            enable_cache=False,
        ),
    )
    coord.invalidate_cache("*")
    q2_opt = coord.execute_query(
        q2_sql,
        options=OptimizerOptions(enable_cache=False),
    )
    ok_q2, _ = compare_results(q2_unopt.rows, q2_opt.rows, ordered=True)
    assert ok_q2, "Q2 unoptimized vs optimized mismatch!"

    # Ablation 2: Q1 Two-Phase Partial/Final Agg vs Single-Phase Raw Shuffle
    coord.invalidate_cache("*")
    q1_single = coord.execute_query(
        q1_sql,
        options=OptimizerOptions(two_phase_agg=False, enable_cache=False),
    )
    coord.invalidate_cache("*")
    q1_two = coord.execute_query(
        q1_sql,
        options=OptimizerOptions(two_phase_agg=True, enable_cache=False),
    )
    ok_q1, _ = compare_results(q1_single.rows, q1_two.rows, ordered=True)
    assert ok_q1, "Q1 single-phase vs two-phase mismatch!"
    shuf_single = max(1, q1_single.profile.total_shuffle_bytes)
    shuf_two = q1_two.profile.total_shuffle_bytes
    shuf_reduction_pct = round((1.0 - (shuf_two / shuf_single)) * 100.0, 2)

    # Ablation 3: Q4 Skew Salting vs Unsalted
    coord.invalidate_cache("*")
    q4_unsalted = coord.execute_query(
        q4_sql,
        options=OptimizerOptions(skew_salting=False, enable_cache=False),
    )
    coord.invalidate_cache("*")
    q4_salted = coord.execute_query(
        q4_sql,
        options=OptimizerOptions(skew_salting=True, salt_factor=4, enable_cache=False),
    )
    ok_q4, _ = compare_results(q4_unsalted.rows, q4_salted.rows, ordered=True)
    assert ok_q4, "Q4 unsalted vs salted mismatch!"

    # Ablation 4: Q3 Cold Distributed Execution vs Warm Versioned LRU Cache
    coord.invalidate_cache("*")
    q3_cold = coord.execute_query(q3_sql, options=OptimizerOptions(enable_cache=True))
    q3_warm = coord.execute_query(q3_sql, options=OptimizerOptions(enable_cache=True))
    assert q3_warm.profile.result_cache_hit, "Expected warm coordinator cache hit on Q3"
    ok_q3, _ = compare_results(q3_cold.rows, q3_warm.rows, ordered=True)
    assert ok_q3, "Q3 cold vs warm cache mismatch!"

    # Ablation 5: Q5 In-Memory vs Tight-Memory Spill-to-Disk Exactness
    coord.invalidate_cache("*")
    q5_mem = coord.execute_query(q5_sql, options=OptimizerOptions(enable_cache=False, max_worker_mem_mb=128.0))
    coord.invalidate_cache("*")
    q5_spill = coord.execute_query(q5_sql, options=OptimizerOptions(enable_cache=False, max_worker_mem_mb=0.5))
    ok_q5, _ = compare_results(q5_mem.rows, q5_spill.rows, ordered=True)
    assert ok_q5, "Q5 in-memory vs spill-to-disk mismatch!"
    assert q5_spill.profile.total_spilled_bytes > 0, "Expected Q5 tight memory cap to spill to disk"

    return {
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "scale": "local",
        "ablations": {
            "q2_pruning_vs_full_scan": {
                "unoptimized_ms": q2_unopt.profile.total_duration_ms,
                "optimized_ms": q2_opt.profile.total_duration_ms,
                "speedup_x": round(
                    q2_unopt.profile.total_duration_ms / max(0.1, q2_opt.profile.total_duration_ms), 2
                ),
                "unoptimized_rows_scanned": q2_unopt.profile.total_rows_scanned,
                "optimized_rows_scanned": q2_opt.profile.total_rows_scanned,
                "partitions_pruned": q2_opt.profile.partitions_pruned,
                "partitions_total": q2_opt.profile.partitions_total,
                "row_groups_pruned": q2_opt.profile.row_groups_pruned,
                "exact_match": ok_q2,
            },
            "q1_two_phase_agg_vs_single_phase": {
                "single_phase_ms": q1_single.profile.total_duration_ms,
                "two_phase_ms": q1_two.profile.total_duration_ms,
                "speedup_x": round(
                    q1_single.profile.total_duration_ms / max(0.1, q1_two.profile.total_duration_ms), 2
                ),
                "single_phase_shuffle_bytes": q1_single.profile.total_shuffle_bytes,
                "two_phase_shuffle_bytes": q1_two.profile.total_shuffle_bytes,
                "shuffle_reduction_pct": shuf_reduction_pct,
                "exact_match": ok_q1,
            },
            "q4_skew_salting_vs_unsalted": {
                "unsalted_ms": q4_unsalted.profile.total_duration_ms,
                "salted_ms": q4_salted.profile.total_duration_ms,
                "salt_factor": 4,
                "salted_stages": len(q4_salted.profile.stages),
                "unsalted_stages": len(q4_unsalted.profile.stages),
                "exact_match": ok_q4,
            },
            "warm_cache_vs_cold": {
                "cold_ms": q3_cold.profile.total_duration_ms,
                "warm_ms": q3_warm.profile.total_duration_ms,
                "speedup_x": round(
                    q3_cold.profile.total_duration_ms / max(0.05, q3_warm.profile.total_duration_ms), 1
                ),
                "warm_cache_hit": q3_warm.profile.result_cache_hit,
                "exact_match": ok_q3,
            },
            "q5_spill_to_disk_vs_in_memory": {
                "in_memory_ms": q5_mem.profile.total_duration_ms,
                "spill_mode_ms": q5_spill.profile.total_duration_ms,
                "memory_cap_mb": 0.5,
                "spilled_bytes": q5_spill.profile.total_spilled_bytes,
                "spill_files": q5_spill.profile.total_spill_files,
                "exact_match": ok_q5,
            },
        },
    }


def _generate_chart(bench: dict[str, Any], ablations: dict[str, Any]) -> pathlib.Path:
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = ASSETS_DIR / "benchmark_distributed_query_engine.png"

    plt.style.use("dark_background")
    fig, axes = plt.subplots(1, 3, figsize=(16.5, 5.2), dpi=150)
    fig.patch.set_facecolor("#0b101b")
    for ax in axes:
        ax.set_facecolor("#121a2a")
        ax.grid(axis="y", alpha=0.18, color="#94a3b8")
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)

    # Panel 1: Local scale latency (DQSE 3-worker gRPC vs DataFusion 55 vs DuckDB 2.0)
    local_q = bench["profiles"]["local"]["queries"]
    q_labels = ["Q1\nFiltAgg", "Q2\nPrune", "Q3\n3-Join", "Q4\nSkewSalt", "Q5\nSpill"]
    q_keys = sorted(local_q.keys())
    x = np.arange(len(q_keys))
    w = 0.26
    dq_vals = [local_q[k]["dqse_p50_ms"] for k in q_keys]
    df_vals = [local_q[k]["datafusion_p50_ms"] for k in q_keys]
    dk_vals = [local_q[k]["duckdb_p50_ms"] for k in q_keys]

    ax0 = axes[0]
    b1 = ax0.bar(x - w, dq_vals, w, label="DQSE (3-Worker gRPC)", color="#38bdf8")
    b2 = ax0.bar(x, df_vals, w, label="DataFusion 55.0 (1-Proc)", color="#a78bfa")
    b3 = ax0.bar(x + w, dk_vals, w, label="DuckDB 2.0 (1-Proc)", color="#22c55e")
    ax0.set_xticks(x)
    ax0.set_xticklabels(q_labels, fontsize=9)
    ax0.set_ylabel("Median Latency (ms, lower is better)")
    ax0.set_title("Local Scale (120K Lineitems): Engine Latency\n(100% Exact Numerical Parity Verified)", fontsize=10.5, pad=10)
    ax0.legend(frameon=False, fontsize=8, loc="upper left")
    ax0.set_ylim(0, max(dq_vals) * 1.16)
    for bars in (b1, b2, b3):
        for bar in bars:
            h = bar.get_height()
            ax0.text(bar.get_x() + bar.get_width() / 2, h + max(dq_vals) * 0.018, f"{h:.0f}", ha="center", va="bottom", fontsize=7.5, color="#e2e8f0")

    # Panel 2: Optimizer Ablation Impact (Latency Before vs After on Local Scale)
    abl = ablations["ablations"]
    ax1 = axes[1]
    ab_labels = ["Q2 Pruning\n(vs Full Scan)", "Q1 2-Phase Agg\n(vs Raw Shuffle)", "Q3 Warm Cache\n(vs Cold Exec)"]
    before_ms = [
        abl["q2_pruning_vs_full_scan"]["unoptimized_ms"],
        abl["q1_two_phase_agg_vs_single_phase"]["single_phase_ms"],
        abl["warm_cache_vs_cold"]["cold_ms"],
    ]
    after_ms = [
        abl["q2_pruning_vs_full_scan"]["optimized_ms"],
        abl["q1_two_phase_agg_vs_single_phase"]["two_phase_ms"],
        abl["warm_cache_vs_cold"]["warm_ms"],
    ]
    speedups = [
        abl["q2_pruning_vs_full_scan"]["speedup_x"],
        abl["q1_two_phase_agg_vs_single_phase"]["speedup_x"],
        abl["warm_cache_vs_cold"]["speedup_x"],
    ]
    x1 = np.arange(len(ab_labels))
    ax1.bar(x1 - 0.18, before_ms, 0.34, label="Baseline / Unoptimized", color="#64748b")
    ax1.bar(x1 + 0.18, after_ms, 0.34, label="Optimized DQSE", color="#38bdf8")
    ax1.set_xticks(x1)
    ax1.set_xticklabels(ab_labels, fontsize=9)
    ax1.set_ylabel("Latency (ms)")
    ax1.set_ylim(0, max(before_ms) * 1.20)
    ax1.set_title("Optimizer Ablations (Local Scale)\nMeasured Before vs After Latency", fontsize=10.5, pad=10)
    ax1.legend(frameon=False, fontsize=8, loc="upper right")
    for i, sp in enumerate(speedups):
        top = max(before_ms[i], after_ms[i])
        ax1.text(i, top + max(before_ms) * 0.03, f"{sp}x faster", ha="center", va="bottom", fontsize=8.5, fontweight="bold", color="#86efac")

    # Panel 3: Network Shuffle Bytes & Spill-to-Disk Telemetry
    ax2 = axes[2]
    shuf_single_kb = abl["q1_two_phase_agg_vs_single_phase"]["single_phase_shuffle_bytes"] / 1024.0
    shuf_two_kb = abl["q1_two_phase_agg_vs_single_phase"]["two_phase_shuffle_bytes"] / 1024.0
    q5_spill_kb = abl["q5_spill_to_disk_vs_in_memory"]["spilled_bytes"] / 1024.0
    stress_q1_kb = bench["profiles"]["stress"]["queries"]["q1_filtered_agg"]["shuffle_bytes"] / 1024.0

    cats = ["Q1 Single-Phase\nShuffle (local)", "Q1 Two-Phase\nShuffle (local)", "Q5 Disk Spill\n(0.5MB cap, local)", "Q1 Two-Phase\nShuffle (stress)"]
    vals = [shuf_single_kb, shuf_two_kb, q5_spill_kb, stress_q1_kb]
    colors = ["#f59e0b", "#22c55e", "#f43f5e", "#38bdf8"]
    bars2 = ax2.bar(np.arange(len(cats)), vals, 0.52, color=colors)
    ax2.set_xticks(np.arange(len(cats)))
    ax2.set_xticklabels(cats, fontsize=8.5)
    ax2.set_ylabel("Volume (KiB)")
    ax2.set_ylim(0, max(vals) * 1.16)
    red_pct = abl["q1_two_phase_agg_vs_single_phase"]["shuffle_reduction_pct"]
    ax2.set_title(f"Arrow IPC Shuffle & Disk Spill Telemetry\n(2-Phase Agg cuts Q1 shuffle by {red_pct}%)", fontsize=10.5, pad=10)
    for bar, v in zip(bars2, vals, strict=True):
        ax2.text(bar.get_x() + bar.get_width() / 2, v + max(vals) * 0.02, f"{v:.0f} KiB", ha="center", va="bottom", fontsize=8, color="#e2e8f0")

    fig.tight_layout(pad=2.0)
    fig.savefig(out_path, facecolor=fig.get_facecolor())
    plt.close(fig)
    return out_path


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    hw = _hardware_info()

    profiles: dict[str, Any] = {}
    ablations: dict[str, Any] | None = None

    print("[bench] Running smoke profile (3 iterations)...")
    profiles["smoke"], _ = run_profile("smoke", iterations=3, base_port=18291)
    print("[bench] Running local profile (3 iterations + optimizer ablations)...")
    profiles["local"], ablations = run_profile("local", iterations=3, base_port=18294)
    print("[bench] Running stress profile (2 iterations)...")
    profiles["stress"], _ = run_profile("stress", iterations=2, base_port=18297)

    assert ablations is not None
    bench_payload = {
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "hardware": hw,
        "notes": (
            "DQSE executes distributed multi-worker gRPC + Arrow IPC stages with network/IPC serialization overhead; "
            "DuckDB 2.0.0 and DataFusion 55.0.0 execute in-process on a single node for reference parity and overhead quantification."
        ),
        "profiles": profiles,
    }

    json_path = RESULTS_DIR / "benchmark_results.json"
    abl_path = RESULTS_DIR / "ablation_results.json"
    csv_path = RESULTS_DIR / "benchmark_results.csv"

    json_path.write_text(json.dumps(bench_payload, indent=2), encoding="utf-8")
    abl_path.write_text(json.dumps(ablations, indent=2), encoding="utf-8")

    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "scale",
                "query",
                "dqse_p50_ms",
                "dqse_p95_ms",
                "datafusion_p50_ms",
                "duckdb_p50_ms",
                "rows_scanned",
                "rows_returned",
                "partitions_pruned",
                "partitions_total",
                "row_groups_pruned",
                "shuffle_bytes",
                "spilled_bytes",
                "spill_files",
                "duckdb_exact_match",
                "datafusion_exact_match",
            ]
        )
        for scale_name, pdata in profiles.items():
            for qname, q in pdata["queries"].items():
                writer.writerow(
                    [
                        scale_name,
                        qname,
                        q["dqse_p50_ms"],
                        q["dqse_p95_ms"],
                        q["datafusion_p50_ms"],
                        q["duckdb_p50_ms"],
                        q["rows_scanned"],
                        q["rows_returned"],
                        q["partitions_pruned"],
                        q["partitions_total"],
                        q["row_groups_pruned"],
                        q["shuffle_bytes"],
                        q["spilled_bytes"],
                        q["spill_files"],
                        q["duckdb_exact_match"],
                        q["datafusion_exact_match"],
                    ]
                )

    chart_path = _generate_chart(bench_payload, ablations)
    print(f"[bench] Wrote {json_path}, {csv_path}, {abl_path}, and {chart_path}")


if __name__ == "__main__":
    main()
