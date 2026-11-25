"""Integration tests verifying distributed gRPC execution and exact parity vs DuckDB 2.0 & DataFusion 55.0."""

from __future__ import annotations

import pathlib

import pytest

from models.contracts import OptimizerOptions

SQL_DIR = pathlib.Path(__file__).resolve().parent.parent.parent / "sql" / "queries"


@pytest.mark.integration
@pytest.mark.parametrize(
    "query_file",
    [
        "q1_filtered_agg.sql",
        "q2_partition_pruned_point.sql",
        "q3_distributed_join_agg.sql",
        "q4_skewed_hotkey_agg.sql",
        "q5_low_memory_spill_agg.sql",
    ],
)
def test_all_benchmark_queries_exact_parity_vs_duckdb_and_datafusion(
    smoke_cluster: dict,
    query_file: str,
) -> None:
    coord = smoke_cluster["coordinator"]
    sql = (SQL_DIR / query_file).read_text(encoding="utf-8")
    opts = OptimizerOptions(
        enable_cache=False,
        max_worker_mem_mb=0.04 if "q5" in query_file else 0.0,
    )
    resp = coord.execute_query(sql, options=opts, verify_reference=True)
    parity = resp.profile.reference_parity
    assert parity is not None
    assert parity["duckdb"]["exact_match"], f"DuckDB mismatch on {query_file}: {parity}"
    assert parity["datafusion"]["exact_match"], f"DataFusion mismatch on {query_file}: {parity}"
    if "q2" in query_file:
        assert resp.profile.partitions_pruned == 29
    if "q4" in query_file:
        assert "skew_salting" in resp.profile.optimizer_rules_applied
    if "q5" in query_file:
        assert resp.profile.total_spilled_bytes > 0
        assert resp.profile.total_spill_files > 0


@pytest.mark.integration
def test_row_group_min_max_pruning_and_explain_sql(smoke_cluster: dict) -> None:
    coord = smoke_cluster["coordinator"]
    # Selective price predicate on sorted orders partitions prunes Parquet row groups
    sql = "SELECT order_id, total_price FROM orders WHERE order_month = '2025-03' AND total_price >= 35000.0 ORDER BY order_id"
    resp = coord.execute_query(sql, options=OptimizerOptions(enable_cache=False), verify_reference=True)
    assert resp.profile.partitions_pruned == 25
    assert resp.profile.reference_parity["duckdb"]["exact_match"]
    assert resp.profile.reference_parity["datafusion"]["exact_match"]

    exp = coord.execute_query(f"EXPLAIN {sql}")
    assert exp.columns == ["plan_type", "plan"]
    assert exp.row_count == 3
    assert "PhysicalDistributedDAG" in exp.rows[2][1]
