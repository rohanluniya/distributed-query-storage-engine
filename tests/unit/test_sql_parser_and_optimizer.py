"""Unit tests for custom recursive-descent SQL parser, logical planner, and 5-rule optimizer."""

from __future__ import annotations

import pathlib

import pytest

from ingestion.ingestor import ingest_synthetic_dataset
from models.catalog import MetadataCatalog
from models.contracts import OptimizerOptions
from processing.logical_plan import (
    PlanError,
    build_logical_plan,
    format_logical_plan,
    optimize_logical_plan,
)
from processing.physical_plan import compile_physical_dag
from processing.sql_parser import SQLParseError, expr_from_dict, parse_sql


@pytest.mark.unit
def test_sql_parser_full_analytical_subset() -> None:
    sql = """
    SELECT
      n.region_name,
      COUNT(*) AS order_cnt,
      SUM(o.total_price * (1 - 0.05)) AS net_rev,
      AVG(o.total_price) AS avg_rev,
      MIN(o.total_price) AS min_rev,
      MAX(o.total_price) AS max_rev
    FROM orders o
    INNER JOIN customer c ON o.customer_id = c.customer_id
    INNER JOIN nation n ON c.nation_id = n.nation_id
    WHERE o.order_month IN ('2025-03', '2025-04')
      AND o.total_price BETWEEN 100.0 AND 50000.0
      AND NOT (c.acct_bal < 0)
    GROUP BY n.region_name
    ORDER BY net_rev DESC, n.region_name ASC
    LIMIT 10;
    """
    ast = parse_sql(sql)
    assert len(ast.select_items) == 6
    assert ast.from_table.name == "orders"
    assert ast.from_table.alias == "o"
    assert len(ast.joins) == 2
    assert ast.where is not None
    # Verify AST JSON round-trip
    assert expr_from_dict(ast.where.to_dict()) == ast.where
    assert len(ast.group_by) == 1
    assert len(ast.order_by) == 2
    assert ast.order_by[0].ascending is False
    assert ast.limit == 10


@pytest.mark.unit
def test_sql_parser_explain_and_errors() -> None:
    ast = parse_sql("EXPLAIN SELECT order_id FROM orders LIMIT 5")
    assert ast.is_explain is True
    assert ast.limit == 5

    with pytest.raises(SQLParseError):
        parse_sql("SELECT FROM orders")
    with pytest.raises(SQLParseError):
        parse_sql("SELECT 'unterminated FROM orders")
    with pytest.raises(SQLParseError):
        parse_sql("SELECT order_id FROM orders o EXTRA_GARBAGE")


@pytest.mark.unit
def test_optimizer_rules_and_dag_compilation(tmp_path: pathlib.Path) -> None:
    catalog = MetadataCatalog(tmp_path / "cat.sqlite3")
    ingest_synthetic_dataset(tmp_path / "wh", catalog, scale="smoke", seed=42)

    # Q2 exercises predicate pushdown, partition pruning, and projection pushdown
    q2 = "SELECT order_id, total_price FROM orders WHERE order_month = '2025-03' AND region_id = 1 AND total_price > 1000"
    raw = build_logical_plan(q2, parse_sql(q2), catalog)
    assert raw.partitions_pruned == 0
    assert len(raw.base_scan.required_columns) == len(raw.base_scan.all_columns)

    opt = optimize_logical_plan(raw, OptimizerOptions())
    assert "predicate_pushdown" in opt.optimizer_rules_applied
    assert "partition_pruning" in opt.optimizer_rules_applied
    assert "projection_pushdown" in opt.optimizer_rules_applied
    assert opt.partitions_pruned == 29
    assert len(opt.base_scan.selected_partitions) == 1
    assert len(opt.base_scan.required_columns) < len(opt.base_scan.all_columns)

    dag = compile_physical_dag(opt, OptimizerOptions())
    dag_text = dag.format_dag()
    assert "SCAN_DIRECT" in dag_text
    assert "LogicalPlan" in format_logical_plan(raw)

    # Q4 exercises two-phase agg + skew salting on hot customer_id
    q4 = "SELECT customer_id, COUNT(*) AS cnt, SUM(total_price) AS rev FROM orders GROUP BY customer_id"
    opt4 = optimize_logical_plan(build_logical_plan(q4, parse_sql(q4), catalog), OptimizerOptions())
    assert "two_phase_agg" in opt4.optimizer_rules_applied
    assert "skew_salting" in opt4.optimizer_rules_applied
    assert opt4.skew_salt_column == "orders.customer_id"
    dag4 = compile_physical_dag(opt4, OptimizerOptions())
    assert any(st.stage_type == "SALTED_AGG" for st in dag4.stages)

    # Unknown table/column raises PlanError
    with pytest.raises(PlanError):
        build_logical_plan("SELECT x FROM missing_table", parse_sql("SELECT x FROM missing_table"), catalog)
    with pytest.raises(PlanError):
        build_logical_plan("SELECT nonexistent_col FROM orders", parse_sql("SELECT nonexistent_col FROM orders"), catalog)
