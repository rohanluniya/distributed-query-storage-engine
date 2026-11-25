"""Performance regression smoke checks verifying pruning speedup and two-phase agg shuffle reduction."""

from __future__ import annotations

import pathlib

import pytest

from models.contracts import OptimizerOptions

SQL_DIR = pathlib.Path(__file__).resolve().parent.parent.parent / "sql" / "queries"


@pytest.mark.performance
def test_two_phase_agg_cuts_shuffle_bytes_and_pruning_cuts_rows(smoke_cluster: dict) -> None:
    coord = smoke_cluster["coordinator"]

    # Q1: Two-phase agg cuts shuffle bytes substantially vs single-phase raw shuffle
    q1_sql = (SQL_DIR / "q1_filtered_agg.sql").read_text(encoding="utf-8")
    r_single = coord.execute_query(q1_sql, options=OptimizerOptions(two_phase_agg=False, enable_cache=False))
    r_two = coord.execute_query(q1_sql, options=OptimizerOptions(two_phase_agg=True, enable_cache=False))
    assert r_two.profile.total_shuffle_bytes < r_single.profile.total_shuffle_bytes * 0.5

    # Q2: Partition pruning reduces rows scanned by > 90%
    q2_sql = (SQL_DIR / "q2_partition_pruned_point.sql").read_text(encoding="utf-8")
    r_full = coord.execute_query(
        q2_sql,
        options=OptimizerOptions(
            projection_pushdown=False,
            predicate_pushdown=False,
            partition_pruning=False,
            enable_cache=False,
        ),
    )
    r_pruned = coord.execute_query(q2_sql, options=OptimizerOptions(enable_cache=False))
    assert r_pruned.profile.total_rows_scanned < r_full.profile.total_rows_scanned * 0.15
    assert r_pruned.profile.partitions_pruned == 29
