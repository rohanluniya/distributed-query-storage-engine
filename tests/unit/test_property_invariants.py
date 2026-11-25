"""Hypothesis property-based invariant tests for spill exactness, consistent hashing, caching, and optimizer parity."""

from __future__ import annotations

import pathlib
import tempfile

import pyarrow as pa
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from models.contracts import OptimizerOptions
from processing.cache import VersionedLRUCache
from processing.consistent_hash import ConsistentHashRing
from processing.memory_pool import MemoryPool, SpillManager
from processing.operators import execute_hash_aggregate, table_to_json_rows
from processing.reference_engines import compare_results, execute_duckdb


@pytest.mark.unit
@settings(max_examples=20, deadline=None)
@given(
    pairs=st.lists(
        st.tuples(
            st.integers(min_value=1, max_value=120),
            st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
        ),
        min_size=10,
        max_size=400,
    )
)
def test_property_spill_agg_equals_in_memory_agg(pairs: list[tuple[int, float]]) -> None:
    """Property: For any key/value distribution, spill-to-disk agg equals in-memory agg."""
    tbl = pa.table(
        {
            "k": pa.array([p[0] for p in pairs], type=pa.int64()),
            "x": pa.array([p[1] for p in pairs], type=pa.float64()),
        }
    )
    aggs = [
        {"output_name": "cnt", "func": "COUNT", "arg": None},
        {"output_name": "s", "func": "SUM", "arg": {"kind": "col", "table": None, "column": "x"}},
        {"output_name": "a", "func": "AVG", "arg": {"kind": "col", "table": None, "column": "x"}},
        {"output_name": "mn", "func": "MIN", "arg": {"kind": "col", "table": None, "column": "x"}},
        {"output_name": "mx", "func": "MAX", "arg": {"kind": "col", "table": None, "column": "x"}},
    ]
    in_mem = execute_hash_aggregate([tbl], ["k"], aggs, "single", MemoryPool(32 * 1024 * 1024))
    with tempfile.TemporaryDirectory() as td:
        sm = SpillManager(pathlib.Path(td), "prop-q", "t1")
        spilled = execute_hash_aggregate([tbl], ["k"], aggs, "single", MemoryPool(4096), spill_manager=sm)
        sm.cleanup()

    _, r1 = table_to_json_rows(in_mem)
    _, r2 = table_to_json_rows(spilled)
    ok, msg = compare_results(r2, r1, ordered=False)
    assert ok, msg


@pytest.mark.unit
@settings(max_examples=20, deadline=None)
@given(keys=st.lists(st.text(min_size=1, max_size=24), min_size=50, max_size=150, unique=True))
def test_property_consistent_hash_minimal_key_movement(keys: list[str]) -> None:
    """Property: Adding a 4th worker never reshuffles unaffected keys to another existing worker."""
    ring = ConsistentHashRing(["w1", "w2", "w3"], vnodes=64)
    before = {k: ring.get_node(k) for k in keys}
    ring.add_node("w4")
    after = {k: ring.get_node(k) for k in keys}
    for k in keys:
        # A key either stays on its original worker OR moves exclusively to the newly added worker w4
        assert after[k] in (before[k], "w4")


@pytest.mark.unit
@settings(max_examples=25, deadline=None)
@given(
    v1=st.integers(min_value=1, max_value=50),
    delta=st.integers(min_value=1, max_value=20),
    payload=st.text(min_size=1, max_size=40),
)
def test_property_cache_never_serves_stale_snapshot(v1: int, delta: int, payload: str) -> None:
    """Property: VersionedLRUCache never returns an entry written under an older table snapshot version."""
    cache: VersionedLRUCache[str] = VersionedLRUCache(max_bytes=4096)
    cache.put("k", payload, size_bytes=64, table_versions={"orders": v1})
    assert cache.get("k", current_versions={"orders": v1}) == payload
    v2 = v1 + delta
    assert cache.get("k", current_versions={"orders": v2}) is None


@pytest.mark.unit
@settings(max_examples=12, deadline=None)
@given(
    region_id=st.integers(min_value=0, max_value=4),
    min_price=st.floats(min_value=0.0, max_value=20000.0, allow_nan=False, allow_infinity=False),
    month_idx=st.integers(min_value=1, max_value=6),
)
def test_property_optimizer_equivalence_vs_duckdb(
    smoke_cluster: dict,
    region_id: int,
    min_price: float,
    month_idx: int,
) -> None:
    """Property: Randomized WHERE + GROUP BY queries match DuckDB 2.0 identically with all optimizer rules ON or OFF."""
    coord = smoke_cluster["coordinator"]
    catalog = smoke_cluster["catalog"]
    month = f"2025-0{month_idx}"
    sql = (
        f"SELECT order_status, COUNT(*) AS cnt, SUM(total_price) AS tot "
        f"FROM orders "
        f"WHERE order_month = '{month}' AND region_id = {region_id} AND total_price >= {min_price:.2f} "
        f"GROUP BY order_status ORDER BY order_status ASC"
    )
    opt_resp = coord.execute_query(sql, options=OptimizerOptions(enable_cache=False))
    unopt_resp = coord.execute_query(
        sql,
        options=OptimizerOptions(
            projection_pushdown=False,
            predicate_pushdown=False,
            partition_pruning=False,
            two_phase_agg=False,
            skew_salting=False,
            enable_cache=False,
        ),
    )
    dk = execute_duckdb(sql, catalog)
    ok1, msg1 = compare_results(opt_resp.rows, dk["rows"], ordered=True)
    ok2, msg2 = compare_results(unopt_resp.rows, dk["rows"], ordered=True)
    assert ok1, f"Optimized vs DuckDB failed: {msg1}"
    assert ok2, f"Unoptimized vs DuckDB failed: {msg2}"
