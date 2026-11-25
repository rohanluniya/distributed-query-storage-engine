"""Unit tests for explicit MemoryPool accounting and Arrow IPC spill-to-disk agg & join exactness."""

from __future__ import annotations

import pathlib

import pyarrow as pa
import pytest

from processing.memory_pool import MemoryLimitExceeded, MemoryPool, SpillManager
from processing.operators import execute_hash_aggregate, execute_hash_join, table_to_json_rows
from processing.reference_engines import compare_results


@pytest.mark.unit
def test_memory_pool_accounting_and_limit() -> None:
    pool = MemoryPool(max_bytes=1024)
    pool.reserve(600)
    assert pool.used_bytes == 600
    assert pool.peak_bytes == 600
    assert not pool.try_reserve(500)
    with pytest.raises(MemoryLimitExceeded):
        pool.reserve(500)
    pool.release(400)
    assert pool.used_bytes == 200
    assert pool.peak_bytes == 600
    assert pool.try_reserve(800)
    assert pool.peak_bytes == 1000


@pytest.mark.unit
def test_hash_aggregate_spill_to_disk_exactness(tmp_path: pathlib.Path) -> None:
    n = 3000
    tbl = pa.table(
        {
            "g": pa.array([i % 750 for i in range(n)], type=pa.int64()),
            "v": pa.array([float((i * 17) % 1000) / 3.0 for i in range(n)], type=pa.float64()),
        }
    )
    aggs = [
        {"output_name": "cnt", "func": "COUNT", "arg": None},
        {"output_name": "sum_v", "func": "SUM", "arg": {"kind": "col", "table": None, "column": "v"}},
        {"output_name": "avg_v", "func": "AVG", "arg": {"kind": "col", "table": None, "column": "v"}},
        {"output_name": "min_v", "func": "MIN", "arg": {"kind": "col", "table": None, "column": "v"}},
        {"output_name": "max_v", "func": "MAX", "arg": {"kind": "col", "table": None, "column": "v"}},
    ]

    # In-memory baseline
    in_mem = execute_hash_aggregate(
        [tbl], ["g"], aggs, mode="single", memory_pool=MemoryPool(64 * 1024 * 1024)
    )

    # Tight 64 KiB budget forcing multiple spill waves to Arrow IPC files
    spill_mgr = SpillManager(tmp_path / "spill", "q-spill-unit", "t1")
    tight_pool = MemoryPool(max_bytes=64 * 1024)
    spilled = execute_hash_aggregate(
        [tbl], ["g"], aggs, mode="single", memory_pool=tight_pool, spill_manager=spill_mgr
    )
    assert spill_mgr.has_spilled
    assert spill_mgr.spilled_bytes > 0
    assert spill_mgr.spill_files > 0
    spill_mgr.cleanup()
    assert not spill_mgr.spill_root.exists()

    _, rows_mem = table_to_json_rows(in_mem)
    _, rows_spill = table_to_json_rows(spilled)
    ok, msg = compare_results(rows_spill, rows_mem, ordered=False)
    assert ok, msg


@pytest.mark.unit
def test_grace_hash_join_spill_to_disk_exactness(tmp_path: pathlib.Path) -> None:
    left = pa.table(
        {
            "o.order_id": pa.array(list(range(1, 1001)), type=pa.int64()),
            "o.customer_id": pa.array([(i % 200) + 1 for i in range(1000)], type=pa.int64()),
            "o.price": pa.array([float(i * 1.5) for i in range(1000)], type=pa.float64()),
        }
    )
    right = pa.table(
        {
            "c.customer_id": pa.array(list(range(1, 201)), type=pa.int64()),
            "c.segment": pa.array([f"SEG-{i % 5}" for i in range(200)], type=pa.string()),
        }
    )

    in_mem = execute_hash_join(
        [left], [right], "o.customer_id", "c.customer_id", MemoryPool(64 * 1024 * 1024)
    )

    spill_mgr = SpillManager(tmp_path / "spill_join", "q-join-spill", "t1")
    spilled = execute_hash_join(
        [left], [right], "o.customer_id", "c.customer_id", MemoryPool(4 * 1024), spill_manager=spill_mgr
    )
    assert spill_mgr.has_spilled
    assert spill_mgr.spilled_bytes > 0
    spill_mgr.cleanup()

    _, r_mem = table_to_json_rows(in_mem)
    _, r_spill = table_to_json_rows(spilled)
    ok, msg = compare_results(r_spill, r_mem, ordered=False)
    assert ok, msg
