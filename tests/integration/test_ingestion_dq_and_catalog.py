"""Integration tests for deterministic seeding, idempotent ingestion, DQ quarantine, replay, and compaction."""

from __future__ import annotations

import pathlib

import pyarrow as pa
import pytest

from ingestion.generator import SCALE_PROFILES, generate_orders_and_lineitems
from ingestion.ingestor import (
    append_orders_batch,
    compact_table_partitions,
    ingest_synthetic_dataset,
    validate_and_quarantine_parquet_file,
)
from models.catalog import MetadataCatalog


@pytest.mark.integration
def test_deterministic_seed_produces_identical_ids_and_totals() -> None:
    prof = SCALE_PROFILES["smoke"]
    o1, l1, a1 = generate_orders_and_lineitems(prof, seed=42)
    o2, l2, a2 = generate_orders_and_lineitems(prof, seed=42)
    assert o1.to_pydict() == o2.to_pydict()
    assert l1.to_pydict() == l2.to_pydict()
    assert a1 == a2


@pytest.mark.integration
def test_idempotent_ingest_quarantine_append_and_compaction(tmp_path: pathlib.Path) -> None:
    cat = MetadataCatalog(tmp_path / "cat.sqlite3")
    wh = tmp_path / "wh"

    r1 = ingest_synthetic_dataset(wh, cat, scale="smoke", seed=42)
    assert r1["data_quality"]["reconciliation_passed"] is True
    assert r1["data_quality"]["invalid_records_quarantined"] == 2
    assert r1["data_quality"]["corrupt_parquet_fragments_quarantined"] == 1
    assert r1["tables"]["orders"]["duplicates_removed"] > 0

    # Re-running with same seed & idempotency key does NOT duplicate snapshots
    r2 = ingest_synthetic_dataset(wh, cat, scale="smoke", seed=42)
    assert r2["tables"]["orders"]["snapshot_version"] == r1["tables"]["orders"]["snapshot_version"]

    # Bad schema parquet gets quarantined cleanly
    bad_file = tmp_path / "bad_schema.parquet"
    import pyarrow.parquet as pq

    pq.write_table(pa.table({"wrong_col": pa.array([1, 2, 3])}), bad_file)
    ok, reason = validate_and_quarantine_parquet_file(bad_file, "orders", cat, wh / "quarantine", "test-run")
    assert not ok
    assert reason and "Missing required columns" in reason

    # Incremental append bumps snapshot version
    v_before = cat.get_table("orders").snapshot_version
    extra = pa.table(
        {
            "order_id": pa.array([900001], type=pa.int64()),
            "customer_id": pa.array([1], type=pa.int64()),
            "order_status": pa.array(["O"], type=pa.string()),
            "total_price": pa.array([1234.56], type=pa.float64()),
            "order_date": pa.array(["2025-03-15"], type=pa.string()),
            "order_month": pa.array(["2025-03"], type=pa.string()),
            "order_priority": pa.array(["1-URGENT"], type=pa.string()),
            "region_id": pa.array([0], type=pa.int64()),
            "event_time": pa.array(["2025-03-15T12:00:00Z"], type=pa.string()),
            "ingested_at": pa.array(["2025-07-01T00:00:00Z"], type=pa.string()),
            "source_version": pa.array([1], type=pa.int64()),
        }
    )
    v_after = append_orders_batch(wh, cat, extra, idempotency_key="append-batch-1")
    assert v_after == v_before + 1
    assert cat.get_table("orders").row_count == r1["tables"]["orders"]["deduplicated_rows"] + 1

    # Compaction reduces partition files and preserves exact row count
    v_compact = compact_table_partitions(wh, cat, "orders")
    assert v_compact == v_after + 1
    assert cat.get_table("orders").row_count == r1["tables"]["orders"]["deduplicated_rows"] + 1
    assert len(cat.list_lineage()) >= 4
    assert len(cat.list_quarantine()) >= 4
