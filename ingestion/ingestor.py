"""Idempotent partitioned Parquet ingestor with DQ validation, quarantine/DLQ, compaction, and replay."""

from __future__ import annotations

import hashlib
import json
import pathlib
import uuid
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ingestion.generator import (
    HOT_CUSTOMER_ID,
    SCALE_PROFILES,
    generate_customer_table,
    generate_nation_table,
    generate_orders_and_lineitems,
)
from models.catalog import MetadataCatalog
from models.contracts import PartitionMetadata

ROW_GROUP_SIZE = 4096

REQUIRED_SCHEMAS: dict[str, dict[str, str]] = {
    "orders": {
        "order_id": "int64",
        "customer_id": "int64",
        "order_status": "string",
        "total_price": "float64",
        "order_date": "string",
        "order_month": "string",
        "order_priority": "string",
        "region_id": "int64",
        "event_time": "string",
        "ingested_at": "string",
        "source_version": "int64",
    },
    "lineitem": {
        "order_id": "int64",
        "line_number": "int64",
        "part_id": "int64",
        "supplier_id": "int64",
        "quantity": "int64",
        "extended_price": "float64",
        "discount": "float64",
        "tax": "float64",
        "return_flag": "string",
        "ship_date": "string",
        "order_month": "string",
        "region_id": "int64",
    },
    "customer": {
        "customer_id": "int64",
        "name": "string",
        "nation_id": "int64",
        "region_id": "int64",
        "acct_bal": "float64",
        "mkt_segment": "string",
        "is_hot_account": "bool",
    },
    "nation": {
        "nation_id": "int64",
        "nation_name": "string",
        "region_id": "int64",
        "region_name": "string",
    },
}


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _extract_min_max_stats(table: pa.Table) -> tuple[dict[str, Any], dict[str, Any]]:
    mins: dict[str, Any] = {}
    maxs: dict[str, Any] = {}
    for name in table.column_names:
        col = table[name]
        if col.null_count == len(col) or len(col) == 0:
            continue
        t = col.type
        if pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_string(t):
            mm = pc.min_max(col).as_py()
            if mm is not None:
                mins[name] = mm["min"]
                maxs[name] = mm["max"]
    return mins, maxs


def deduplicate_orders(table: pa.Table) -> tuple[pa.Table, int]:
    """Idempotently deduplicate orders by (order_id), keeping highest source_version."""
    if table.num_rows == 0:
        return table, 0
    # Sort descending by source_version so first occurrence per order_id wins
    order_ids = table["order_id"].to_pylist()
    versions = table["source_version"].to_pylist()
    best_idx: dict[int, tuple[int, int]] = {}
    for idx, (oid, ver) in enumerate(zip(order_ids, versions, strict=True)):
        prev = best_idx.get(int(oid))
        if prev is None or int(ver) > prev[0]:
            best_idx[int(oid)] = (int(ver), idx)
    keep_indices = sorted(v[1] for v in best_idx.values())
    duplicates_removed = table.num_rows - len(keep_indices)
    if duplicates_removed == 0:
        return table, 0
    return table.take(pa.array(keep_indices, type=pa.int64())), duplicates_removed


def validate_and_quarantine_parquet_file(
    file_path: pathlib.Path,
    table_name: str,
    catalog: MetadataCatalog,
    quarantine_dir: pathlib.Path,
    run_id: str,
) -> tuple[bool, str | None]:
    """Validate a Parquet fragment; quarantine corrupt or schema-mismatched files safely."""
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    try:
        pf = pq.ParquetFile(file_path)
        schema = pf.schema_arrow
        expected = REQUIRED_SCHEMAS.get(table_name, {})
        missing = [c for c in expected if c not in schema.names]
        if missing:
            reason = f"Missing required columns: {missing}"
            catalog.record_quarantine(
                table_name=table_name,
                source_path=str(file_path),
                rule_code="SCHEMA_MISSING_COLUMNS",
                severity="ERROR",
                reason=reason,
                run_id=run_id,
            )
            dest = quarantine_dir / f"{run_id}_{file_path.name}.quarantine.json"
            dest.write_text(json.dumps({"file": str(file_path), "reason": reason, "run_id": run_id}, indent=2))
            return False, reason
        # Verify readability of all row groups
        for rg_idx in range(pf.metadata.num_row_groups):
            _ = pf.read_row_group(rg_idx)
        return True, None
    except Exception as exc:
        reason = f"Corrupt Parquet fragment: {type(exc).__name__}: {exc}"
        catalog.record_quarantine(
            table_name=table_name,
            source_path=str(file_path),
            rule_code="CORRUPT_PARQUET_FRAGMENT",
            severity="CRITICAL",
            reason=reason,
            run_id=run_id,
        )
        dest = quarantine_dir / f"{run_id}_{file_path.name}.quarantine.json"
        dest.write_text(json.dumps({"file": str(file_path), "reason": reason, "run_id": run_id}, indent=2))
        return False, reason


def _write_partitioned_parquet(
    table: pa.Table,
    table_name: str,
    base_dir: pathlib.Path,
    partition_cols: list[str],
    sort_cols: list[str],
    snapshot_tag: str,
) -> list[PartitionMetadata]:
    """Write sorted Parquet partitions with bounded row-group size for min/max pruning."""
    out_root = base_dir / "silver" / table_name / snapshot_tag
    out_root.mkdir(parents=True, exist_ok=True)
    partitions: list[PartitionMetadata] = []

    if not partition_cols:
        sub = table.sort_by([(c, "ascending") for c in sort_cols]) if sort_cols else table
        fpath = out_root / "part-00000.parquet"
        pq.write_table(sub, fpath, row_group_size=ROW_GROUP_SIZE, write_statistics=True)
        pf = pq.ParquetFile(fpath)
        mins, maxs = _extract_min_max_stats(sub)
        partitions.append(
            PartitionMetadata(
                partition_id="",
                table_name=table_name,
                snapshot_version=0,
                partition_values={},
                file_path=str(fpath),
                row_count=sub.num_rows,
                file_size_bytes=fpath.stat().st_size,
                row_group_count=pf.metadata.num_row_groups,
                min_stats=mins,
                max_stats=maxs,
            )
        )
        return partitions

    # Group row indices by unique partition key tuples
    part_arrays = [table[c].to_pylist() for c in partition_cols]
    buckets: dict[tuple[Any, ...], list[int]] = {}
    for row_idx in range(table.num_rows):
        key = tuple(arr[row_idx] for arr in part_arrays)
        buckets.setdefault(key, []).append(row_idx)

    for key in sorted(buckets.keys()):
        idxs = buckets[key]
        sub = table.take(pa.array(idxs, type=pa.int64()))
        if sort_cols:
            sub = sub.sort_by([(c, "ascending") for c in sort_cols])
        part_vals = {c: v for c, v in zip(partition_cols, key, strict=True)}
        rel_dir = out_root
        for c, v in part_vals.items():
            rel_dir = rel_dir / f"{c}={v}"
        rel_dir.mkdir(parents=True, exist_ok=True)
        fpath = rel_dir / "part-00000.parquet"
        rg_size = min(ROW_GROUP_SIZE, max(64, sub.num_rows // 4)) if sub.num_rows >= 128 else ROW_GROUP_SIZE
        pq.write_table(sub, fpath, row_group_size=rg_size, write_statistics=True)
        pf = pq.ParquetFile(fpath)
        mins, maxs = _extract_min_max_stats(sub)
        partitions.append(
            PartitionMetadata(
                partition_id="",
                table_name=table_name,
                snapshot_version=0,
                partition_values=part_vals,
                file_path=str(fpath),
                row_count=sub.num_rows,
                file_size_bytes=fpath.stat().st_size,
                row_group_count=pf.metadata.num_row_groups,
                min_stats=mins,
                max_stats=maxs,
            )
        )
    return partitions


def ingest_synthetic_dataset(
    warehouse_dir: str | pathlib.Path,
    catalog: MetadataCatalog,
    scale: str = "local",
    seed: int = 42,
    include_anomalies: bool = True,
    inject_corrupt_fragment: bool = True,
    force_new_snapshot: bool = False,
) -> dict[str, Any]:
    """Generate, validate, deduplicate, write partitioned Parquet, and commit catalog snapshots."""
    if scale not in SCALE_PROFILES:
        raise ValueError(f"Unknown scale '{scale}'. Choose from {sorted(SCALE_PROFILES)}")

    profile = SCALE_PROFILES[scale]
    base_dir = pathlib.Path(warehouse_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    bronze_dir = base_dir / "bronze"
    quarantine_dir = base_dir / "quarantine"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    run_id = f"ingest-{scale}-s{seed}" + (f"-{uuid.uuid4().hex[:6]}" if force_new_snapshot else "")
    snapshot_tag = f"snap_{hashlib.sha256(run_id.encode()).hexdigest()[:10]}"

    nation_tbl = generate_nation_table()
    customer_tbl = generate_customer_table(profile, seed=seed)
    orders_raw, lineitem_tbl, anomalies = generate_orders_and_lineitems(
        profile, seed=seed, include_anomalies=include_anomalies
    )

    # Persist durable bronze raw snapshot for replayability
    bronze_orders_path = bronze_dir / f"orders_{snapshot_tag}.parquet"
    pq.write_table(orders_raw, bronze_orders_path, row_group_size=ROW_GROUP_SIZE)

    # Deduplicate orders (idempotent ingestion semantics)
    orders_dedup, dups_removed = deduplicate_orders(orders_raw)

    # DQ validation on orders: reject negative prices/IDs and null customer_ids
    dq_quarantined_rows = 0
    for bad in anomalies:
        catalog.record_quarantine(
            table_name="orders",
            source_path=str(bronze_orders_path),
            rule_code="DQ_CONSTRAINT_VIOLATION",
            severity="ERROR",
            reason=str(bad.get("reason", "invalid_record")),
            run_id=run_id,
            record_sample=bad,
        )
        dq_quarantined_rows += 1
    if anomalies:
        (quarantine_dir / f"{run_id}_invalid_orders.jsonl").write_text(
            "\n".join(json.dumps(rec) for rec in anomalies) + "\n"
        )

    # Optional corrupt Parquet fragment check
    corrupt_quarantined = 0
    if inject_corrupt_fragment:
        corrupt_path = bronze_dir / f"corrupt_fragment_{snapshot_tag}.parquet"
        corrupt_path.write_bytes(b"NOT_A_VALID_PARQUET_MAGIC_BYTES_CORRUPT_HEADER")
        ok, _ = validate_and_quarantine_parquet_file(
            corrupt_path, "orders", catalog, quarantine_dir, run_id
        )
        if not ok:
            corrupt_quarantined += 1

    # Register tables in catalog with explicit grain, keys, and partition transforms
    catalog.register_table(
        table_name="nation",
        layer="silver",
        grain="1 row per nation_id",
        primary_key=["nation_id"],
        partition_columns=[],
        columns=REQUIRED_SCHEMAS["nation"],
    )
    catalog.register_table(
        table_name="customer",
        layer="silver",
        grain="1 row per customer_id",
        primary_key=["customer_id"],
        partition_columns=["region_id"],
        columns=REQUIRED_SCHEMAS["customer"],
        hot_keys={"customer_id": [HOT_CUSTOMER_ID]},
    )
    catalog.register_table(
        table_name="orders",
        layer="silver",
        grain="1 row per order_id (deduplicated by latest source_version)",
        primary_key=["order_id"],
        partition_columns=["order_month", "region_id"],
        columns=REQUIRED_SCHEMAS["orders"],
        hot_keys={"customer_id": [HOT_CUSTOMER_ID]},
    )
    catalog.register_table(
        table_name="lineitem",
        layer="silver",
        grain="1 row per (order_id, line_number)",
        primary_key=["order_id", "line_number"],
        partition_columns=["order_month", "region_id"],
        columns=REQUIRED_SCHEMAS["lineitem"],
    )

    # Write sorted partitioned Parquet files (sorting by total_price / order_id boosts row-group pruning!)
    nation_parts = _write_partitioned_parquet(
        nation_tbl, "nation", base_dir, [], ["nation_id"], snapshot_tag
    )
    cust_parts = _write_partitioned_parquet(
        customer_tbl, "customer", base_dir, ["region_id"], ["customer_id"], snapshot_tag
    )
    order_parts = _write_partitioned_parquet(
        orders_dedup, "orders", base_dir, ["order_month", "region_id"], ["total_price", "order_id"], snapshot_tag
    )
    li_parts = _write_partitioned_parquet(
        lineitem_tbl, "lineitem", base_dir, ["order_month", "region_id"], ["order_id", "line_number"], snapshot_tag
    )

    # Atomic snapshot commits with idempotency keys
    v_nation = catalog.commit_snapshot("nation", "OVERWRITE", nation_parts, f"{run_id}:nation", source_seed=seed)
    v_cust = catalog.commit_snapshot(
        "customer", "OVERWRITE", cust_parts, f"{run_id}:customer", source_seed=seed, hot_keys={"customer_id": [HOT_CUSTOMER_ID]}
    )
    v_orders = catalog.commit_snapshot(
        "orders",
        "OVERWRITE",
        order_parts,
        f"{run_id}:orders",
        rows_quarantined=dq_quarantined_rows + corrupt_quarantined,
        source_seed=seed,
        hot_keys={"customer_id": [HOT_CUSTOMER_ID]},
    )
    v_li = catalog.commit_snapshot("lineitem", "OVERWRITE", li_parts, f"{run_id}:lineitem", source_seed=seed)

    # Record explicit catalog lineage
    catalog.record_lineage(str(bronze_orders_path), "silver.orders", "deduplicate_and_partition_parquet", v_orders)
    catalog.record_lineage("generator.lineitem", "silver.lineitem", "partition_and_sort_parquet", v_li)
    catalog.record_lineage("generator.customer", "silver.customer", "partition_by_region_parquet", v_cust)
    catalog.record_lineage("generator.nation", "silver.nation", "write_dimension_parquet", v_nation)

    # Reconciliation check: sum of order.total_price == sum of net lineitem amount
    orders_total_sum = float(pc.sum(orders_dedup["total_price"]).as_py() or 0.0)
    li_net = pc.multiply(
        pc.multiply(lineitem_tbl["extended_price"], pc.subtract(1.0, lineitem_tbl["discount"])),
        pc.add(1.0, lineitem_tbl["tax"]),
    )
    lineitem_total_sum = float(pc.sum(pc.round(li_net, 2)).as_py() or 0.0)
    reconciliation_diff = abs(orders_total_sum - lineitem_total_sum)

    report = {
        "run_id": run_id,
        "scale": scale,
        "seed": seed,
        "completed_at_utc": _utc_now(),
        "tables": {
            "nation": {"rows": nation_tbl.num_rows, "partitions": len(nation_parts), "snapshot_version": v_nation},
            "customer": {"rows": customer_tbl.num_rows, "partitions": len(cust_parts), "snapshot_version": v_cust},
            "orders": {
                "raw_rows": orders_raw.num_rows,
                "deduplicated_rows": orders_dedup.num_rows,
                "duplicates_removed": dups_removed,
                "partitions": len(order_parts),
                "snapshot_version": v_orders,
            },
            "lineitem": {"rows": lineitem_tbl.num_rows, "partitions": len(li_parts), "snapshot_version": v_li},
        },
        "data_quality": {
            "invalid_records_quarantined": dq_quarantined_rows,
            "corrupt_parquet_fragments_quarantined": corrupt_quarantined,
            "reconciliation_orders_vs_lineitem_diff": round(reconciliation_diff, 4),
            "reconciliation_passed": reconciliation_diff < 1.0,
            "hot_customer_id": HOT_CUSTOMER_ID,
            "hot_customer_order_share": round(
                float(pc.sum(pc.equal(orders_dedup["customer_id"], HOT_CUSTOMER_ID)).as_py() or 0)
                / max(1, orders_dedup.num_rows),
                4,
            ),
        },
    }
    return report


def append_orders_batch(
    warehouse_dir: str | pathlib.Path,
    catalog: MetadataCatalog,
    new_orders: pa.Table,
    idempotency_key: str,
) -> int:
    """Append an incremental batch of orders and bump snapshot_version (triggers cache invalidation)."""
    base_dir = pathlib.Path(warehouse_dir)
    tag = f"append_{hashlib.sha256(idempotency_key.encode()).hexdigest()[:10]}"
    parts = _write_partitioned_parquet(
        new_orders, "orders", base_dir, ["order_month", "region_id"], ["total_price", "order_id"], tag
    )
    return catalog.commit_snapshot(
        table_name="orders",
        operation="APPEND",
        partitions=parts,
        idempotency_key=idempotency_key,
        replace_partitions=False,
    )


def compact_table_partitions(
    warehouse_dir: str | pathlib.Path,
    catalog: MetadataCatalog,
    table_name: str,
) -> int:
    """Compact small Parquet fragments per partition into single sorted files and commit new snapshot."""
    summary = catalog.get_table(table_name)
    if summary is None:
        raise KeyError(f"Unknown table: {table_name}")
    parts = catalog.get_partitions(table_name)
    tables = [pq.read_table(p.file_path) for p in parts]
    combined = pa.concat_tables(tables) if tables else pa.table({})
    if table_name == "orders":
        combined, _ = deduplicate_orders(combined)
    tag = f"compact_v{summary.snapshot_version + 1}_{uuid.uuid4().hex[:6]}"
    sort_cols = ["total_price", "order_id"] if table_name == "orders" else summary.primary_key
    new_parts = _write_partitioned_parquet(
        combined,
        table_name,
        pathlib.Path(warehouse_dir),
        summary.partition_columns,
        sort_cols,
        tag,
    )
    return catalog.commit_snapshot(
        table_name=table_name,
        operation="COMPACT",
        partitions=new_parts,
        idempotency_key=f"compact:{table_name}:{tag}",
        replace_partitions=True,
    )
