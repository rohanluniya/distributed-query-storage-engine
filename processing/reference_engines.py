"""Reference engine verification & benchmarking adapter for DuckDB 2.0.0 and Apache DataFusion 55.0.0."""

from __future__ import annotations

import math
import time
from typing import Any

import datafusion
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from models.catalog import MetadataCatalog
from processing.operators import table_to_json_rows


def _load_catalog_tables(catalog: MetadataCatalog) -> dict[str, list[str]]:
    table_files: dict[str, list[str]] = {}
    for summary in catalog.list_tables():
        parts = catalog.get_partitions(summary.table_name, summary.snapshot_version)
        table_files[summary.table_name] = [p.file_path for p in parts]
    return table_files


def execute_duckdb(sql: str, catalog: MetadataCatalog) -> dict[str, Any]:
    """Execute SQL directly on DuckDB over the exact active catalog Parquet files."""
    table_files = _load_catalog_tables(catalog)
    conn = duckdb.connect(":memory:")
    try:
        for tname, files in table_files.items():
            if files:
                quoted = ", ".join(f"'{f}'" for f in files)
                conn.execute(f"CREATE VIEW {tname} AS SELECT * FROM read_parquet([{quoted}])")
        t0 = time.perf_counter()
        arrow_tbl = conn.sql(sql).to_arrow_table()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        cols, rows = table_to_json_rows(arrow_tbl)
        return {
            "engine": "duckdb",
            "version": duckdb.__version__,
            "duration_ms": round(elapsed_ms, 3),
            "columns": cols,
            "rows": rows,
            "row_count": len(rows),
        }
    finally:
        conn.close()


def execute_datafusion(sql: str, catalog: MetadataCatalog) -> dict[str, Any]:
    """Execute SQL directly on Rust-compiled Apache DataFusion over the exact active catalog Parquet files."""
    table_files = _load_catalog_tables(catalog)
    ctx = datafusion.SessionContext()
    for tname, files in table_files.items():
        if not files:
            continue
        if len(files) == 1:
            ctx.register_parquet(tname, files[0])
        else:
            # Register exact active snapshot files as Arrow RecordBatches (respects snapshot isolation!)
            tables = [pq.read_table(f) for f in files]
            combined = pa.concat_tables(tables)
            ctx.register_record_batches(tname, [combined.to_batches()])

    t0 = time.perf_counter()
    df = ctx.sql(sql)
    batches = df.collect()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    arrow_tbl = pa.Table.from_batches(batches) if batches else pa.table({})
    cols, rows = table_to_json_rows(arrow_tbl)
    return {
        "engine": "datafusion",
        "version": datafusion.__version__,
        "duration_ms": round(elapsed_ms, 3),
        "columns": cols,
        "rows": rows,
        "row_count": len(rows),
    }


def _normalize_cell(v: Any) -> Any:
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    return v


def _sort_key_row(row: list[Any]) -> tuple[Any, ...]:
    out: list[tuple[int, Any]] = []
    for v in row:
        if v is None:
            out.append((0, ""))
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            out.append((1, round(float(v), 5)))
        else:
            out.append((2, str(v)))
    return tuple(out)


def compare_results(
    dqse_rows: list[list[Any]],
    ref_rows: list[list[Any]],
    ordered: bool = False,
    float_tol: float = 1e-3,
) -> tuple[bool, str]:
    """Assert numerical equivalence between DQSE output and DuckDB/DataFusion reference output."""
    if len(dqse_rows) != len(ref_rows):
        return False, f"Row count mismatch: dqse={len(dqse_rows)} vs ref={len(ref_rows)}"

    lhs = dqse_rows if ordered else sorted(dqse_rows, key=_sort_key_row)
    rhs = ref_rows if ordered else sorted(ref_rows, key=_sort_key_row)

    for r_idx, (lr, rr) in enumerate(zip(lhs, rhs, strict=True)):
        if len(lr) != len(rr):
            return False, f"Column count mismatch at row {r_idx}: {len(lr)} vs {len(rr)}"
        for c_idx, (lv, rv) in enumerate(zip(lr, rr, strict=True)):
            if lv is None and rv is None:
                continue
            if isinstance(lv, (int, float)) and isinstance(rv, (int, float)) and not isinstance(lv, bool):
                denom = max(1.0, abs(float(rv)))
                if not math.isclose(float(lv), float(rv), rel_tol=1e-5, abs_tol=float_tol * denom):
                    return (
                        False,
                        f"Numeric mismatch at row {r_idx} col {c_idx}: dqse={lv} vs ref={rv}",
                    )
            elif lv != rv:
                return False, f"Value mismatch at row {r_idx} col {c_idx}: dqse={lv!r} vs ref={rv!r}"
    return True, "exact_match"
