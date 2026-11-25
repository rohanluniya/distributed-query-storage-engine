"""Vectorized Apache Arrow execution operators with Parquet row-group pruning, skew salting, and spill-to-disk."""

from __future__ import annotations

import math
import zlib
from dataclasses import dataclass
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq

from processing.memory_pool import MemoryPool, SpillManager
from processing.sql_parser import (
    BetweenExpr,
    BinaryOp,
    ColumnRef,
    Expr,
    InExpr,
    Literal,
    UnaryOp,
    expr_from_dict,
)


def serialize_table(table: pa.Table) -> bytes:
    """Serialize an Apache Arrow table to Arrow IPC stream bytes for gRPC wire transport."""
    sink = pa.BufferOutputStream()
    with pa_ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def deserialize_table(payload: bytes) -> pa.Table:
    """Deserialize Arrow IPC stream bytes back into an Apache Arrow table."""
    if not payload:
        return pa.table({})
    reader = pa_ipc.open_stream(pa.py_buffer(payload))
    return reader.read_all()


def concat_non_empty(tables: list[pa.Table], fallback_schema: pa.Schema | None = None) -> pa.Table:
    valid = [t for t in tables if t.num_columns > 0 and t.num_rows > 0]
    if valid:
        if len(valid) == 1:
            return valid[0]
        return pa.concat_tables(valid, promote_options="default")
    non_empty_schema = [t for t in tables if t.num_columns > 0]
    if non_empty_schema:
        return non_empty_schema[0].slice(0, 0)
    if fallback_schema is not None:
        return fallback_schema.empty_table()
    return pa.table({})


def _resolve_col_array(table: pa.Table, ref: ColumnRef) -> pa.Array | pa.ChunkedArray:
    full = ref.full_name()
    if full in table.column_names:
        return table[full]
    if ref.column in table.column_names:
        return table[ref.column]
    # Suffix fallback for unqualified columns
    suffix = f".{ref.column}"
    matches = [c for c in table.column_names if c.endswith(suffix)]
    if len(matches) == 1:
        return table[matches[0]]
    raise KeyError(f"Column '{full}' not found in batch columns {table.column_names}")


def eval_expr(table: pa.Table, expr: Expr | dict[str, Any]) -> pa.Array | pa.ChunkedArray:
    """Evaluate a bound AST expression vectorially using PyArrow compute kernels."""
    node = expr_from_dict(expr) if isinstance(expr, dict) else expr
    n = table.num_rows

    if isinstance(node, Literal):
        val = node.value
        if isinstance(val, bool):
            return pa.array([val] * n, type=pa.bool_())
        if isinstance(val, int):
            return pa.array([val] * n, type=pa.int64())
        if isinstance(val, float):
            return pa.array([val] * n, type=pa.float64())
        if isinstance(val, str):
            return pa.array([val] * n, type=pa.string())
        return pa.array([val] * n)

    if isinstance(node, ColumnRef):
        return _resolve_col_array(table, node)

    if isinstance(node, UnaryOp):
        inner = eval_expr(table, node.operand)
        if node.op == "NOT":
            return pc.invert(inner)
        if node.op == "-":
            return pc.negate(inner)
        raise ValueError(f"Unsupported unary op: {node.op}")

    if isinstance(node, InExpr):
        lhs = eval_expr(table, node.expr)
        value_set = pa.array([v.value for v in node.values])
        res = pc.is_in(lhs, value_set=value_set)
        return pc.invert(res) if node.negated else res

    if isinstance(node, BetweenExpr):
        lhs = eval_expr(table, node.expr)
        ge = pc.greater_equal(lhs, node.low.value)
        le = pc.less_equal(lhs, node.high.value)
        res = pc.and_kleene(ge, le)
        return pc.invert(res) if node.negated else res

    if isinstance(node, BinaryOp):
        left_is_lit = isinstance(node.left, Literal)
        right_is_lit = isinstance(node.right, Literal)
        left_val: Any = node.left.value if isinstance(node.left, Literal) else eval_expr(table, node.left)
        right_val: Any = node.right.value if isinstance(node.right, Literal) else eval_expr(table, node.right)

        if left_is_lit and right_is_lit:
            left_val = eval_expr(table, node.left)

        op = node.op
        if op == "AND":
            return pc.and_kleene(left_val, right_val)
        if op == "OR":
            return pc.or_kleene(left_val, right_val)
        if op == "=":
            return pc.equal(left_val, right_val)
        if op == "!=":
            return pc.not_equal(left_val, right_val)
        if op == "<":
            return pc.less(left_val, right_val)
        if op == "<=":
            return pc.less_equal(left_val, right_val)
        if op == ">":
            return pc.greater(left_val, right_val)
        if op == ">=":
            return pc.greater_equal(left_val, right_val)
        if op == "+":
            return pc.add(left_val, right_val)
        if op == "-":
            return pc.subtract(left_val, right_val)
        if op == "*":
            return pc.multiply(left_val, right_val)
        if op == "/":
            lhs_f = pc.cast(left_val, pa.float64()) if not left_is_lit else float(left_val)
            rhs_f = pc.cast(right_val, pa.float64()) if not right_is_lit else float(right_val)
            return pc.divide(lhs_f, rhs_f)
        raise ValueError(f"Unsupported binary op: {op}")

    raise ValueError(f"Unsupported expr node: {node}")


def _row_group_may_match(
    rg_meta: Any,
    col_name_to_idx: dict[str, int],
    pred: Expr,
) -> bool:
    """Check Parquet row-group column min/max statistics to skip non-matching row groups."""
    if isinstance(pred, BinaryOp):
        if pred.op == "AND":
            return _row_group_may_match(rg_meta, col_name_to_idx, pred.left) and _row_group_may_match(
                rg_meta, col_name_to_idx, pred.right
            )
        if pred.op == "OR":
            return _row_group_may_match(rg_meta, col_name_to_idx, pred.left) or _row_group_may_match(
                rg_meta, col_name_to_idx, pred.right
            )
        if isinstance(pred.left, ColumnRef) and isinstance(pred.right, Literal):
            col_idx = col_name_to_idx.get(pred.left.column)
            if col_idx is None:
                return True
            stats = rg_meta.column(col_idx).statistics
            if stats is None or not stats.has_min_max:
                return True
            lo, hi = stats.min, stats.max
            val = pred.right.value
            try:
                if pred.op == "=":
                    return not (val < lo or val > hi)
                if pred.op == "!=":
                    return not (lo == hi and lo == val)
                if pred.op == "<":
                    return bool(lo < val)
                if pred.op == "<=":
                    return bool(lo <= val)
                if pred.op == ">":
                    return bool(hi > val)
                if pred.op == ">=":
                    return bool(hi >= val)
            except TypeError:
                return True
        if isinstance(pred.left, Literal) and isinstance(pred.right, ColumnRef):
            flip = {"=": "=", "!=": "!=", "<": ">", "<=": ">=", ">": "<", ">=": "<="}
            if pred.op in flip:
                return _row_group_may_match(
                    rg_meta, col_name_to_idx, BinaryOp(flip[pred.op], pred.right, pred.left)
                )
        return True

    if isinstance(pred, BetweenExpr) and isinstance(pred.expr, ColumnRef) and not pred.negated:
        col_idx = col_name_to_idx.get(pred.expr.column)
        if col_idx is None:
            return True
        stats = rg_meta.column(col_idx).statistics
        if stats is None or not stats.has_min_max:
            return True
        try:
            return not (stats.max < pred.low.value or stats.min > pred.high.value)
        except TypeError:
            return True

    if isinstance(pred, InExpr) and isinstance(pred.expr, ColumnRef) and not pred.negated:
        col_idx = col_name_to_idx.get(pred.expr.column)
        if col_idx is None:
            return True
        stats = rg_meta.column(col_idx).statistics
        if stats is None or not stats.has_min_max:
            return True
        try:
            return any(not (lit.value < stats.min or lit.value > stats.max) for lit in pred.values)
        except TypeError:
            return True

    return True


@dataclass
class ScanResult:
    table: pa.Table
    rows_in: int
    rows_out: int
    row_groups_scanned: int
    row_groups_pruned: int


def scan_parquet_partition(
    file_path: str,
    alias: str,
    columns: list[str],
    predicates: list[dict[str, Any]],
) -> ScanResult:
    """Scan a Parquet partition with projection pushdown and row-group min/max pruning."""
    pf = pq.ParquetFile(file_path)
    meta = pf.metadata
    schema_names = pf.schema_arrow.names
    col_name_to_idx = {name: i for i, name in enumerate(schema_names)}
    parsed_preds = [expr_from_dict(p) for p in predicates]

    rgs_to_read: list[int] = []
    rgs_pruned = 0
    for rg_i in range(meta.num_row_groups):
        rg_meta = meta.row_group(rg_i)
        if all(_row_group_may_match(rg_meta, col_name_to_idx, pr) for pr in parsed_preds):
            rgs_to_read.append(rg_i)
        else:
            rgs_pruned += 1

    if not rgs_to_read:
        empty_fields = [
            pa.field(f"{alias}.{c}", pf.schema_arrow.field(c).type) for c in columns
        ]
        return ScanResult(
            table=pa.schema(empty_fields).empty_table(),
            rows_in=0,
            rows_out=0,
            row_groups_scanned=0,
            row_groups_pruned=rgs_pruned,
        )

    tbl = pf.read_row_groups(rgs_to_read, columns=columns)
    rows_in = tbl.num_rows
    # Prefix columns with "{alias}." for unambiguous join namespace
    tbl = tbl.rename_columns([f"{alias}.{c}" for c in tbl.column_names])

    for pred in parsed_preds:
        if tbl.num_rows == 0:
            break
        mask = eval_expr(tbl, pred)
        tbl = tbl.filter(mask)

    return ScanResult(
        table=tbl,
        rows_in=rows_in,
        rows_out=tbl.num_rows,
        row_groups_scanned=len(rgs_to_read),
        row_groups_pruned=rgs_pruned,
    )


def _hash_value(val: Any) -> int:
    if isinstance(val, int):
        return (val * 2654435761) & 0xFFFFFFFF
    return zlib.crc32(str(val).encode("utf-8")) & 0xFFFFFFFF


def hash_partition_table(
    table: pa.Table,
    key_cols: list[str],
    num_partitions: int,
    skew_salt_col: str | None = None,
    skew_hot_values: list[Any] | None = None,
    salt_factor: int = 4,
) -> dict[int, pa.Table]:
    """Partition an Arrow table into N hash buckets, optionally salting hot keys across workers."""
    if num_partitions <= 1:
        if skew_salt_col and "__dqse_salt" not in table.column_names:
            table = table.append_column("__dqse_salt", pa.array(np.zeros(table.num_rows, dtype=np.int64)))
        return {0: table}

    n = table.num_rows
    if n == 0:
        out_tbl = table
        if skew_salt_col and "__dqse_salt" not in out_tbl.column_names:
            out_tbl = out_tbl.append_column("__dqse_salt", pa.array([], type=pa.int64()))
        return {0: out_tbl}

    if not key_cols:
        return {0: table}

    hot_set = set(skew_hot_values or [])
    salt_arr: np.ndarray | None = None
    if skew_salt_col and hot_set and skew_salt_col in table.column_names:
        col_vals = table[skew_salt_col].to_pylist()
        salts = np.zeros(n, dtype=np.int64)
        for i, v in enumerate(col_vals):
            if v in hot_set:
                salts[i] = (i % salt_factor) + 1
        salt_arr = salts
        table = table.append_column("__dqse_salt", pa.array(salts, type=pa.int64()))

    # Compute bucket ID per row
    if len(key_cols) == 1 and pa.types.is_integer(table[key_cols[0]].type) and salt_arr is None:
        arr = table[key_cols[0]].to_numpy(zero_copy_only=False)
        buckets = ((arr.astype(np.uint64) * np.uint64(2654435761)) % np.uint64(num_partitions)).astype(np.int64)
    else:
        col_lists = [table[c].to_pylist() for c in key_cols]
        if salt_arr is not None:
            col_lists.append(salt_arr.tolist())
        buckets = np.empty(n, dtype=np.int64)
        for i in range(n):
            h = 1469598103934665603
            for cl in col_lists:
                h ^= _hash_value(cl[i])
                h = (h * 1099511628211) & 0xFFFFFFFFFFFFFFFF
            buckets[i] = int(h % num_partitions)

    result: dict[int, pa.Table] = {}
    for p in range(num_partitions):
        idxs = np.nonzero(buckets == p)[0]
        if len(idxs) > 0:
            result[p] = table.take(pa.array(idxs, type=pa.int64()))
    return result


# --- Vectorized + Spillable Hash Aggregation ---


def _partial_col_names(agg_idx: int, func: str) -> list[str]:
    if func == "AVG":
        return [f"__p_sum_{agg_idx}", f"__p_cnt_{agg_idx}"]
    return [f"__p_{func.lower()}_{agg_idx}"]


def _build_partial_table_from_state(
    group_cols: list[str],
    group_types: list[pa.DataType],
    aggs: list[dict[str, Any]],
    state: dict[tuple[Any, ...], list[Any]],
) -> pa.Table:
    cols: dict[str, pa.Array] = {}
    keys = list(state.keys())
    for gi, (gcol, gtype) in enumerate(zip(group_cols, group_types, strict=True)):
        cols[gcol] = pa.array([k[gi] for k in keys], type=gtype)

    slot = 0
    for ai, agg in enumerate(aggs):
        func = agg["func"]
        if func == "AVG":
            cols[f"__p_sum_{ai}"] = pa.array([state[k][slot] for k in keys], type=pa.float64())
            cols[f"__p_cnt_{ai}"] = pa.array([state[k][slot + 1] for k in keys], type=pa.int64())
            slot += 2
        elif func == "COUNT":
            cols[f"__p_count_{ai}"] = pa.array([state[k][slot] for k in keys], type=pa.int64())
            slot += 1
        elif func == "SUM":
            cols[f"__p_sum_{ai}"] = pa.array([state[k][slot] for k in keys], type=pa.float64())
            slot += 1
        elif func in ("MIN", "MAX"):
            vals = [state[k][slot] for k in keys]
            cols[f"__p_{func.lower()}_{ai}"] = pa.array(vals)
            slot += 1
    return pa.table(cols)


def _finalize_state_to_table(
    group_cols: list[str],
    group_types: list[pa.DataType],
    aggs: list[dict[str, Any]],
    state: dict[tuple[Any, ...], list[Any]],
    emit_partial: bool,
) -> pa.Table:
    if emit_partial:
        return _build_partial_table_from_state(group_cols, group_types, aggs, state)

    keys = list(state.keys())
    cols: dict[str, pa.Array] = {}
    for gi, (gcol, gtype) in enumerate(zip(group_cols, group_types, strict=True)):
        cols[gcol] = pa.array([k[gi] for k in keys], type=gtype)

    slot = 0
    for agg in aggs:
        out_name = agg["output_name"]
        func = agg["func"]
        if func == "AVG":
            vals = [
                (state[k][slot] / state[k][slot + 1]) if state[k][slot + 1] > 0 else None
                for k in keys
            ]
            cols[out_name] = pa.array(vals, type=pa.float64())
            slot += 2
        elif func == "COUNT":
            cols[out_name] = pa.array([int(state[k][slot]) for k in keys], type=pa.int64())
            slot += 1
        elif func == "SUM":
            cols[out_name] = pa.array([state[k][slot] for k in keys], type=pa.float64())
            slot += 1
        elif func in ("MIN", "MAX"):
            cols[out_name] = pa.array([state[k][slot] for k in keys])
            slot += 1
    return pa.table(cols)


def _slot_count(aggs: list[dict[str, Any]]) -> int:
    return sum(2 if a["func"] == "AVG" else 1 for a in aggs)


def _init_slots(aggs: list[dict[str, Any]]) -> list[Any]:
    slots: list[Any] = []
    for a in aggs:
        f = a["func"]
        if f == "AVG":
            slots.extend([0.0, 0])
        elif f == "COUNT":
            slots.append(0)
        elif f == "SUM":
            slots.append(0.0)
        else:  # MIN / MAX
            slots.append(None)
    return slots


def _accumulate_raw_batch(
    batch: pa.Table,
    group_cols: list[str],
    aggs: list[dict[str, Any]],
    state: dict[tuple[Any, ...], list[Any]],
) -> int:
    """Accumulate raw input rows into partial hash-agg state; returns number of new groups added."""
    n = batch.num_rows
    if n == 0:
        return 0

    group_lists = [batch[c].to_pylist() for c in group_cols]
    eval_args: list[list[Any] | None] = []
    for agg in aggs:
        if agg.get("arg") is None:
            eval_args.append(None)
        else:
            eval_args.append(eval_expr(batch, agg["arg"]).to_pylist())

    new_groups = 0
    for i in range(n):
        key = tuple(gl[i] for gl in group_lists) if group_lists else ()
        acc = state.get(key)
        if acc is None:
            acc = _init_slots(aggs)
            state[key] = acc
            new_groups += 1
        slot = 0
        for ai, agg in enumerate(aggs):
            func = agg["func"]
            arg_list = eval_args[ai]
            val = None if arg_list is None else arg_list[i]
            if func == "COUNT":
                if arg_list is None or val is not None:
                    acc[slot] += 1
                slot += 1
            elif func == "SUM":
                if val is not None:
                    acc[slot] += float(val)
                slot += 1
            elif func == "AVG":
                if val is not None:
                    acc[slot] += float(val)
                    acc[slot + 1] += 1
                slot += 2
            elif func == "MIN":
                if val is not None and (acc[slot] is None or val < acc[slot]):
                    acc[slot] = val
                slot += 1
            elif func == "MAX":
                if val is not None and (acc[slot] is None or val > acc[slot]):
                    acc[slot] = val
                slot += 1
    return new_groups


def _accumulate_partial_batch(
    batch: pa.Table,
    group_cols: list[str],
    aggs: list[dict[str, Any]],
    state: dict[tuple[Any, ...], list[Any]],
) -> int:
    """Merge already-partial aggregate rows into state; returns number of new groups added."""
    n = batch.num_rows
    if n == 0:
        return 0
    group_lists = [batch[c].to_pylist() for c in group_cols]
    p_cols: list[Any] = []
    for ai, agg in enumerate(aggs):
        func = agg["func"]
        if func == "AVG":
            p_cols.append((batch[f"__p_sum_{ai}"].to_pylist(), batch[f"__p_cnt_{ai}"].to_pylist()))
        else:
            p_cols.append(batch[f"__p_{func.lower()}_{ai}"].to_pylist())

    new_groups = 0
    for i in range(n):
        key = tuple(gl[i] for gl in group_lists) if group_lists else ()
        acc = state.get(key)
        if acc is None:
            acc = _init_slots(aggs)
            state[key] = acc
            new_groups += 1
        slot = 0
        for ai, agg in enumerate(aggs):
            func = agg["func"]
            if func == "AVG":
                s_list, c_list = p_cols[ai]
                acc[slot] += float(s_list[i] or 0.0)
                acc[slot + 1] += int(c_list[i] or 0)
                slot += 2
            elif func == "COUNT":
                acc[slot] += int(p_cols[ai][i] or 0)
                slot += 1
            elif func == "SUM":
                acc[slot] += float(p_cols[ai][i] or 0.0)
                slot += 1
            elif func == "MIN":
                val = p_cols[ai][i]
                if val is not None and (acc[slot] is None or val < acc[slot]):
                    acc[slot] = val
                slot += 1
            elif func == "MAX":
                val = p_cols[ai][i]
                if val is not None and (acc[slot] is None or val > acc[slot]):
                    acc[slot] = val
                slot += 1
    return new_groups


def execute_hash_aggregate(
    tables: list[pa.Table],
    group_by: list[str],
    aggs: list[dict[str, Any]],
    mode: str,  # "single", "partial", "final", "partial_merge"
    memory_pool: MemoryPool,
    spill_manager: SpillManager | None = None,
    spill_buckets: int = 8,
    batch_rows: int = 2048,
) -> pa.Table:
    """Execute hash aggregation with explicit MemoryPool tracking and Arrow IPC spill-to-disk."""
    combined = concat_non_empty(tables)
    if combined.num_rows == 0:
        # Handle global aggregate on 0 rows vs grouped aggregate on 0 rows
        if not group_by and mode in ("single", "final"):
            cols: dict[str, pa.Array] = {}
            for a in aggs:
                if a["func"] == "COUNT":
                    cols[a["output_name"]] = pa.array([0], type=pa.int64())
                else:
                    cols[a["output_name"]] = pa.array([None], type=pa.float64())
            return pa.table(cols)
        return pa.table({})

    group_types = [combined.schema.field(c).type for c in group_by]
    bytes_per_group = max(256, 128 + 64 * len(group_by) + 64 * _slot_count(aggs))
    emit_partial = mode in ("partial", "partial_merge")
    is_partial_input = mode in ("final", "partial_merge")

    # Fast vectorized PyArrow path when input fits comfortably in MemoryPool and no spill needed
    # (Skip fast path when tight memory_pool is smaller than estimated hash table so spill triggers!)
    est_input_mem = max(combined.nbytes, combined.num_rows * 64)
    can_use_fast_arrow = (
        not is_partial_input
        and all(a["func"] in ("COUNT", "SUM", "AVG", "MIN", "MAX") for a in aggs)
        and est_input_mem * 2 < memory_pool.available_bytes
    )

    if can_use_fast_arrow and not group_by:
        # Global scalar aggregate (fast vectorized PyArrow compute)
        memory_pool.try_reserve(est_input_mem)
        try:
            state: dict[tuple[Any, ...], list[Any]] = {(): _init_slots(aggs)}
            acc = state[()]
            slot = 0
            for a in aggs:
                func = a["func"]
                if a.get("arg") is None:
                    acc[slot] = combined.num_rows
                    slot += 1
                    continue
                arr = eval_expr(combined, a["arg"])
                if func == "COUNT":
                    acc[slot] = int(pc.count(arr).as_py() or 0)
                    slot += 1
                elif func == "SUM":
                    acc[slot] = float(pc.sum(arr).as_py() or 0.0)
                    slot += 1
                elif func == "AVG":
                    acc[slot] = float(pc.sum(arr).as_py() or 0.0)
                    acc[slot + 1] = int(pc.count(arr).as_py() or 0)
                    slot += 2
                elif func == "MIN":
                    acc[slot] = pc.min(arr).as_py()
                    slot += 1
                elif func == "MAX":
                    acc[slot] = pc.max(arr).as_py()
                    slot += 1
            return _finalize_state_to_table(group_by, group_types, aggs, state, emit_partial=emit_partial)
        finally:
            memory_pool.release(est_input_mem)

    # Chunked streaming hash aggregation with explicit MemoryPool reservation and spill-to-disk
    state = {}
    reserved_bytes = 0

    def flush_state_to_spill() -> None:
        nonlocal reserved_bytes
        if not state or spill_manager is None:
            return
        ptbl = _build_partial_table_from_state(group_by, group_types, aggs, state)
        partitioned = hash_partition_table(ptbl, group_by, spill_buckets)
        for bid, btbl in partitioned.items():
            if btbl.num_rows > 0:
                spill_manager.spill_table(bid, btbl)
        state.clear()
        memory_pool.release(reserved_bytes)
        reserved_bytes = 0

    effective_batch = min(batch_rows, 256) if memory_pool.max_bytes <= 2 * 1024 * 1024 else batch_rows
    for offset in range(0, combined.num_rows, effective_batch):
        chunk = combined.slice(offset, effective_batch)
        if is_partial_input:
            added = _accumulate_partial_batch(chunk, group_by, aggs, state)
        else:
            added = _accumulate_raw_batch(chunk, group_by, aggs, state)

        delta_bytes = added * bytes_per_group
        if delta_bytes > 0:
            if memory_pool.try_reserve(delta_bytes):
                reserved_bytes += delta_bytes
            else:
                memory_pool.force_record_peak(delta_bytes)
                if spill_manager is not None and group_by:
                    flush_state_to_spill()
                else:
                    # Global agg has only 1 group; safe to keep in memory
                    reserved_bytes += 0

    if spill_manager is not None and spill_manager.has_spilled:
        if state:
            flush_state_to_spill()
        merged_buckets: list[pa.Table] = []
        for bid in spill_manager.bucket_ids():
            bucket_tables = spill_manager.read_bucket(bid)
            b_state: dict[tuple[Any, ...], list[Any]] = {}
            for bt in bucket_tables:
                _accumulate_partial_batch(bt, group_by, aggs, b_state)
            b_mem = len(b_state) * bytes_per_group
            memory_pool.force_record_peak(b_mem)
            if b_state:
                merged_buckets.append(
                    _finalize_state_to_table(
                        group_by, group_types, aggs, b_state, emit_partial=emit_partial
                    )
                )
        return concat_non_empty(merged_buckets)

    try:
        return _finalize_state_to_table(
            group_by, group_types, aggs, state, emit_partial=emit_partial
        )
    finally:
        memory_pool.release(reserved_bytes)


# --- Repartitioned Hash Join with Spill-to-Disk ---


def _in_memory_inner_hash_join(
    left: pa.Table,
    right: pa.Table,
    left_key: str,
    right_key: str,
) -> pa.Table:
    if left.num_rows == 0 or right.num_rows == 0:
        fields = [left.schema.field(c) for c in left.column_names] + [
            right.schema.field(c) for c in right.column_names if c not in left.column_names
        ]
        return pa.schema(fields).empty_table()

    # Build hash table on smaller side
    build_right = right.num_rows <= left.num_rows
    if build_right:
        ht: dict[Any, list[int]] = {}
        for r_idx, k in enumerate(right[right_key].to_pylist()):
            if k is not None:
                ht.setdefault(k, []).append(r_idx)
        left_indices: list[int] = []
        right_indices: list[int] = []
        for l_idx, k in enumerate(left[left_key].to_pylist()):
            matches = ht.get(k)
            if matches:
                for r_idx in matches:
                    left_indices.append(l_idx)
                    right_indices.append(r_idx)
    else:
        ht = {}
        for l_idx, k in enumerate(left[left_key].to_pylist()):
            if k is not None:
                ht.setdefault(k, []).append(l_idx)
        left_indices = []
        right_indices = []
        for r_idx, k in enumerate(right[right_key].to_pylist()):
            matches = ht.get(k)
            if matches:
                for l_idx in matches:
                    left_indices.append(l_idx)
                    right_indices.append(r_idx)

    if not left_indices:
        fields = [left.schema.field(c) for c in left.column_names] + [
            right.schema.field(c) for c in right.column_names if c not in left.column_names
        ]
        return pa.schema(fields).empty_table()

    l_take = left.take(pa.array(left_indices, type=pa.int64()))
    r_take = right.take(pa.array(right_indices, type=pa.int64()))
    out_cols: dict[str, pa.ChunkedArray] = {c: l_take[c] for c in l_take.column_names}
    for c in r_take.column_names:
        if c not in out_cols:
            out_cols[c] = r_take[c]
    return pa.table(out_cols)


def execute_hash_join(
    left_tables: list[pa.Table],
    right_tables: list[pa.Table],
    left_key: str,
    right_key: str,
    memory_pool: MemoryPool,
    spill_manager: SpillManager | None = None,
    grace_buckets: int = 8,
) -> pa.Table:
    """Execute inner hash join with Grace Hash Join spill-to-disk when build side exceeds MemoryPool."""
    left = concat_non_empty(left_tables)
    right = concat_non_empty(right_tables)
    if left.num_rows == 0 or right.num_rows == 0:
        return _in_memory_inner_hash_join(left, right, left_key, right_key)

    build_bytes = max(min(left.nbytes, right.nbytes) * 2, min(left.num_rows, right.num_rows) * 96)
    if memory_pool.try_reserve(build_bytes):
        try:
            return _in_memory_inner_hash_join(left, right, left_key, right_key)
        finally:
            memory_pool.release(build_bytes)

    # Grace hash join fallback: spill both sides into matching hash buckets and join bucket-by-bucket
    memory_pool.force_record_peak(build_bytes)
    if spill_manager is None:
        return _in_memory_inner_hash_join(left, right, left_key, right_key)

    left_parts = hash_partition_table(left, [left_key], grace_buckets)
    right_parts = hash_partition_table(right, [right_key], grace_buckets)

    # Offset bucket IDs: 0..grace_buckets-1 for left, 100..100+grace_buckets-1 for right
    for bid, tbl in left_parts.items():
        if tbl.num_rows > 0:
            spill_manager.spill_table(bid, tbl)
    for bid, tbl in right_parts.items():
        if tbl.num_rows > 0:
            spill_manager.spill_table(100 + bid, tbl)

    joined_parts: list[pa.Table] = []
    for bid in range(grace_buckets):
        l_chunks = spill_manager.read_bucket(bid)
        r_chunks = spill_manager.read_bucket(100 + bid)
        if l_chunks and r_chunks:
            jt = _in_memory_inner_hash_join(
                concat_non_empty(l_chunks), concat_non_empty(r_chunks), left_key, right_key
            )
            if jt.num_rows > 0:
                joined_parts.append(jt)
    return concat_non_empty(joined_parts)


def apply_projections(table: pa.Table, projections: list[dict[str, Any]]) -> pa.Table:
    if table.num_rows == 0 and table.num_columns == 0:
        return pa.table({p["output_name"]: pa.array([], type=pa.float64()) for p in projections})
    cols: dict[str, pa.Array | pa.ChunkedArray] = {}
    for proj in projections:
        out_name = proj["output_name"]
        expr = proj["expr"]
        if expr["kind"] == "col" and expr.get("table") is None and expr["column"] in table.column_names:
            cols[out_name] = table[expr["column"]]
        else:
            cols[out_name] = eval_expr(table, expr)
    return pa.table(cols)


def apply_sort_limit(
    tables: list[pa.Table],
    order_by: list[dict[str, Any]],
    limit: int | None,
) -> pa.Table:
    combined = concat_non_empty(tables)
    if combined.num_rows == 0:
        return combined
    if order_by:
        sort_keys: list[tuple[str, str]] = []
        temp_cols: list[str] = []
        working = combined
        for idx, ob in enumerate(order_by):
            expr = ob["expr"]
            direction = "ascending" if ob.get("ascending", True) else "descending"
            if expr["kind"] == "col":
                col_name = (
                    f"{expr['table']}.{expr['column']}" if expr.get("table") else expr["column"]
                )
                if col_name in working.column_names:
                    sort_keys.append((col_name, direction))
                    continue
                if expr["column"] in working.column_names:
                    sort_keys.append((expr["column"], direction))
                    continue
            tmp_name = f"__sort_{idx}"
            working = working.append_column(tmp_name, eval_expr(working, expr))
            sort_keys.append((tmp_name, direction))
            temp_cols.append(tmp_name)
        working = working.sort_by(sort_keys)
        if temp_cols:
            working = working.drop_columns(temp_cols)
        combined = working

    if limit is not None:
        combined = combined.slice(0, limit)
    return combined


def table_to_json_rows(table: pa.Table) -> tuple[list[str], list[list[Any]]]:
    cols = list(table.column_names)
    if table.num_rows == 0:
        return cols, []
    py_cols = [table[c].to_pylist() for c in cols]
    rows: list[list[Any]] = []
    for i in range(table.num_rows):
        row = []
        for col_vals in py_cols:
            v = col_vals[i]
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                v = None
            row.append(v)
        rows.append(row)
    return cols, rows
