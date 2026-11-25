"""Logical query planner and 5-rule analytical query optimizer.

Optimization passes (each independently toggleable for before/after ablation):
  1. Projection pushdown (prune unreferenced Parquet columns at scan)
  2. Predicate pushdown (split AND conjuncts and push single-table filters to scans)
  3. Partition pruning (skip catalog partitions via partition values & min/max stats)
  4. Two-phase distributed hash aggregation (PartialAgg -> Shuffle -> FinalAgg)
  5. Skew-aware salted hash shuffle (spread known hot keys across workers)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from models.catalog import MetadataCatalog
from models.contracts import OptimizerOptions, PartitionMetadata
from processing.sql_parser import (
    AggCall,
    BetweenExpr,
    BinaryOp,
    ColumnRef,
    Expr,
    InExpr,
    Literal,
    OrderByItem,
    SelectItem,
    SelectQuery,
    UnaryOp,
    collect_columns,
    contains_agg,
    format_expr,
)


class PlanError(ValueError):
    """Raised when a parsed SQL query references invalid tables, columns, or unsupported semantics."""


@dataclass
class ScanNode:
    table_name: str
    alias: str
    snapshot_version: int
    all_columns: list[str]
    required_columns: list[str]
    all_partitions: list[PartitionMetadata]
    selected_partitions: list[PartitionMetadata]
    pushed_predicates: list[Expr] = field(default_factory=list)
    hot_keys: dict[str, list[Any]] = field(default_factory=dict)


@dataclass
class JoinSpec:
    right_scan: ScanNode
    left_key: ColumnRef
    right_key: ColumnRef


@dataclass
class AggregateSpec:
    output_name: str
    func: str  # COUNT, SUM, AVG, MIN, MAX
    arg: Expr | None


@dataclass
class ProjectionSpec:
    output_name: str
    expr: Expr


@dataclass
class LogicalPlan:
    sql: str
    base_scan: ScanNode
    joins: list[JoinSpec]
    residual_predicates: list[Expr]
    group_by: list[ColumnRef]
    aggregates: list[AggregateSpec]
    projections: list[ProjectionSpec]
    order_by: list[OrderByItem]
    limit: int | None
    is_aggregate_query: bool
    two_phase_agg: bool = False
    skew_salt_column: str | None = None
    skew_hot_values: list[Any] = field(default_factory=list)
    skew_salt_factor: int = 4
    optimizer_rules_applied: list[str] = field(default_factory=list)
    table_versions: dict[str, int] = field(default_factory=dict)

    @property
    def all_scans(self) -> list[ScanNode]:
        return [self.base_scan, *(j.right_scan for j in self.joins)]

    @property
    def partitions_total(self) -> int:
        return sum(len(s.all_partitions) for s in self.all_scans)

    @property
    def partitions_selected(self) -> int:
        return sum(len(s.selected_partitions) for s in self.all_scans)

    @property
    def partitions_pruned(self) -> int:
        return self.partitions_total - self.partitions_selected

    @property
    def estimated_scan_bytes(self) -> int:
        total = 0
        for s in self.all_scans:
            col_ratio = max(0.1, len(s.required_columns) / max(1, len(s.all_columns)))
            total += int(sum(p.file_size_bytes for p in s.selected_partitions) * col_ratio)
        return total


def _split_conjuncts(expr: Expr | None) -> list[Expr]:
    if expr is None:
        return []
    if isinstance(expr, BinaryOp) and expr.op == "AND":
        return _split_conjuncts(expr.left) + _split_conjuncts(expr.right)
    return [expr]


def _resolve_column(
    ref: ColumnRef,
    alias_to_cols: dict[str, set[str]],
    output_aliases: set[str] | None = None,
) -> ColumnRef:
    if ref.table is not None:
        if ref.table not in alias_to_cols:
            raise PlanError(f"Unknown table/alias '{ref.table}' in '{ref.full_name()}'")
        if ref.column not in alias_to_cols[ref.table]:
            raise PlanError(f"Column '{ref.column}' not found in table/alias '{ref.table}'")
        return ref

    if output_aliases and ref.column in output_aliases:
        return ColumnRef(column=ref.column, table=None)

    matches = [alias for alias, cols in alias_to_cols.items() if ref.column in cols]
    if len(matches) == 1:
        return ColumnRef(column=ref.column, table=matches[0])
    if len(matches) > 1:
        raise PlanError(f"Ambiguous column '{ref.column}' found in aliases {matches}")
    raise PlanError(f"Unknown column '{ref.column}'")


def _bind_expr(
    expr: Expr,
    alias_to_cols: dict[str, set[str]],
    output_aliases: set[str] | None = None,
) -> Expr:
    if isinstance(expr, Literal):
        return expr
    if isinstance(expr, ColumnRef):
        return _resolve_column(expr, alias_to_cols, output_aliases=output_aliases)
    if isinstance(expr, BinaryOp):
        return BinaryOp(
            op=expr.op,
            left=_bind_expr(expr.left, alias_to_cols, output_aliases),
            right=_bind_expr(expr.right, alias_to_cols, output_aliases),
        )
    if isinstance(expr, UnaryOp):
        return UnaryOp(op=expr.op, operand=_bind_expr(expr.operand, alias_to_cols, output_aliases))
    if isinstance(expr, InExpr):
        return InExpr(
            expr=_bind_expr(expr.expr, alias_to_cols, output_aliases),
            values=expr.values,
            negated=expr.negated,
        )
    if isinstance(expr, BetweenExpr):
        return BetweenExpr(
            expr=_bind_expr(expr.expr, alias_to_cols, output_aliases),
            low=expr.low,
            high=expr.high,
            negated=expr.negated,
        )
    if isinstance(expr, AggCall):
        return AggCall(
            func=expr.func,
            arg=_bind_expr(expr.arg, alias_to_cols, output_aliases) if expr.arg is not None else None,
        )
    return expr


def _partition_may_match_predicate(
    part: PartitionMetadata,
    pred: Expr,
    alias: str,
) -> bool:
    """Return False iff partition values or min/max stats prove no row in this partition can match."""

    def get_bounds(col_ref: ColumnRef) -> tuple[Any, Any] | None:
        if col_ref.table not in (None, alias):
            return None
        c = col_ref.column
        if c in part.partition_values:
            v = part.partition_values[c]
            return (v, v)
        if c in part.min_stats and c in part.max_stats:
            return (part.min_stats[c], part.max_stats[c])
        return None

    if isinstance(pred, BinaryOp):
        if pred.op == "AND":
            return _partition_may_match_predicate(part, pred.left, alias) and _partition_may_match_predicate(
                part, pred.right, alias
            )
        if pred.op == "OR":
            return _partition_may_match_predicate(part, pred.left, alias) or _partition_may_match_predicate(
                part, pred.right, alias
            )
        if isinstance(pred.left, ColumnRef) and isinstance(pred.right, Literal):
            bounds = get_bounds(pred.left)
            if bounds is None:
                return True
            lo, hi = bounds
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
                return _partition_may_match_predicate(
                    part, BinaryOp(flip[pred.op], pred.right, pred.left), alias
                )
        return True

    if isinstance(pred, InExpr) and isinstance(pred.expr, ColumnRef) and not pred.negated:
        bounds = get_bounds(pred.expr)
        if bounds is None:
            return True
        lo, hi = bounds
        try:
            return any(not (lit.value < lo or lit.value > hi) for lit in pred.values)
        except TypeError:
            return True

    if isinstance(pred, BetweenExpr) and isinstance(pred.expr, ColumnRef) and not pred.negated:
        bounds = get_bounds(pred.expr)
        if bounds is None:
            return True
        lo, hi = bounds
        try:
            return not (hi < pred.low.value or lo > pred.high.value)
        except TypeError:
            return True

    return True


def _default_output_name(item: SelectItem, idx: int) -> str:
    if item.alias:
        return item.alias
    if isinstance(item.expr, ColumnRef):
        return item.expr.column
    if isinstance(item.expr, AggCall):
        inner = "star" if item.expr.arg is None else format_expr(item.expr.arg).replace(".", "_")
        return f"{item.expr.func.lower()}_{inner}"
    return f"col_{idx}"


def build_logical_plan(
    sql: str,
    query: SelectQuery,
    catalog: MetadataCatalog,
) -> LogicalPlan:
    """Bind a parsed SelectQuery against MetadataCatalog into an unoptimized LogicalPlan."""
    alias_to_cols: dict[str, set[str]] = {}
    table_versions: dict[str, int] = {}

    def make_scan(table_name: str, alias: str) -> ScanNode:
        summary = catalog.get_table(table_name)
        if summary is None:
            raise PlanError(f"Table '{table_name}' does not exist in catalog")
        if alias in alias_to_cols:
            raise PlanError(f"Duplicate table alias '{alias}'")
        cols = list(summary.columns.keys())
        alias_to_cols[alias] = set(cols)
        table_versions[table_name] = summary.snapshot_version
        parts = catalog.get_partitions(table_name, summary.snapshot_version)
        return ScanNode(
            table_name=table_name,
            alias=alias,
            snapshot_version=summary.snapshot_version,
            all_columns=cols,
            required_columns=list(cols),  # unoptimized reads all columns
            all_partitions=parts,
            selected_partitions=list(parts),  # unoptimized scans all partitions
            pushed_predicates=[],
            hot_keys=dict(summary.hot_keys),
        )

    base_scan = make_scan(query.from_table.name, query.from_table.ref_name)
    joins: list[JoinSpec] = []
    for jc in query.joins:
        right_scan = make_scan(jc.table.name, jc.table.ref_name)
        lk = _resolve_column(jc.left_key, alias_to_cols)
        rk = _resolve_column(jc.right_key, alias_to_cols)
        # Normalize so right_key always belongs to right_scan
        if lk.table == right_scan.alias and rk.table != right_scan.alias:
            lk, rk = rk, lk
        if rk.table != right_scan.alias:
            raise PlanError(
                f"JOIN ON clause must reference joined table '{right_scan.alias}', got {lk.full_name()} = {rk.full_name()}"
            )
        joins.append(JoinSpec(right_scan=right_scan, left_key=lk, right_key=rk))

    residual_predicates: list[Expr] = []
    if query.where is not None:
        bound_where = _bind_expr(query.where, alias_to_cols)
        residual_predicates = _split_conjuncts(bound_where)

    group_by = [_resolve_column(c, alias_to_cols) for c in query.group_by]

    has_any_agg = bool(group_by) or any(contains_agg(item.expr) for item in query.select_items)
    aggregates: list[AggregateSpec] = []
    projections: list[ProjectionSpec] = []
    output_aliases: set[str] = set()

    for idx, item in enumerate(query.select_items):
        out_name = _default_output_name(item, idx)
        output_aliases.add(out_name)
        bound_expr = _bind_expr(item.expr, alias_to_cols)
        if has_any_agg:
            if isinstance(bound_expr, AggCall):
                aggregates.append(AggregateSpec(output_name=out_name, func=bound_expr.func, arg=bound_expr.arg))
                projections.append(ProjectionSpec(output_name=out_name, expr=ColumnRef(column=out_name, table=None)))
            elif isinstance(bound_expr, ColumnRef):
                if bound_expr not in group_by:
                    raise PlanError(f"Column '{bound_expr.full_name()}' must appear in GROUP BY or an aggregate")
                projections.append(ProjectionSpec(output_name=out_name, expr=bound_expr))
            else:
                raise PlanError(
                    f"Unsupported non-aggregate expression '{format_expr(bound_expr)}' in aggregate SELECT list"
                )
        else:
            projections.append(ProjectionSpec(output_name=out_name, expr=bound_expr))

    order_by: list[OrderByItem] = []
    for ob in query.order_by:
        bound_ob = _bind_expr(ob.expr, alias_to_cols, output_aliases=output_aliases)
        order_by.append(OrderByItem(expr=bound_ob, ascending=ob.ascending))

    return LogicalPlan(
        sql=sql,
        base_scan=base_scan,
        joins=joins,
        residual_predicates=residual_predicates,
        group_by=group_by,
        aggregates=aggregates,
        projections=projections,
        order_by=order_by,
        limit=query.limit,
        is_aggregate_query=has_any_agg,
        table_versions=table_versions,
    )


def optimize_logical_plan(plan: LogicalPlan, options: OptimizerOptions) -> LogicalPlan:
    """Apply the 5 toggleable optimization rules to produce an optimized LogicalPlan."""
    # Deep-copy mutable scan lists so original plan stays intact for EXPLAIN comparison
    def clone_scan(s: ScanNode) -> ScanNode:
        return ScanNode(
            table_name=s.table_name,
            alias=s.alias,
            snapshot_version=s.snapshot_version,
            all_columns=list(s.all_columns),
            required_columns=list(s.all_columns),
            all_partitions=list(s.all_partitions),
            selected_partitions=list(s.all_partitions),
            pushed_predicates=list(s.pushed_predicates),
            hot_keys=dict(s.hot_keys),
        )

    opt = LogicalPlan(
        sql=plan.sql,
        base_scan=clone_scan(plan.base_scan),
        joins=[
            JoinSpec(right_scan=clone_scan(j.right_scan), left_key=j.left_key, right_key=j.right_key)
            for j in plan.joins
        ],
        residual_predicates=list(plan.residual_predicates),
        group_by=list(plan.group_by),
        aggregates=list(plan.aggregates),
        projections=list(plan.projections),
        order_by=list(plan.order_by),
        limit=plan.limit,
        is_aggregate_query=plan.is_aggregate_query,
        table_versions=dict(plan.table_versions),
    )
    rules: list[str] = []
    scan_by_alias = {s.alias: s for s in opt.all_scans}

    # Rule 1: Predicate Pushdown
    if options.predicate_pushdown and opt.residual_predicates:
        kept_residual: list[Expr] = []
        pushed_any = False
        for pred in opt.residual_predicates:
            cols = collect_columns(pred)
            aliases = {c.table for c in cols if c.table is not None}
            if len(aliases) == 1:
                alias = next(iter(aliases))
                scan_by_alias[alias].pushed_predicates.append(pred)
                pushed_any = True
            else:
                kept_residual.append(pred)
        opt.residual_predicates = kept_residual
        if pushed_any:
            rules.append("predicate_pushdown")

    # Rule 2: Partition Pruning (uses pushed predicates + catalog partition/min-max metadata)
    if options.partition_pruning:
        pruned_any = False
        for scan in opt.all_scans:
            preds = list(scan.pushed_predicates)
            if not preds and not options.predicate_pushdown:
                # Even if predicate_pushdown is disabled, partition_pruning can inspect single-table WHERE clauses
                for pred in opt.residual_predicates:
                    aliases = {c.table for c in collect_columns(pred) if c.table is not None}
                    if aliases == {scan.alias}:
                        preds.append(pred)
            if preds:
                selected = [
                    p
                    for p in scan.all_partitions
                    if all(_partition_may_match_predicate(p, pred, scan.alias) for pred in preds)
                ]
                if len(selected) < len(scan.all_partitions):
                    pruned_any = True
                scan.selected_partitions = selected
        if pruned_any:
            rules.append("partition_pruning")

    # Rule 3: Projection Pushdown
    if options.projection_pushdown:
        needed_by_alias: dict[str, set[str]] = {a: set() for a in scan_by_alias}

        def record_cols(expr: Expr | None) -> None:
            if expr is None:
                return
            for col in collect_columns(expr):
                if col.table and col.table in needed_by_alias:
                    needed_by_alias[col.table].add(col.column)

        for j in opt.joins:
            record_cols(j.left_key)
            record_cols(j.right_key)
        for pred in opt.residual_predicates:
            record_cols(pred)
        for s in opt.all_scans:
            for pred in s.pushed_predicates:
                record_cols(pred)
        for gb in opt.group_by:
            record_cols(gb)
        for agg in opt.aggregates:
            record_cols(agg.arg)
        for proj in opt.projections:
            record_cols(proj.expr)
        for ob in opt.order_by:
            record_cols(ob.expr)

        pruned_cols = False
        for alias, scan in scan_by_alias.items():
            req = sorted(needed_by_alias[alias])
            if not req:
                # COUNT(*) with no predicates still needs 1 lightweight column to count rows
                req = [scan.all_columns[0]]
            if len(req) < len(scan.all_columns):
                pruned_cols = True
            scan.required_columns = req
        if pruned_cols:
            rules.append("projection_pushdown")

    # Rule 4: Two-Phase Distributed Hash Aggregation
    if options.two_phase_agg and opt.is_aggregate_query:
        opt.two_phase_agg = True
        rules.append("two_phase_agg")

    # Rule 5: Skew-Aware Salted Hash Shuffle (for GROUP BY on hot-key columns)
    if options.skew_salting and opt.is_aggregate_query and opt.group_by:
        for gb in opt.group_by:
            if gb.table and gb.table in scan_by_alias:
                scan = scan_by_alias[gb.table]
                hot_vals = scan.hot_keys.get(gb.column)
                if hot_vals:
                    opt.skew_salt_column = gb.full_name()
                    opt.skew_hot_values = list(hot_vals)
                    opt.skew_salt_factor = options.salt_factor
                    rules.append("skew_salting")
                    break

    opt.optimizer_rules_applied = rules
    return opt


def format_logical_plan(plan: LogicalPlan, title: str = "LogicalPlan") -> str:
    lines: list[str] = [f"{title}:"]
    indent = "  "
    if plan.limit is not None:
        lines.append(f"{indent}Limit: {plan.limit}")
        indent += "  "
    if plan.order_by:
        ob_str = ", ".join(
            f"{format_expr(o.expr)} {'ASC' if o.ascending else 'DESC'}" for o in plan.order_by
        )
        lines.append(f"{indent}Sort: [{ob_str}]")
        indent += "  "
    proj_str = ", ".join(f"{format_expr(p.expr)} AS {p.output_name}" for p in plan.projections)
    lines.append(f"{indent}Projection: [{proj_str}]")
    indent += "  "
    if plan.is_aggregate_query:
        gb_str = ", ".join(g.full_name() for g in plan.group_by)
        agg_str = ", ".join(
            f"{a.func}({format_expr(a.arg) if a.arg else '*'}) AS {a.output_name}"
            for a in plan.aggregates
        )
        if plan.two_phase_agg:
            salt_note = (
                f", skew_salt={plan.skew_salt_column}(factor={plan.skew_salt_factor})"
                if plan.skew_salt_column
                else ""
            )
            lines.append(f"{indent}FinalHashAggregate: group_by=[{gb_str}], aggs=[{agg_str}]{salt_note}")
            indent += "  "
            lines.append(f"{indent}ShuffleExchange: hash([{gb_str or 'global'}]){salt_note}")
            indent += "  "
            lines.append(f"{indent}PartialHashAggregate: group_by=[{gb_str}], aggs=[{agg_str}]")
            indent += "  "
        else:
            lines.append(f"{indent}SinglePhaseHashAggregate: group_by=[{gb_str}], aggs=[{agg_str}]")
            indent += "  "
    if plan.residual_predicates:
        pred_str = " AND ".join(format_expr(p) for p in plan.residual_predicates)
        lines.append(f"{indent}Filter: {pred_str}")
        indent += "  "
    for j in reversed(plan.joins):
        lines.append(
            f"{indent}InnerHashJoin: {j.left_key.full_name()} = {j.right_key.full_name()}"
        )
        r = j.right_scan
        r_preds = " AND ".join(format_expr(p) for p in r.pushed_predicates) or "none"
        lines.append(
            f"{indent}  ParquetScan({r.table_name} AS {r.alias}): cols={len(r.required_columns)}/{len(r.all_columns)} "
            f"{r.required_columns}, partitions={len(r.selected_partitions)}/{len(r.all_partitions)}, "
            f"pushed_filter=[{r_preds}]"
        )
        indent += "  "
    b = plan.base_scan
    b_preds = " AND ".join(format_expr(p) for p in b.pushed_predicates) or "none"
    lines.append(
        f"{indent}ParquetScan({b.table_name} AS {b.alias}): cols={len(b.required_columns)}/{len(b.all_columns)} "
        f"{b.required_columns}, partitions={len(b.selected_partitions)}/{len(b.all_partitions)}, "
        f"pushed_filter=[{b_preds}]"
    )
    if plan.optimizer_rules_applied:
        lines.append(f"  AppliedRules: {plan.optimizer_rules_applied}")
    return "\n".join(lines)
