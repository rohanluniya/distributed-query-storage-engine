"""Physical DAG compiler turning an OptimizedLogicalPlan into distributed worker stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from models.contracts import OptimizerOptions
from processing.logical_plan import LogicalPlan, ScanNode


@dataclass
class PhysicalStage:
    stage_id: str
    stage_type: str  # SCAN_SHUFFLE, SCAN_PARTIAL_AGG, SCAN_DIRECT, HASH_JOIN, SALTED_AGG, FINAL_AGG, SORT_LIMIT
    description: str
    input_stage_ids: list[str] = field(default_factory=list)
    num_partitions: int = 1
    shuffle_output: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PhysicalDAG:
    plan: LogicalPlan
    options: OptimizerOptions
    stages: list[PhysicalStage]

    def format_dag(self) -> str:
        lines = ["PhysicalDistributedDAG:"]
        for st in self.stages:
            deps = f" <- {st.input_stage_ids}" if st.input_stage_ids else " (leaf)"
            out_mode = f"shuffle({st.num_partitions})" if st.shuffle_output else "direct_arrow_ipc"
            lines.append(
                f"  [{st.stage_id}] {st.stage_type} | tasks={st.num_partitions} | out={out_mode}{deps}"
            )
            lines.append(f"      {st.description}")
        return "\n".join(lines)


def _scan_desc(scan: ScanNode) -> str:
    return (
        f"table={scan.table_name}({scan.alias}) v{scan.snapshot_version}, "
        f"partitions={len(scan.selected_partitions)}/{len(scan.all_partitions)}, "
        f"cols={scan.required_columns}"
    )


def compile_physical_dag(plan: LogicalPlan, options: OptimizerOptions) -> PhysicalDAG:
    """Compile an OptimizedLogicalPlan into an explicit multi-stage distributed PhysicalDAG."""
    stages: list[PhysicalStage] = []
    shuffle_parts = options.shuffle_partitions

    if not plan.joins:
        # Single-table query path
        base = plan.base_scan
        n_scan_tasks = max(1, len(base.selected_partitions))
        if plan.is_aggregate_query:
            if plan.two_phase_agg:
                if plan.skew_salt_column:
                    # 3-stage skew-salted 2-phase agg:
                    # S1: Scan + Local PartialAgg + Salted Hash Shuffle
                    # S2: Parallel Salted PartialAgg across shuffle partitions
                    # S3: Final Unsalted Merge + Project + Sort + Limit
                    stages.append(
                        PhysicalStage(
                            stage_id="stage_1_scan_partial_salted",
                            stage_type="SCAN_PARTIAL_AGG",
                            description=f"Scan({_scan_desc(base)}) -> LocalPartialAgg -> SaltedShuffle(col={plan.skew_salt_column}, factor={plan.skew_salt_factor})",
                            num_partitions=n_scan_tasks,
                            shuffle_output=True,
                            metadata={"shuffle_buckets": shuffle_parts, "salted": True},
                        )
                    )
                    stages.append(
                        PhysicalStage(
                            stage_id="stage_2_salted_partial_merge",
                            stage_type="SALTED_AGG",
                            description=f"Parallel SaltedPartialAgg on {shuffle_parts} shuffle partitions (splits hot key across workers)",
                            input_stage_ids=["stage_1_scan_partial_salted"],
                            num_partitions=shuffle_parts,
                            shuffle_output=False,
                            metadata={"salted": True},
                        )
                    )
                    stages.append(
                        PhysicalStage(
                            stage_id="stage_3_final_unsalted_merge",
                            stage_type="FINAL_AGG",
                            description="Unsalted FinalHashAggregate + Projection + Sort + Limit",
                            input_stage_ids=["stage_2_salted_partial_merge"],
                            num_partitions=1,
                            shuffle_output=False,
                        )
                    )
                else:
                    # Standard 2-stage distributed hash aggregation:
                    # S1: Scan + Local PartialAgg -> Shuffle(hash(group_by))
                    # S2: Parallel FinalAgg across shuffle buckets -> Coordinator Sort/Limit
                    stages.append(
                        PhysicalStage(
                            stage_id="stage_1_scan_partial_agg",
                            stage_type="SCAN_PARTIAL_AGG",
                            description=f"Scan({_scan_desc(base)}) -> LocalPartialAgg -> HashShuffle({shuffle_parts})",
                            num_partitions=n_scan_tasks,
                            shuffle_output=True,
                            metadata={"shuffle_buckets": shuffle_parts, "salted": False},
                        )
                    )
                    final_parts = 1 if not plan.group_by else shuffle_parts
                    stages.append(
                        PhysicalStage(
                            stage_id="stage_2_final_agg",
                            stage_type="FINAL_AGG",
                            description=f"Distributed FinalHashAggregate across {final_parts} partitions + Projection",
                            input_stage_ids=["stage_1_scan_partial_agg"],
                            num_partitions=final_parts,
                            shuffle_output=False,
                        )
                    )
                    if plan.order_by or plan.limit is not None or final_parts > 1:
                        stages.append(
                            PhysicalStage(
                                stage_id="stage_3_sort_limit",
                                stage_type="SORT_LIMIT",
                                description=f"Merge + Sort({len(plan.order_by)} keys) + Limit({plan.limit})",
                                input_stage_ids=["stage_2_final_agg"],
                                num_partitions=1,
                                shuffle_output=False,
                            )
                        )
            else:
                # Single-phase shuffle (unoptimized ablation baseline):
                # S1: Scan raw rows -> HashShuffle (no local partial agg!)
                # S2: SinglePhase HashAgg on shuffle workers
                stages.append(
                    PhysicalStage(
                        stage_id="stage_1_scan_raw_shuffle",
                        stage_type="SCAN_SHUFFLE",
                        description=f"Scan({_scan_desc(base)}) -> RawRowShuffle({shuffle_parts}) [no partial agg]",
                        num_partitions=n_scan_tasks,
                        shuffle_output=True,
                        metadata={"shuffle_buckets": shuffle_parts},
                    )
                )
                agg_parts = 1 if not plan.group_by else shuffle_parts
                stages.append(
                    PhysicalStage(
                        stage_id="stage_2_single_phase_agg",
                        stage_type="FINAL_AGG",
                        description=f"SinglePhaseHashAggregate across {agg_parts} partitions",
                        input_stage_ids=["stage_1_scan_raw_shuffle"],
                        num_partitions=agg_parts,
                        shuffle_output=False,
                        metadata={"single_phase": True},
                    )
                )
                if plan.order_by or plan.limit is not None or agg_parts > 1:
                    stages.append(
                        PhysicalStage(
                            stage_id="stage_3_sort_limit",
                            stage_type="SORT_LIMIT",
                            description=f"Merge + Sort({len(plan.order_by)} keys) + Limit({plan.limit})",
                            input_stage_ids=["stage_2_single_phase_agg"],
                            num_partitions=1,
                            shuffle_output=False,
                        )
                    )
        else:
            # Non-aggregate scan + filter + project + sort/limit
            stages.append(
                PhysicalStage(
                    stage_id="stage_1_scan_project",
                    stage_type="SCAN_DIRECT",
                    description=f"Scan({_scan_desc(base)}) -> Filter -> Project",
                    num_partitions=n_scan_tasks,
                    shuffle_output=False,
                )
            )
            stages.append(
                PhysicalStage(
                    stage_id="stage_2_sort_limit",
                    stage_type="SORT_LIMIT",
                    description=f"Merge + Sort({len(plan.order_by)} keys) + Limit({plan.limit})",
                    input_stage_ids=["stage_1_scan_project"],
                    num_partitions=1,
                    shuffle_output=False,
                )
            )
    else:
        # Multi-table repartitioned distributed Hash Join chain
        first_join = plan.joins[0]
        base = plan.base_scan
        stages.append(
            PhysicalStage(
                stage_id="stage_1_scan_left",
                stage_type="SCAN_SHUFFLE",
                description=f"Scan({_scan_desc(base)}) -> HashShuffle(key={first_join.left_key.full_name()}, parts={shuffle_parts})",
                num_partitions=max(1, len(base.selected_partitions)),
                shuffle_output=True,
                metadata={"shuffle_key": first_join.left_key.full_name(), "shuffle_buckets": shuffle_parts},
            )
        )
        prev_stage_id = "stage_1_scan_left"
        stage_num = 2
        for j_idx, join_spec in enumerate(plan.joins):
            r_scan = join_spec.right_scan
            r_stage_id = f"stage_{stage_num}_scan_right_{j_idx}"
            stage_num += 1
            stages.append(
                PhysicalStage(
                    stage_id=r_stage_id,
                    stage_type="SCAN_SHUFFLE",
                    description=f"Scan({_scan_desc(r_scan)}) -> HashShuffle(key={join_spec.right_key.full_name()}, parts={shuffle_parts})",
                    num_partitions=max(1, len(r_scan.selected_partitions)),
                    shuffle_output=True,
                    metadata={"shuffle_key": join_spec.right_key.full_name(), "shuffle_buckets": shuffle_parts},
                )
            )
            join_stage_id = f"stage_{stage_num}_hash_join_{j_idx}"
            stage_num += 1
            has_next_join = j_idx + 1 < len(plan.joins)
            next_key = plan.joins[j_idx + 1].left_key.full_name() if has_next_join else None
            do_partial_agg = (not has_next_join) and plan.is_aggregate_query and plan.two_phase_agg
            shuffle_out = has_next_join or plan.is_aggregate_query
            stages.append(
                PhysicalStage(
                    stage_id=join_stage_id,
                    stage_type="HASH_JOIN",
                    description=(
                        f"RepartitionedHashJoin({join_spec.left_key.full_name()} = {join_spec.right_key.full_name()})"
                        + (f" -> Reshuffle({next_key})" if next_key else "")
                        + (" -> LocalPartialAgg -> Shuffle" if do_partial_agg else "")
                    ),
                    input_stage_ids=[prev_stage_id, r_stage_id],
                    num_partitions=shuffle_parts,
                    shuffle_output=shuffle_out,
                    metadata={
                        "join_index": j_idx,
                        "repartition_key": next_key,
                        "partial_agg": do_partial_agg,
                        "shuffle_buckets": shuffle_parts,
                    },
                )
            )
            prev_stage_id = join_stage_id

        if plan.is_aggregate_query:
            final_parts = 1 if not plan.group_by else shuffle_parts
            agg_stage_id = f"stage_{stage_num}_final_agg"
            stage_num += 1
            stages.append(
                PhysicalStage(
                    stage_id=agg_stage_id,
                    stage_type="FINAL_AGG",
                    description=(
                        f"{'FinalHashAggregate' if plan.two_phase_agg else 'SinglePhaseHashAggregate'} "
                        f"across {final_parts} partitions"
                    ),
                    input_stage_ids=[prev_stage_id],
                    num_partitions=final_parts,
                    shuffle_output=False,
                    metadata={"single_phase": not plan.two_phase_agg},
                )
            )
            prev_stage_id = agg_stage_id

        stages.append(
            PhysicalStage(
                stage_id=f"stage_{stage_num}_sort_limit",
                stage_type="SORT_LIMIT",
                description=f"Merge + Project + Sort({len(plan.order_by)} keys) + Limit({plan.limit})",
                input_stage_ids=[prev_stage_id],
                num_partitions=1,
                shuffle_output=False,
            )
        )

    return PhysicalDAG(plan=plan, options=options, stages=stages)
