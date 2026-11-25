"""Pydantic request/response models and typed domain contracts for DQSE."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class OptimizerOptions(BaseModel):
    projection_pushdown: bool = True
    predicate_pushdown: bool = True
    partition_pruning: bool = True
    two_phase_agg: bool = True
    skew_salting: bool = True
    enable_cache: bool = True
    shuffle_partitions: int = Field(default=6, ge=1, le=64)
    salt_factor: int = Field(default=4, ge=2, le=16)
    max_worker_mem_mb: float = Field(default=0.0, ge=0.0, le=4096.0)


class QueryRequest(BaseModel):
    sql: str = Field(..., min_length=6, max_length=8000)
    optimizer: OptimizerOptions = Field(default_factory=OptimizerOptions)
    verify_reference: bool = False
    query_id: str | None = None


class ExplainRequest(BaseModel):
    sql: str = Field(..., min_length=6, max_length=8000)
    optimizer: OptimizerOptions = Field(default_factory=OptimizerOptions)


class TaskExecutionProfile(BaseModel):
    stage_id: str
    task_id: str
    task_type: str
    partition_id: int
    worker_id: str
    attempt: int
    lease_id: str
    status: str
    rows_in: int = 0
    rows_out: int = 0
    row_groups_scanned: int = 0
    row_groups_pruned: int = 0
    shuffle_bytes_written: int = 0
    peak_mem_bytes: int = 0
    spilled_bytes: int = 0
    spill_files: int = 0
    cache_hit: bool = False
    duration_ms: float = 0.0
    start_offset_ms: float = 0.0
    error: str | None = None


class StageProfile(BaseModel):
    stage_id: str
    stage_name: str
    task_count: int
    retries: int = 0
    duration_ms: float = 0.0
    rows_in: int = 0
    rows_out: int = 0
    shuffle_bytes: int = 0
    spilled_bytes: int = 0
    spill_files: int = 0
    row_groups_scanned: int = 0
    row_groups_pruned: int = 0
    tasks: list[TaskExecutionProfile] = Field(default_factory=list)


class QueryProfile(BaseModel):
    query_id: str
    trace_id: str
    sql: str
    status: str
    started_at_utc: str
    finished_at_utc: str
    total_duration_ms: float
    planning_ms: float
    execution_ms: float
    result_cache_hit: bool = False
    scan_cache_hits: int = 0
    total_rows_scanned: int = 0
    total_rows_returned: int = 0
    partitions_total: int = 0
    partitions_pruned: int = 0
    row_groups_scanned: int = 0
    row_groups_pruned: int = 0
    total_shuffle_bytes: int = 0
    total_spilled_bytes: int = 0
    total_spill_files: int = 0
    peak_worker_mem_bytes: int = 0
    task_retries: int = 0
    duplicate_commits_ignored: int = 0
    optimizer_rules_applied: list[str] = Field(default_factory=list)
    table_versions: dict[str, int] = Field(default_factory=dict)
    logical_plan: str = ""
    optimized_plan: str = ""
    physical_dag: str = ""
    stages: list[StageProfile] = Field(default_factory=list)
    reference_parity: dict[str, Any] | None = None


class QueryResponse(BaseModel):
    query_id: str
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    freshness_utc: str
    table_versions: dict[str, int]
    profile: QueryProfile


class ExplainResponse(BaseModel):
    sql: str
    logical_plan: str
    optimized_plan: str
    physical_dag: str
    partitions_total: int
    partitions_pruned: int
    optimizer_rules_applied: list[str]
    estimated_scan_bytes: int


class WorkerStatus(BaseModel):
    worker_id: str
    endpoint: str
    status: str
    active_tasks: int = 0
    mem_used_bytes: int = 0
    mem_limit_bytes: int = 0
    cache_entries: int = 0
    cache_bytes: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    total_spilled_bytes: int = 0
    last_heartbeat_utc: str = ""
    assigned_vnodes: int = 0


class PartitionMetadata(BaseModel):
    partition_id: str
    table_name: str
    snapshot_version: int
    partition_values: dict[str, Any]
    file_path: str
    row_count: int
    file_size_bytes: int
    row_group_count: int
    min_stats: dict[str, Any] = Field(default_factory=dict)
    max_stats: dict[str, Any] = Field(default_factory=dict)


class TableSummary(BaseModel):
    table_name: str
    layer: str
    grain: str
    primary_key: list[str]
    partition_columns: list[str]
    snapshot_version: int
    schema_version: int
    row_count: int
    partition_count: int
    total_bytes: int
    hot_keys: dict[str, list[Any]] = Field(default_factory=dict)
    updated_at_utc: str
    columns: dict[str, str] = Field(default_factory=dict)
