"""Distributed Query Coordinator: worker registry, consistent-hash scheduling, leases, retries, idempotency, caching, and cancellation."""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any

import grpc
import pyarrow as pa

from models.catalog import MetadataCatalog
from models.contracts import (
    ExplainResponse,
    OptimizerOptions,
    QueryProfile,
    QueryResponse,
    StageProfile,
    TaskExecutionProfile,
    WorkerStatus,
)
from processing.cache import VersionedLRUCache
from processing.consistent_hash import ConsistentHashRing
from processing.logical_plan import (
    LogicalPlan,
    build_logical_plan,
    format_logical_plan,
    optimize_logical_plan,
)
from processing.memory_pool import MemoryPool
from processing.operators import (
    apply_projections,
    apply_sort_limit,
    deserialize_table,
    execute_hash_aggregate,
    table_to_json_rows,
)
from processing.physical_plan import PhysicalDAG, compile_physical_dag
from processing.reference_engines import compare_results, execute_datafusion, execute_duckdb
from processing.sql_parser import parse_sql
from services.observability import (
    CACHE_EVENTS_TOTAL,
    PARTITIONS_PRUNED_TOTAL,
    QUERIES_TOTAL,
    QUERY_DURATION_MS,
    ROW_GROUPS_PRUNED_TOTAL,
    SHUFFLE_BYTES_TOTAL,
    SPILLED_BYTES_TOTAL,
    TASK_RETRIES_TOTAL,
    TASKS_EXECUTED_TOTAL,
    WORKERS_HEALTHY,
    get_logger,
    log_event,
    trace_span,
)
from services.proto_gen import query_engine_pb2 as pb2
from services.proto_gen import query_engine_pb2_grpc as pb2_grpc
from services.worker_server import GRPC_OPTIONS


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class QueryCancelledError(RuntimeError):
    """Raised when an active query is cancelled via the coordinator API."""


class QueryCoordinator:
    """Coordinator orchestrating multi-stage distributed SQL execution over gRPC Arrow workers."""

    def __init__(
        self,
        catalog: MetadataCatalog,
        worker_endpoints: dict[str, str],
        cache_max_bytes: int = 64 * 1024 * 1024,
        task_lease_ms: int = 15_000,
        max_retries: int = 2,
    ) -> None:
        self.catalog = catalog
        self.task_lease_ms = task_lease_ms
        self.max_retries = max_retries
        self.result_cache: VersionedLRUCache[tuple[list[str], list[list[Any]]]] = VersionedLRUCache(
            max_bytes=cache_max_bytes
        )
        self.logger = get_logger("dqse.coordinator")

        self._lock = threading.RLock()
        self._worker_endpoints: dict[str, str] = dict(worker_endpoints)
        self._worker_status: dict[str, WorkerStatus] = {}
        self._channels: dict[str, grpc.Channel] = {}
        self._stubs: dict[str, pb2_grpc.WorkerServiceStub] = {}
        self._ring = ConsistentHashRing(nodes=worker_endpoints.keys(), vnodes=64)

        # Active query cancellation events and historical profiles
        self._active_cancels: dict[str, threading.Event] = {}
        self._profiles: dict[str, QueryProfile] = {}
        self._recent_query_ids: list[str] = []

        for wid, ep in worker_endpoints.items():
            self._register_channel(wid, ep)
            self._worker_status[wid] = WorkerStatus(
                worker_id=wid,
                endpoint=ep,
                status="REGISTERED",
                assigned_vnodes=64,
            )

    def _register_channel(self, worker_id: str, endpoint: str) -> None:
        ch = grpc.insecure_channel(endpoint, options=GRPC_OPTIONS)
        self._channels[worker_id] = ch
        self._stubs[worker_id] = pb2_grpc.WorkerServiceStub(ch)

    def register_worker(self, worker_id: str, endpoint: str) -> None:
        with self._lock:
            self._worker_endpoints[worker_id] = endpoint
            self._register_channel(worker_id, endpoint)
            self._ring.add_node(worker_id)
            self._worker_status[worker_id] = WorkerStatus(
                worker_id=worker_id,
                endpoint=endpoint,
                status="HEALTHY",
                assigned_vnodes=self._ring.vnode_counts().get(worker_id, 0),
                last_heartbeat_utc=_utc_now(),
            )
            WORKERS_HEALTHY.set(len(self._ring.nodes))

    def unregister_worker(self, worker_id: str) -> None:
        with self._lock:
            self._ring.remove_node(worker_id)
            if worker_id in self._worker_status:
                self._worker_status[worker_id].status = "OFFLINE"
                self._worker_status[worker_id].assigned_vnodes = 0
            WORKERS_HEALTHY.set(len(self._ring.nodes))

    def refresh_workers(self) -> list[WorkerStatus]:
        """Probe all registered gRPC workers via Heartbeat and update consistent-hash ring membership."""
        with self._lock:
            items = list(self._worker_endpoints.items())

        healthy_count = 0
        for wid, ep in items:
            stub = self._stubs[wid]
            try:
                resp = stub.Heartbeat(
                    pb2.HeartbeatRequest(coordinator_id="coordinator-1", timestamp_ms=int(time.time() * 1000)),
                    timeout=2.0,
                )
                with self._lock:
                    self._ring.add_node(wid)
                    vnodes = self._ring.vnode_counts().get(wid, 0)
                    self._worker_status[wid] = WorkerStatus(
                        worker_id=wid,
                        endpoint=ep,
                        status=resp.status,
                        active_tasks=int(resp.active_tasks),
                        mem_used_bytes=int(resp.mem_used_bytes),
                        mem_limit_bytes=int(resp.mem_limit_bytes),
                        cache_entries=int(resp.cache_entries),
                        cache_bytes=int(resp.cache_bytes),
                        cache_hits=int(resp.cache_hits),
                        cache_misses=int(resp.cache_misses),
                        total_spilled_bytes=int(resp.total_spilled_bytes),
                        last_heartbeat_utc=_utc_now(),
                        assigned_vnodes=vnodes,
                    )
                    healthy_count += 1
            except grpc.RpcError:
                with self._lock:
                    self._ring.remove_node(wid)
                    prev = self._worker_status.get(wid)
                    self._worker_status[wid] = WorkerStatus(
                        worker_id=wid,
                        endpoint=ep,
                        status="UNHEALTHY",
                        last_heartbeat_utc=prev.last_heartbeat_utc if prev else "",
                        assigned_vnodes=0,
                    )
        WORKERS_HEALTHY.set(healthy_count)
        with self._lock:
            return [self._worker_status[k] for k in sorted(self._worker_status)]

    def get_workers(self) -> list[WorkerStatus]:
        return self.refresh_workers()

    def invalidate_cache(self, table_name: str = "*", new_version: int | None = None) -> dict[str, Any]:
        """Invalidate coordinator result cache and broadcast CacheInvalidate RPC to all workers."""
        coord_evicted = self.result_cache.invalidate_table(table_name, new_version=new_version)
        worker_evicted: dict[str, int] = {}
        with self._lock:
            stubs = list(self._stubs.items())
        for wid, stub in stubs:
            try:
                resp = stub.InvalidateCache(
                    pb2.CacheInvalidateRequest(
                        table_name=table_name,
                        new_version=new_version if new_version is not None else 0,
                    ),
                    timeout=3.0,
                )
                worker_evicted[wid] = int(resp.evicted_entries)
            except grpc.RpcError:
                worker_evicted[wid] = -1
        return {
            "table_name": table_name,
            "new_version": new_version,
            "coordinator_evicted": coord_evicted,
            "worker_evicted": worker_evicted,
        }

    def cancel_query(self, query_id: str, reason: str = "user_requested") -> dict[str, Any]:
        """Cancel an in-flight query and broadcast CancelQuery RPC to release all worker leases/buffers."""
        with self._lock:
            ev = self._active_cancels.setdefault(query_id, threading.Event())
            ev.set()
            stubs = list(self._stubs.items())

        worker_results: dict[str, dict[str, Any]] = {}
        for wid, stub in stubs:
            try:
                resp = stub.CancelQuery(pb2.CancelRequest(query_id=query_id, reason=reason), timeout=3.0)
                worker_results[wid] = {
                    "cancelled_tasks": int(resp.cancelled_tasks),
                    "freed_shuffle_bytes": int(resp.freed_shuffle_bytes),
                }
            except grpc.RpcError:
                worker_results[wid] = {"status": "unreachable"}

        log_event(
            self.logger,
            "Broadcast query cancellation",
            query_id=query_id,
            reason=reason,
            workers=worker_results,
        )
        return {"query_id": query_id, "cancelled": True, "workers": worker_results}

    def explain(self, sql: str, options: OptimizerOptions | None = None) -> ExplainResponse:
        opts = options or OptimizerOptions()
        parsed = parse_sql(sql)
        raw_plan = build_logical_plan(sql, parsed, self.catalog)
        opt_plan = optimize_logical_plan(raw_plan, opts)
        dag = compile_physical_dag(opt_plan, opts)
        return ExplainResponse(
            sql=sql,
            logical_plan=format_logical_plan(raw_plan, "LogicalPlan (Unoptimized)"),
            optimized_plan=format_logical_plan(opt_plan, "OptimizedLogicalPlan"),
            physical_dag=dag.format_dag(),
            partitions_total=opt_plan.partitions_total,
            partitions_pruned=opt_plan.partitions_pruned,
            optimizer_rules_applied=opt_plan.optimizer_rules_applied,
            estimated_scan_bytes=opt_plan.estimated_scan_bytes,
        )

    def _check_cancelled(self, query_id: str) -> None:
        with self._lock:
            ev = self._active_cancels.get(query_id)
            if ev is not None and ev.is_set():
                raise QueryCancelledError(f"Query {query_id} cancelled")

    def _dispatch_task_with_retry(
        self,
        query_id: str,
        trace_id: str,
        stage_id: str,
        task_id: str,
        partition_id: int,
        task_type: str,
        spec: dict[str, Any],
        affinity_key: str,
        options: OptimizerOptions,
        query_start_mono: float,
        committed_keys: set[tuple[str, str, int]],
        commit_lock: threading.Lock,
        duplicate_commits_counter: list[int],
    ) -> tuple[pb2.TaskResponse, list[TaskExecutionProfile], int]:
        """Dispatch one physical task with consistent-hash affinity, leases, retries, and idempotent commit."""
        attempts_profiles: list[TaskExecutionProfile] = []
        retries = 0
        last_err = "no healthy workers available"
        tried_workers: set[str] = set()

        for attempt in range(1, self.max_retries + 2):
            self._check_cancelled(query_id)
            with self._lock:
                pref = self._ring.get_preference_list(affinity_key)
                if not pref:
                    raise RuntimeError("No healthy gRPC workers available in ring")
                # Prefer untried worker first on retry, else cycle through healthy workers
                candidates = [w for w in pref if w not in tried_workers] or pref
                worker_id = candidates[0]
                stub = self._stubs[worker_id]
                tried_workers.add(worker_id)

            lease_id = f"lease-{uuid.uuid4().hex[:10]}"
            start_off_ms = (time.perf_counter() - query_start_mono) * 1000.0
            req = pb2.TaskRequest(
                query_id=query_id,
                stage_id=stage_id,
                task_id=task_id,
                partition_id=partition_id,
                attempt=attempt,
                lease_id=lease_id,
                lease_ttl_ms=self.task_lease_ms,
                task_type=task_type,
                spec_json=json.dumps(spec),
                max_mem_bytes=int(options.max_worker_mem_mb * 1024 * 1024) if options.max_worker_mem_mb > 0 else 0,
                enable_cache=options.enable_cache,
                trace_id=trace_id,
            )

            try:
                resp = stub.ExecuteTask(req, timeout=max(5.0, self.task_lease_ms / 1000.0))
                if resp.ok:
                    # Idempotent output commit guard: ignore duplicate completions for same (query, stage, part)
                    commit_key = (query_id, stage_id, partition_id)
                    with commit_lock:
                        if commit_key in committed_keys:
                            duplicate_commits_counter[0] += 1
                        else:
                            committed_keys.add(commit_key)

                    TASKS_EXECUTED_TOTAL.labels(task_type=task_type, status="ok").inc()
                    SHUFFLE_BYTES_TOTAL.inc(resp.shuffle_bytes_written)
                    SPILLED_BYTES_TOTAL.inc(resp.spilled_bytes)
                    ROW_GROUPS_PRUNED_TOTAL.inc(resp.row_groups_pruned)
                    if resp.cache_hit:
                        CACHE_EVENTS_TOTAL.labels(layer="worker_scan", outcome="hit").inc()
                    elif task_type.startswith("SCAN"):
                        CACHE_EVENTS_TOTAL.labels(layer="worker_scan", outcome="miss").inc()

                    attempts_profiles.append(
                        TaskExecutionProfile(
                            stage_id=stage_id,
                            task_id=task_id,
                            task_type=task_type,
                            partition_id=partition_id,
                            worker_id=worker_id,
                            attempt=attempt,
                            lease_id=lease_id,
                            status="SUCCEEDED",
                            rows_in=int(resp.rows_in),
                            rows_out=int(resp.rows_out),
                            row_groups_scanned=int(resp.row_groups_scanned),
                            row_groups_pruned=int(resp.row_groups_pruned),
                            shuffle_bytes_written=int(resp.shuffle_bytes_written),
                            peak_mem_bytes=int(resp.peak_mem_bytes),
                            spilled_bytes=int(resp.spilled_bytes),
                            spill_files=int(resp.spill_files),
                            cache_hit=bool(resp.cache_hit),
                            duration_ms=round(float(resp.duration_ms), 3),
                            start_offset_ms=round(start_off_ms, 3),
                        )
                    )
                    return resp, attempts_profiles, retries
                else:
                    last_err = resp.error or "worker returned error"
                    TASKS_EXECUTED_TOTAL.labels(task_type=task_type, status="error").inc()
                    attempts_profiles.append(
                        TaskExecutionProfile(
                            stage_id=stage_id,
                            task_id=task_id,
                            task_type=task_type,
                            partition_id=partition_id,
                            worker_id=worker_id,
                            attempt=attempt,
                            lease_id=lease_id,
                            status="FAILED",
                            duration_ms=round(float(resp.duration_ms), 3),
                            start_offset_ms=round(start_off_ms, 3),
                            error=last_err,
                        )
                    )
                    if "cancelled" in last_err.lower():
                        raise QueryCancelledError(last_err)
            except grpc.RpcError as exc:
                last_err = f"gRPC {exc.code().name}: {exc.details()}"
                TASKS_EXECUTED_TOTAL.labels(task_type=task_type, status="rpc_error").inc()
                # Mark failed worker UNHEALTHY and evict from consistent-hash ring immediately!
                with self._lock:
                    self._ring.remove_node(worker_id)
                    if worker_id in self._worker_status:
                        self._worker_status[worker_id].status = "UNHEALTHY"
                        self._worker_status[worker_id].assigned_vnodes = 0
                    WORKERS_HEALTHY.set(len(self._ring.nodes))
                attempts_profiles.append(
                    TaskExecutionProfile(
                        stage_id=stage_id,
                        task_id=task_id,
                        task_type=task_type,
                        partition_id=partition_id,
                        worker_id=worker_id,
                        attempt=attempt,
                        lease_id=lease_id,
                        status="FAILED_RETRYING" if attempt <= self.max_retries else "FAILED",
                        duration_ms=round((time.perf_counter() - query_start_mono) * 1000.0 - start_off_ms, 3),
                        start_offset_ms=round(start_off_ms, 3),
                        error=last_err,
                    )
                )
                log_event(
                    self.logger,
                    "Worker RPC failure; evicting worker and scheduling task retry",
                    query_id=query_id,
                    stage_id=stage_id,
                    task_id=task_id,
                    failed_worker=worker_id,
                    attempt=attempt,
                    error=last_err,
                )

            if attempt <= self.max_retries:
                retries += 1
                TASK_RETRIES_TOTAL.inc()

        raise RuntimeError(f"Task {stage_id}/{task_id} failed after {self.max_retries + 1} attempts: {last_err}")

    def _run_stage_tasks(
        self,
        query_id: str,
        trace_id: str,
        stage_id: str,
        stage_name: str,
        task_defs: list[tuple[str, int, str, dict[str, Any], str]],
        options: OptimizerOptions,
        query_start_mono: float,
        committed_keys: set[tuple[str, str, int]],
        commit_lock: threading.Lock,
        duplicate_commits_counter: list[int],
    ) -> tuple[list[pb2.TaskResponse], StageProfile]:
        """Run a stage's tasks concurrently across workers via gRPC."""
        st0 = time.perf_counter()
        responses: list[pb2.TaskResponse] = []
        all_task_profiles: list[TaskExecutionProfile] = []
        total_retries = 0

        max_workers = max(1, min(len(task_defs), 12))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futs = [
                pool.submit(
                    self._dispatch_task_with_retry,
                    query_id,
                    trace_id,
                    stage_id,
                    tid,
                    pid,
                    ttype,
                    spec,
                    akey,
                    options,
                    query_start_mono,
                    committed_keys,
                    commit_lock,
                    duplicate_commits_counter,
                )
                for tid, pid, ttype, spec, akey in task_defs
            ]
            for fut in as_completed(futs):
                resp, tprofs, retries = fut.result()
                responses.append(resp)
                all_task_profiles.extend(tprofs)
                total_retries += retries

        dur_ms = (time.perf_counter() - st0) * 1000.0
        stage_profile = StageProfile(
            stage_id=stage_id,
            stage_name=stage_name,
            task_count=len(task_defs),
            retries=total_retries,
            duration_ms=round(dur_ms, 3),
            rows_in=sum(int(r.rows_in) for r in responses),
            rows_out=sum(int(r.rows_out) for r in responses),
            shuffle_bytes=sum(int(r.shuffle_bytes_written) for r in responses),
            spilled_bytes=sum(int(r.spilled_bytes) for r in responses),
            spill_files=sum(int(r.spill_files) for r in responses),
            row_groups_scanned=sum(int(r.row_groups_scanned) for r in responses),
            row_groups_pruned=sum(int(r.row_groups_pruned) for r in responses),
            tasks=sorted(all_task_profiles, key=lambda t: (t.partition_id, t.attempt)),
        )
        return responses, stage_profile

    def _serialize_aggs(self, plan: LogicalPlan) -> list[dict[str, Any]]:
        return [
            {
                "output_name": a.output_name,
                "func": a.func,
                "arg": a.arg.to_dict() if a.arg is not None else None,
            }
            for a in plan.aggregates
        ]

    def _serialize_projections(self, plan: LogicalPlan) -> list[dict[str, Any]]:
        return [{"output_name": p.output_name, "expr": p.expr.to_dict()} for p in plan.projections]

    def _serialize_order_by(self, plan: LogicalPlan) -> list[dict[str, Any]]:
        return [{"expr": o.expr.to_dict(), "ascending": o.ascending} for o in plan.order_by]

    def _group_shuffle_by_partition(
        self, responses: list[pb2.TaskResponse]
    ) -> dict[int, list[dict[str, Any]]]:
        by_part: dict[int, list[dict[str, Any]]] = {}
        for r in responses:
            for meta in r.shuffle_outputs:
                by_part.setdefault(int(meta.partition_id), []).append(
                    {
                        "worker_id": meta.worker_id,
                        "worker_endpoint": meta.worker_endpoint,
                        "shuffle_key": meta.shuffle_key,
                        "num_rows": int(meta.num_rows),
                        "num_bytes": int(meta.num_bytes),
                    }
                )
        return by_part

    def execute_query(
        self,
        sql: str,
        options: OptimizerOptions | None = None,
        verify_reference: bool = False,
        query_id: str | None = None,
        inject_delay_ms: float = 0.0,
        duplicate_test_partition: bool = False,
    ) -> QueryResponse:
        """Parse, optimize, compile DAG, execute across gRPC workers, and return results + full profile."""
        opts = options or OptimizerOptions()
        qid = query_id or f"q-{uuid.uuid4().hex[:10]}"
        trace_id = uuid.uuid4().hex
        started_utc = _utc_now()
        t_total_0 = time.perf_counter()

        with self._lock:
            self._active_cancels[qid] = threading.Event()

        with trace_span("coordinator.execute_query", {"query_id": qid, "sql": sql[:180]}):
            try:
                # 1. Planning & Optimization
                t_plan_0 = time.perf_counter()
                parsed = parse_sql(sql)
                raw_plan = build_logical_plan(sql, parsed, self.catalog)
                opt_plan = optimize_logical_plan(raw_plan, opts)
                dag = compile_physical_dag(opt_plan, opts)
                planning_ms = (time.perf_counter() - t_plan_0) * 1000.0
                PARTITIONS_PRUNED_TOTAL.inc(opt_plan.partitions_pruned)

                logical_str = format_logical_plan(raw_plan, "LogicalPlan (Unoptimized)")
                optimized_str = format_logical_plan(opt_plan, "OptimizedLogicalPlan")
                dag_str = dag.format_dag()

                # Handle EXPLAIN statements directly
                if parsed.is_explain and not parsed.is_analyze:
                    finished_utc = _utc_now()
                    rows = [
                        ["logical_plan", logical_str],
                        ["optimized_plan", optimized_str],
                        ["physical_dag", dag_str],
                    ]
                    prof = QueryProfile(
                        query_id=qid,
                        trace_id=trace_id,
                        sql=sql,
                        status="SUCCEEDED",
                        started_at_utc=started_utc,
                        finished_at_utc=finished_utc,
                        total_duration_ms=round((time.perf_counter() - t_total_0) * 1000.0, 3),
                        planning_ms=round(planning_ms, 3),
                        execution_ms=0.0,
                        total_rows_returned=len(rows),
                        partitions_total=opt_plan.partitions_total,
                        partitions_pruned=opt_plan.partitions_pruned,
                        optimizer_rules_applied=opt_plan.optimizer_rules_applied,
                        table_versions=opt_plan.table_versions,
                        logical_plan=logical_str,
                        optimized_plan=optimized_str,
                        physical_dag=dag_str,
                    )
                    self._record_profile(prof)
                    return QueryResponse(
                        query_id=qid,
                        columns=["plan_type", "plan"],
                        rows=rows,
                        row_count=len(rows),
                        freshness_utc=self.catalog.latest_freshness_utc(),
                        table_versions=opt_plan.table_versions,
                        profile=prof,
                    )

                # 2. Coordinator Query Result Cache lookup (epoch-keyed on table_versions!)
                cache_key_payload = json.dumps(
                    {
                        "sql": " ".join(sql.strip().split()),
                        "table_versions": opt_plan.table_versions,
                        "optimizer": opts.model_dump(),
                    },
                    sort_keys=True,
                )
                result_cache_key = hashlib.sha256(cache_key_payload.encode("utf-8")).hexdigest()
                if opts.enable_cache and inject_delay_ms <= 0 and not duplicate_test_partition:
                    cached_res = self.result_cache.get(
                        result_cache_key, current_versions=opt_plan.table_versions
                    )
                    if cached_res is not None:
                        CACHE_EVENTS_TOTAL.labels(layer="coordinator_result", outcome="hit").inc()
                        cols, rows = cached_res
                        total_ms = (time.perf_counter() - t_total_0) * 1000.0
                        QUERIES_TOTAL.labels(status="cache_hit").inc()
                        QUERY_DURATION_MS.observe(total_ms)
                        prof = QueryProfile(
                            query_id=qid,
                            trace_id=trace_id,
                            sql=sql,
                            status="SUCCEEDED",
                            started_at_utc=started_utc,
                            finished_at_utc=_utc_now(),
                            total_duration_ms=round(total_ms, 3),
                            planning_ms=round(planning_ms, 3),
                            execution_ms=0.0,
                            result_cache_hit=True,
                            total_rows_returned=len(rows),
                            partitions_total=opt_plan.partitions_total,
                            partitions_pruned=opt_plan.partitions_pruned,
                            optimizer_rules_applied=opt_plan.optimizer_rules_applied,
                            table_versions=opt_plan.table_versions,
                            logical_plan=logical_str,
                            optimized_plan=optimized_str,
                            physical_dag=dag_str,
                        )
                        self._record_profile(prof)
                        return QueryResponse(
                            query_id=qid,
                            columns=cols,
                            rows=rows,
                            row_count=len(rows),
                            freshness_utc=self.catalog.latest_freshness_utc(),
                            table_versions=opt_plan.table_versions,
                            profile=prof,
                        )
                    CACHE_EVENTS_TOTAL.labels(layer="coordinator_result", outcome="miss").inc()

                # Ensure we know currently live workers
                if not self._ring.nodes:
                    self.refresh_workers()

                # 3. Distributed Physical DAG Execution
                t_exec_0 = time.perf_counter()
                stage_profiles: list[StageProfile] = []
                committed_keys: set[tuple[str, str, int]] = set()
                commit_lock = threading.Lock()
                dup_commits = [0]

                result_table = self._execute_dag(
                    query_id=qid,
                    trace_id=trace_id,
                    plan=opt_plan,
                    dag=dag,
                    options=opts,
                    query_start_mono=t_total_0,
                    stage_profiles=stage_profiles,
                    committed_keys=committed_keys,
                    commit_lock=commit_lock,
                    dup_commits=dup_commits,
                    inject_delay_ms=inject_delay_ms,
                    duplicate_test_partition=duplicate_test_partition,
                )

                cols, rows = table_to_json_rows(result_table)
                if not cols:
                    cols = [p.output_name for p in opt_plan.projections]
                exec_ms = (time.perf_counter() - t_exec_0) * 1000.0
                total_ms = (time.perf_counter() - t_total_0) * 1000.0

                # Store in Coordinator Result Cache
                if opts.enable_cache:
                    est_bytes = max(256, len(json.dumps(rows, default=str).encode("utf-8")))
                    self.result_cache.put(
                        result_cache_key,
                        (cols, rows),
                        size_bytes=est_bytes,
                        table_versions=opt_plan.table_versions,
                    )

                # Optional Head-to-Head Parity Verification vs DuckDB & DataFusion
                ref_parity: dict[str, Any] | None = None
                if verify_reference:
                    ordered = bool(opt_plan.order_by)
                    d_res = execute_duckdb(sql, self.catalog)
                    df_res = execute_datafusion(sql, self.catalog)
                    d_ok, d_msg = compare_results(rows, d_res["rows"], ordered=ordered)
                    df_ok, df_msg = compare_results(rows, df_res["rows"], ordered=ordered)
                    ref_parity = {
                        "duckdb": {
                            "version": d_res["version"],
                            "duration_ms": d_res["duration_ms"],
                            "row_count": d_res["row_count"],
                            "exact_match": d_ok,
                            "detail": d_msg,
                        },
                        "datafusion": {
                            "version": df_res["version"],
                            "duration_ms": df_res["duration_ms"],
                            "row_count": df_res["row_count"],
                            "exact_match": df_ok,
                            "detail": df_msg,
                        },
                    }

                scan_cache_hits = sum(
                    1 for sp in stage_profiles for tp in sp.tasks if tp.cache_hit and tp.status == "SUCCEEDED"
                )
                peak_mem = max(
                    (tp.peak_mem_bytes for sp in stage_profiles for tp in sp.tasks),
                    default=0,
                )
                total_rows_scanned = sum(
                    sp.rows_in for sp in stage_profiles if "scan" in sp.stage_id.lower()
                )

                prof = QueryProfile(
                    query_id=qid,
                    trace_id=trace_id,
                    sql=sql,
                    status="SUCCEEDED",
                    started_at_utc=started_utc,
                    finished_at_utc=_utc_now(),
                    total_duration_ms=round(total_ms, 3),
                    planning_ms=round(planning_ms, 3),
                    execution_ms=round(exec_ms, 3),
                    result_cache_hit=False,
                    scan_cache_hits=scan_cache_hits,
                    total_rows_scanned=total_rows_scanned,
                    total_rows_returned=len(rows),
                    partitions_total=opt_plan.partitions_total,
                    partitions_pruned=opt_plan.partitions_pruned,
                    row_groups_scanned=sum(sp.row_groups_scanned for sp in stage_profiles),
                    row_groups_pruned=sum(sp.row_groups_pruned for sp in stage_profiles),
                    total_shuffle_bytes=sum(sp.shuffle_bytes for sp in stage_profiles),
                    total_spilled_bytes=sum(sp.spilled_bytes for sp in stage_profiles),
                    total_spill_files=sum(sp.spill_files for sp in stage_profiles),
                    peak_worker_mem_bytes=peak_mem,
                    task_retries=sum(sp.retries for sp in stage_profiles),
                    duplicate_commits_ignored=dup_commits[0],
                    optimizer_rules_applied=opt_plan.optimizer_rules_applied,
                    table_versions=opt_plan.table_versions,
                    logical_plan=logical_str,
                    optimized_plan=optimized_str,
                    physical_dag=dag_str,
                    stages=stage_profiles,
                    reference_parity=ref_parity,
                )
                self._record_profile(prof)
                QUERIES_TOTAL.labels(status="succeeded").inc()
                QUERY_DURATION_MS.observe(total_ms)
                log_event(
                    self.logger,
                    "Distributed query completed",
                    query_id=qid,
                    trace_id=trace_id,
                    total_duration_ms=prof.total_duration_ms,
                    rows_returned=prof.total_rows_returned,
                    shuffle_bytes=prof.total_shuffle_bytes,
                    spilled_bytes=prof.total_spilled_bytes,
                    retries=prof.task_retries,
                )
                return QueryResponse(
                    query_id=qid,
                    columns=cols,
                    rows=rows,
                    row_count=len(rows),
                    freshness_utc=self.catalog.latest_freshness_utc(),
                    table_versions=opt_plan.table_versions,
                    profile=prof,
                )

            except QueryCancelledError as exc:
                QUERIES_TOTAL.labels(status="cancelled").inc()
                prof = QueryProfile(
                    query_id=qid,
                    trace_id=trace_id,
                    sql=sql,
                    status="CANCELLED",
                    started_at_utc=started_utc,
                    finished_at_utc=_utc_now(),
                    total_duration_ms=round((time.perf_counter() - t_total_0) * 1000.0, 3),
                    planning_ms=0.0,
                    execution_ms=0.0,
                )
                self._record_profile(prof)
                raise exc
            except Exception as exc:
                QUERIES_TOTAL.labels(status="failed").inc()
                log_event(self.logger, "Query execution failed", query_id=qid, error=str(exc))
                raise
            finally:
                with self._lock:
                    self._active_cancels.pop(qid, None)

    def _execute_dag(
        self,
        query_id: str,
        trace_id: str,
        plan: LogicalPlan,
        dag: PhysicalDAG,
        options: OptimizerOptions,
        query_start_mono: float,
        stage_profiles: list[StageProfile],
        committed_keys: set[tuple[str, str, int]],
        commit_lock: threading.Lock,
        dup_commits: list[int],
        inject_delay_ms: float = 0.0,
        duplicate_test_partition: bool = False,
    ) -> pa.Table:
        aggs_json = self._serialize_aggs(plan)
        projs_json = self._serialize_projections(plan)
        order_json = self._serialize_order_by(plan)
        gb_cols = [g.full_name() for g in plan.group_by]
        residual_json = [p.to_dict() for p in plan.residual_predicates]
        shuffle_parts = options.shuffle_partitions

        # Case A: Single-table queries
        if not plan.joins:
            base = plan.base_scan
            if not base.selected_partitions:
                if plan.is_aggregate_query and not plan.group_by:
                    empty_agg = execute_hash_aggregate(
                        [], [], aggs_json, mode="final", memory_pool=MemoryPool(8 * 1024 * 1024)
                    )
                    return apply_projections(empty_agg, projs_json)
                return pa.table({p.output_name: pa.array([], type=pa.float64()) for p in plan.projections})

            base_preds = [p.to_dict() for p in base.pushed_predicates]
            if not plan.is_aggregate_query:
                # SCAN_DIRECT -> Sort/Limit
                task_defs = []
                for idx, part in enumerate(base.selected_partitions):
                    spec = {
                        "table_name": base.table_name,
                        "snapshot_version": base.snapshot_version,
                        "file_path": part.file_path,
                        "alias": base.alias,
                        "columns": base.required_columns,
                        "predicates": base_preds,
                        "residual_predicates": residual_json,
                        "projections": projs_json,
                        "inject_delay_ms": inject_delay_ms,
                    }
                    task_defs.append(
                        (f"scan-{idx}", idx, "SCAN_DIRECT", spec, f"{base.table_name}:{part.file_path}")
                    )
                resps, st_prof = self._run_stage_tasks(
                    query_id,
                    trace_id,
                    "stage_1_scan_project",
                    "Scan + Filter + Project",
                    task_defs,
                    options,
                    query_start_mono,
                    committed_keys,
                    commit_lock,
                    dup_commits,
                )
                stage_profiles.append(st_prof)
                tables = [deserialize_table(r.arrow_ipc_payload) for r in resps if r.arrow_ipc_payload]
                return apply_sort_limit(tables, order_json, plan.limit)

            # Single-table Aggregate query
            is_salted = bool(plan.two_phase_agg and plan.skew_salt_column)
            stage1_type = "SCAN_PARTIAL_AGG" if plan.two_phase_agg else "SCAN_SHUFFLE"
            stage1_id = dag.stages[0].stage_id
            s1_tasks = []
            for idx, part in enumerate(base.selected_partitions):
                spec = {
                    "table_name": base.table_name,
                    "snapshot_version": base.snapshot_version,
                    "file_path": part.file_path,
                    "alias": base.alias,
                    "columns": base.required_columns,
                    "predicates": base_preds,
                    "residual_predicates": residual_json,
                    "group_by": gb_cols,
                    "aggs": aggs_json,
                    "shuffle_keys": gb_cols,
                    "num_shuffle_partitions": 1 if not gb_cols else shuffle_parts,
                    "skew_salt_col": plan.skew_salt_column if is_salted else None,
                    "skew_hot_values": plan.skew_hot_values if is_salted else [],
                    "salt_factor": plan.skew_salt_factor,
                    "inject_delay_ms": inject_delay_ms,
                }
                s1_tasks.append(
                    (f"scan-{idx}", idx, stage1_type, spec, f"{base.table_name}:{part.file_path}")
                )

            if duplicate_test_partition and s1_tasks:
                # Deterministically dispatch a duplicate attempt of partition 0 to prove idempotent dedup!
                dup_tid, dup_pid, dup_type, dup_spec, dup_akey = s1_tasks[0]
                s1_tasks.append((f"{dup_tid}-dup", dup_pid, dup_type, dup_spec, dup_akey))

            s1_resps, s1_prof = self._run_stage_tasks(
                query_id,
                trace_id,
                stage1_id,
                "Scan + PartialAgg + Shuffle" if plan.two_phase_agg else "Scan + RawShuffle",
                s1_tasks,
                options,
                query_start_mono,
                committed_keys,
                commit_lock,
                dup_commits,
            )
            stage_profiles.append(s1_prof)

            # Deduplicate responses by partition_id (idempotent stage output commit!)
            dedup_s1: dict[int, pb2.TaskResponse] = {}
            for r in s1_resps:
                # Extract original scan partition from task_id (scan-<idx>)
                base_tid = r.task_id.replace("-dup", "")
                pnum = int(base_tid.split("-")[1])
                if pnum not in dedup_s1:
                    dedup_s1[pnum] = r

            shuffle_by_part = self._group_shuffle_by_partition(list(dedup_s1.values()))

            if is_salted:
                # Stage 2: Parallel Salted Partial Merge across workers
                s2_tasks = []
                for p_id, sources in sorted(shuffle_by_part.items()):
                    spec = {
                        "shuffle_sources": sources,
                        "group_by": gb_cols,
                        "aggs": aggs_json,
                    }
                    s2_tasks.append((f"salt-agg-{p_id}", p_id, "SALTED_AGG", spec, f"shuffle:{p_id}"))
                s2_resps, s2_prof = self._run_stage_tasks(
                    query_id,
                    trace_id,
                    "stage_2_salted_partial_merge",
                    "Salted Partial Aggregate",
                    s2_tasks,
                    options,
                    query_start_mono,
                    committed_keys,
                    commit_lock,
                    dup_commits,
                )
                stage_profiles.append(s2_prof)
                salted_tables = [deserialize_table(r.arrow_ipc_payload) for r in s2_resps if r.arrow_ipc_payload]
                # Stage 3: Final Unsalted Merge + Project + Sort/Limit on coordinator
                t_s3 = time.perf_counter()
                final_tbl = execute_hash_aggregate(
                    salted_tables,
                    group_by=gb_cols,
                    aggs=aggs_json,
                    mode="final",
                    memory_pool=MemoryPool(
                        int(max(64 * 1024 * 1024, (options.max_worker_mem_mb or 128) * 1024 * 1024))
                    ),
                )
                final_tbl = apply_projections(final_tbl, projs_json)
                final_tbl = apply_sort_limit([final_tbl], order_json, plan.limit)
                stage_profiles.append(
                    StageProfile(
                        stage_id="stage_3_final_unsalted_merge",
                        stage_name="Unsalted Final Merge + Sort/Limit",
                        task_count=1,
                        duration_ms=round((time.perf_counter() - t_s3) * 1000.0, 3),
                        rows_in=sum(t.num_rows for t in salted_tables),
                        rows_out=final_tbl.num_rows,
                    )
                )
                return final_tbl

            # Standard Stage 2: Distributed FINAL_AGG across shuffle buckets
            s2_tasks = []
            for p_id, sources in sorted(shuffle_by_part.items()):
                spec = {
                    "shuffle_sources": sources,
                    "group_by": gb_cols,
                    "aggs": aggs_json,
                    "projections": projs_json,
                    "single_phase": not plan.two_phase_agg,
                }
                s2_tasks.append((f"final-agg-{p_id}", p_id, "FINAL_AGG", spec, f"shuffle:{p_id}"))

            if not s2_tasks:
                empty_agg = execute_hash_aggregate(
                    [], gb_cols, aggs_json, mode="final", memory_pool=MemoryPool(8 * 1024 * 1024)
                )
                return apply_projections(empty_agg, projs_json)

            s2_resps, s2_prof = self._run_stage_tasks(
                query_id,
                trace_id,
                dag.stages[1].stage_id,
                "Final Hash Aggregate" if plan.two_phase_agg else "Single-Phase Hash Aggregate",
                s2_tasks,
                options,
                query_start_mono,
                committed_keys,
                commit_lock,
                dup_commits,
            )
            stage_profiles.append(s2_prof)
            final_parts = [deserialize_table(r.arrow_ipc_payload) for r in s2_resps if r.arrow_ipc_payload]
            return apply_sort_limit(final_parts, order_json, plan.limit)

        # Case B: Multi-table Repartitioned Distributed Hash Join Chain
        first_join = plan.joins[0]
        base = plan.base_scan
        left_scan_tasks = []
        for idx, part in enumerate(base.selected_partitions):
            spec = {
                "table_name": base.table_name,
                "snapshot_version": base.snapshot_version,
                "file_path": part.file_path,
                "alias": base.alias,
                "columns": base.required_columns,
                "predicates": [p.to_dict() for p in base.pushed_predicates],
                "shuffle_keys": [first_join.left_key.full_name()],
                "num_shuffle_partitions": shuffle_parts,
                "inject_delay_ms": inject_delay_ms,
            }
            left_scan_tasks.append(
                (f"lscan-{idx}", idx, "SCAN_SHUFFLE", spec, f"{base.table_name}:{part.file_path}")
            )
        if not left_scan_tasks:
            return pa.table({p.output_name: pa.array([], type=pa.float64()) for p in plan.projections})

        l_resps, l_prof = self._run_stage_tasks(
            query_id,
            trace_id,
            "stage_1_scan_left",
            f"Scan & Shuffle {base.table_name}",
            left_scan_tasks,
            options,
            query_start_mono,
            committed_keys,
            commit_lock,
            dup_commits,
        )
        stage_profiles.append(l_prof)
        current_shuffle = self._group_shuffle_by_partition(l_resps)
        final_direct_tables: list[pa.Table] = []

        for j_idx, join_spec in enumerate(plan.joins):
            r_scan = join_spec.right_scan
            r_tasks = []
            for idx, part in enumerate(r_scan.selected_partitions):
                spec = {
                    "table_name": r_scan.table_name,
                    "snapshot_version": r_scan.snapshot_version,
                    "file_path": part.file_path,
                    "alias": r_scan.alias,
                    "columns": r_scan.required_columns,
                    "predicates": [p.to_dict() for p in r_scan.pushed_predicates],
                    "shuffle_keys": [join_spec.right_key.full_name()],
                    "num_shuffle_partitions": shuffle_parts,
                }
                r_tasks.append(
                    (f"rscan-{j_idx}-{idx}", idx, "SCAN_SHUFFLE", spec, f"{r_scan.table_name}:{part.file_path}")
                )
            if not r_tasks:
                return pa.table({p.output_name: pa.array([], type=pa.float64()) for p in plan.projections})

            r_resps, r_prof = self._run_stage_tasks(
                query_id,
                trace_id,
                f"stage_scan_right_{j_idx}",
                f"Scan & Shuffle {r_scan.table_name}",
                r_tasks,
                options,
                query_start_mono,
                committed_keys,
                commit_lock,
                dup_commits,
            )
            stage_profiles.append(r_prof)
            right_shuffle = self._group_shuffle_by_partition(r_resps)

            is_last_join = j_idx == len(plan.joins) - 1
            next_join_key = plan.joins[j_idx + 1].left_key.full_name() if not is_last_join else None
            do_partial_agg = is_last_join and plan.is_aggregate_query and plan.two_phase_agg
            do_raw_agg_shuffle = is_last_join and plan.is_aggregate_query and not plan.two_phase_agg

            join_tasks = []
            for p_id in range(shuffle_parts):
                l_srcs = current_shuffle.get(p_id, [])
                r_srcs = right_shuffle.get(p_id, [])
                if not l_srcs or not r_srcs:
                    continue
                j_spec: dict[str, Any] = {
                    "left_sources": l_srcs,
                    "right_sources": r_srcs,
                    "left_key": join_spec.left_key.full_name(),
                    "right_key": join_spec.right_key.full_name(),
                    "residual_predicates": residual_json if is_last_join else [],
                    "num_shuffle_partitions": 1 if (is_last_join and not gb_cols) else shuffle_parts,
                }
                if not is_last_join and next_join_key:
                    j_spec["shuffle_output"] = True
                    j_spec["shuffle_keys"] = [next_join_key]
                elif do_partial_agg:
                    j_spec["partial_agg"] = True
                    j_spec["group_by"] = gb_cols
                    j_spec["aggs"] = aggs_json
                elif do_raw_agg_shuffle:
                    j_spec["shuffle_output"] = True
                    j_spec["shuffle_keys"] = gb_cols
                else:
                    j_spec["projections"] = projs_json

                join_tasks.append((f"join-{j_idx}-p{p_id}", p_id, "HASH_JOIN", j_spec, f"join:{j_idx}:{p_id}"))

            if not join_tasks:
                return pa.table({p.output_name: pa.array([], type=pa.float64()) for p in plan.projections})

            j_resps, j_prof = self._run_stage_tasks(
                query_id,
                trace_id,
                f"stage_hash_join_{j_idx}",
                f"Repartitioned Hash Join ({join_spec.left_key.full_name()} = {join_spec.right_key.full_name()})",
                join_tasks,
                options,
                query_start_mono,
                committed_keys,
                commit_lock,
                dup_commits,
            )
            stage_profiles.append(j_prof)

            if (not is_last_join) or plan.is_aggregate_query:
                current_shuffle = self._group_shuffle_by_partition(j_resps)
            else:
                final_direct_tables = [
                    deserialize_table(r.arrow_ipc_payload) for r in j_resps if r.arrow_ipc_payload
                ]

        if not plan.is_aggregate_query:
            return apply_sort_limit(final_direct_tables, order_json, plan.limit)

        # Final distributed aggregation stage after join chain
        final_tasks = []
        for p_id, sources in sorted(current_shuffle.items()):
            spec = {
                "shuffle_sources": sources,
                "group_by": gb_cols,
                "aggs": aggs_json,
                "projections": projs_json,
                "single_phase": not plan.two_phase_agg,
            }
            final_tasks.append((f"join-final-agg-{p_id}", p_id, "FINAL_AGG", spec, f"shuffle:{p_id}"))

        if not final_tasks:
            return pa.table({p.output_name: pa.array([], type=pa.float64()) for p in plan.projections})

        f_resps, f_prof = self._run_stage_tasks(
            query_id,
            trace_id,
            "stage_final_agg",
            "Final Distributed Hash Aggregate",
            final_tasks,
            options,
            query_start_mono,
            committed_keys,
            commit_lock,
            dup_commits,
        )
        stage_profiles.append(f_prof)
        final_tables = [deserialize_table(r.arrow_ipc_payload) for r in f_resps if r.arrow_ipc_payload]
        return apply_sort_limit(final_tables, order_json, plan.limit)

    def _record_profile(self, profile: QueryProfile) -> None:
        with self._lock:
            self._profiles[profile.query_id] = profile
            self._recent_query_ids.append(profile.query_id)
            if len(self._recent_query_ids) > 100:
                oldest = self._recent_query_ids.pop(0)
                self._profiles.pop(oldest, None)

    def get_query_profile(self, query_id: str) -> QueryProfile | None:
        with self._lock:
            return self._profiles.get(query_id)

    def list_recent_profiles(self, limit: int = 25) -> list[QueryProfile]:
        with self._lock:
            ids = list(reversed(self._recent_query_ids[-limit:]))
            return [self._profiles[i] for i in ids if i in self._profiles]

    def cache_stats(self) -> dict[str, Any]:
        workers = self.refresh_workers()
        return {
            "coordinator_result_cache": self.result_cache.stats(),
            "worker_scan_caches": {
                w.worker_id: {
                    "entries": w.cache_entries,
                    "bytes_used": w.cache_bytes,
                    "hits": w.cache_hits,
                    "misses": w.cache_misses,
                }
                for w in workers
            },
        }
