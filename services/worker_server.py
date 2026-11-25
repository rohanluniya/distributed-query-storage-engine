"""Standalone gRPC Worker process executing Arrow columnar scans, shuffles, hash aggs, joins, and spill-to-disk."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import signal
import threading
import time
from concurrent import futures
from typing import Any

import grpc
import pyarrow as pa

from processing.cache import VersionedLRUCache
from processing.memory_pool import MemoryPool, SpillManager
from processing.operators import (
    apply_projections,
    deserialize_table,
    eval_expr,
    execute_hash_aggregate,
    execute_hash_join,
    hash_partition_table,
    scan_parquet_partition,
    serialize_table,
)
from services.observability import get_logger, log_event
from services.proto_gen import query_engine_pb2 as pb2
from services.proto_gen import query_engine_pb2_grpc as pb2_grpc

GRPC_MAX_MSG_BYTES = 128 * 1024 * 1024
GRPC_OPTIONS = [
    ("grpc.max_send_message_length", GRPC_MAX_MSG_BYTES),
    ("grpc.max_receive_message_length", GRPC_MAX_MSG_BYTES),
]


class WorkerServicer(pb2_grpc.WorkerServiceServicer):
    """Real gRPC Worker implementation with shuffle store, memory pool, spill manager, and scan LRU cache."""

    def __init__(
        self,
        worker_id: str,
        endpoint: str,
        spill_dir: str | pathlib.Path,
        max_mem_bytes: int = 128 * 1024 * 1024,
        cache_max_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        self.worker_id = worker_id
        self.endpoint = endpoint
        self.spill_dir = pathlib.Path(spill_dir) / worker_id
        self.spill_dir.mkdir(parents=True, exist_ok=True)
        self.default_max_mem_bytes = max_mem_bytes
        self.memory_pool = MemoryPool(max_bytes=max_mem_bytes)
        self.scan_cache: VersionedLRUCache[bytes] = VersionedLRUCache(max_bytes=cache_max_bytes)

        self._started_at = time.monotonic()
        self._active_tasks = 0
        self._total_spilled_bytes = 0
        self._lock = threading.RLock()
        # query_id -> {shuffle_key -> (arrow_ipc_bytes, num_rows)}
        self._shuffle_store: dict[str, dict[str, tuple[bytes, int]]] = {}
        # query_id -> threading.Event (set when cancelled)
        self._cancelled_queries: dict[str, threading.Event] = {}
        # endpoint -> reusable gRPC stub for peer-to-peer shuffle fetches
        self._peer_channels: dict[str, grpc.Channel] = {}
        self._peer_stubs: dict[str, pb2_grpc.WorkerServiceStub] = {}
        self.logger = get_logger(f"dqse.worker.{worker_id}")

    def _is_cancelled(self, query_id: str) -> bool:
        with self._lock:
            ev = self._cancelled_queries.get(query_id)
            return ev is not None and ev.is_set()

    def _get_peer_stub(self, endpoint: str) -> pb2_grpc.WorkerServiceStub:
        with self._lock:
            stub = self._peer_stubs.get(endpoint)
            if stub is None:
                ch = grpc.insecure_channel(endpoint, options=GRPC_OPTIONS)
                self._peer_channels[endpoint] = ch
                stub = pb2_grpc.WorkerServiceStub(ch)
                self._peer_stubs[endpoint] = stub
            return stub

    def _fetch_shuffle_tables(
        self,
        query_id: str,
        shuffle_sources: list[dict[str, Any]],
    ) -> list[pa.Table]:
        tables: list[pa.Table] = []
        for src in shuffle_sources:
            if self._is_cancelled(query_id):
                raise RuntimeError(f"Query {query_id} cancelled during shuffle fetch")
            ep = src["worker_endpoint"]
            skey = src["shuffle_key"]
            if ep == self.endpoint:
                with self._lock:
                    entry = self._shuffle_store.get(query_id, {}).get(skey)
                if entry is None:
                    raise RuntimeError(f"Local shuffle key '{skey}' missing on {self.worker_id}")
                payload = entry[0]
            else:
                stub = self._get_peer_stub(ep)
                resp = stub.FetchShuffle(
                    pb2.ShuffleFetchRequest(query_id=query_id, shuffle_key=skey),
                    timeout=20.0,
                )
                if not resp.ok:
                    raise RuntimeError(f"Peer FetchShuffle failed from {ep}: {resp.error}")
                payload = resp.arrow_ipc_payload
            if payload:
                tbl = deserialize_table(payload)
                if tbl.num_rows > 0:
                    tables.append(tbl)
        return tables

    def _store_shuffle_partitions(
        self,
        query_id: str,
        stage_id: str,
        task_id: str,
        partitioned: dict[int, pa.Table],
    ) -> tuple[list[pb2.ShufflePartitionMeta], int]:
        metas: list[pb2.ShufflePartitionMeta] = []
        total_bytes = 0
        with self._lock:
            q_store = self._shuffle_store.setdefault(query_id, {})
        for part_id, tbl in sorted(partitioned.items()):
            payload = serialize_table(tbl)
            nbytes = len(payload)
            total_bytes += nbytes
            skey = f"{stage_id}:{task_id}:p{part_id}"
            with self._lock:
                q_store[skey] = (payload, tbl.num_rows)
            metas.append(
                pb2.ShufflePartitionMeta(
                    partition_id=part_id,
                    worker_id=self.worker_id,
                    worker_endpoint=self.endpoint,
                    shuffle_key=skey,
                    num_rows=tbl.num_rows,
                    num_bytes=nbytes,
                )
            )
        return metas, total_bytes

    def _scan_with_cache(
        self,
        spec: dict[str, Any],
        enable_cache: bool,
    ) -> tuple[pa.Table, int, int, int, int, bool]:
        table_name = spec["table_name"]
        version = int(spec["snapshot_version"])
        file_path = spec["file_path"]
        alias = spec["alias"]
        columns = list(spec["columns"])
        predicates = list(spec.get("predicates", []))

        key_material = json.dumps(
            {
                "table": table_name,
                "version": version,
                "file": file_path,
                "alias": alias,
                "cols": columns,
                "preds": predicates,
            },
            sort_keys=True,
        )
        cache_key = hashlib.sha256(key_material.encode("utf-8")).hexdigest()
        if enable_cache:
            cached = self.scan_cache.get(cache_key, current_versions={table_name: version})
            if cached is not None:
                tbl = deserialize_table(cached)
                return tbl, tbl.num_rows, tbl.num_rows, 0, 0, True

        res = scan_parquet_partition(
            file_path=file_path,
            alias=alias,
            columns=columns,
            predicates=predicates,
        )
        if enable_cache and res.table.num_rows > 0:
            payload = serialize_table(res.table)
            self.scan_cache.put(
                cache_key,
                payload,
                size_bytes=len(payload),
                table_versions={table_name: version},
            )
        return (
            res.table,
            res.rows_in,
            res.rows_out,
            res.row_groups_scanned,
            res.row_groups_pruned,
            False,
        )

    def Health(self, request: pb2.HealthRequest, context: grpc.ServicerContext) -> pb2.HealthResponse:
        cstats = self.scan_cache.stats()
        with self._lock:
            active = self._active_tasks
            spilled = self._total_spilled_bytes
        return pb2.HealthResponse(
            worker_id=self.worker_id,
            status="HEALTHY",
            uptime_ms=int((time.monotonic() - self._started_at) * 1000),
            active_tasks=active,
            mem_used_bytes=self.memory_pool.used_bytes,
            mem_limit_bytes=self.memory_pool.max_bytes,
            cache_entries=int(cstats["entries"]),
            cache_bytes=int(cstats["bytes_used"]),
            cache_hits=int(cstats["hits"]),
            cache_misses=int(cstats["misses"]),
            total_spilled_bytes=spilled,
        )

    def Heartbeat(
        self, request: pb2.HeartbeatRequest, context: grpc.ServicerContext
    ) -> pb2.HeartbeatResponse:
        cstats = self.scan_cache.stats()
        with self._lock:
            active = self._active_tasks
            spilled = self._total_spilled_bytes
        return pb2.HeartbeatResponse(
            worker_id=self.worker_id,
            endpoint=self.endpoint,
            status="HEALTHY",
            active_tasks=active,
            mem_used_bytes=self.memory_pool.used_bytes,
            mem_limit_bytes=self.memory_pool.max_bytes,
            cache_entries=int(cstats["entries"]),
            cache_bytes=int(cstats["bytes_used"]),
            cache_hits=int(cstats["hits"]),
            cache_misses=int(cstats["misses"]),
            total_spilled_bytes=spilled,
            timestamp_ms=int(time.time() * 1000),
        )

    def FetchShuffle(
        self, request: pb2.ShuffleFetchRequest, context: grpc.ServicerContext
    ) -> pb2.ShuffleFetchResponse:
        with self._lock:
            entry = self._shuffle_store.get(request.query_id, {}).get(request.shuffle_key)
        if entry is None:
            return pb2.ShuffleFetchResponse(
                ok=False,
                error=f"Shuffle partition '{request.shuffle_key}' not found on {self.worker_id}",
            )
        payload, num_rows = entry
        return pb2.ShuffleFetchResponse(ok=True, arrow_ipc_payload=payload, num_rows=num_rows)

    def CancelQuery(
        self, request: pb2.CancelRequest, context: grpc.ServicerContext
    ) -> pb2.CancelResponse:
        freed = 0
        with self._lock:
            ev = self._cancelled_queries.setdefault(request.query_id, threading.Event())
            ev.set()
            q_shuf = self._shuffle_store.pop(request.query_id, {})
            for payload, _ in q_shuf.values():
                freed += len(payload)
            active = self._active_tasks
        # Clean up any spill directory for this query
        q_spill_dir = self.spill_dir / request.query_id
        if q_spill_dir.exists():
            import shutil

            shutil.rmtree(q_spill_dir, ignore_errors=True)
        log_event(
            self.logger,
            "Cancelled query on worker",
            query_id=request.query_id,
            worker_id=self.worker_id,
            reason=request.reason,
            freed_shuffle_bytes=freed,
        )
        return pb2.CancelResponse(
            worker_id=self.worker_id,
            cancelled_tasks=active,
            freed_shuffle_bytes=freed,
        )

    def InvalidateCache(
        self, request: pb2.CacheInvalidateRequest, context: grpc.ServicerContext
    ) -> pb2.CacheInvalidateResponse:
        new_ver = int(request.new_version) if request.new_version > 0 else None
        evicted = self.scan_cache.invalidate_table(request.table_name, new_version=new_ver)
        return pb2.CacheInvalidateResponse(worker_id=self.worker_id, evicted_entries=evicted)

    def ExecuteTask(
        self, request: pb2.TaskRequest, context: grpc.ServicerContext
    ) -> pb2.TaskResponse:
        t0 = time.perf_counter()
        with self._lock:
            self._active_tasks += 1

        task_pool = MemoryPool(
            max_bytes=request.max_mem_bytes if request.max_mem_bytes > 0 else self.default_max_mem_bytes
        )
        spill_mgr = SpillManager(self.spill_dir, request.query_id, f"{request.task_id}_a{request.attempt}")

        try:
            if self._is_cancelled(request.query_id):
                raise RuntimeError(f"Query {request.query_id} was cancelled before task start")

            spec = json.loads(request.spec_json)

            # Optional test-only delay to deterministically test active cancellation
            inject_delay_ms = float(spec.get("inject_delay_ms", 0))
            if inject_delay_ms > 0:
                deadline = time.monotonic() + inject_delay_ms / 1000.0
                while time.monotonic() < deadline:
                    if self._is_cancelled(request.query_id):
                        raise RuntimeError(f"Query {request.query_id} cancelled during execution")
                    time.sleep(0.02)

            task_type = request.task_type
            rows_in = 0
            rows_out = 0
            rgs_scanned = 0
            rgs_pruned = 0
            shuffle_bytes = 0
            cache_hit = False
            arrow_payload = b""
            shuffle_metas: list[pb2.ShufflePartitionMeta] = []

            if task_type == "SCAN_DIRECT":
                tbl, rows_in, _, rgs_scanned, rgs_pruned, cache_hit = self._scan_with_cache(
                    spec, request.enable_cache
                )
                for rpred in spec.get("residual_predicates", []):
                    if tbl.num_rows > 0:
                        tbl = tbl.filter(eval_expr(tbl, rpred))
                if spec.get("projections") and tbl.num_rows > 0:
                    tbl = apply_projections(tbl, spec["projections"])
                rows_out = tbl.num_rows
                arrow_payload = serialize_table(tbl)

            elif task_type == "SCAN_SHUFFLE":
                tbl, rows_in, _, rgs_scanned, rgs_pruned, cache_hit = self._scan_with_cache(
                    spec, request.enable_cache
                )
                for rpred in spec.get("residual_predicates", []):
                    if tbl.num_rows > 0:
                        tbl = tbl.filter(eval_expr(tbl, rpred))
                rows_out = tbl.num_rows
                partitioned = hash_partition_table(
                    tbl,
                    key_cols=list(spec.get("shuffle_keys", [])),
                    num_partitions=int(spec.get("num_shuffle_partitions", 1)),
                    skew_salt_col=spec.get("skew_salt_col"),
                    skew_hot_values=spec.get("skew_hot_values"),
                    salt_factor=int(spec.get("salt_factor", 4)),
                )
                shuffle_metas, shuffle_bytes = self._store_shuffle_partitions(
                    request.query_id, request.stage_id, request.task_id, partitioned
                )

            elif task_type == "SCAN_PARTIAL_AGG":
                tbl, rows_in, _, rgs_scanned, rgs_pruned, cache_hit = self._scan_with_cache(
                    spec, request.enable_cache
                )
                for rpred in spec.get("residual_predicates", []):
                    if tbl.num_rows > 0:
                        tbl = tbl.filter(eval_expr(tbl, rpred))
                group_by = list(spec.get("group_by", []))
                aggs = list(spec.get("aggs", []))
                skew_col = spec.get("skew_salt_col")
                skew_hot = spec.get("skew_hot_values")
                salt_factor = int(spec.get("salt_factor", 4))

                if skew_col and skew_hot and tbl.num_rows > 0:
                    # Append salt before local partial agg so hot key spreads across salt buckets
                    hot_set = set(skew_hot)
                    col_vals = tbl[skew_col].to_pylist()
                    salts = [((i % salt_factor) + 1) if v in hot_set else 0 for i, v in enumerate(col_vals)]
                    tbl = tbl.append_column("__dqse_salt", pa.array(salts, type=pa.int64()))
                    local_gb = group_by + ["__dqse_salt"]
                    partial_tbl = execute_hash_aggregate(
                        [tbl],
                        group_by=local_gb,
                        aggs=aggs,
                        mode="partial",
                        memory_pool=task_pool,
                        spill_manager=spill_mgr,
                    )
                    partitioned = hash_partition_table(
                        partial_tbl,
                        key_cols=local_gb,
                        num_partitions=int(spec.get("num_shuffle_partitions", 1)),
                    )
                else:
                    partial_tbl = execute_hash_aggregate(
                        [tbl],
                        group_by=group_by,
                        aggs=aggs,
                        mode="partial",
                        memory_pool=task_pool,
                        spill_manager=spill_mgr,
                    )
                    partitioned = hash_partition_table(
                        partial_tbl,
                        key_cols=group_by,
                        num_partitions=int(spec.get("num_shuffle_partitions", 1)),
                    )
                rows_out = partial_tbl.num_rows
                shuffle_metas, shuffle_bytes = self._store_shuffle_partitions(
                    request.query_id, request.stage_id, request.task_id, partitioned
                )

            elif task_type == "SALTED_AGG":
                upstream_tables = self._fetch_shuffle_tables(
                    request.query_id, spec.get("shuffle_sources", [])
                )
                rows_in = sum(t.num_rows for t in upstream_tables)
                group_by = list(spec.get("group_by", [])) + ["__dqse_salt"]
                aggs = list(spec.get("aggs", []))
                merged = execute_hash_aggregate(
                    upstream_tables,
                    group_by=group_by,
                    aggs=aggs,
                    mode="partial_merge",
                    memory_pool=task_pool,
                    spill_manager=spill_mgr,
                )
                if "__dqse_salt" in merged.column_names:
                    merged = merged.drop_columns(["__dqse_salt"])
                rows_out = merged.num_rows
                arrow_payload = serialize_table(merged)

            elif task_type == "FINAL_AGG":
                upstream_tables = self._fetch_shuffle_tables(
                    request.query_id, spec.get("shuffle_sources", [])
                )
                rows_in = sum(t.num_rows for t in upstream_tables)
                mode = "single" if spec.get("single_phase", False) else "final"
                group_by = list(spec.get("group_by", []))
                aggs = list(spec.get("aggs", []))
                out_tbl = execute_hash_aggregate(
                    upstream_tables,
                    group_by=group_by,
                    aggs=aggs,
                    mode=mode,
                    memory_pool=task_pool,
                    spill_manager=spill_mgr,
                )
                if spec.get("projections") and out_tbl.num_rows > 0:
                    out_tbl = apply_projections(out_tbl, spec["projections"])
                rows_out = out_tbl.num_rows
                arrow_payload = serialize_table(out_tbl)

            elif task_type == "HASH_JOIN":
                left_tables = self._fetch_shuffle_tables(
                    request.query_id, spec.get("left_sources", [])
                )
                right_tables = self._fetch_shuffle_tables(
                    request.query_id, spec.get("right_sources", [])
                )
                rows_in = sum(t.num_rows for t in left_tables) + sum(t.num_rows for t in right_tables)
                joined = execute_hash_join(
                    left_tables=left_tables,
                    right_tables=right_tables,
                    left_key=spec["left_key"],
                    right_key=spec["right_key"],
                    memory_pool=task_pool,
                    spill_manager=spill_mgr,
                )
                for rpred in spec.get("residual_predicates", []):
                    if joined.num_rows > 0:
                        joined = joined.filter(eval_expr(joined, rpred))

                if spec.get("partial_agg"):
                    partial_tbl = execute_hash_aggregate(
                        [joined],
                        group_by=list(spec.get("group_by", [])),
                        aggs=list(spec.get("aggs", [])),
                        mode="partial",
                        memory_pool=task_pool,
                        spill_manager=spill_mgr,
                    )
                    partitioned = hash_partition_table(
                        partial_tbl,
                        key_cols=list(spec.get("group_by", [])),
                        num_partitions=int(spec.get("num_shuffle_partitions", 1)),
                    )
                    rows_out = partial_tbl.num_rows
                    shuffle_metas, shuffle_bytes = self._store_shuffle_partitions(
                        request.query_id, request.stage_id, request.task_id, partitioned
                    )
                elif spec.get("shuffle_output"):
                    rows_out = joined.num_rows
                    partitioned = hash_partition_table(
                        joined,
                        key_cols=list(spec.get("shuffle_keys", [])),
                        num_partitions=int(spec.get("num_shuffle_partitions", 1)),
                    )
                    shuffle_metas, shuffle_bytes = self._store_shuffle_partitions(
                        request.query_id, request.stage_id, request.task_id, partitioned
                    )
                else:
                    if spec.get("projections") and joined.num_rows > 0:
                        joined = apply_projections(joined, spec["projections"])
                    rows_out = joined.num_rows
                    arrow_payload = serialize_table(joined)

            else:
                raise ValueError(f"Unsupported task_type: {task_type}")

            if self._is_cancelled(request.query_id):
                raise RuntimeError(f"Query {request.query_id} cancelled prior to commit")

            spilled_b = spill_mgr.spilled_bytes
            spill_f = spill_mgr.spill_files
            with self._lock:
                self._total_spilled_bytes += spilled_b

            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            return pb2.TaskResponse(
                query_id=request.query_id,
                stage_id=request.stage_id,
                task_id=request.task_id,
                attempt=request.attempt,
                lease_id=request.lease_id,
                ok=True,
                arrow_ipc_payload=arrow_payload,
                shuffle_outputs=shuffle_metas,
                rows_in=rows_in,
                rows_out=rows_out,
                row_groups_scanned=rgs_scanned,
                row_groups_pruned=rgs_pruned,
                shuffle_bytes_written=shuffle_bytes,
                peak_mem_bytes=task_pool.peak_bytes,
                spilled_bytes=spilled_b,
                spill_files=spill_f,
                cache_hit=cache_hit,
                duration_ms=round(elapsed_ms, 3),
                worker_id=self.worker_id,
            )
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            return pb2.TaskResponse(
                query_id=request.query_id,
                stage_id=request.stage_id,
                task_id=request.task_id,
                attempt=request.attempt,
                lease_id=request.lease_id,
                ok=False,
                error=f"{type(exc).__name__}: {exc}",
                duration_ms=round(elapsed_ms, 3),
                worker_id=self.worker_id,
            )
        finally:
            spill_mgr.cleanup()
            with self._lock:
                self._active_tasks = max(0, self._active_tasks - 1)


def create_worker_server(
    worker_id: str,
    host: str,
    port: int,
    spill_dir: str | pathlib.Path,
    max_mem_mb: int = 128,
    cache_max_mb: int = 64,
    max_workers: int = 8,
) -> tuple[grpc.Server, WorkerServicer]:
    endpoint = f"{host}:{port}"
    servicer = WorkerServicer(
        worker_id=worker_id,
        endpoint=endpoint,
        spill_dir=spill_dir,
        max_mem_bytes=max_mem_mb * 1024 * 1024,
        cache_max_bytes=cache_max_mb * 1024 * 1024,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers), options=GRPC_OPTIONS)
    pb2_grpc.add_WorkerServiceServicer_to_server(servicer, server)
    server.add_insecure_port(endpoint)
    return server, servicer


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a standalone DQSE gRPC Worker process.")
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--spill-dir", default=".local_data/spill")
    parser.add_argument("--max-mem-mb", type=int, default=128)
    parser.add_argument("--cache-max-mb", type=int, default=64)
    args = parser.parse_args()

    logger = get_logger(f"dqse.worker.{args.worker_id}")
    server, _ = create_worker_server(
        worker_id=args.worker_id,
        host=args.host,
        port=args.port,
        spill_dir=args.spill_dir,
        max_mem_mb=args.max_mem_mb,
        cache_max_mb=args.cache_max_mb,
    )
    server.start()
    log_event(
        logger,
        "Started gRPC Worker",
        worker_id=args.worker_id,
        endpoint=f"{args.host}:{args.port}",
        max_mem_mb=args.max_mem_mb,
        cache_max_mb=args.cache_max_mb,
    )

    stop_event = threading.Event()

    def _handle_sig(signum: int, _frame: Any) -> None:
        log_event(logger, "Received shutdown signal", worker_id=args.worker_id, signum=signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT, _handle_sig)

    while not stop_event.is_set():
        stop_event.wait(0.5)
    server.stop(grace=2.0)


if __name__ == "__main__":
    main()
