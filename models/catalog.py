"""SQLite transactional metadata catalog with snapshot versioning, partition stats, lineage, and DQ quarantine."""

from __future__ import annotations

import json
import pathlib
import sqlite3
import threading
import uuid
from datetime import UTC, datetime
from typing import Any

from models.contracts import PartitionMetadata, TableSummary


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class MetadataCatalog:
    """Thread-safe SQLite metadata catalog supporting immutable table snapshots and partition stats."""

    def __init__(self, db_path: str | pathlib.Path) -> None:
        self.db_path = pathlib.Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_schema(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tables (
                    table_name TEXT PRIMARY KEY,
                    layer TEXT NOT NULL,
                    grain TEXT NOT NULL,
                    primary_key_json TEXT NOT NULL,
                    partition_columns_json TEXT NOT NULL,
                    columns_json TEXT NOT NULL,
                    current_snapshot INTEGER NOT NULL DEFAULT 0,
                    schema_version INTEGER NOT NULL DEFAULT 1,
                    hot_keys_json TEXT NOT NULL DEFAULT '{}',
                    updated_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    snapshot_version INTEGER NOT NULL,
                    operation TEXT NOT NULL,
                    event_time_utc TEXT NOT NULL,
                    processing_time_utc TEXT NOT NULL,
                    rows_added INTEGER NOT NULL,
                    rows_quarantined INTEGER NOT NULL DEFAULT 0,
                    partition_count INTEGER NOT NULL,
                    schema_version INTEGER NOT NULL,
                    source_seed INTEGER,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    FOREIGN KEY (table_name) REFERENCES tables(table_name)
                );

                CREATE TABLE IF NOT EXISTS partitions (
                    partition_id TEXT PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    snapshot_version INTEGER NOT NULL,
                    partition_values_json TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    file_size_bytes INTEGER NOT NULL,
                    row_group_count INTEGER NOT NULL,
                    min_stats_json TEXT NOT NULL,
                    max_stats_json TEXT NOT NULL,
                    FOREIGN KEY (table_name) REFERENCES tables(table_name)
                );

                CREATE INDEX IF NOT EXISTS idx_partitions_table_snap
                    ON partitions(table_name, snapshot_version);

                CREATE TABLE IF NOT EXISTS dq_quarantine (
                    quarantine_id TEXT PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    rule_code TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    detected_at_utc TEXT NOT NULL,
                    record_sample_json TEXT,
                    run_id TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS lineage_edges (
                    edge_id TEXT PRIMARY KEY,
                    upstream_asset TEXT NOT NULL,
                    downstream_asset TEXT NOT NULL,
                    transformation TEXT NOT NULL,
                    snapshot_version INTEGER NOT NULL,
                    recorded_at_utc TEXT NOT NULL
                );
                """
            )

    def register_table(
        self,
        table_name: str,
        layer: str,
        grain: str,
        primary_key: list[str],
        partition_columns: list[str],
        columns: dict[str, str],
        schema_version: int = 1,
        hot_keys: dict[str, list[Any]] | None = None,
    ) -> None:
        now = _utc_now()
        with self._lock, self._connect() as conn:
            existing = conn.execute(
                "SELECT current_snapshot FROM tables WHERE table_name = ?",
                (table_name,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO tables (
                        table_name, layer, grain, primary_key_json, partition_columns_json,
                        columns_json, current_snapshot, schema_version, hot_keys_json, updated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                    """,
                    (
                        table_name,
                        layer,
                        grain,
                        json.dumps(primary_key),
                        json.dumps(partition_columns),
                        json.dumps(columns),
                        schema_version,
                        json.dumps(hot_keys or {}),
                        now,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE tables
                    SET layer = ?, grain = ?, primary_key_json = ?, partition_columns_json = ?,
                        columns_json = ?, schema_version = ?, hot_keys_json = ?, updated_at_utc = ?
                    WHERE table_name = ?
                    """,
                    (
                        layer,
                        grain,
                        json.dumps(primary_key),
                        json.dumps(partition_columns),
                        json.dumps(columns),
                        schema_version,
                        json.dumps(hot_keys or {}),
                        now,
                        table_name,
                    ),
                )

    def commit_snapshot(
        self,
        table_name: str,
        operation: str,
        partitions: list[PartitionMetadata],
        idempotency_key: str,
        rows_quarantined: int = 0,
        source_seed: int | None = None,
        event_time_utc: str | None = None,
        hot_keys: dict[str, list[Any]] | None = None,
        replace_partitions: bool = True,
    ) -> int:
        """Atomically commit a new immutable snapshot version. Idempotent on idempotency_key."""
        now = _utc_now()
        ev_time = event_time_utc or now
        with self._lock, self._connect() as conn:
            dup = conn.execute(
                "SELECT snapshot_version FROM snapshots WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if dup is not None:
                return int(dup["snapshot_version"])

            row = conn.execute(
                "SELECT current_snapshot, schema_version, hot_keys_json FROM tables WHERE table_name = ?",
                (table_name,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Table '{table_name}' is not registered in catalog")

            prev_version = int(row["current_snapshot"])
            new_version = prev_version + 1
            schema_version = int(row["schema_version"])

            active_partitions: list[PartitionMetadata] = []
            if not replace_partitions and prev_version > 0:
                prev_rows = conn.execute(
                    "SELECT * FROM partitions WHERE table_name = ? AND snapshot_version = ?",
                    (table_name, prev_version),
                ).fetchall()
                for pr in prev_rows:
                    active_partitions.append(
                        PartitionMetadata(
                            partition_id=f"{table_name}-v{new_version}-{uuid.uuid4().hex[:8]}",
                            table_name=table_name,
                            snapshot_version=new_version,
                            partition_values=json.loads(pr["partition_values_json"]),
                            file_path=pr["file_path"],
                            row_count=int(pr["row_count"]),
                            file_size_bytes=int(pr["file_size_bytes"]),
                            row_group_count=int(pr["row_group_count"]),
                            min_stats=json.loads(pr["min_stats_json"]),
                            max_stats=json.loads(pr["max_stats_json"]),
                        )
                    )

            rows_added = 0
            for part in partitions:
                rows_added += part.row_count
                active_partitions.append(
                    part.model_copy(
                        update={
                            "snapshot_version": new_version,
                            "partition_id": f"{table_name}-v{new_version}-{uuid.uuid4().hex[:8]}",
                        }
                    )
                )

            for p in active_partitions:
                conn.execute(
                    """
                    INSERT INTO partitions (
                        partition_id, table_name, snapshot_version, partition_values_json,
                        file_path, row_count, file_size_bytes, row_group_count,
                        min_stats_json, max_stats_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        p.partition_id,
                        table_name,
                        new_version,
                        json.dumps(p.partition_values),
                        p.file_path,
                        p.row_count,
                        p.file_size_bytes,
                        p.row_group_count,
                        json.dumps(p.min_stats),
                        json.dumps(p.max_stats),
                    ),
                )

            conn.execute(
                """
                INSERT INTO snapshots (
                    snapshot_id, table_name, snapshot_version, operation, event_time_utc,
                    processing_time_utc, rows_added, rows_quarantined, partition_count,
                    schema_version, source_seed, idempotency_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"snap-{table_name}-{new_version}-{uuid.uuid4().hex[:6]}",
                    table_name,
                    new_version,
                    operation,
                    ev_time,
                    now,
                    rows_added,
                    rows_quarantined,
                    len(active_partitions),
                    schema_version,
                    source_seed,
                    idempotency_key,
                ),
            )

            merged_hot_keys = hot_keys if hot_keys is not None else json.loads(row["hot_keys_json"])
            conn.execute(
                """
                UPDATE tables
                SET current_snapshot = ?, hot_keys_json = ?, updated_at_utc = ?
                WHERE table_name = ?
                """,
                (new_version, json.dumps(merged_hot_keys), now, table_name),
            )
            return new_version

    def record_quarantine(
        self,
        table_name: str,
        source_path: str,
        rule_code: str,
        severity: str,
        reason: str,
        run_id: str,
        record_sample: dict[str, Any] | None = None,
    ) -> str:
        qid = f"dq-{uuid.uuid4().hex[:12]}"
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO dq_quarantine (
                    quarantine_id, table_name, source_path, rule_code, severity,
                    reason, detected_at_utc, record_sample_json, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    qid,
                    table_name,
                    source_path,
                    rule_code,
                    severity,
                    reason,
                    _utc_now(),
                    json.dumps(record_sample) if record_sample is not None else None,
                    run_id,
                ),
            )
        return qid

    def record_lineage(
        self,
        upstream_asset: str,
        downstream_asset: str,
        transformation: str,
        snapshot_version: int,
    ) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO lineage_edges (
                    edge_id, upstream_asset, downstream_asset, transformation,
                    snapshot_version, recorded_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    f"lin-{uuid.uuid4().hex[:10]}",
                    upstream_asset,
                    downstream_asset,
                    transformation,
                    snapshot_version,
                    _utc_now(),
                ),
            )

    def get_table(self, table_name: str) -> TableSummary | None:
        with self._lock, self._connect() as conn:
            t = conn.execute(
                "SELECT * FROM tables WHERE table_name = ?", (table_name,)
            ).fetchone()
            if t is None:
                return None
            snap = int(t["current_snapshot"])
            agg = conn.execute(
                """
                SELECT COALESCE(SUM(row_count), 0) AS total_rows,
                       COUNT(*) AS part_count,
                       COALESCE(SUM(file_size_bytes), 0) AS total_bytes
                FROM partitions
                WHERE table_name = ? AND snapshot_version = ?
                """,
                (table_name, snap),
            ).fetchone()
            return TableSummary(
                table_name=t["table_name"],
                layer=t["layer"],
                grain=t["grain"],
                primary_key=json.loads(t["primary_key_json"]),
                partition_columns=json.loads(t["partition_columns_json"]),
                snapshot_version=snap,
                schema_version=int(t["schema_version"]),
                row_count=int(agg["total_rows"]),
                partition_count=int(agg["part_count"]),
                total_bytes=int(agg["total_bytes"]),
                hot_keys=json.loads(t["hot_keys_json"]),
                updated_at_utc=t["updated_at_utc"],
                columns=json.loads(t["columns_json"]),
            )

    def list_tables(self) -> list[TableSummary]:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT table_name FROM tables ORDER BY table_name").fetchall()
        result: list[TableSummary] = []
        for r in rows:
            summary = self.get_table(r["table_name"])
            if summary is not None:
                result.append(summary)
        return result

    def get_table_versions(self) -> dict[str, int]:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT table_name, current_snapshot FROM tables").fetchall()
        return {str(r["table_name"]): int(r["current_snapshot"]) for r in rows}

    def get_partitions(
        self,
        table_name: str,
        snapshot_version: int | None = None,
    ) -> list[PartitionMetadata]:
        with self._lock, self._connect() as conn:
            if snapshot_version is None:
                row = conn.execute(
                    "SELECT current_snapshot FROM tables WHERE table_name = ?",
                    (table_name,),
                ).fetchone()
                if row is None:
                    raise KeyError(f"Unknown table: {table_name}")
                snapshot_version = int(row["current_snapshot"])

            rows = conn.execute(
                """
                SELECT * FROM partitions
                WHERE table_name = ? AND snapshot_version = ?
                ORDER BY file_path
                """,
                (table_name, snapshot_version),
            ).fetchall()

        return [
            PartitionMetadata(
                partition_id=r["partition_id"],
                table_name=r["table_name"],
                snapshot_version=int(r["snapshot_version"]),
                partition_values=json.loads(r["partition_values_json"]),
                file_path=r["file_path"],
                row_count=int(r["row_count"]),
                file_size_bytes=int(r["file_size_bytes"]),
                row_group_count=int(r["row_group_count"]),
                min_stats=json.loads(r["min_stats_json"]),
                max_stats=json.loads(r["max_stats_json"]),
            )
            for r in rows
        ]

    def list_snapshots(self, table_name: str | None = None) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            if table_name:
                rows = conn.execute(
                    "SELECT * FROM snapshots WHERE table_name = ? ORDER BY snapshot_version DESC",
                    (table_name,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM snapshots ORDER BY processing_time_utc DESC LIMIT 100"
                ).fetchall()
        return [dict(r) for r in rows]

    def list_quarantine(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM dq_quarantine ORDER BY detected_at_utc DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_lineage(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM lineage_edges ORDER BY recorded_at_utc DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def latest_freshness_utc(self) -> str:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT MAX(updated_at_utc) AS ts FROM tables").fetchone()
        if row and row["ts"]:
            return str(row["ts"])
        return _utc_now()
