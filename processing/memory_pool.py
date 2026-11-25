"""Explicit worker memory pool accounting and Arrow IPC disk spill-to-disk manager."""

from __future__ import annotations

import pathlib
import shutil
import threading

import pyarrow as pa
import pyarrow.ipc as pa_ipc


class MemoryLimitExceeded(MemoryError):
    """Raised when a strict reservation cannot be satisfied within the memory pool budget."""


class MemoryPool:
    """Thread-safe byte-accurate memory reservation pool with peak tracking."""

    def __init__(self, max_bytes: int) -> None:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be > 0")
        self.max_bytes = int(max_bytes)
        self._used_bytes = 0
        self._peak_bytes = 0
        self._lock = threading.Lock()

    @property
    def used_bytes(self) -> int:
        with self._lock:
            return self._used_bytes

    @property
    def peak_bytes(self) -> int:
        with self._lock:
            return self._peak_bytes

    @property
    def available_bytes(self) -> int:
        with self._lock:
            return max(0, self.max_bytes - self._used_bytes)

    def try_reserve(self, nbytes: int) -> bool:
        nbytes = max(0, int(nbytes))
        with self._lock:
            if self._used_bytes + nbytes > self.max_bytes:
                return False
            self._used_bytes += nbytes
            if self._used_bytes > self._peak_bytes:
                self._peak_bytes = self._used_bytes
            return True

    def reserve(self, nbytes: int) -> None:
        if not self.try_reserve(nbytes):
            raise MemoryLimitExceeded(
                f"Cannot reserve {nbytes} bytes (used={self.used_bytes}, limit={self.max_bytes})"
            )

    def force_record_peak(self, nbytes: int) -> None:
        with self._lock:
            candidate = self._used_bytes + max(0, int(nbytes))
            if candidate > self._peak_bytes:
                self._peak_bytes = candidate

    def release(self, nbytes: int) -> None:
        nbytes = max(0, int(nbytes))
        with self._lock:
            self._used_bytes = max(0, self._used_bytes - nbytes)

    def reset_task_usage(self) -> None:
        with self._lock:
            self._used_bytes = 0


class SpillManager:
    """Writes and reads partitioned Arrow IPC spill files on local disk under memory pressure."""

    def __init__(self, base_dir: str | pathlib.Path, query_id: str, task_id: str) -> None:
        self.spill_root = pathlib.Path(base_dir) / query_id / task_id
        self.spill_root.mkdir(parents=True, exist_ok=True)
        self._bucket_files: dict[int, list[pathlib.Path]] = {}
        self._seq = 0
        self.spilled_bytes = 0
        self.spill_files = 0
        self._lock = threading.Lock()

    @property
    def has_spilled(self) -> bool:
        return self.spill_files > 0

    def spill_table(self, bucket_id: int, table: pa.Table) -> pathlib.Path:
        if table.num_rows == 0:
            raise ValueError("Refusing to spill empty table")
        with self._lock:
            self._seq += 1
            fpath = self.spill_root / f"bucket_{bucket_id:03d}_seq_{self._seq:05d}.arrow"
        with pa.OSFile(str(fpath), "wb") as sink, pa_ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        fsize = fpath.stat().st_size
        with self._lock:
            self._bucket_files.setdefault(bucket_id, []).append(fpath)
            self.spilled_bytes += fsize
            self.spill_files += 1
        return fpath

    def bucket_ids(self) -> list[int]:
        with self._lock:
            return sorted(self._bucket_files.keys())

    def read_bucket(self, bucket_id: int) -> list[pa.Table]:
        with self._lock:
            files = list(self._bucket_files.get(bucket_id, []))
        tables: list[pa.Table] = []
        for fpath in files:
            with pa.OSFile(str(fpath), "rb") as src, pa_ipc.open_stream(src) as reader:
                tables.append(reader.read_all())
        return tables

    def cleanup(self) -> None:
        shutil.rmtree(self.spill_root, ignore_errors=True)
        with self._lock:
            self._bucket_files.clear()
