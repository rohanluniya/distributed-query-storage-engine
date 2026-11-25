"""Bounded byte-budget LRU cache with snapshot epoch invalidation."""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass
class _CacheEntry[T]:
    value: T
    size_bytes: int
    table_versions: dict[str, int]


class VersionedLRUCache[T]:
    """Thread-safe bounded LRU cache that automatically invalidates stale table snapshots."""

    def __init__(self, max_bytes: int) -> None:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be > 0")
        self.max_bytes = int(max_bytes)
        self._entries: OrderedDict[str, _CacheEntry[T]] = OrderedDict()
        self._current_bytes = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._invalidations = 0
        self._table_epochs: dict[str, int] = {}
        self._lock = threading.RLock()

    def _is_stale(self, entry: _CacheEntry[T], current_versions: dict[str, int] | None) -> bool:
        for tbl, ver in entry.table_versions.items():
            if tbl in self._table_epochs and ver < self._table_epochs[tbl]:
                return True
            if current_versions is not None and tbl in current_versions and ver != current_versions[tbl]:
                return True
        return False

    def get(self, key: str, current_versions: dict[str, int] | None = None) -> T | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self._misses += 1
                return None
            if self._is_stale(entry, current_versions):
                self._entries.pop(key, None)
                self._current_bytes = max(0, self._current_bytes - entry.size_bytes)
                self._invalidations += 1
                self._misses += 1
                return None
            self._entries.move_to_end(key)
            self._hits += 1
            return entry.value

    def put(
        self,
        key: str,
        value: T,
        size_bytes: int,
        table_versions: dict[str, int],
    ) -> bool:
        size_bytes = max(1, int(size_bytes))
        with self._lock:
            if size_bytes > self.max_bytes:
                return False
            for tbl, ver in table_versions.items():
                if tbl in self._table_epochs and ver < self._table_epochs[tbl]:
                    return False

            existing = self._entries.pop(key, None)
            if existing is not None:
                self._current_bytes = max(0, self._current_bytes - existing.size_bytes)

            while self._entries and self._current_bytes + size_bytes > self.max_bytes:
                _, evicted = self._entries.popitem(last=False)
                self._current_bytes = max(0, self._current_bytes - evicted.size_bytes)
                self._evictions += 1

            self._entries[key] = _CacheEntry(
                value=value,
                size_bytes=size_bytes,
                table_versions=dict(table_versions),
            )
            self._current_bytes += size_bytes
            return True

    def invalidate_table(self, table_name: str, new_version: int | None = None) -> int:
        """Invalidate all cached entries referencing table_name (or all entries if table_name=='*')."""
        with self._lock:
            if table_name == "*":
                count = len(self._entries)
                self._entries.clear()
                self._current_bytes = 0
                self._invalidations += count
                return count

            if new_version is not None:
                prev = self._table_epochs.get(table_name, 0)
                self._table_epochs[table_name] = max(prev, int(new_version))

            to_drop = [
                k
                for k, e in self._entries.items()
                if table_name in e.table_versions
                and (new_version is None or e.table_versions[table_name] < new_version)
            ]
            for k in to_drop:
                removed = self._entries.pop(k)
                self._current_bytes = max(0, self._current_bytes - removed.size_bytes)
            self._invalidations += len(to_drop)
            return len(to_drop)

    def stats(self) -> dict[str, int | float]:
        with self._lock:
            total = self._hits + self._misses
            hit_rate = round(self._hits / total, 4) if total > 0 else 0.0
            return {
                "entries": len(self._entries),
                "bytes_used": self._current_bytes,
                "max_bytes": self.max_bytes,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
                "evictions": self._evictions,
                "invalidations": self._invalidations,
            }
