"""Unit tests for consistent-hashing ring and versioned bounded LRU cache."""

from __future__ import annotations

import pytest

from processing.cache import VersionedLRUCache
from processing.consistent_hash import ConsistentHashRing


@pytest.mark.unit
def test_consistent_hash_ring_balance_and_minimal_movement() -> None:
    ring = ConsistentHashRing(["worker-1", "worker-2", "worker-3"], vnodes=64)
    keys = [f"orders:order_month=2025-0{(i % 6) + 1}:region_id={i % 5}:part-{i}" for i in range(600)]
    before = {k: ring.get_node(k) for k in keys}

    # Check reasonable load spread across 3 workers
    counts = {w: sum(1 for v in before.values() if v == w) for w in ring.nodes}
    for w, c in counts.items():
        assert c > 110, f"Worker {w} received too few keys ({c}/600)"

    # Preference list returns all distinct nodes in deterministic failover order
    pref = ring.get_preference_list(keys[0])
    assert len(pref) == 3
    assert len(set(pref)) == 3
    assert pref[0] == before[keys[0]]

    # Adding worker-4 moves only ~25% of keys (consistent hashing minimal movement)
    ring.add_node("worker-4")
    after = {k: ring.get_node(k) for k in keys}
    moved = sum(1 for k in keys if before[k] != after[k])
    assert moved < 260, f"Too many keys moved on node addition: {moved}/600"

    # Removing worker-2 shifts ONLY keys previously owned by worker-2
    ring2 = ConsistentHashRing(["worker-1", "worker-2", "worker-3"], vnodes=64)
    ring2.remove_node("worker-2")
    for k in keys:
        if before[k] != "worker-2":
            assert ring2.get_node(k) == before[k]


@pytest.mark.unit
def test_versioned_lru_cache_eviction_and_epoch_invalidation() -> None:
    cache: VersionedLRUCache[str] = VersionedLRUCache(max_bytes=100)
    assert cache.put("k1", "val1", size_bytes=45, table_versions={"orders": 1})
    assert cache.put("k2", "val2", size_bytes=45, table_versions={"orders": 1, "customer": 1})

    # Access k1 so k2 becomes least-recently-used
    assert cache.get("k1", current_versions={"orders": 1}) == "val1"
    assert cache.put("k3", "val3", size_bytes=45, table_versions={"lineitem": 1})

    # k2 must have been evicted to stay <= 100 bytes
    assert cache.get("k2", current_versions={"orders": 1, "customer": 1}) is None
    assert cache.get("k1", current_versions={"orders": 1}) == "val1"
    assert cache.stats()["evictions"] >= 1

    # Version bump on orders invalidates k1 automatically
    assert cache.get("k1", current_versions={"orders": 2}) is None
    assert cache.get("k3", current_versions={"lineitem": 1}) == "val3"

    # Explicit invalidate_table evicts matching entries and blocks stale puts
    cache.put("k4", "stale_attempt", size_bytes=20, table_versions={"orders": 2})
    evicted = cache.invalidate_table("orders", new_version=3)
    assert evicted == 1
    assert not cache.put("k5", "old_epoch", size_bytes=20, table_versions={"orders": 2})
