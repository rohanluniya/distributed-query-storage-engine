"""Consistent hashing ring with virtual nodes for partition-to-worker affinity and minimal reshuffling."""

from __future__ import annotations

import bisect
import hashlib
from collections.abc import Iterable


def _hash64(key: str) -> int:
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=False)


class ConsistentHashRing:
    """Deterministic consistent-hashing ring with configurable virtual nodes per physical worker."""

    def __init__(self, nodes: Iterable[str] = (), vnodes: int = 64) -> None:
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        self.vnodes = vnodes
        self._ring_positions: list[int] = []
        self._pos_to_node: dict[int, str] = {}
        self._nodes: set[str] = set()
        for node in sorted(set(nodes)):
            self.add_node(node)

    @property
    def nodes(self) -> list[str]:
        return sorted(self._nodes)

    def add_node(self, node_id: str) -> None:
        if node_id in self._nodes:
            return
        self._nodes.add(node_id)
        for v in range(self.vnodes):
            pos = _hash64(f"{node_id}#vnode-{v}")
            while pos in self._pos_to_node:
                pos = (pos + 1) & ((1 << 64) - 1)
            self._pos_to_node[pos] = node_id
            bisect.insort(self._ring_positions, pos)

    def remove_node(self, node_id: str) -> None:
        if node_id not in self._nodes:
            return
        self._nodes.remove(node_id)
        to_remove = {pos for pos, n in self._pos_to_node.items() if n == node_id}
        for pos in to_remove:
            del self._pos_to_node[pos]
        self._ring_positions = [p for p in self._ring_positions if p not in to_remove]

    def get_node(self, key: str) -> str:
        if not self._ring_positions:
            raise RuntimeError("ConsistentHashRing has no active worker nodes")
        pos = _hash64(key)
        idx = bisect.bisect_left(self._ring_positions, pos)
        if idx == len(self._ring_positions):
            idx = 0
        return self._pos_to_node[self._ring_positions[idx]]

    def get_preference_list(self, key: str, count: int | None = None) -> list[str]:
        """Return ordered distinct worker preference list starting from the primary owner."""
        if not self._ring_positions:
            return []
        target = len(self._nodes) if count is None else min(count, len(self._nodes))
        pos = _hash64(key)
        idx = bisect.bisect_left(self._ring_positions, pos)
        seen: list[str] = []
        seen_set: set[str] = set()
        n = len(self._ring_positions)
        for offset in range(n):
            node = self._pos_to_node[self._ring_positions[(idx + offset) % n]]
            if node not in seen_set:
                seen.append(node)
                seen_set.add(node)
                if len(seen) >= target:
                    break
        return seen

    def vnode_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {n: 0 for n in self._nodes}
        for node in self._pos_to_node.values():
            counts[node] = counts.get(node, 0) + 1
        return counts
