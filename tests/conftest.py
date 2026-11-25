"""Shared pytest fixtures for DQSE unit, integration, E2E, and chaos test suites."""

from __future__ import annotations

import pathlib
import socket
import tempfile
from collections.abc import Iterator
from typing import Any

import pytest

from ingestion.ingestor import ingest_synthetic_dataset
from models.catalog import MetadataCatalog
from services.coordinator import QueryCoordinator
from services.worker_server import create_worker_server


def find_free_ports(count: int) -> list[int]:
    ports: list[int] = []
    sockets: list[socket.socket] = []
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        ports.append(int(s.getsockname()[1]))
        sockets.append(s)
    for s in sockets:
        s.close()
    return ports


@pytest.fixture(scope="module")
def smoke_cluster() -> Iterator[dict[str, Any]]:
    """Spin up an isolated temporary warehouse, SQLite catalog, 3 gRPC workers, and Coordinator."""
    with tempfile.TemporaryDirectory(prefix="dqse_test_cluster_") as tmpdir:
        root = pathlib.Path(tmpdir)
        catalog = MetadataCatalog(root / "catalog.sqlite3")
        wh_dir = root / "warehouse"
        ingest_report = ingest_synthetic_dataset(wh_dir, catalog, scale="smoke", seed=42)

        ports = find_free_ports(3)
        servers = []
        endpoints: dict[str, str] = {}
        for idx, port in enumerate(ports, start=1):
            wid = f"worker-{idx}"
            srv, _ = create_worker_server(wid, "127.0.0.1", port, root / "spill", max_mem_mb=64)
            srv.start()
            servers.append(srv)
            endpoints[wid] = f"127.0.0.1:{port}"

        coord = QueryCoordinator(catalog=catalog, worker_endpoints=endpoints)
        coord.refresh_workers()

        yield {
            "root": root,
            "warehouse_dir": wh_dir,
            "catalog": catalog,
            "coordinator": coord,
            "servers": servers,
            "endpoints": endpoints,
            "ingest_report": ingest_report,
        }

        for srv in servers:
            srv.stop(grace=0.5)
