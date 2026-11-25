"""Contract tests verifying gRPC protobuf wire definitions, Arrow IPC round-trip, and Avro event schemas."""

from __future__ import annotations

import json
import pathlib

import pyarrow as pa
import pytest

from processing.operators import deserialize_table, serialize_table
from services.proto_gen import query_engine_pb2 as pb2
from services.proto_gen import query_engine_pb2_grpc as pb2_grpc

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent


@pytest.mark.contracts
def test_grpc_proto_contract_methods_and_messages() -> None:
    proto_path = REPO_ROOT / "schemas" / "query_engine.proto"
    text = proto_path.read_text(encoding="utf-8")
    for rpc_name in ("Health", "Heartbeat", "ExecuteTask", "FetchShuffle", "CancelQuery", "InvalidateCache"):
        assert f"rpc {rpc_name}(" in text
        assert hasattr(pb2_grpc.WorkerServiceServicer, rpc_name)

    req = pb2.TaskRequest(
        query_id="q-contract-1",
        stage_id="stage_1",
        task_id="task_0",
        partition_id=0,
        attempt=1,
        lease_id="lease-abc",
        lease_ttl_ms=15000,
        task_type="SCAN_DIRECT",
        spec_json="{}",
        max_mem_bytes=64 * 1024 * 1024,
        enable_cache=True,
        trace_id="trace-123",
    )
    raw = req.SerializeToString()
    decoded = pb2.TaskRequest()
    decoded.ParseFromString(raw)
    assert decoded.query_id == "q-contract-1"
    assert decoded.lease_id == "lease-abc"
    assert decoded.max_mem_bytes == 64 * 1024 * 1024


@pytest.mark.contracts
def test_arrow_ipc_wire_payload_contract() -> None:
    tbl = pa.table(
        {
            "order_id": pa.array([1, 2, 3], type=pa.int64()),
            "order_month": pa.array(["2025-01", "2025-02", "2025-03"], type=pa.string()),
            "total_price": pa.array([10.5, 20.25, 30.75], type=pa.float64()),
        }
    )
    wire_bytes = serialize_table(tbl)
    assert len(wire_bytes) > 0
    restored = deserialize_table(wire_bytes)
    assert restored.schema == tbl.schema
    assert restored.to_pydict() == tbl.to_pydict()


@pytest.mark.contracts
def test_avro_catalog_and_quarantine_contracts() -> None:
    for fname, expected_fields in [
        ("catalog_event.avsc", {"event_id", "table_name", "snapshot_version", "operation", "idempotency_key"}),
        ("dq_quarantine.avsc", {"quarantine_id", "table_name", "source_path", "rule_code", "severity", "run_id"}),
    ]:
        path = REPO_ROOT / "schemas" / fname
        schema = json.loads(path.read_text(encoding="utf-8"))
        assert schema["type"] == "record"
        field_names = {f["name"] for f in schema["fields"]}
        assert expected_fields.issubset(field_names)
