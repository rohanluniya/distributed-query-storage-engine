"""Structured JSON logging with redaction, OpenTelemetry tracing, and Prometheus metrics."""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

REDACTED_KEYS = {"api_key", "authorization", "x-api-key", "password", "secret", "token"}


def redact_dict(payload: dict[str, Any]) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for k, v in payload.items():
        if k.lower() in REDACTED_KEYS:
            clean[k] = "***REDACTED***"
        elif isinstance(v, dict):
            clean[k] = redact_dict(v)
        else:
            clean[k] = v
    return clean


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base: dict[str, Any] = {
            "timestamp_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "structured", None)
        if isinstance(extra, dict):
            base.update(redact_dict(extra))
        if record.exc_info:
            base["exception"] = self.formatException(record.exc_info)
        return json.dumps(base, default=str)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_event(logger: logging.Logger, message: str, level: int = logging.INFO, **fields: Any) -> None:
    logger.log(level, message, extra={"structured": fields})


# OpenTelemetry Tracer with InMemorySpanExporter for verifiable local trace capture
_SPAN_EXPORTER = InMemorySpanExporter()
_RESOURCE = Resource.create({"service.name": "dqse-distributed-query-engine", "service.version": "1.0.0"})
_PROVIDER = TracerProvider(resource=_RESOURCE)
_PROVIDER.add_span_processor(SimpleSpanProcessor(_SPAN_EXPORTER))
trace.set_tracer_provider(_PROVIDER)
TRACER = trace.get_tracer("dqse.engine")


@contextmanager
def trace_span(name: str, attributes: dict[str, Any] | None = None) -> Iterator[Any]:
    with TRACER.start_as_current_span(name) as span:
        if attributes:
            for k, v in attributes.items():
                span.set_attribute(k, str(v) if not isinstance(v, (bool, int, float, str)) else v)
        yield span


def recent_traces(limit: int = 25) -> list[dict[str, Any]]:
    spans = _SPAN_EXPORTER.get_finished_spans()[-limit:]
    out: list[dict[str, Any]] = []
    for sp in reversed(spans):
        duration_ms = 0.0
        if sp.start_time and sp.end_time:
            duration_ms = round((sp.end_time - sp.start_time) / 1e6, 3)
        out.append(
            {
                "name": sp.name,
                "trace_id": format(sp.context.trace_id, "032x"),
                "span_id": format(sp.context.span_id, "016x"),
                "duration_ms": duration_ms,
                "attributes": dict(sp.attributes or {}),
            }
        )
    return out


# Prometheus metrics registry
METRICS_REGISTRY = CollectorRegistry()

QUERIES_TOTAL = Counter(
    "dqse_queries_total",
    "Total queries executed by status",
    ["status"],
    registry=METRICS_REGISTRY,
)
QUERY_DURATION_MS = Histogram(
    "dqse_query_duration_ms",
    "End-to-end distributed query latency in milliseconds",
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 15000),
    registry=METRICS_REGISTRY,
)
TASKS_EXECUTED_TOTAL = Counter(
    "dqse_tasks_executed_total",
    "Distributed worker tasks executed by type and outcome",
    ["task_type", "status"],
    registry=METRICS_REGISTRY,
)
TASK_RETRIES_TOTAL = Counter(
    "dqse_task_retries_total",
    "Total worker task retries triggered on worker failure or lease timeout",
    registry=METRICS_REGISTRY,
)
SHUFFLE_BYTES_TOTAL = Counter(
    "dqse_shuffle_bytes_total",
    "Cumulative Arrow IPC shuffle bytes written across workers",
    registry=METRICS_REGISTRY,
)
SPILLED_BYTES_TOTAL = Counter(
    "dqse_spilled_bytes_total",
    "Cumulative bytes spilled to disk under worker memory pressure",
    registry=METRICS_REGISTRY,
)
CACHE_EVENTS_TOTAL = Counter(
    "dqse_cache_events_total",
    "Cache lookup outcomes across coordinator and workers",
    ["layer", "outcome"],
    registry=METRICS_REGISTRY,
)
PARTITIONS_PRUNED_TOTAL = Counter(
    "dqse_partitions_pruned_total",
    "Partitions pruned before worker scheduling",
    registry=METRICS_REGISTRY,
)
ROW_GROUPS_PRUNED_TOTAL = Counter(
    "dqse_row_groups_pruned_total",
    "Parquet row groups skipped via min/max statistics",
    registry=METRICS_REGISTRY,
)
WORKERS_HEALTHY = Gauge(
    "dqse_workers_healthy",
    "Current count of healthy gRPC workers in the consistent-hash ring",
    registry=METRICS_REGISTRY,
)


def render_prometheus_metrics() -> bytes:
    return generate_latest(METRICS_REGISTRY)
