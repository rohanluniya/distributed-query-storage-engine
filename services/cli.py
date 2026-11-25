"""Command-line interface for DQSE ingestion, query execution, EXPLAIN, cluster health, and compaction."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

from ingestion.ingestor import compact_table_partitions, ingest_synthetic_dataset
from models.catalog import MetadataCatalog
from models.contracts import OptimizerOptions
from services.api import _parse_workers_env
from services.coordinator import QueryCoordinator


def _default_catalog() -> MetadataCatalog:
    return MetadataCatalog(os.environ.get("DQSE_CATALOG_DB", ".local_data/catalog.sqlite3"))


def _default_coordinator(catalog: MetadataCatalog) -> QueryCoordinator:
    workers_raw = os.environ.get(
        "DQSE_WORKERS",
        "worker-1=127.0.0.1:18091,worker-2=127.0.0.1:18092,worker-3=127.0.0.1:18093",
    )
    return QueryCoordinator(catalog=catalog, worker_endpoints=_parse_workers_env(workers_raw))


def main() -> None:
    parser = argparse.ArgumentParser(prog="dqse", description="Distributed Query & Storage Engine CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ing = sub.add_parser("ingest", help="Generate and ingest partitioned Parquet dataset")
    p_ing.add_argument("--scale", choices=["smoke", "local", "stress"], default="local")
    p_ing.add_argument("--seed", type=int, default=42)
    p_ing.add_argument("--warehouse-dir", default=os.environ.get("DQSE_DATA_DIR", ".local_data/warehouse"))
    p_ing.add_argument("--report-out", default="")

    p_q = sub.add_parser("query", help="Run a SQL query against live gRPC workers")
    p_q.add_argument("--sql", required=True)
    p_q.add_argument("--verify", action="store_true", help="Verify exact parity vs DuckDB & DataFusion")
    p_q.add_argument("--max-mem-mb", type=float, default=0.0)

    p_exp = sub.add_parser("explain", help="Print Logical Plan, Optimized Plan, and Physical DAG")
    p_exp.add_argument("--sql", required=True)

    sub.add_parser("status", help="Show registered workers and catalog tables")

    p_cmp = sub.add_parser("compact", help="Compact Parquet fragments for a table")
    p_cmp.add_argument("--table", required=True)
    p_cmp.add_argument("--warehouse-dir", default=os.environ.get("DQSE_DATA_DIR", ".local_data/warehouse"))

    args = parser.parse_args()
    cat = _default_catalog()

    if args.command == "ingest":
        report = ingest_synthetic_dataset(args.warehouse_dir, cat, scale=args.scale, seed=args.seed)
        out_json = json.dumps(report, indent=2)
        if args.report_out:
            p = pathlib.Path(args.report_out)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(out_json, encoding="utf-8")
        print(out_json)
    elif args.command == "explain":
        coord = _default_coordinator(cat)
        res = coord.explain(args.sql)
        print(res.logical_plan)
        print()
        print(res.optimized_plan)
        print()
        print(res.physical_dag)
    elif args.command == "query":
        coord = _default_coordinator(cat)
        opts = OptimizerOptions(max_worker_mem_mb=args.max_mem_mb)
        qres = coord.execute_query(args.sql, options=opts, verify_reference=args.verify)
        print(json.dumps(qres.model_dump(), indent=2, default=str))
    elif args.command == "status":
        coord = _default_coordinator(cat)
        payload = {
            "workers": [w.model_dump() for w in coord.refresh_workers()],
            "tables": [t.model_dump() for t in cat.list_tables()],
        }
        print(json.dumps(payload, indent=2))
    elif args.command == "compact":
        new_ver = compact_table_partitions(args.warehouse_dir, cat, args.table)
        print(json.dumps({"table": args.table, "new_snapshot_version": new_ver}, indent=2))
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
