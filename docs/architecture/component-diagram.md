# Architecture Diagrams & Failure Domains

## Component & Failure-Domain Architecture

```mermaid
flowchart TB
    subgraph ClientTier["Client & Control Plane"]
        UI["Query Workbench UI (HTML/CSS/JS)"]
        CLI["DQSE CLI (services/cli.py)"]
        API["FastAPI Coordinator API (:8090)\nRBAC + Audit + /metrics + OTEL"]
    end

    subgraph CoordinatorDomain["Coordinator Failure Domain"]
        Parser["SQL Lexer & Parser"]
        Opt["5-Rule Logical Optimizer\n(Pushdowns, Pruning, 2P-Agg, Skew Salt)"]
        DAG["Physical DAG Compiler"]
        Sched["Stage Scheduler\nConsistent-Hash Ring + Leases + Retries"]
        RCache["Versioned LRU Result Cache"]
        Catalog[("SQLite WAL Catalog\nSnapshots, Partition Stats, Lineage, DQ")]
    end

    subgraph WorkerRing["gRPC Worker Failure Domains (Arrow IPC over HTTP/2)"]
        W1["Worker 1 (:18091)\nMemoryPool + Spill + ScanCache + ShuffleStore"]
        W2["Worker 2 (:18092)\nMemoryPool + Spill + ScanCache + ShuffleStore"]
        W3["Worker 3 (:18093)\nMemoryPool + Spill + ScanCache + ShuffleStore"]
    end

    subgraph StorageDomain["Durable Storage Layer"]
        Bronze[("Bronze Raw Parquet")]
        Silver[("Silver Partitioned Parquet\norder_month / region_id")]
        Quarantine[("DQ Quarantine / DLQ")]
    end

    subgraph Verification["Reference Parity Engines"]
        Duck["DuckDB 2.0.0"]
        DF["Apache DataFusion 55.0.0"]
    end

    UI --> API
    CLI --> Sched
    API --> Parser --> Opt --> DAG --> Sched
    Opt --> Catalog
    Sched --> RCache
    Sched -- "gRPC ExecuteTask / Heartbeat / Cancel" --> W1
    Sched -- "gRPC ExecuteTask / Heartbeat / Cancel" --> W2
    Sched -- "gRPC ExecuteTask / Heartbeat / Cancel" --> W3
    W1 <-- "Peer gRPC FetchShuffle (Arrow IPC)" --> W2
    W2 <-- "Peer gRPC FetchShuffle (Arrow IPC)" --> W3
    W1 <-- "Peer gRPC FetchShuffle (Arrow IPC)" --> W3
    W1 --> Silver
    W2 --> Silver
    W3 --> Silver
    Bronze --> Silver
    Bronze --> Quarantine
    API -. "Optional Exactness Verify" .-> Duck
    API -. "Optional Exactness Verify" .-> DF
```

## Failure Domain Isolation

1. **Worker Process Crash (`SIGKILL`):** Isolated to a single worker process. Coordinator catches gRPC `UNAVAILABLE`, evicts the worker from the `ConsistentHashRing`, and retries affected stage tasks on surviving preference-list workers with a fresh `lease_id` and incremented `attempt`.
2. **Memory Exhaustion on a Worker:** Confined to the task's `MemoryPool`. Exceeding budget triggers partitioned Arrow IPC spill-to-disk in `spill_dir/<query_id>/<task_id>/` instead of OOM-killing the worker process; spill files are removed in `finally`.
3. **Corrupt Data Fragment:** Rejected by `validate_and_quarantine_parquet_file` before snapshot commit, isolating bad files in `quarantine/` while keeping active table snapshots consistent.
4. **Stale Cache Risk:** Mitigated via immutable snapshot versions in `Catalog` + `table_versions` validation on every cache lookup + synchronous `InvalidateCache` RPC broadcast on mutation.
