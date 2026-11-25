# Incident Report INC-001: Mid-Query gRPC Worker Process Crash (`SIGKILL`)

## Summary
- **Incident Type:** Controlled fault-injection drill (`CHAOS-01` & `CHAOS-02`, verified in `results/chaos/chaos_results.json`)
- **Fault Injected:** Abrupt `SIGKILL` (`kill -9`) termination of independent OS worker process `worker-1` while executing Stage 1 (`SCAN_PARTIAL_AGG`) of `q1_filtered_agg.sql` across a 3-worker cluster.
- **User Impact:** Zero query failures; zero numerical inaccuracy. End-to-end query latency increased to `621.83 ms` (including simulated delay + retry) instead of normal ~46 ms smoke latency.

## Observed Recovery Timeline
1. Coordinator assigned partition scans using `ConsistentHashRing.get_preference_list()` across `worker-1`, `worker-2`, and `worker-3`.
2. `80 ms` after query start, `worker-1` received `SIGKILL`, dropping its HTTP/2 gRPC channel immediately.
3. Coordinator caught `grpc.StatusCode.UNAVAILABLE (Socket closed)`, logged structured JSON event `"Worker RPC failure; evicting worker and scheduling task retry"`, removed `worker-1` from the consistent-hash ring (`dqse_workers_healthy` dropped `3 -> 2`), incremented `dqse_task_retries_total`, and rescheduled failed tasks with `attempt=2` and fresh `lease_id` onto surviving workers (`worker-2` and `worker-3`).
4. Surviving workers completed all tasks; Coordinator verified exact numerical parity against DuckDB 2.0.0 (`exact_match: true`) and DataFusion 55.0.0 (`exact_match: true`).
5. In `CHAOS-02`, `worker-1` was restarted on its original endpoint. `refresh_workers()` verified `Heartbeat`, restored `worker-1` (`64` vnodes, `192` total), and returned `dqse_workers_healthy` to `3` within `1.17 s`.

## Follow-up Actions
- Prometheus alert `DqseWorkerRingDegraded` triggers if healthy workers drop below `2` for `30 s`.
- Future enhancement: add background heartbeat monitor thread to detect crashed workers even during idle periods without waiting for API refresh or query RPC failure.
