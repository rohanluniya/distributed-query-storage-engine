//! Companion Rust gRPC Worker using Tonic + Apache Arrow + DataFusion.
//! Built inside `infra/docker/Dockerfile.rust` (`rust:1.86.0-slim-bookworm`).
//! Implements the shared `schemas/query_engine.proto` wire contract.

use std::net::SocketAddr;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tonic::{transport::Server, Request, Response, Status};

pub mod dqse_proto {
    tonic::include_proto!("dqse.v1");
}

use dqse_proto::worker_service_server::{WorkerService, WorkerServiceServer};
use dqse_proto::{
    CacheInvalidateRequest, CacheInvalidateResponse, CancelRequest, CancelResponse, HealthRequest,
    HealthResponse, HeartbeatRequest, HeartbeatResponse, ShuffleFetchRequest, ShuffleFetchResponse,
    TaskRequest, TaskResponse,
};

struct RustWorkerService {
    worker_id: String,
    endpoint: String,
    started_at: Instant,
    mem_limit_bytes: i64,
}

#[tonic::async_trait]
impl WorkerService for RustWorkerService {
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            worker_id: self.worker_id.clone(),
            status: "HEALTHY".to_string(),
            uptime_ms: self.started_at.elapsed().as_millis() as i64,
            active_tasks: 0,
            mem_used_bytes: 0,
            mem_limit_bytes: self.mem_limit_bytes,
            cache_entries: 0,
            cache_bytes: 0,
            cache_hits: 0,
            cache_misses: 0,
            total_spilled_bytes: 0,
        }))
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Ok(Response::new(HeartbeatResponse {
            worker_id: self.worker_id.clone(),
            endpoint: self.endpoint.clone(),
            status: "HEALTHY".to_string(),
            active_tasks: 0,
            mem_used_bytes: 0,
            mem_limit_bytes: self.mem_limit_bytes,
            cache_entries: 0,
            cache_bytes: 0,
            cache_hits: 0,
            cache_misses: 0,
            total_spilled_bytes: 0,
            timestamp_ms: now_ms,
        }))
    }

    async fn execute_task(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<TaskResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(TaskResponse {
            query_id: req.query_id,
            stage_id: req.stage_id,
            task_id: req.task_id,
            attempt: req.attempt,
            lease_id: req.lease_id,
            ok: false,
            error: "Companion Rust worker task kernels require container build and full shuffle wiring; see docs/limitations.md".to_string(),
            worker_id: self.worker_id.clone(),
            ..Default::default()
        }))
    }

    async fn fetch_shuffle(
        &self,
        _request: Request<ShuffleFetchRequest>,
    ) -> Result<Response<ShuffleFetchResponse>, Status> {
        Ok(Response::new(ShuffleFetchResponse {
            ok: false,
            error: "Shuffle store unavailable on companion skeleton".to_string(),
            arrow_ipc_payload: Vec::new(),
            num_rows: 0,
        }))
    }

    async fn cancel_query(
        &self,
        _request: Request<CancelRequest>,
    ) -> Result<Response<CancelResponse>, Status> {
        Ok(Response::new(CancelResponse {
            worker_id: self.worker_id.clone(),
            cancelled_tasks: 0,
            freed_shuffle_bytes: 0,
        }))
    }

    async fn invalidate_cache(
        &self,
        _request: Request<CacheInvalidateRequest>,
    ) -> Result<Response<CacheInvalidateResponse>, Status> {
        Ok(Response::new(CacheInvalidateResponse {
            worker_id: self.worker_id.clone(),
            evicted_entries: 0,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();
    let addr: SocketAddr = std::env::var("DQSE_RUST_WORKER_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:18094".to_string())
        .parse()?;
    let worker_id =
        std::env::var("DQSE_WORKER_ID").unwrap_or_else(|_| "rust-worker-1".to_string());

    let svc = RustWorkerService {
        worker_id: worker_id.clone(),
        endpoint: addr.to_string(),
        started_at: Instant::now(),
        mem_limit_bytes: 256 * 1024 * 1024,
    };

    tracing::info!(worker_id = %worker_id, addr = %addr, "Starting DQSE Rust gRPC worker");
    Server::builder()
        .add_service(WorkerServiceServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}
