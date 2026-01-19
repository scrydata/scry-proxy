//! Benchmark execution engine.

use crate::params::{QueryParams, SharedParams};
use crate::queries::QueryType;
use crate::results::{BenchmarkConfig, BenchmarkResults, LatencyHistogram, ResourceUsage};
use anyhow::{Context, Result};
use deadpool_postgres::{Config as PoolConfig, Pool, Runtime};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::sync::Semaphore;
use tokio_postgres::NoTls;

/// Create a connection pool from a database URL.
pub fn create_pool(database_url: &str, pool_size: usize) -> Result<Pool> {
    let mut config = PoolConfig::new();

    let url = url::Url::parse(database_url).context("Invalid DATABASE_URL format")?;

    config.host = url.host_str().map(String::from);
    config.port = url.port();
    config.user = if url.username().is_empty() { None } else { Some(url.username().to_string()) };
    config.password = url.password().map(String::from);
    config.dbname = url.path().strip_prefix('/').map(String::from);

    config.pool = Some(deadpool_postgres::PoolConfig {
        max_size: pool_size,
        ..Default::default()
    });

    config.create_pool(Some(Runtime::Tokio1), NoTls).context("Failed to create connection pool")
}

/// Shared state for benchmark workers.
struct BenchmarkState {
    params: SharedParams,
    histogram: Arc<LatencyHistogram>,
    successful: AtomicU64,
    failed: AtomicU64,
    remaining: AtomicU64,
}

/// Run the benchmark.
pub async fn run_benchmark(
    database_url: &str,
    connections: usize,
    total_queries: usize,
    label: &str,
    proxy_name: &str,
    anonymize: Option<bool>,
    events_enabled: Option<bool>,
) -> Result<BenchmarkResults> {
    let pool = create_pool(database_url, connections)?;

    // Wait for pool to be ready
    let _ = pool.get().await.context("Failed to connect to database")?;

    // Load query parameters
    let params = Arc::new(QueryParams::load(&pool).await?);
    if !params.is_valid() {
        anyhow::bail!("Database has insufficient data for benchmarking");
    }

    let state = Arc::new(BenchmarkState {
        params,
        histogram: Arc::new(LatencyHistogram::new()),
        successful: AtomicU64::new(0),
        failed: AtomicU64::new(0),
        remaining: AtomicU64::new(total_queries as u64),
    });

    // Semaphore to limit concurrent queries
    let semaphore = Arc::new(Semaphore::new(connections));

    // Start resource monitoring
    let resource_samples = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let resource_samples_clone = resource_samples.clone();
    let monitor_handle = tokio::spawn(async move {
        let mut sys = System::new();
        let pid = Pid::from_u32(std::process::id());
        loop {
            sys.refresh_processes(ProcessesToUpdate::Some(&[pid]));
            if let Some(process) = sys.process(pid) {
                resource_samples_clone.lock().push((process.cpu_usage(), process.memory()));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let start = Instant::now();

    // Spawn worker tasks
    let mut handles = Vec::new();
    for _ in 0..connections {
        let pool = pool.clone();
        let state = state.clone();
        let semaphore = semaphore.clone();

        let handle = tokio::spawn(async move {
            loop {
                // Check if we have work remaining
                let prev = state.remaining.fetch_sub(1, Ordering::SeqCst);
                if prev == 0 {
                    state.remaining.fetch_add(1, Ordering::SeqCst); // Restore
                    break;
                }

                let _permit = semaphore.acquire().await.unwrap();

                // Get connection and execute query
                let query_start = Instant::now();
                let result = async {
                    let client = pool.get().await?;
                    let query_type = QueryType::random();
                    query_type.execute(&client, &state.params).await
                }
                .await;

                let elapsed = query_start.elapsed();
                state.histogram.record(elapsed);

                match result {
                    Ok(_) => {
                        state.successful.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        state.failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in handles {
        let _ = handle.await;
    }

    let duration = start.elapsed();
    monitor_handle.abort();

    // Calculate resource usage
    let samples = resource_samples.lock();
    let resource_usage = if !samples.is_empty() {
        let cpu_sum: f32 = samples.iter().map(|(cpu, _)| cpu).sum();
        let mem_sum: u64 = samples.iter().map(|(_, mem)| mem).sum();
        Some(ResourceUsage {
            cpu_percent: cpu_sum / samples.len() as f32,
            memory_mb: (mem_sum as f64 / samples.len() as f64) / (1024.0 * 1024.0),
            sample_count: samples.len(),
        })
    } else {
        None
    };

    let successful = state.successful.load(Ordering::Relaxed);
    let failed = state.failed.load(Ordering::Relaxed);
    let throughput = successful as f64 / duration.as_secs_f64();

    Ok(BenchmarkResults {
        label: label.to_string(),
        proxy: proxy_name.to_string(),
        config: BenchmarkConfig {
            connections,
            target_queries: total_queries,
            anonymize,
            events_enabled,
        },
        timestamp: chrono::Utc::now().to_rfc3339(),
        duration_secs: duration.as_secs_f64(),
        total_queries: successful + failed,
        successful_queries: successful,
        failed_queries: failed,
        throughput_qps: throughput,
        latency_us: state.histogram.percentiles(),
        resource_usage,
    })
}
