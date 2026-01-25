//! Benchmark execution engine.

use crate::params::{QueryParams, SharedParams};
use crate::queries::QueryType;
use crate::results::{BenchmarkConfig, BenchmarkResults, LatencyHistogram, ResourceUsage};
use anyhow::{Context, Result};
use deadpool_postgres::{Config as PoolConfig, Pool, Runtime};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio_postgres::NoTls;

/// Configuration for running a benchmark.
pub struct RunConfig<'a> {
    pub database_url: &'a str,
    pub connections: usize,
    pub total_queries: usize,
    pub label: &'a str,
    pub proxy_name: &'a str,
    pub anonymize: Option<bool>,
    pub events_enabled: Option<bool>,
    pub proxy_container: Option<&'a str>,
    pub postgres_container: Option<&'a str>,
}

/// Create a connection pool from a database URL.
pub fn create_pool(database_url: &str, pool_size: usize) -> Result<Pool> {
    let mut config = PoolConfig::new();

    let url = url::Url::parse(database_url).context("Invalid DATABASE_URL format")?;

    config.host = url.host_str().map(String::from);
    config.port = url.port();
    config.user = if url.username().is_empty() { None } else { Some(url.username().to_string()) };
    config.password = url.password().map(String::from);
    config.dbname = url.path().strip_prefix('/').map(String::from);

    config.pool = Some(deadpool_postgres::PoolConfig { max_size: pool_size, ..Default::default() });

    // Disable statement caching for compatibility with transaction-mode connection poolers
    // (PgBouncer, PgCat). Statement caching doesn't work when the backend connection
    // can change between transactions.
    config.manager = Some(deadpool_postgres::ManagerConfig {
        recycling_method: deadpool_postgres::RecyclingMethod::Fast,
    });

    config.create_pool(Some(Runtime::Tokio1), NoTls).context("Failed to create connection pool")
}

/// Docker stats response from `docker stats --format json`
#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerStats {
    #[serde(rename = "CPUPerc")]
    cpu_perc: String,
    #[serde(rename = "MemUsage")]
    mem_usage: String,
}

/// Parse CPU percentage from Docker stats output (e.g., "0.50%" -> 0.5)
fn parse_cpu_percent(cpu_str: &str) -> f32 {
    cpu_str.trim_end_matches('%').parse().unwrap_or(0.0)
}

/// Parse memory usage from Docker stats output (e.g., "10.5MiB / 16GiB" -> bytes)
fn parse_memory_bytes(mem_str: &str) -> u64 {
    // Format: "10.5MiB / 16GiB" - we want the first part
    let usage_part = mem_str.split('/').next().unwrap_or("0").trim();

    // Parse value and unit (e.g., "10.5MiB")
    let (value_str, unit) = if let Some(stripped) = usage_part.strip_suffix("GiB") {
        (stripped, "GiB")
    } else if let Some(stripped) = usage_part.strip_suffix("MiB") {
        (stripped, "MiB")
    } else if let Some(stripped) = usage_part.strip_suffix("KiB") {
        (stripped, "KiB")
    } else if let Some(stripped) = usage_part.strip_suffix("B") {
        (stripped, "B")
    } else {
        (usage_part, "B")
    };

    let value: f64 = value_str.parse().unwrap_or(0.0);

    match unit {
        "GiB" => (value * 1024.0 * 1024.0 * 1024.0) as u64,
        "MiB" => (value * 1024.0 * 1024.0) as u64,
        "KiB" => (value * 1024.0) as u64,
        _ => value as u64,
    }
}

/// Get current stats for a Docker container using the Docker CLI (blocking).
fn get_docker_stats_blocking(container_name: &str) -> Option<(f32, u64)> {
    let output = Command::new("docker")
        .args(["stats", "--no-stream", "--format", "{{json .}}", container_name])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stats: DockerStats = serde_json::from_str(stdout.trim()).ok()?;

    let cpu_percent = parse_cpu_percent(&stats.cpu_perc);
    let memory_bytes = parse_memory_bytes(&stats.mem_usage);

    Some((cpu_percent, memory_bytes))
}

/// Get current stats for a Docker container (async wrapper).
async fn get_docker_stats(container_name: String) -> Option<(f32, u64)> {
    tokio::task::spawn_blocking(move || get_docker_stats_blocking(&container_name))
        .await
        .ok()
        .flatten()
}

/// Monitor a Docker container's resource usage using Docker CLI.
async fn monitor_docker_container(
    container_name: String,
    samples: Arc<parking_lot::Mutex<Vec<(f32, u64)>>>,
) {
    loop {
        let name = container_name.clone();
        if let Some((cpu, mem)) = get_docker_stats(name).await {
            samples.lock().push((cpu, mem));
        }
        // Small sleep between polls (docker stats takes ~1s anyway)
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Shared state for benchmark workers.
struct BenchmarkState {
    params: SharedParams,
    histogram: Arc<LatencyHistogram>,
    successful: AtomicU64,
    failed: AtomicU64,
    remaining: AtomicU64,
}

/// Helper to calculate resource usage from samples
fn calculate_resource_usage(samples: &[(f32, u64)]) -> Option<ResourceUsage> {
    if samples.is_empty() {
        return None;
    }
    let cpu_sum: f32 = samples.iter().map(|(cpu, _)| cpu).sum();
    let mem_sum: u64 = samples.iter().map(|(_, mem)| mem).sum();
    Some(ResourceUsage {
        cpu_percent: cpu_sum / samples.len() as f32,
        memory_mb: (mem_sum as f64 / samples.len() as f64) / (1024.0 * 1024.0),
        sample_count: samples.len(),
    })
}

/// Run the benchmark.
pub async fn run_benchmark(config: RunConfig<'_>) -> Result<BenchmarkResults> {
    let pool = create_pool(config.database_url, config.connections)?;

    // Wait for database to be ready with retries
    let mut attempts = 0;
    let max_attempts = 30;
    loop {
        match pool.get().await {
            Ok(client) => {
                // Verify the connection works
                if client.query_one("SELECT 1", &[]).await.is_ok() {
                    break;
                }
            }
            Err(e) => {
                attempts += 1;
                if attempts >= max_attempts {
                    anyhow::bail!(
                        "Failed to connect to database after {} attempts: {}",
                        max_attempts,
                        e
                    );
                }
                eprintln!("Waiting for database... (attempt {}/{})", attempts, max_attempts);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

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
        remaining: AtomicU64::new(config.total_queries as u64),
    });

    // Semaphore to limit concurrent queries
    let semaphore = Arc::new(Semaphore::new(config.connections));

    // Start proxy container monitoring (if specified)
    let proxy_samples = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let proxy_monitor_handle = config.proxy_container.map(|name| {
        let container_name = name.to_string();
        let samples = proxy_samples.clone();
        tokio::spawn(async move {
            monitor_docker_container(container_name, samples).await;
        })
    });

    // Start postgres container monitoring (if specified)
    let postgres_samples = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let postgres_monitor_handle = config.postgres_container.map(|name| {
        let container_name = name.to_string();
        let samples = postgres_samples.clone();
        tokio::spawn(async move {
            monitor_docker_container(container_name, samples).await;
        })
    });

    let start = Instant::now();

    // Spawn worker tasks
    let mut handles = Vec::new();
    for _ in 0..config.connections {
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

    // Stop monitoring tasks
    if let Some(handle) = proxy_monitor_handle {
        handle.abort();
    }
    if let Some(handle) = postgres_monitor_handle {
        handle.abort();
    }

    // Calculate resource usage for both containers
    let proxy_resource_usage = calculate_resource_usage(&proxy_samples.lock());
    let postgres_resource_usage = calculate_resource_usage(&postgres_samples.lock());

    let successful = state.successful.load(Ordering::Relaxed);
    let failed = state.failed.load(Ordering::Relaxed);
    let throughput = successful as f64 / duration.as_secs_f64();

    Ok(BenchmarkResults {
        label: config.label.to_string(),
        proxy: config.proxy_name.to_string(),
        config: BenchmarkConfig {
            connections: config.connections,
            target_queries: config.total_queries,
            anonymize: config.anonymize,
            events_enabled: config.events_enabled,
        },
        timestamp: chrono::Utc::now().to_rfc3339(),
        duration_secs: duration.as_secs_f64(),
        total_queries: successful + failed,
        successful_queries: successful,
        failed_queries: failed,
        throughput_qps: throughput,
        latency_us: state.histogram.percentiles(),
        proxy_resource_usage,
        postgres_resource_usage,
    })
}
