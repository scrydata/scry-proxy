mod params;
mod queries;
mod results;
mod runner;

use anyhow::Result;
use clap::Parser;
use std::fs;

#[derive(Parser)]
#[command(name = "bench-runner")]
#[command(about = "Benchmark runner for proxy comparison")]
struct Args {
    /// Database URL to benchmark against
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Number of concurrent connections
    #[arg(long, default_value = "10")]
    connections: usize,

    /// Total number of queries to execute
    #[arg(long, default_value = "10000")]
    queries: usize,

    /// Output file for JSON results
    #[arg(long, default_value = "results.json")]
    output: String,

    /// Label for this benchmark run (e.g., "scry-anon-on-events-on")
    #[arg(long)]
    label: String,

    /// Proxy name (scry, pgbouncer, pgcat)
    #[arg(long, default_value = "unknown")]
    proxy: String,

    /// Whether anonymization is enabled (for scry)
    #[arg(long)]
    anonymize: Option<bool>,

    /// Whether event publishing is enabled (for scry)
    #[arg(long)]
    events: Option<bool>,

    /// Docker container name for proxy to monitor for resource usage
    #[arg(long)]
    proxy_container: Option<String>,

    /// Docker container name for postgres to monitor for resource usage
    #[arg(long)]
    postgres_container: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    eprintln!("=== Benchmark Runner ===");
    eprintln!("Label: {}", args.label);
    eprintln!("Proxy: {}", args.proxy);
    eprintln!("Target: {}", args.database_url);
    eprintln!("Connections: {}", args.connections);
    eprintln!("Queries: {}", args.queries);
    if let Some(ref container) = args.proxy_container {
        eprintln!("Proxy Container: {}", container);
    }
    if let Some(ref container) = args.postgres_container {
        eprintln!("Postgres Container: {}", container);
    }
    eprintln!();

    let results = runner::run_benchmark(runner::RunConfig {
        database_url: &args.database_url,
        connections: args.connections,
        total_queries: args.queries,
        label: &args.label,
        proxy_name: &args.proxy,
        anonymize: args.anonymize,
        events_enabled: args.events,
        proxy_container: args.proxy_container.as_deref(),
        postgres_container: args.postgres_container.as_deref(),
    })
    .await?;

    eprintln!("=== Results ===");
    eprintln!("Duration: {:.2}s", results.duration_secs);
    eprintln!("Throughput: {:.2} qps", results.throughput_qps);
    eprintln!("Success: {} / Failed: {}", results.successful_queries, results.failed_queries);
    eprintln!();
    eprintln!("Latency (microseconds):");
    eprintln!("  p50:  {:>8}", results.latency_us.p50);
    eprintln!("  p95:  {:>8}", results.latency_us.p95);
    eprintln!("  p99:  {:>8}", results.latency_us.p99);
    eprintln!("  max:  {:>8}", results.latency_us.max);

    if let Some(ref usage) = results.proxy_resource_usage {
        eprintln!();
        eprintln!("Proxy Resource Usage:");
        eprintln!("  CPU:    {:.1}%", usage.cpu_percent);
        eprintln!("  Memory: {:.1} MB", usage.memory_mb);
    }

    if let Some(ref usage) = results.postgres_resource_usage {
        eprintln!();
        eprintln!("Postgres Resource Usage:");
        eprintln!("  CPU:    {:.1}%", usage.cpu_percent);
        eprintln!("  Memory: {:.1} MB", usage.memory_mb);
    }

    // Write JSON output
    let json = serde_json::to_string_pretty(&results)?;
    fs::write(&args.output, &json)?;
    eprintln!();
    eprintln!("Results written to: {}", args.output);

    Ok(())
}
