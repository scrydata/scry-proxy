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
    eprintln!();

    let results = runner::run_benchmark(
        &args.database_url,
        args.connections,
        args.queries,
        &args.label,
        &args.proxy,
        args.anonymize,
        args.events,
    )
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

    if let Some(ref usage) = results.resource_usage {
        eprintln!();
        eprintln!("Resource Usage:");
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
