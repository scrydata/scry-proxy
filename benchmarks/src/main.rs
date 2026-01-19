mod params;
mod queries;
mod results;

use clap::Parser;

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

    /// Label for this benchmark run (e.g., "scry-anon-on")
    #[arg(long)]
    label: String,
}

fn main() {
    let args = Args::parse();
    println!("Benchmark runner - label: {}", args.label);
    println!("Target: {}", args.database_url);
    println!("Connections: {}, Queries: {}", args.connections, args.queries);
}
