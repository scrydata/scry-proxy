# Proxy Comparison Benchmarks Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a benchmark framework to compare scry-proxy against PgBouncer and PgCat, measuring latency (p50/p95/p99), throughput, and resource usage.

**Architecture:** Docker Compose spins up fresh Postgres container per run. A Rust benchmark binary generates OLTP workload (reusing query patterns from scry-platform), captures HDR histograms, and outputs JSON. Shell scripts orchestrate runs across all proxy configurations. Python script generates comparison charts.

**Tech Stack:** Rust (load driver), Docker Compose (containers), hdrhistogram (latency capture), serde_json (output), matplotlib (charts)

---

## Task 1: Create benchmarks crate structure

**Files:**
- Create: `benchmarks/Cargo.toml`
- Create: `benchmarks/src/main.rs`
- Modify: `Cargo.toml` (workspace members)

**Step 1: Add benchmarks to workspace**

In `Cargo.toml`, add "benchmarks" to workspace members:

```toml
[workspace]
members = ["scry-proxy", "benchmarks"]
resolver = "2"
```

**Step 2: Verify workspace change**

Run: `cargo check --workspace`
Expected: Should recognize new member (will fail until we create it)

**Step 3: Create benchmarks Cargo.toml**

Create `benchmarks/Cargo.toml`:

```toml
[package]
name = "scry-benchmarks"
version = "0.1.0"
edition = "2021"
publish = false

[[bin]]
name = "bench-runner"
path = "src/main.rs"

[dependencies]
tokio = { version = "1.35", features = ["full"] }
tokio-postgres = "0.7"
deadpool-postgres = "0.12"
clap = { version = "4.4", features = ["derive", "env"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
hdrhistogram = "7.5"
rand = "0.8"
anyhow = "1.0"
url = "2.5"
chrono = { version = "0.4", features = ["serde"] }
sysinfo = "0.31"
```

**Step 4: Create minimal main.rs**

Create `benchmarks/src/main.rs`:

```rust
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
```

**Step 5: Verify crate compiles**

Run: `cargo build -p scry-benchmarks`
Expected: Compiles successfully

**Step 6: Commit**

```bash
git add Cargo.toml benchmarks/
git commit -m "feat(benchmarks): add benchmark crate skeleton"
```

---

## Task 2: Copy schema and seed SQL files

**Files:**
- Create: `benchmarks/sql/01-schema.sql`
- Create: `benchmarks/sql/02-seed-data.sql`
- Create: `benchmarks/sql/03-scale-data.sql`

**Step 1: Create sql directory**

Run: `mkdir -p benchmarks/sql`

**Step 2: Copy schema from scry-platform**

Copy `scry-platform/demo/init-scripts/01-schema.sql` to `benchmarks/sql/01-schema.sql`

**Step 3: Copy seed data from scry-platform**

Copy `scry-platform/demo/init-scripts/02-seed-data.sql` to `benchmarks/sql/02-seed-data.sql`

**Step 4: Create scale-up seed script**

Create `benchmarks/sql/03-scale-data.sql`:

```sql
-- Scale up data for benchmarks: 1000 users, 1000 products, 10000 orders

-- Generate 995 more users (already have 5)
INSERT INTO users (email, name, password_hash)
SELECT
    'user' || generate_series || '@example.com',
    'User ' || generate_series,
    'hashed_password'
FROM generate_series(6, 1000);

-- Generate 990 more products (already have 10)
INSERT INTO products (sku, name, description, price, cost, stock_quantity, category, is_active)
SELECT
    'SKU-' || LPAD(generate_series::text, 6, '0'),
    'Product ' || generate_series,
    'Description for product ' || generate_series,
    (random() * 500 + 10)::numeric(10,2),
    (random() * 200 + 5)::numeric(10,2),
    (random() * 500)::int,
    (ARRAY['Electronics', 'Accessories', 'Audio', 'Office', 'Gaming'])[1 + (random() * 4)::int],
    random() > 0.1
FROM generate_series(11, 1000);

-- Generate 9992 more orders (already have 8)
INSERT INTO orders (order_number, user_id, status, total_amount, shipping_address)
SELECT
    'ORD-BENCH-' || LPAD(generate_series::text, 8, '0'),
    1 + (random() * 999)::int,
    (ARRAY['pending', 'confirmed', 'processing', 'shipped', 'delivered'])[1 + (random() * 4)::int],
    (random() * 2000 + 50)::numeric(10,2),
    generate_series || ' Benchmark St, City ' || (generate_series % 100) || ', ST ' || (10000 + generate_series % 90000)
FROM generate_series(9, 10000);

-- Generate order items (1-3 items per order)
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT
    o.id,
    1 + (random() * 999)::int,
    1 + (random() * 3)::int,
    (random() * 500 + 10)::numeric(10,2)
FROM orders o
CROSS JOIN generate_series(1, 1 + (random() * 2)::int)
WHERE o.id > 8;

-- Analyze tables for query planner
ANALYZE users;
ANALYZE products;
ANALYZE orders;
ANALYZE order_items;
```

**Step 5: Verify SQL syntax**

Run: `cat benchmarks/sql/03-scale-data.sql`
Expected: Valid SQL without syntax errors

**Step 6: Commit**

```bash
git add benchmarks/sql/
git commit -m "feat(benchmarks): add schema and seed SQL files"
```

---

## Task 3: Create Docker Compose for Postgres, PgBouncer, PgCat

**Files:**
- Create: `benchmarks/docker-compose.yml`
- Create: `benchmarks/pgbouncer/pgbouncer.ini`
- Create: `benchmarks/pgbouncer/userlist.txt`
- Create: `benchmarks/pgcat/pgcat.toml`

**Step 1: Create config directories**

Run: `mkdir -p benchmarks/pgbouncer benchmarks/pgcat`

**Step 2: Create PgBouncer config**

Create `benchmarks/pgbouncer/pgbouncer.ini`:

```ini
[databases]
postgres = host=postgres port=5432 dbname=postgres

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 6432
auth_type = md5
auth_file = /etc/pgbouncer/userlist.txt
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 20
min_pool_size = 5
reserve_pool_size = 5
reserve_pool_timeout = 3
server_lifetime = 3600
server_idle_timeout = 600
log_connections = 0
log_disconnections = 0
log_pooler_errors = 1
admin_users = postgres
stats_users = postgres
```

**Step 3: Create PgBouncer userlist**

Create `benchmarks/pgbouncer/userlist.txt`:

```
"postgres" "md5$(echo -n 'postgrespostgres' | md5sum | cut -d' ' -f1)"
```

Note: We'll generate the actual hash in the setup script.

**Step 4: Create PgCat config**

Create `benchmarks/pgcat/pgcat.toml`:

```toml
[general]
host = "0.0.0.0"
port = 6433
connect_timeout = 5000
idle_timeout = 30000
admin_username = "admin"
admin_password = "admin"

[pools.postgres]
pool_mode = "transaction"
default_role = "primary"
query_parser_enabled = false
primary_reads_enabled = true
sharding_function = "pg_bigint_hash"

[pools.postgres.users.0]
username = "postgres"
password = "postgres"
pool_size = 20
min_pool_size = 5

[pools.postgres.shards.0]
database = "postgres"
servers = [["postgres", 5432, "primary"]]
```

**Step 5: Create Docker Compose**

Create `benchmarks/docker-compose.yml`:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: postgres
    ports:
      - "5432:5432"
    volumes:
      - ./sql:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 2s
      timeout: 5s
      retries: 10

  pgbouncer:
    image: edoburu/pgbouncer:1.21.0
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      DATABASE_URL: postgres://postgres:postgres@postgres:5432/postgres
    ports:
      - "6432:6432"
    volumes:
      - ./pgbouncer/pgbouncer.ini:/etc/pgbouncer/pgbouncer.ini:ro
      - ./pgbouncer/userlist.txt:/etc/pgbouncer/userlist.txt:ro

  pgcat:
    image: ghcr.io/postgresml/pgcat:main
    depends_on:
      postgres:
        condition: service_healthy
    ports:
      - "6433:6433"
    volumes:
      - ./pgcat/pgcat.toml:/etc/pgcat/pgcat.toml:ro
    command: ["/usr/bin/pgcat", "/etc/pgcat/pgcat.toml"]

networks:
  default:
    name: scry-bench
```

**Step 6: Verify Docker Compose syntax**

Run: `cd benchmarks && docker compose config`
Expected: Valid YAML output

**Step 7: Commit**

```bash
git add benchmarks/docker-compose.yml benchmarks/pgbouncer/ benchmarks/pgcat/
git commit -m "feat(benchmarks): add Docker Compose with PgBouncer and PgCat"
```

---

## Task 4: Implement query generator module

**Files:**
- Create: `benchmarks/src/queries.rs`
- Create: `benchmarks/src/params.rs`

**Step 1: Create params module**

Create `benchmarks/src/params.rs`:

```rust
//! Parameter bootstrapping - load valid IDs from database at startup.

use anyhow::{Context, Result};
use deadpool_postgres::Pool;
use std::sync::Arc;

/// Cached parameters for query generation.
#[derive(Debug, Clone)]
pub struct QueryParams {
    pub user_ids: Vec<i32>,
    pub product_ids: Vec<i32>,
    pub order_ids: Vec<i32>,
    pub categories: Vec<String>,
}

impl QueryParams {
    /// Bootstrap parameters from the database.
    pub async fn load(pool: &Pool) -> Result<Self> {
        let client = pool.get().await.context("Failed to get connection")?;

        let user_ids: Vec<i32> = client
            .query("SELECT id FROM users ORDER BY random() LIMIT 500", &[])
            .await
            .context("Failed to load user IDs")?
            .iter()
            .map(|row| row.get("id"))
            .collect();

        let product_ids: Vec<i32> = client
            .query("SELECT id FROM products WHERE is_active = true ORDER BY random() LIMIT 500", &[])
            .await
            .context("Failed to load product IDs")?
            .iter()
            .map(|row| row.get("id"))
            .collect();

        let order_ids: Vec<i32> = client
            .query("SELECT id FROM orders ORDER BY random() LIMIT 500", &[])
            .await
            .context("Failed to load order IDs")?
            .iter()
            .map(|row| row.get("id"))
            .collect();

        let categories: Vec<String> = client
            .query("SELECT DISTINCT category FROM products WHERE category IS NOT NULL", &[])
            .await
            .context("Failed to load categories")?
            .iter()
            .map(|row| row.get("category"))
            .collect();

        Ok(Self { user_ids, product_ids, order_ids, categories })
    }

    pub fn is_valid(&self) -> bool {
        !self.user_ids.is_empty() && !self.product_ids.is_empty()
    }
}

pub type SharedParams = Arc<QueryParams>;
```

**Step 2: Create queries module**

Create `benchmarks/src/queries.rs`:

```rust
//! Query definitions for the e-commerce benchmark schema.

use anyhow::Result;
use deadpool_postgres::Object as Client;
use rand::seq::SliceRandom;
use rand::Rng;

use crate::params::QueryParams;

/// Execute a "browse products" query.
pub async fn browse_products(client: &Client, params: &QueryParams) -> Result<u64> {
    let category = params.categories.choose(&mut rand::thread_rng());
    let offset: i64 = rand::thread_rng().gen_range(0..5) * 20;

    let rows = if let Some(cat) = category {
        client
            .query(
                "SELECT id, sku, name, price, category
                 FROM products
                 WHERE is_active = true AND category = $1
                 ORDER BY created_at DESC
                 LIMIT 20 OFFSET $2",
                &[cat, &offset],
            )
            .await?
    } else {
        client
            .query(
                "SELECT id, sku, name, price, category
                 FROM products
                 WHERE is_active = true
                 ORDER BY created_at DESC
                 LIMIT 20 OFFSET $1",
                &[&offset],
            )
            .await?
    };

    Ok(rows.len() as u64)
}

/// Execute a "view product detail" query.
pub async fn view_product(client: &Client, params: &QueryParams) -> Result<u64> {
    let product_id = {
        let mut rng = rand::thread_rng();
        params.product_ids.choose(&mut rng).copied()
    };
    if let Some(product_id) = product_id {
        let rows = client
            .query("SELECT * FROM products WHERE id = $1", &[&product_id])
            .await?;
        Ok(rows.len() as u64)
    } else {
        Ok(0)
    }
}

/// Execute a "search products" query.
pub async fn search_products(client: &Client, _params: &QueryParams) -> Result<u64> {
    let search_terms = ["laptop", "mouse", "keyboard", "monitor", "headset", "wireless", "gaming", "Product"];
    let term = {
        let mut rng = rand::thread_rng();
        *search_terms.choose(&mut rng).unwrap_or(&"laptop")
    };

    let rows = client
        .query(
            "SELECT id, sku, name, price
             FROM products
             WHERE is_active = true AND name ILIKE '%' || $1 || '%'
             LIMIT 10",
            &[&term],
        )
        .await?;

    Ok(rows.len() as u64)
}

/// Execute a "check order history" query.
pub async fn order_history(client: &Client, params: &QueryParams) -> Result<u64> {
    let user_id = {
        let mut rng = rand::thread_rng();
        params.user_ids.choose(&mut rng).copied()
    };
    if let Some(user_id) = user_id {
        let rows = client
            .query(
                "SELECT o.id, o.order_number, o.status, o.total_amount, o.created_at
                 FROM orders o
                 WHERE o.user_id = $1
                 ORDER BY o.created_at DESC
                 LIMIT 5",
                &[&user_id],
            )
            .await?;
        Ok(rows.len() as u64)
    } else {
        Ok(0)
    }
}

/// Execute a "view order details" query.
pub async fn order_details(client: &Client, params: &QueryParams) -> Result<u64> {
    let order_id = {
        let mut rng = rand::thread_rng();
        params.order_ids.choose(&mut rng).copied()
    };
    if let Some(order_id) = order_id {
        let rows = client
            .query(
                "SELECT oi.*, p.name as product_name
                 FROM order_items oi
                 JOIN products p ON p.id = oi.product_id
                 WHERE oi.order_id = $1",
                &[&order_id],
            )
            .await?;
        Ok(rows.len() as u64)
    } else {
        Ok(0)
    }
}

/// Query type with associated weight for random selection.
#[derive(Debug, Clone, Copy)]
pub enum QueryType {
    BrowseProducts,
    ViewProduct,
    SearchProducts,
    OrderHistory,
    OrderDetails,
}

impl QueryType {
    /// Get all query types with their weights (must sum to 100).
    pub fn weighted_all() -> Vec<(Self, u8)> {
        vec![
            (Self::BrowseProducts, 42),
            (Self::ViewProduct, 26),
            (Self::SearchProducts, 16),
            (Self::OrderHistory, 11),
            (Self::OrderDetails, 5),
        ]
    }

    /// Select a random query type based on weights.
    pub fn random() -> Self {
        let weighted = Self::weighted_all();
        let total: u8 = weighted.iter().map(|(_, w)| w).sum();
        let mut rng = rand::thread_rng();
        let mut pick = rng.gen_range(0..total);

        for (qt, weight) in weighted {
            if pick < weight {
                return qt;
            }
            pick -= weight;
        }

        Self::BrowseProducts
    }

    /// Execute this query type and return row count.
    pub async fn execute(&self, client: &Client, params: &QueryParams) -> Result<u64> {
        match self {
            Self::BrowseProducts => browse_products(client, params).await,
            Self::ViewProduct => view_product(client, params).await,
            Self::SearchProducts => search_products(client, params).await,
            Self::OrderHistory => order_history(client, params).await,
            Self::OrderDetails => order_details(client, params).await,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::BrowseProducts => "browse_products",
            Self::ViewProduct => "view_product",
            Self::SearchProducts => "search_products",
            Self::OrderHistory => "order_history",
            Self::OrderDetails => "order_details",
        }
    }
}
```

**Step 3: Verify modules compile**

Add to `benchmarks/src/main.rs` at the top:

```rust
mod params;
mod queries;
```

Run: `cargo check -p scry-benchmarks`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add benchmarks/src/params.rs benchmarks/src/queries.rs benchmarks/src/main.rs
git commit -m "feat(benchmarks): add query generator modules"
```

---

## Task 5: Implement histogram and results module

**Files:**
- Create: `benchmarks/src/results.rs`

**Step 1: Create results module**

Create `benchmarks/src/results.rs`:

```rust
//! Benchmark results and histogram management.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Latency percentiles in microseconds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50: u64,
    pub p75: u64,
    pub p90: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
    pub min: u64,
    pub mean: f64,
    pub stddev: f64,
}

impl LatencyPercentiles {
    pub fn from_histogram(hist: &Histogram<u64>) -> Self {
        Self {
            p50: hist.value_at_quantile(0.50),
            p75: hist.value_at_quantile(0.75),
            p90: hist.value_at_quantile(0.90),
            p95: hist.value_at_quantile(0.95),
            p99: hist.value_at_quantile(0.99),
            p999: hist.value_at_quantile(0.999),
            max: hist.max(),
            min: hist.min(),
            mean: hist.mean(),
            stddev: hist.stdev(),
        }
    }
}

/// Resource usage snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub cpu_percent: f32,
    pub memory_mb: f64,
    pub sample_count: usize,
}

/// Full benchmark results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResults {
    pub label: String,
    pub proxy: String,
    pub config: BenchmarkConfig,
    pub timestamp: String,
    pub duration_secs: f64,
    pub total_queries: u64,
    pub successful_queries: u64,
    pub failed_queries: u64,
    pub throughput_qps: f64,
    pub latency_us: LatencyPercentiles,
    pub resource_usage: Option<ResourceUsage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    pub connections: usize,
    pub target_queries: usize,
    pub anonymize: Option<bool>,
    pub events_enabled: Option<bool>,
}

/// Thread-safe histogram wrapper.
pub struct LatencyHistogram {
    hist: parking_lot::Mutex<Histogram<u64>>,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        // Record latencies from 1us to 60 seconds with 3 significant figures
        let hist = Histogram::new_with_bounds(1, 60_000_000, 3)
            .expect("Failed to create histogram");
        Self { hist: parking_lot::Mutex::new(hist) }
    }

    pub fn record(&self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        let micros = micros.max(1); // Minimum 1us
        let mut hist = self.hist.lock();
        let _ = hist.record(micros);
    }

    pub fn percentiles(&self) -> LatencyPercentiles {
        let hist = self.hist.lock();
        LatencyPercentiles::from_histogram(&hist)
    }

    pub fn count(&self) -> u64 {
        self.hist.lock().len()
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}
```

**Step 2: Add parking_lot dependency**

In `benchmarks/Cargo.toml`, add:

```toml
parking_lot = "0.12"
```

**Step 3: Add module to main.rs**

Add to `benchmarks/src/main.rs`:

```rust
mod results;
```

**Step 4: Verify compiles**

Run: `cargo check -p scry-benchmarks`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add benchmarks/src/results.rs benchmarks/Cargo.toml benchmarks/src/main.rs
git commit -m "feat(benchmarks): add latency histogram and results module"
```

---

## Task 6: Implement benchmark runner core

**Files:**
- Create: `benchmarks/src/runner.rs`
- Modify: `benchmarks/src/main.rs`

**Step 1: Create runner module**

Create `benchmarks/src/runner.rs`:

```rust
//! Benchmark execution engine.

use crate::params::{QueryParams, SharedParams};
use crate::queries::QueryType;
use crate::results::{BenchmarkConfig, BenchmarkResults, LatencyHistogram, ResourceUsage};
use anyhow::{Context, Result};
use deadpool_postgres::{Config as PoolConfig, Pool, Runtime};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Pid, System};
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
            sys.refresh_process(pid);
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
```

**Step 2: Update main.rs with full implementation**

Replace `benchmarks/src/main.rs`:

```rust
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
```

**Step 3: Verify compiles**

Run: `cargo build -p scry-benchmarks`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add benchmarks/src/runner.rs benchmarks/src/main.rs
git commit -m "feat(benchmarks): implement benchmark runner with histogram capture"
```

---

## Task 7: Create orchestration shell script

**Files:**
- Create: `benchmarks/run-comparison.sh`

**Step 1: Create orchestration script**

Create `benchmarks/run-comparison.sh`:

```bash
#!/bin/bash
set -e

# Configuration
QUERIES=${QUERIES:-100000}
RESULTS_DIR="results/$(date +%Y%m%d-%H%M%S)"
CONNECTION_COUNTS="1 10 50 100 200"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Create results directory
mkdir -p "$RESULTS_DIR"
log "Results will be saved to: $RESULTS_DIR"

# Build the benchmark runner
log "Building benchmark runner..."
cargo build --release -p scry-benchmarks

BENCH_BIN="./target/release/bench-runner"

# Function to run a single benchmark
run_benchmark() {
    local label=$1
    local proxy=$2
    local url=$3
    local connections=$4
    local anonymize=$5
    local events=$6

    local output_file="$RESULTS_DIR/${label}-${connections}conn.json"

    log "Running: $label with $connections connections"

    local extra_args=""
    [ -n "$anonymize" ] && extra_args="$extra_args --anonymize=$anonymize"
    [ -n "$events" ] && extra_args="$extra_args --events=$events"

    $BENCH_BIN \
        --database-url "$url" \
        --connections "$connections" \
        --queries "$QUERIES" \
        --label "$label" \
        --proxy "$proxy" \
        --output "$output_file" \
        $extra_args
}

# Function to start fresh postgres and wait for it
start_fresh_postgres() {
    log "Starting fresh Postgres container..."
    docker compose down -v 2>/dev/null || true
    docker compose up -d postgres

    # Wait for postgres to be ready
    log "Waiting for Postgres to be ready..."
    for i in {1..30}; do
        if docker compose exec -T postgres pg_isready -U postgres >/dev/null 2>&1; then
            log "Postgres is ready"
            return 0
        fi
        sleep 1
    done
    error "Postgres failed to start"
    exit 1
}

# Function to start a proxy
start_proxy() {
    local proxy=$1
    log "Starting $proxy..."
    docker compose up -d "$proxy"
    sleep 3  # Give proxy time to connect to postgres
}

# Test matrix
run_all_benchmarks() {
    for conns in $CONNECTION_COUNTS; do
        # Direct Postgres (baseline)
        start_fresh_postgres
        run_benchmark "direct" "postgres" "postgres://postgres:postgres@localhost:5432/postgres" "$conns"

        # PgBouncer (transaction mode)
        start_fresh_postgres
        start_proxy pgbouncer
        run_benchmark "pgbouncer" "pgbouncer" "postgres://postgres:postgres@localhost:6432/postgres" "$conns"
        docker compose stop pgbouncer

        # PgCat (no sharding)
        start_fresh_postgres
        start_proxy pgcat
        run_benchmark "pgcat" "pgcat" "postgres://postgres:postgres@localhost:6433/postgres" "$conns"
        docker compose stop pgcat

        # Scry: anon=off, events=off
        start_fresh_postgres
        # TODO: Start scry-proxy with appropriate config
        # For now, skip scry tests - they require scry-proxy to be running
        warn "Scry benchmarks require scry-proxy to be running externally"

        # Cleanup
        docker compose down -v
    done
}

# Main
cd "$(dirname "$0")"

log "=== Proxy Comparison Benchmark ==="
log "Queries per run: $QUERIES"
log "Connection counts: $CONNECTION_COUNTS"
log ""

run_all_benchmarks

log ""
log "=== Benchmark Complete ==="
log "Results saved to: $RESULTS_DIR"
log ""
log "Generate charts with: python3 generate_charts.py $RESULTS_DIR"
```

**Step 2: Make script executable**

Run: `chmod +x benchmarks/run-comparison.sh`

**Step 3: Commit**

```bash
git add benchmarks/run-comparison.sh
git commit -m "feat(benchmarks): add orchestration shell script"
```

---

## Task 8: Create chart generation script

**Files:**
- Create: `benchmarks/generate_charts.py`
- Create: `benchmarks/requirements.txt`

**Step 1: Create requirements.txt**

Create `benchmarks/requirements.txt`:

```
matplotlib>=3.7.0
numpy>=1.24.0
```

**Step 2: Create chart generation script**

Create `benchmarks/generate_charts.py`:

```python
#!/usr/bin/env python3
"""Generate comparison charts from benchmark results."""

import json
import sys
from pathlib import Path
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np


def load_results(results_dir: Path) -> list[dict]:
    """Load all JSON result files from a directory."""
    results = []
    for path in results_dir.glob("*.json"):
        with open(path) as f:
            results.append(json.load(f))
    return results


def group_by_connections(results: list[dict]) -> dict[int, list[dict]]:
    """Group results by connection count."""
    grouped = defaultdict(list)
    for r in results:
        conn = r["config"]["connections"]
        grouped[conn].append(r)
    return dict(sorted(grouped.items()))


def group_by_proxy(results: list[dict]) -> dict[str, list[dict]]:
    """Group results by proxy name."""
    grouped = defaultdict(list)
    for r in results:
        grouped[r["label"]].append(r)
    return grouped


def plot_latency_comparison(results: list[dict], output_path: Path):
    """Generate latency percentile comparison bar chart."""
    # Group by label, pick a representative connection count (50)
    by_proxy = group_by_proxy(results)

    labels = []
    p50s = []
    p95s = []
    p99s = []

    for proxy, proxy_results in sorted(by_proxy.items()):
        # Find result with 50 connections, or closest
        target = 50
        closest = min(proxy_results, key=lambda r: abs(r["config"]["connections"] - target))
        labels.append(proxy)
        p50s.append(closest["latency_us"]["p50"])
        p95s.append(closest["latency_us"]["p95"])
        p99s.append(closest["latency_us"]["p99"])

    x = np.arange(len(labels))
    width = 0.25

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x - width, p50s, width, label='p50', color='#2ecc71')
    ax.bar(x, p95s, width, label='p95', color='#f39c12')
    ax.bar(x + width, p99s, width, label='p99', color='#e74c3c')

    ax.set_ylabel('Latency (microseconds)')
    ax.set_title('Latency Comparison (50 connections)')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path / "latency_comparison.png", dpi=150)
    plt.close()
    print(f"Generated: {output_path / 'latency_comparison.png'}")


def plot_latency_vs_connections(results: list[dict], output_path: Path):
    """Generate latency vs connection count line chart."""
    by_proxy = group_by_proxy(results)

    fig, ax = plt.subplots(figsize=(12, 6))

    colors = plt.cm.tab10(np.linspace(0, 1, len(by_proxy)))

    for (proxy, proxy_results), color in zip(sorted(by_proxy.items()), colors):
        sorted_results = sorted(proxy_results, key=lambda r: r["config"]["connections"])
        conns = [r["config"]["connections"] for r in sorted_results]
        p99s = [r["latency_us"]["p99"] for r in sorted_results]
        ax.plot(conns, p99s, marker='o', label=proxy, color=color, linewidth=2)

    ax.set_xlabel('Concurrent Connections')
    ax.set_ylabel('p99 Latency (microseconds)')
    ax.set_title('p99 Latency vs Connection Count')
    ax.legend()
    ax.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path / "latency_vs_connections.png", dpi=150)
    plt.close()
    print(f"Generated: {output_path / 'latency_vs_connections.png'}")


def plot_throughput_comparison(results: list[dict], output_path: Path):
    """Generate throughput comparison bar chart."""
    by_proxy = group_by_proxy(results)

    labels = []
    throughputs = []

    for proxy, proxy_results in sorted(by_proxy.items()):
        target = 50
        closest = min(proxy_results, key=lambda r: abs(r["config"]["connections"] - target))
        labels.append(proxy)
        throughputs.append(closest["throughput_qps"])

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(labels, throughputs, color='#3498db')

    # Add value labels on bars
    for bar, val in zip(bars, throughputs):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
                f'{val:.0f}', ha='center', va='bottom', fontsize=9)

    ax.set_ylabel('Throughput (queries/second)')
    ax.set_title('Throughput Comparison (50 connections)')
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path / "throughput_comparison.png", dpi=150)
    plt.close()
    print(f"Generated: {output_path / 'throughput_comparison.png'}")


def plot_throughput_vs_connections(results: list[dict], output_path: Path):
    """Generate throughput vs connection count line chart."""
    by_proxy = group_by_proxy(results)

    fig, ax = plt.subplots(figsize=(12, 6))

    colors = plt.cm.tab10(np.linspace(0, 1, len(by_proxy)))

    for (proxy, proxy_results), color in zip(sorted(by_proxy.items()), colors):
        sorted_results = sorted(proxy_results, key=lambda r: r["config"]["connections"])
        conns = [r["config"]["connections"] for r in sorted_results]
        throughputs = [r["throughput_qps"] for r in sorted_results]
        ax.plot(conns, throughputs, marker='o', label=proxy, color=color, linewidth=2)

    ax.set_xlabel('Concurrent Connections')
    ax.set_ylabel('Throughput (queries/second)')
    ax.set_title('Throughput vs Connection Count')
    ax.legend()
    ax.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path / "throughput_vs_connections.png", dpi=150)
    plt.close()
    print(f"Generated: {output_path / 'throughput_vs_connections.png'}")


def generate_summary_table(results: list[dict], output_path: Path):
    """Generate a markdown summary table."""
    by_conn = group_by_connections(results)

    lines = ["# Benchmark Results Summary\n"]

    for conn, conn_results in by_conn.items():
        lines.append(f"\n## {conn} Connections\n")
        lines.append("| Proxy | p50 (μs) | p95 (μs) | p99 (μs) | Throughput (qps) |")
        lines.append("|-------|----------|----------|----------|------------------|")

        for r in sorted(conn_results, key=lambda x: x["label"]):
            lines.append(
                f"| {r['label']} | {r['latency_us']['p50']} | "
                f"{r['latency_us']['p95']} | {r['latency_us']['p99']} | "
                f"{r['throughput_qps']:.0f} |"
            )

    summary_path = output_path / "SUMMARY.md"
    with open(summary_path, "w") as f:
        f.write("\n".join(lines))
    print(f"Generated: {summary_path}")


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <results_directory>")
        sys.exit(1)

    results_dir = Path(sys.argv[1])
    if not results_dir.exists():
        print(f"Error: Directory not found: {results_dir}")
        sys.exit(1)

    print(f"Loading results from: {results_dir}")
    results = load_results(results_dir)

    if not results:
        print("No results found!")
        sys.exit(1)

    print(f"Found {len(results)} result files")

    # Generate all charts
    plot_latency_comparison(results, results_dir)
    plot_latency_vs_connections(results, results_dir)
    plot_throughput_comparison(results, results_dir)
    plot_throughput_vs_connections(results, results_dir)
    generate_summary_table(results, results_dir)

    print("\nAll charts generated successfully!")


if __name__ == "__main__":
    main()
```

**Step 3: Make script executable**

Run: `chmod +x benchmarks/generate_charts.py`

**Step 4: Commit**

```bash
git add benchmarks/generate_charts.py benchmarks/requirements.txt
git commit -m "feat(benchmarks): add Python chart generation script"
```

---

## Task 9: Fix PgBouncer userlist hash generation

**Files:**
- Modify: `benchmarks/pgbouncer/userlist.txt`
- Create: `benchmarks/setup-pgbouncer.sh`

**Step 1: Create proper userlist**

Replace `benchmarks/pgbouncer/userlist.txt`:

```
"postgres" "md53175bce1d3201d16594cebf9d7eb3f9d"
```

Note: This is the MD5 hash of "postgrespostgres" (password + username).

**Step 2: Create setup script for regenerating hash**

Create `benchmarks/setup-pgbouncer.sh`:

```bash
#!/bin/bash
# Generate PgBouncer userlist.txt with proper MD5 hash

USER="postgres"
PASS="postgres"

# MD5 hash is: md5 + md5(password + username)
HASH=$(echo -n "${PASS}${USER}" | md5sum | cut -d' ' -f1)

echo "\"${USER}\" \"md5${HASH}\"" > "$(dirname "$0")/pgbouncer/userlist.txt"
echo "Generated userlist.txt with hash for user: ${USER}"
```

**Step 3: Make executable and run**

Run: `chmod +x benchmarks/setup-pgbouncer.sh && ./benchmarks/setup-pgbouncer.sh`

**Step 4: Commit**

```bash
git add benchmarks/pgbouncer/userlist.txt benchmarks/setup-pgbouncer.sh
git commit -m "fix(benchmarks): add proper PgBouncer MD5 password hash"
```

---

## Task 10: Add justfile commands for benchmarks

**Files:**
- Modify: `justfile`

**Step 1: Add benchmark commands to justfile**

Add to `justfile`:

```just
# ============================================
# Benchmark Commands
# ============================================

# Build the benchmark runner
bench-build:
    cargo build --release -p scry-benchmarks

# Run a quick benchmark against direct Postgres
bench-quick:
    cd benchmarks; docker compose up -d postgres
    Start-Sleep -Seconds 5
    ./target/release/bench-runner --database-url "postgres://postgres:postgres@localhost:5432/postgres" --connections 10 --queries 1000 --label "quick-test" --proxy "postgres" --output "benchmarks/quick-test.json"
    cd benchmarks; docker compose down -v

# Run full comparison benchmark suite
bench-full:
    cd benchmarks; bash run-comparison.sh

# Generate charts from benchmark results
bench-charts RESULTS_DIR:
    python3 benchmarks/generate_charts.py {{RESULTS_DIR}}

# Clean up benchmark containers
bench-clean:
    cd benchmarks; docker compose down -v
```

**Step 2: Verify justfile syntax**

Run: `just --list`
Expected: Shows new benchmark commands

**Step 3: Commit**

```bash
git add justfile
git commit -m "feat(benchmarks): add just commands for benchmark workflow"
```

---

## Task 11: Add .gitignore for benchmark results

**Files:**
- Create: `benchmarks/.gitignore`

**Step 1: Create .gitignore**

Create `benchmarks/.gitignore`:

```
# Benchmark results (generated)
results/
*.json

# Python
__pycache__/
*.pyc
.venv/
venv/

# Charts (generated)
*.png
*.svg

# Temporary files
*.tmp
```

**Step 2: Commit**

```bash
git add benchmarks/.gitignore
git commit -m "chore(benchmarks): add .gitignore for generated files"
```

---

## Task 12: Create README for benchmarks

**Files:**
- Create: `benchmarks/README.md`

**Step 1: Create README**

Create `benchmarks/README.md`:

```markdown
# Proxy Comparison Benchmarks

Performance comparison framework for scry-proxy against PgBouncer and PgCat.

## Quick Start

```bash
# Build the benchmark runner
just bench-build

# Run a quick sanity check
just bench-quick

# Run full comparison suite
just bench-full

# Generate charts from results
just bench-charts benchmarks/results/YYYYMMDD-HHMMSS
```

## Test Matrix

| Proxy | Configuration |
|-------|---------------|
| Direct Postgres | Baseline (no proxy) |
| PgBouncer | Transaction pooling mode |
| PgCat | Sharding disabled |
| Scry | anon=off, events=off |
| Scry | anon=off, events=on |
| Scry | anon=on, events=off |
| Scry | anon=on, events=on |

## Metrics Captured

- **Latency**: p50, p75, p90, p95, p99, p99.9, min, max, mean, stddev (microseconds)
- **Throughput**: Queries per second
- **Resource Usage**: CPU %, Memory MB (sampled every 100ms)

## Workload

OLTP e-commerce workload with weighted query distribution:
- 42% Browse products (pagination)
- 26% View product detail (point lookup)
- 16% Search products (ILIKE)
- 11% Order history (user-specific)
- 5% Order details (JOIN)

Data volume: ~1000 users, ~1000 products, ~10000 orders

## Running Individual Benchmarks

```bash
# Direct to Postgres
./target/release/bench-runner \
  --database-url "postgres://postgres:postgres@localhost:5432/postgres" \
  --connections 50 \
  --queries 100000 \
  --label "direct" \
  --proxy "postgres"

# Through PgBouncer
./target/release/bench-runner \
  --database-url "postgres://postgres:postgres@localhost:6432/postgres" \
  --connections 50 \
  --queries 100000 \
  --label "pgbouncer" \
  --proxy "pgbouncer"

# Through scry-proxy
./target/release/bench-runner \
  --database-url "postgres://postgres:postgres@localhost:5433/postgres" \
  --connections 50 \
  --queries 100000 \
  --label "scry-anon-on-events-on" \
  --proxy "scry" \
  --anonymize true \
  --events true
```

## Docker Compose Services

```bash
cd benchmarks

# Start just Postgres
docker compose up -d postgres

# Start PgBouncer (requires Postgres)
docker compose up -d pgbouncer

# Start PgCat (requires Postgres)
docker compose up -d pgcat

# Tear down everything
docker compose down -v
```

## Output Format

Results are saved as JSON:

```json
{
  "label": "pgbouncer",
  "proxy": "pgbouncer",
  "config": {
    "connections": 50,
    "target_queries": 100000
  },
  "duration_secs": 45.2,
  "throughput_qps": 2212.4,
  "latency_us": {
    "p50": 412,
    "p95": 1230,
    "p99": 2100,
    "max": 15400
  }
}
```

## Generating Charts

```bash
# Install Python dependencies
pip install -r benchmarks/requirements.txt

# Generate charts
python3 benchmarks/generate_charts.py benchmarks/results/YYYYMMDD-HHMMSS
```

Generated charts:
- `latency_comparison.png` - Bar chart of p50/p95/p99 by proxy
- `latency_vs_connections.png` - Line chart of p99 across connection counts
- `throughput_comparison.png` - Bar chart of QPS by proxy
- `throughput_vs_connections.png` - Line chart of QPS across connection counts
- `SUMMARY.md` - Markdown table of all results
```

**Step 2: Commit**

```bash
git add benchmarks/README.md
git commit -m "docs(benchmarks): add README with usage instructions"
```

---

## Task 13: Integration test - verify benchmark runs

**Step 1: Build everything**

Run: `cargo build --release -p scry-benchmarks`
Expected: Compiles successfully

**Step 2: Start Postgres**

Run: `cd benchmarks && docker compose up -d postgres`
Expected: Postgres container starts

**Step 3: Wait for Postgres and verify data**

Run: `sleep 10 && docker compose exec postgres psql -U postgres -c "SELECT COUNT(*) FROM users;"`
Expected: Shows count (should be 1000+ after init scripts run)

**Step 4: Run quick benchmark**

Run: `./target/release/bench-runner --database-url "postgres://postgres:postgres@localhost:5432/postgres" --connections 5 --queries 1000 --label "integration-test" --proxy "postgres" --output "benchmarks/integration-test.json"`
Expected: Completes and shows results

**Step 5: Verify JSON output**

Run: `cat benchmarks/integration-test.json | head -20`
Expected: Valid JSON with latency metrics

**Step 6: Clean up**

Run: `cd benchmarks && docker compose down -v && rm -f integration-test.json`

**Step 7: Commit if any fixes were needed**

```bash
git add -A
git commit -m "fix(benchmarks): integration test fixes" --allow-empty
```

---

Plan complete and saved to `docs/plans/2026-01-18-proxy-comparison-benchmarks.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

Which approach?
