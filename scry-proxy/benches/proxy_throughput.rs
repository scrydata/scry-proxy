use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use scry::{
    config::*,
    observability::{HealthConfig, ProxyMetrics},
    proxy::*,
    publisher::*,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use testcontainers::{clients::Cli, RunnableImage};
use testcontainers_modules::postgres::Postgres;
use tokio::runtime::Runtime;

/// Simple no-op publisher for benchmarks
#[derive(Debug, Clone)]
struct NoOpPublisher;

#[async_trait::async_trait]
impl EventPublisher for NoOpPublisher {
    async fn publish_batch(&self, _events: Vec<QueryEvent>) -> anyhow::Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Publisher that tracks event count for verification
#[derive(Debug, Clone)]
struct CountingPublisher {
    count: Arc<Mutex<usize>>,
}

impl CountingPublisher {
    fn new() -> Self {
        Self { count: Arc::new(Mutex::new(0)) }
    }

    fn count(&self) -> usize {
        *self.count.lock().unwrap()
    }
}

#[async_trait::async_trait]
impl EventPublisher for CountingPublisher {
    async fn publish_batch(&self, events: Vec<QueryEvent>) -> anyhow::Result<()> {
        *self.count.lock().unwrap() += events.len();
        Ok(())
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

fn create_test_config(backend_host: String, backend_port: u16) -> Config {
    Config {
        proxy: ProxyConfig {
            listen_address: "127.0.0.1:0".to_string(),
            max_connections: 100,
            shutdown_timeout_secs: 30,
        },
        backend: BackendConfig {
            protocol: DatabaseProtocol::Postgres,
            host: backend_host,
            port: backend_port,
            database: "postgres".to_string(),
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            pool_size: 10,
            connection_timeout_ms: 5000,
        },
        observability: ObservabilityConfig {
            enable_tracing: false,
            otlp_endpoint: None,
            service_name: "scry-bench".to_string(),
            metrics_server_address: "127.0.0.1:0".to_string(),
            enable_metrics_server: false,
        },
        protocol: ProtocolConfig { max_prepared_statements: 1000 },
        publisher: PublisherConfig {
            enabled: true,
            batch_size: 100,
            flush_interval_ms: 100,
            anonymize: false,
            publisher_type: "debug".to_string(),
            max_queue_size: 10000,
            http_endpoint: None,
            http_timeout_ms: 500,
            http_max_retries: 2,
            http_api_key: None,
            http_compression: true,
            shadow_id: None,
        },
        performance: PerformanceConfig {
            target_latency_ms: 1,
            connection_pooling: PoolingStrategy::Disabled,
            pool_size: 100,
            pool_min_idle: 10,
            pool_timeout_secs: 30,
            pool_recycle_secs: 3600,
            pool_aggressive_unpinning: false,
            buffer_size: 8192,
            pool_queue_depth: 50,
            pool_idle_unpin_secs: 60,
            pool_lifo: true,
        },
        resilience: ResilienceConfig {
            circuit_breaker: CircuitBreakerConfig {
                enabled: false, // Disable for benchmarks to avoid interference
                failure_threshold: 5,
                success_threshold: 2,
                window_secs: 30,
                open_timeout_secs: 60,
                use_health_monitor: false,
            },
            connection_retry: ConnectionRetryConfig {
                enabled: false, // Disable for benchmarks
                max_attempts: 1,
                initial_backoff_ms: 50,
                max_backoff_ms: 5000,
                backoff_multiplier: 2.0,
                jitter_factor: 0.1,
            },
            healthcheck: HealthcheckConfig {
                active_enabled: false, // Disable for benchmarks
                interval_secs: 30,
                timeout_ms: 1000,
                failure_threshold: 3,
            },
        },
    }
}

async fn start_test_proxy(
    config: Config,
    publisher: Arc<dyn EventPublisher>,
) -> anyhow::Result<u16> {
    let batcher = EventBatcher::new(
        publisher,
        config.publisher.batch_size,
        config.publisher.flush_interval_ms,
        config.publisher.max_queue_size,
    );

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server = ProxyServer::new(config.clone(), batcher, metrics).await?;
    let port = server.local_addr()?.port();

    tokio::spawn(async move {
        let _ = server.run().await;
    });

    Ok(port)
}

fn benchmark_proxy_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Set up Postgres container (once for all benchmarks)
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);

    // Wait for Postgres to be ready
    std::thread::sleep(Duration::from_secs(3));

    let config = create_test_config("127.0.0.1".to_string(), postgres_port);

    // Start proxy with no-op publisher for minimal overhead
    let publisher = Arc::new(NoOpPublisher);
    let proxy_port =
        rt.block_on(start_test_proxy(config.clone(), publisher)).expect("Failed to start proxy");

    std::thread::sleep(Duration::from_millis(200));

    let mut group = c.benchmark_group("query_latency");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50); // Reduce sample size to avoid port exhaustion

    // Benchmark: Direct connection to Postgres
    group.bench_function("direct_postgres", |b| {
        // Create persistent connection outside benchmark loop
        let (client, connection) = rt
            .block_on(tokio_postgres::connect(
                &format!(
                    "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
                    postgres_port
                ),
                tokio_postgres::NoTls,
            ))
            .unwrap();

        rt.spawn(async move {
            let _ = connection.await;
        });

        b.to_async(&rt).iter(|| async {
            black_box(client.execute("SELECT 1", &[]).await.unwrap());
        });
    });

    // Benchmark: Through proxy
    group.bench_function("through_proxy", |b| {
        // Create persistent connection outside benchmark loop
        let (client, connection) = rt
            .block_on(tokio_postgres::connect(
                &format!(
                    "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
                    proxy_port
                ),
                tokio_postgres::NoTls,
            ))
            .unwrap();

        rt.spawn(async move {
            let _ = connection.await;
        });

        b.to_async(&rt).iter(|| async {
            black_box(client.execute("SELECT 1", &[]).await.unwrap());
        });
    });

    group.finish();
}

fn benchmark_query_types(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);

    std::thread::sleep(Duration::from_secs(3));

    let config = create_test_config("127.0.0.1".to_string(), postgres_port);
    let publisher = Arc::new(NoOpPublisher);
    let proxy_port =
        rt.block_on(start_test_proxy(config.clone(), publisher)).expect("Failed to start proxy");

    std::thread::sleep(Duration::from_millis(200));

    let mut group = c.benchmark_group("query_types");
    group.sample_size(50); // Reduce sample size to avoid port exhaustion

    let queries = vec![
        ("simple_select", "SELECT 1"),
        ("arithmetic", "SELECT 2 + 2"),
        ("string_concat", "SELECT 'hello' || ' world'"),
        ("current_time", "SELECT NOW()"),
    ];

    for (name, query) in queries {
        group.bench_with_input(BenchmarkId::new("proxy", name), &query, |b, &query| {
            // Create persistent connection outside benchmark loop
            let (client, connection) = rt
                .block_on(tokio_postgres::connect(
                    &format!(
                        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
                        proxy_port
                    ),
                    tokio_postgres::NoTls,
                ))
                .unwrap();

            rt.spawn(async move {
                let _ = connection.await;
            });

            b.to_async(&rt).iter(|| async {
                black_box(client.execute(query, &[]).await.unwrap());
            });
        });
    }

    group.finish();
}

fn benchmark_event_publishing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);

    std::thread::sleep(Duration::from_secs(3));

    let config = create_test_config("127.0.0.1".to_string(), postgres_port);

    let mut group = c.benchmark_group("event_publishing");
    group.sample_size(50); // Reduce sample size to avoid port exhaustion

    // Benchmark with no-op publisher
    group.bench_function("noop_publisher", |b| {
        let publisher = Arc::new(NoOpPublisher);
        let proxy_port = rt
            .block_on(start_test_proxy(config.clone(), publisher))
            .expect("Failed to start proxy");

        std::thread::sleep(Duration::from_millis(200));

        // Create persistent connection outside benchmark loop
        let (client, connection) = rt
            .block_on(tokio_postgres::connect(
                &format!(
                    "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
                    proxy_port
                ),
                tokio_postgres::NoTls,
            ))
            .unwrap();

        rt.spawn(async move {
            let _ = connection.await;
        });

        b.to_async(&rt).iter(|| async {
            black_box(client.execute("SELECT 1", &[]).await.unwrap());
        });
    });

    // Benchmark with counting publisher (minimal overhead)
    group.bench_function("counting_publisher", |b| {
        let publisher = Arc::new(CountingPublisher::new());
        let proxy_port = rt
            .block_on(start_test_proxy(config.clone(), publisher))
            .expect("Failed to start proxy");

        std::thread::sleep(Duration::from_millis(200));

        // Create persistent connection outside benchmark loop
        let (client, connection) = rt
            .block_on(tokio_postgres::connect(
                &format!(
                    "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
                    proxy_port
                ),
                tokio_postgres::NoTls,
            ))
            .unwrap();

        rt.spawn(async move {
            let _ = connection.await;
        });

        b.to_async(&rt).iter(|| async {
            black_box(client.execute("SELECT 1", &[]).await.unwrap());
        });
    });

    group.finish();
}

fn benchmark_anonymization_overhead(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);

    std::thread::sleep(Duration::from_secs(3));

    let mut group = c.benchmark_group("anonymization");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    // Benchmark with anonymization disabled
    group.bench_function("anonymization_disabled", |b| {
        let mut config = create_test_config("127.0.0.1".to_string(), postgres_port);
        config.publisher.anonymize = false;

        let publisher = Arc::new(NoOpPublisher);
        let proxy_port = rt
            .block_on(start_test_proxy(config.clone(), publisher))
            .expect("Failed to start proxy");

        std::thread::sleep(Duration::from_millis(200));

        let (client, connection) = rt
            .block_on(tokio_postgres::connect(
                &format!(
                    "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
                    proxy_port
                ),
                tokio_postgres::NoTls,
            ))
            .unwrap();

        rt.spawn(async move {
            let _ = connection.await;
        });

        b.to_async(&rt).iter(|| async {
            black_box(
                client
                    .execute("SELECT * FROM pg_catalog.pg_tables WHERE tablename = 'pg_class'", &[])
                    .await
                    .unwrap(),
            );
        });
    });

    // Benchmark with anonymization enabled
    group.bench_function("anonymization_enabled", |b| {
        let mut config = create_test_config("127.0.0.1".to_string(), postgres_port);
        config.publisher.anonymize = true;

        let publisher = Arc::new(NoOpPublisher);
        let proxy_port = rt
            .block_on(start_test_proxy(config.clone(), publisher))
            .expect("Failed to start proxy");

        std::thread::sleep(Duration::from_millis(200));

        let (client, connection) = rt
            .block_on(tokio_postgres::connect(
                &format!(
                    "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
                    proxy_port
                ),
                tokio_postgres::NoTls,
            ))
            .unwrap();

        rt.spawn(async move {
            let _ = connection.await;
        });

        b.to_async(&rt).iter(|| async {
            black_box(
                client
                    .execute("SELECT * FROM pg_catalog.pg_tables WHERE tablename = 'pg_class'", &[])
                    .await
                    .unwrap(),
            );
        });
    });

    // Benchmark with a more complex query with multiple values
    group.bench_function("anonymization_enabled_complex", |b| {
        let mut config = create_test_config("127.0.0.1".to_string(), postgres_port);
        config.publisher.anonymize = true;

        let publisher = Arc::new(NoOpPublisher);
        let proxy_port = rt
            .block_on(start_test_proxy(config.clone(), publisher))
            .expect("Failed to start proxy");

        std::thread::sleep(Duration::from_millis(200));

        let (client, connection) = rt
            .block_on(tokio_postgres::connect(
                &format!(
                    "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
                    proxy_port
                ),
                tokio_postgres::NoTls,
            ))
            .unwrap();

        rt.spawn(async move {
            let _ = connection.await;
        });

        b.to_async(&rt).iter(|| async {
            black_box(
                client
                    .execute("SELECT 1, 2, 3, 'hello', 'world', 42 WHERE 1 = 1 AND 2 = 2", &[])
                    .await
                    .unwrap(),
            );
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_proxy_latency,
    benchmark_query_types,
    benchmark_event_publishing,
    benchmark_anonymization_overhead
);
criterion_main!(benches);
