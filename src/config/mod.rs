use serde::{Deserialize, Serialize};

/// Supported database protocols
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseProtocol {
    /// PostgreSQL (and compatible databases like CockroachDB)
    Postgres,
    /// MySQL (and compatible databases like MariaDB) - Future
    #[cfg(feature = "mysql")]
    Mysql,
    /// MongoDB - Future
    #[cfg(feature = "mongodb")]
    Mongodb,
}

impl DatabaseProtocol {
    /// Get the protocol name as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            DatabaseProtocol::Postgres => "postgres",
            #[cfg(feature = "mysql")]
            DatabaseProtocol::Mysql => "mysql",
            #[cfg(feature = "mongodb")]
            DatabaseProtocol::Mongodb => "mongodb",
        }
    }
}

/// Configuration for the Scry proxy
///
/// Supports loading from:
/// - config.toml file
/// - Environment variables (12-factor app style)
/// - Command line arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub proxy: ProxyConfig,
    pub backend: BackendConfig,
    pub observability: ObservabilityConfig,
    pub publisher: PublisherConfig,
    pub performance: PerformanceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    pub listen_address: String,
    pub max_connections: usize,
    /// How long to wait for connections to drain during shutdown (seconds)
    pub shutdown_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendConfig {
    /// Database protocol to use
    pub protocol: DatabaseProtocol,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub pool_size: usize,
    pub connection_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    pub enable_tracing: bool,
    pub otlp_endpoint: Option<String>,
    pub service_name: String,
    pub metrics_server_address: String,
    pub enable_metrics_server: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublisherConfig {
    pub enabled: bool,
    pub batch_size: usize,
    pub flush_interval_ms: u64,
    pub anonymize: bool,

    // Publisher type: "debug" or "http"
    pub publisher_type: String,

    // Max events to queue before dropping (memory safety)
    // Uses ring buffer semantics: drops oldest events when full
    pub max_queue_size: usize,

    // HTTP publisher settings (only used when publisher_type = "http")
    pub http_endpoint: Option<String>,
    pub http_timeout_ms: u64,
    pub http_max_retries: u32,
    pub http_api_key: Option<String>,
    pub http_compression: bool,
}

/// Connection pooling strategy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PoolingStrategy {
    /// No pooling - 1:1 client-to-backend mapping (current behavior)
    Disabled,
    /// Session pooling - connection assigned for entire client session
    Session,
    /// Hybrid pooling - dynamic pinning with automatic state tracking
    Hybrid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub target_latency_ms: u64,
    pub connection_pooling: PoolingStrategy,
    pub pool_size: usize,
    pub pool_min_idle: usize,
    pub pool_timeout_secs: u64,
    pub pool_recycle_secs: u64,
    pub pool_aggressive_unpinning: bool,
    pub buffer_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            proxy: ProxyConfig {
                listen_address: "127.0.0.1:5433".to_string(),
                max_connections: 100,
                shutdown_timeout_secs: 30,
            },
            backend: BackendConfig {
                protocol: DatabaseProtocol::Postgres,
                host: "localhost".to_string(),
                port: 5432,
                database: "postgres".to_string(),
                user: "postgres".to_string(),
                password: "password".to_string(),
                pool_size: 10,
                connection_timeout_ms: 5000,
            },
            observability: ObservabilityConfig {
                enable_tracing: true,
                otlp_endpoint: Some("http://localhost:4317".to_string()),
                service_name: "scry-proxy".to_string(),
                metrics_server_address: "127.0.0.1:9090".to_string(),
                enable_metrics_server: true,
            },
            publisher: PublisherConfig {
                enabled: true,
                batch_size: 100,
                flush_interval_ms: 1000,
                anonymize: true,
                publisher_type: "debug".to_string(),
                max_queue_size: 10000, // ~1MB of events (100 bytes/event avg)
                http_endpoint: None,
                http_timeout_ms: 500,
                http_max_retries: 2,
                http_api_key: None,
                http_compression: true,
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
            },
        }
    }
}

impl Config {
    /// Load configuration from environment and config files
    ///
    /// Loading priority (highest to lowest):
    /// 1. Environment variables (SCRY_*)
    /// 2. Config file (scry.toml or SCRY_CONFIG_FILE)
    /// 3. Default values
    ///
    /// Environment variable examples:
    /// - SCRY_PROXY__LISTEN_ADDRESS=127.0.0.1:5433
    /// - SCRY_BACKEND__HOST=localhost
    /// - SCRY_BACKEND__PORT=5432
    /// - SCRY_OBSERVABILITY__ENABLE_TRACING=true
    /// - SCRY_PUBLISHER__ENABLED=true
    pub fn load() -> anyhow::Result<Self> {
        use config::{Config as ConfigBuilder, Environment, File};
        use std::env;

        let mut builder = ConfigBuilder::builder();

        // 1. Start with defaults
        let defaults = Self::default();
        builder = builder.add_source(config::Config::try_from(&defaults)?);

        // 2. Load from config file if it exists
        let config_file = env::var("SCRY_CONFIG_FILE").unwrap_or_else(|_| "scry.toml".to_string());
        if std::path::Path::new(&config_file).exists() {
            builder = builder.add_source(File::with_name(&config_file).required(false));
        }

        // 3. Override with environment variables
        // Use separator "__" to support nested config
        // e.g., SCRY_BACKEND__HOST=localhost
        builder = builder.add_source(
            Environment::with_prefix("SCRY")
                .separator("__")
                .try_parsing(true),
        );

        let config = builder.build()?;
        let loaded: Config = config.try_deserialize()?;

        Ok(loaded)
    }
}
