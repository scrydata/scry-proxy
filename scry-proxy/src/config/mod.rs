use serde::{Deserialize, Serialize};

pub mod pgbouncer;
pub use pgbouncer::PgBouncerConfig;

/// Supported database protocols
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseProtocol {
    /// PostgreSQL (and compatible databases like CockroachDB)
    Postgres,
    // Future: MySQL (and compatible databases like MariaDB)
    // Mysql,
    // Future: MongoDB
    // Mongodb,
}

impl DatabaseProtocol {
    /// Get the protocol name as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            DatabaseProtocol::Postgres => "postgres",
            // Future protocol support:
            // DatabaseProtocol::Mysql => "mysql",
            // DatabaseProtocol::Mongodb => "mongodb",
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
    /// Additional databases for multi-database routing.
    /// If a client connects to a database name matching an entry here,
    /// they will be routed to that specific backend.
    /// If no match, falls back to the default `backend` config.
    #[serde(default)]
    pub databases: Vec<DatabaseConfig>,
    pub observability: ObservabilityConfig,
    pub protocol: ProtocolConfig,
    pub publisher: PublisherConfig,
    pub performance: PerformanceConfig,
    pub resilience: ResilienceConfig,
    pub tls: TlsConfig,
    pub auth: AuthConfig,
    #[serde(default)]
    pub admin: AdminConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    pub listen_address: String,
    /// UNIX socket path for listening (e.g., /var/run/scry/.s.PGSQL.6432)
    /// Only available on Unix platforms.
    #[serde(default)]
    pub unix_socket: Option<String>,
    pub max_connections: usize,
    /// How long to wait for connections to drain during shutdown (seconds)
    pub shutdown_timeout_secs: u64,
}

/// Debug placeholder for a present secret value. Renders as `<redacted>` with no
/// surrounding quotes, so secrets never leak through `{:?}` (P1 §4.7).
///
/// `pub(crate)` so other in-crate consumers that need to report secret
/// *presence* without ever touching the plaintext (e.g. `SHOW CONFIG`,
/// WP-10 P4 §4.3/§5.5) can reuse this exact redaction instead of hand-rolling
/// a second one that could drift from this Debug impl.
pub(crate) struct RedactedSecret;

impl std::fmt::Debug for RedactedSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Render an optional secret's *presence* for Debug output, never its value.
pub(crate) fn redacted_opt(value: &Option<String>) -> Option<RedactedSecret> {
    value.as_ref().map(|_| RedactedSecret)
}

#[derive(Clone, Serialize, Deserialize)]
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

// Manual Debug so the backend password never renders in logs/panics (P1 §4.7).
impl std::fmt::Debug for BackendConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendConfig")
            .field("protocol", &self.protocol)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &RedactedSecret)
            .field("pool_size", &self.pool_size)
            .field("connection_timeout_ms", &self.connection_timeout_ms)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    pub enable_tracing: bool,
    pub otlp_endpoint: Option<String>,
    pub service_name: String,
    pub metrics_server_address: String,
    pub enable_metrics_server: bool,
    /// Allow verbose/debug logging paths that may log unredacted query text
    /// or credentials. Must be explicitly opted into (P1 §4.7).
    #[serde(default)]
    pub unsafe_debug_logging: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolConfig {
    /// Maximum prepared statements cached per connection.
    /// Uses LRU eviction when limit is reached.
    pub max_prepared_statements: usize,
}

#[derive(Clone, Serialize, Deserialize)]
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

    /// Shadow ID for routing events to a specific shadow instance.
    /// Used when sending events to scry-platform.
    /// Can be set via SCRY_PUBLISHER__SHADOW_ID or read from SHADOW_ID_FILE env var.
    #[serde(default)]
    pub shadow_id: Option<String>,

    /// Explicitly acknowledge sending events to a non-HTTPS endpoint.
    /// Required when `http_endpoint` does not use the `https://` scheme (P1 §4.5).
    #[serde(default)]
    pub allow_insecure: bool,

    /// Salt used when anonymizing query data before publishing.
    /// Required when `anonymize = true` (P1 §4.1).
    #[serde(default)]
    pub anonymize_salt: Option<String>,

    /// Behavior when query parsing/anonymization fails for an observed query.
    #[serde(default)]
    pub parse_failure_mode: ParseFailureMode,
}

// Manual Debug so the API key and anonymization salt never render in
// logs/panics; their presence is shown but not their value (P1 §4.7).
impl std::fmt::Debug for PublisherConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublisherConfig")
            .field("enabled", &self.enabled)
            .field("batch_size", &self.batch_size)
            .field("flush_interval_ms", &self.flush_interval_ms)
            .field("anonymize", &self.anonymize)
            .field("publisher_type", &self.publisher_type)
            .field("max_queue_size", &self.max_queue_size)
            .field("http_endpoint", &self.http_endpoint)
            .field("http_timeout_ms", &self.http_timeout_ms)
            .field("http_max_retries", &self.http_max_retries)
            .field("http_api_key", &redacted_opt(&self.http_api_key))
            .field("http_compression", &self.http_compression)
            .field("shadow_id", &self.shadow_id)
            .field("allow_insecure", &self.allow_insecure)
            .field("anonymize_salt", &redacted_opt(&self.anonymize_salt))
            .field("parse_failure_mode", &self.parse_failure_mode)
            .finish()
    }
}

/// Behavior when query parsing (or anonymization) fails for an observed query.
///
/// Defaults to `Redact` (P1 §9.2): never emit a potentially-unsafe raw query
/// on parse failure; either replace it with a redaction placeholder or drop
/// the event entirely.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ParseFailureMode {
    /// Replace the query text with a redaction placeholder and still emit the event.
    #[default]
    Redact,
    /// Drop the event entirely rather than emit anything for it.
    Drop,
}

/// Connection pooling strategy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PoolingStrategy {
    /// No pooling - 1:1 client-to-backend mapping (current behavior)
    Disabled,
    /// Session pooling - connection assigned for entire client session
    Session,
    /// Transaction pooling - connection released after each transaction (strict mode)
    Transaction,
    /// Hybrid pooling - dynamic pinning with automatic state tracking (default)
    Hybrid,
}

/// Backpressure behavior when connection pool queue is full
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BackpressureMode {
    /// Reject immediately with error (default, current behavior)
    #[default]
    RejectImmediate,
    /// Return "server busy" error with retry hint
    /// Clients receive SQLSTATE 53300 with retry delay suggestion
    RetryHint,
    /// Log and reject (for debugging high load scenarios)
    /// Same as RejectImmediate but logs each rejection at WARN level
    LogAndReject,
}

/// Authentication type - matches PgBouncer naming
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AuthType {
    /// No authentication required
    #[default]
    Trust,
    /// MD5 password authentication
    Md5,
    /// SCRAM-SHA-256 password authentication
    #[serde(rename = "scram-sha-256")]
    ScramSha256,
    /// Certificate-based authentication
    Cert,
}

/// Database routing configuration for multi-database support
/// Each entry defines a named database with its connection parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Logical name clients use to connect (matched against startup message database)
    pub name: String,
    /// Backend host for this database
    pub host: String,
    /// Backend port for this database
    pub port: u16,
    /// Actual database name on the backend
    pub database: String,
    /// User for backend connection (if different from client user)
    pub user: String,
    /// Password for backend connection
    pub password: String,
    /// Pool size override for this database (uses default if None)
    pub pool_size: Option<usize>,
}

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Authentication type: trust, md5, scram-sha-256, cert
    pub auth_type: AuthType,

    /// Path to userlist.txt file (PgBouncer format)
    /// Format: "username" "password" (one per line)
    pub auth_file: Option<String>,

    /// Query to execute against backend to validate credentials
    /// Example: SELECT usename, passwd FROM pg_shadow WHERE usename=$1
    pub auth_query: Option<String>,

    /// Explicit acknowledgement that `auth_type = trust` disables authentication.
    /// Trust mode is refused by `Config::validate()` unless this is `true` (P1 §9.1).
    #[serde(default)]
    pub allow_trust: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { auth_type: AuthType::Trust, auth_file: None, auth_query: None, allow_trust: false }
    }
}

/// Admin console configuration (PgBouncer-style `pgbouncer` virtual database).
///
/// Disabled by default; enabling it exposes SHOW/PAUSE/RESUME/RELOAD style
/// commands, so it must be explicitly turned on and (when enabled) should be
/// paired with a userlist or inline credential.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct AdminConfig {
    /// Enable the admin console (disabled by default).
    #[serde(default)]
    pub enabled: bool,
    /// Path to a PgBouncer-style userlist.txt, or an inline user, for admin auth.
    #[serde(default)]
    pub admin_users: Option<String>,
    /// Admin password (when not using a userlist file).
    #[serde(default)]
    pub admin_password: Option<String>,
}

// Manual Debug so the admin password never renders in logs/panics (P1 §4.7).
impl std::fmt::Debug for AdminConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminConfig")
            .field("enabled", &self.enabled)
            .field("admin_users", &self.admin_users)
            .field("admin_password", &redacted_opt(&self.admin_password))
            .finish()
    }
}

/// TLS SSL mode - matches PgBouncer naming for familiarity
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum TlsSslMode {
    /// Plain TCP, TLS disabled (default)
    #[default]
    Disable,
    /// If client requests TLS, use it; otherwise plain TCP
    Allow,
    /// Client must use TLS, but certificate not validated
    Require,
    /// Client must use TLS with valid certificate (CA verified)
    #[serde(rename = "verify-ca")]
    VerifyCa,
    /// Client must use TLS with valid certificate + hostname match
    #[serde(rename = "verify-full")]
    VerifyFull,
}

impl TlsSslMode {
    /// Whether this mode requires the client connection to be encrypted.
    ///
    /// `require`/`verify-ca`/`verify-full` mandate TLS; a client that attempts
    /// to bypass it (no SSLRequest) must be rejected, not silently served in
    /// plaintext (P1 §4.2 downgrade protection). `disable`/`allow` do not
    /// require encryption.
    pub fn requires_encryption(&self) -> bool {
        matches!(self, TlsSslMode::Require | TlsSslMode::VerifyCa | TlsSslMode::VerifyFull)
    }
}

/// TLS configuration for client and server connections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    // Client-facing TLS (clients -> proxy)
    /// TLS mode for client connections
    pub client_tls_sslmode: TlsSslMode,
    /// Path to server certificate file (PEM format)
    pub client_tls_cert_file: Option<String>,
    /// Path to server private key file (PEM format)
    pub client_tls_key_file: Option<String>,
    /// Path to CA certificate for client certificate validation
    pub client_tls_ca_file: Option<String>,

    // Server-facing TLS (proxy -> backend)
    /// TLS mode for backend connections
    pub server_tls_sslmode: TlsSslMode,
    /// Path to CA certificate for server certificate validation
    pub server_tls_ca_file: Option<String>,
    /// Path to client certificate for backend authentication
    pub server_tls_cert_file: Option<String>,
    /// Path to client private key for backend authentication
    pub server_tls_key_file: Option<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            client_tls_sslmode: TlsSslMode::Disable,
            client_tls_cert_file: None,
            client_tls_key_file: None,
            client_tls_ca_file: None,
            server_tls_sslmode: TlsSslMode::Disable,
            server_tls_ca_file: None,
            server_tls_cert_file: None,
            server_tls_key_file: None,
        }
    }
}

/// Latency budget for the proxy's *added* overhead (P5 §4.2).
///
/// "Added latency" is the wall-clock time attributable to the proxy itself —
/// `total − backend − pool_acquire − queue` (see
/// `observability::TimelinePhases::proxy_overhead_micros`) — NOT the total
/// client-observed latency, which is dominated by backend execution. The budget
/// is expressed per percentile because tail overhead matters more than the mean,
/// and is defined against a named reference workload so the numbers are
/// comparable across runs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatencyBudget {
    /// p50 added-latency budget, microseconds.
    pub overhead_p50_micros: u64,
    /// p95 added-latency budget, microseconds.
    pub overhead_p95_micros: u64,
    /// p99 added-latency budget, microseconds.
    pub overhead_p99_micros: u64,
    /// Name of the reference workload the budget is measured against.
    pub reference_workload: String,
}

impl Default for LatencyBudget {
    fn default() -> Self {
        // Target <1ms added latency (CLAUDE.md, P5). Tail percentiles get more
        // headroom; the numbers are a starting baseline, tightened under load in
        // WP-13 once measured on a stable runner.
        Self {
            overhead_p50_micros: 250,
            overhead_p95_micros: 750,
            overhead_p99_micros: 1000,
            reference_workload: "oltp-point-select".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Budget for the proxy's own added latency, per percentile (P5 §4.2).
    #[serde(default)]
    pub latency_budget: LatencyBudget,
    /// Maximum time a single client query may run on the backend before the
    /// proxy cancels it by closing the connection (seconds; 0 = disabled).
    /// A timed-out connection is never returned clean to the pool (P3 §4.3).
    #[serde(default)]
    pub query_timeout_secs: u64,
    pub connection_pooling: PoolingStrategy,
    pub pool_size: usize,
    pub pool_min_idle: usize,
    pub pool_timeout_secs: u64,
    pub pool_recycle_secs: u64,
    pub pool_aggressive_unpinning: bool,
    pub buffer_size: usize,
    /// Maximum clients waiting for a connection (0 = unlimited)
    pub pool_queue_depth: usize,
    /// Idle timeout before unpinning in hybrid mode (seconds)
    pub pool_idle_unpin_secs: u64,
    /// Use LIFO connection selection (true) or FIFO (false)
    pub pool_lifo: bool,
    /// Timeout for DISCARD ALL response during connection reset (milliseconds)
    pub pool_reset_timeout_ms: u64,
    /// Maximum ratio of max_connections to pool_size before warning.
    /// Transaction pooling can handle 20:1 multiplexing; higher ratios
    /// may cause excessive wait times. Set to 0 to disable warning.
    #[serde(default = "default_pool_ratio_warning_threshold")]
    pub pool_ratio_warning_threshold: usize,
    /// Backpressure behavior when pool queue is full
    #[serde(default)]
    pub pool_backpressure_mode: BackpressureMode,
    /// Suggested retry delay in milliseconds (for RetryHint mode)
    #[serde(default = "default_pool_retry_hint_ms")]
    pub pool_retry_hint_ms: u64,
    /// Saturation threshold for warning logs (0.0-1.0)
    /// Logs a warning when queue saturation exceeds this threshold
    #[serde(default = "default_pool_queue_saturation_warn_threshold")]
    pub pool_queue_saturation_warn_threshold: f64,
}

fn default_pool_ratio_warning_threshold() -> usize {
    20
}

fn default_pool_retry_hint_ms() -> u64 {
    200
}

fn default_pool_queue_saturation_warn_threshold() -> f64 {
    0.8
}

/// Resilience configuration - circuit breaking, retries, healthchecks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResilienceConfig {
    pub circuit_breaker: CircuitBreakerConfig,
    pub connection_retry: ConnectionRetryConfig,
    pub healthcheck: HealthcheckConfig,
}

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Enable circuit breaker
    pub enabled: bool,

    /// Failure threshold to open circuit (consecutive failures)
    pub failure_threshold: u32,

    /// Success threshold to close circuit from half-open (consecutive successes)
    pub success_threshold: u32,

    /// Time window for failure counting (seconds)
    pub window_secs: u64,

    /// Timeout in open state before transitioning to half-open (seconds)
    pub open_timeout_secs: u64,

    /// Use health monitor for intelligent state transitions
    pub use_health_monitor: bool,
}

/// Connection retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionRetryConfig {
    /// Enable connection retries
    pub enabled: bool,

    /// Maximum retry attempts
    pub max_attempts: u32,

    /// Initial backoff delay in milliseconds
    pub initial_backoff_ms: u64,

    /// Maximum backoff delay in milliseconds
    pub max_backoff_ms: u64,

    /// Backoff multiplier
    pub backoff_multiplier: f64,

    /// Jitter factor (0.0-1.0) to prevent thundering herd
    pub jitter_factor: f64,
}

/// Active healthcheck configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthcheckConfig {
    /// Enable active healthchecks (passive healthchecks always enabled)
    pub active_enabled: bool,

    /// Active healthcheck interval (seconds)
    pub interval_secs: u64,

    /// Active healthcheck timeout (milliseconds)
    pub timeout_ms: u64,

    /// Number of consecutive failures before marking unhealthy
    pub failure_threshold: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            proxy: ProxyConfig {
                listen_address: "127.0.0.1:5433".to_string(),
                unix_socket: None,
                max_connections: 100,
                shutdown_timeout_secs: 30,
            },
            backend: BackendConfig {
                protocol: DatabaseProtocol::Postgres,
                host: "localhost".to_string(),
                port: 5432,
                database: "postgres".to_string(),
                user: "postgres".to_string(),
                // No default backend password: an unset password is caught by
                // `validate()` (fail closed rather than shipping a guessable default).
                password: String::new(),
                pool_size: 10,
                connection_timeout_ms: 5000,
            },
            databases: Vec::new(),
            observability: ObservabilityConfig {
                enable_tracing: true,
                otlp_endpoint: Some("http://localhost:4317".to_string()),
                service_name: "scry-proxy".to_string(),
                metrics_server_address: "127.0.0.1:9090".to_string(),
                enable_metrics_server: true,
                unsafe_debug_logging: false,
            },
            protocol: ProtocolConfig { max_prepared_statements: 1000 },
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
                shadow_id: None,
                allow_insecure: false,
                anonymize_salt: None,
                parse_failure_mode: ParseFailureMode::Redact,
            },
            performance: PerformanceConfig {
                latency_budget: LatencyBudget::default(),
                query_timeout_secs: 0,
                connection_pooling: PoolingStrategy::Hybrid,
                pool_size: 50,    // Sensible default; 10:1 with max_connections=500
                pool_min_idle: 5, // Keep low for dev/test, increase for production
                pool_timeout_secs: 30,
                pool_recycle_secs: 3600,
                pool_aggressive_unpinning: false,
                buffer_size: 8192,
                pool_queue_depth: 500, // Production needs larger queue for bursts
                pool_idle_unpin_secs: 60,
                pool_lifo: true,
                pool_reset_timeout_ms: 5000,
                pool_ratio_warning_threshold: 20,
                pool_backpressure_mode: BackpressureMode::RejectImmediate,
                pool_retry_hint_ms: 200,
                pool_queue_saturation_warn_threshold: 0.8,
            },
            resilience: ResilienceConfig {
                circuit_breaker: CircuitBreakerConfig {
                    enabled: true,
                    failure_threshold: 5,
                    success_threshold: 2,
                    window_secs: 30,
                    open_timeout_secs: 60,
                    use_health_monitor: true,
                },
                connection_retry: ConnectionRetryConfig {
                    enabled: true,
                    max_attempts: 3,
                    initial_backoff_ms: 50,
                    max_backoff_ms: 5000,
                    backoff_multiplier: 2.0,
                    jitter_factor: 0.1,
                },
                healthcheck: HealthcheckConfig {
                    active_enabled: true,
                    interval_secs: 30,
                    timeout_ms: 1000,
                    failure_threshold: 3,
                },
            },
            tls: TlsConfig::default(),
            auth: AuthConfig::default(),
            admin: AdminConfig::default(),
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
        // prefix_separator("_") means: SCRY_BACKEND__HOST (single underscore after prefix)
        // separator("__") means: nested keys use double underscore
        builder = builder.add_source(
            Environment::with_prefix("SCRY")
                .prefix_separator("_")
                .separator("__")
                .try_parsing(true),
        );

        // Reject unknown SCRY_* variables by default (P4 §4.4, §9.3): a typo'd
        // or unimplemented key (e.g. SCRY_BACKEND__PASSWORD_FILE) must fail loudly
        // instead of silently no-opping. Operators can opt out with
        // SCRY_ALLOW_UNKNOWN_KEYS=true.
        let allow_unknown = env::var("SCRY_ALLOW_UNKNOWN_KEYS")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);
        if !allow_unknown {
            let valid = valid_config_paths();
            let unknown = unknown_scry_env_keys(env::vars().map(|(k, _)| k), &valid);
            if !unknown.is_empty() {
                anyhow::bail!(
                    "unknown SCRY_* configuration variable(s): {}. Check for typos, or set \
                     SCRY_ALLOW_UNKNOWN_KEYS=true to ignore. (This catches keys like \
                     SCRY_BACKEND__PASSWORD_FILE that would otherwise silently no-op.)",
                    unknown.join(", ")
                );
            }
        }

        let config = builder.build()?;
        let loaded: Config = config.try_deserialize()?;

        Ok(loaded)
    }

    /// Validate configuration and return warnings
    ///
    /// Returns Ok(warnings) on success, Err on fatal (fail-closed) misconfigurations.
    /// Warnings are logged but don't prevent startup; Err MUST prevent startup —
    /// callers must treat a returned Err as fatal and abort before binding any
    /// listener (see `main.rs`). Never fall back to a permissive default (e.g.
    /// trust auth) when a case below is triggered.
    pub fn validate(&self) -> anyhow::Result<Vec<String>> {
        let mut warnings = Vec::new();

        // --- Fail-closed security checks (P1 §4.1, §5.1) ---

        // 1. SCRAM-SHA-256 client auth is intentionally unsupported. Refuse
        // rather than silently downgrading to trust.
        if self.auth.auth_type == AuthType::ScramSha256 {
            anyhow::bail!(
                "auth.auth_type = scram-sha-256 is unsupported; refusing to start \
                 rather than falling back to trust. Use auth.auth_type = md5 (with \
                 auth.auth_file set) or auth.auth_type = cert instead."
            );
        }

        // 2. Cert auth requires a client TLS mode that actually verifies the
        // presented certificate.
        if self.auth.auth_type == AuthType::Cert
            && !matches!(self.tls.client_tls_sslmode, TlsSslMode::VerifyCa | TlsSslMode::VerifyFull)
        {
            anyhow::bail!(
                "auth.auth_type = cert requires tls.client_tls_sslmode = verify-ca or \
                 verify-full (got {:?}); certificate identity cannot be trusted otherwise.",
                self.tls.client_tls_sslmode
            );
        }

        // 3. MD5 auth requires a backing auth_file; otherwise there is nothing
        // to verify passwords against.
        if self.auth.auth_type == AuthType::Md5 && self.auth.auth_file.is_none() {
            anyhow::bail!(
                "auth.auth_type = md5 requires auth.auth_file to be set (path to a \
                 PgBouncer-style userlist.txt)."
            );
        }

        // 4. Trust mode disables authentication entirely and must be explicitly
        // acknowledged by the operator.
        if self.auth.auth_type == AuthType::Trust && !self.auth.allow_trust {
            anyhow::bail!(
                "auth.auth_type = trust disables authentication for all clients. \
                 Set auth.allow_trust = true to acknowledge this and start anyway."
            );
        }

        // 5. There is no safe default backend password; an unset password must
        // be explicitly configured.
        if self.backend.password.is_empty() {
            anyhow::bail!(
                "backend.password is not set. Configure a backend password (there is \
                 no default) via config file or SCRY_BACKEND__PASSWORD."
            );
        }

        // 6. Anonymization requires a salt; otherwise "anonymized" output can be
        // trivially reversed/correlated.
        if self.publisher.enabled
            && self.publisher.anonymize
            && self.publisher.anonymize_salt.is_none()
        {
            anyhow::bail!(
                "publisher.anonymize = true requires publisher.anonymize_salt to be set."
            );
        }

        // 7. A non-HTTPS publisher endpoint must be explicitly acknowledged as
        // insecure; a missing endpoint for an http publisher is always an error.
        if self.publisher.enabled && self.publisher.publisher_type == "http" {
            match &self.publisher.http_endpoint {
                None => {
                    anyhow::bail!(
                        "publisher.publisher_type = \"http\" requires publisher.http_endpoint \
                         to be set."
                    );
                }
                Some(endpoint) => {
                    if !endpoint.starts_with("https://") && !self.publisher.allow_insecure {
                        anyhow::bail!(
                            "publisher.http_endpoint ({}) is not https://. Set \
                             publisher.allow_insecure = true to acknowledge sending \
                             (possibly anonymized) query events over a non-HTTPS endpoint.",
                            endpoint
                        );
                    }
                }
            }
        }

        // --- Fail-closed capacity checks (P4 §4.4) ---
        // Genuinely-unsafe pool/connection settings are refused, not merely
        // warned about: they guarantee the proxy cannot serve traffic.

        // 8. Zero max_connections means no client can ever connect.
        if self.proxy.max_connections == 0 {
            anyhow::bail!(
                "proxy.max_connections is 0; the proxy would accept no client connections."
            );
        }

        // 9. A zero-size pool while pooling is enabled can never hand out a
        // backend connection — every query would block forever.
        if self.performance.connection_pooling != PoolingStrategy::Disabled
            && self.performance.pool_size == 0
        {
            anyhow::bail!(
                "performance.pool_size is 0 while connection pooling is enabled \
                 ({:?}); no backend connection could ever be acquired. Set a non-zero \
                 pool_size or performance.connection_pooling = disabled.",
                self.performance.connection_pooling
            );
        }

        // --- Non-fatal warnings ---

        // Check pool ratio (only if threshold is non-zero)
        if self.performance.pool_ratio_warning_threshold > 0 && self.performance.pool_size > 0 {
            let ratio = self.proxy.max_connections as f64 / self.performance.pool_size as f64;
            if ratio > self.performance.pool_ratio_warning_threshold as f64 {
                warnings.push(format!(
                    "max_connections ({}) is {}x pool_size ({}). \
                     Clients may experience long wait times. \
                     Consider increasing pool_size or decreasing max_connections.",
                    self.proxy.max_connections, ratio as usize, self.performance.pool_size
                ));
            }
        }

        // Check queue depth relative to multiplexing ratio
        let expected_waiters =
            self.proxy.max_connections.saturating_sub(self.performance.pool_size);
        if expected_waiters > 0 && self.performance.pool_queue_depth < expected_waiters / 2 {
            warnings.push(format!(
                "pool_queue_depth ({}) may be too small. \
                 With {} max_connections and {} pool_size, \
                 up to {} clients may need to queue. \
                 Consider setting pool_queue_depth >= {}.",
                self.performance.pool_queue_depth,
                self.proxy.max_connections,
                self.performance.pool_size,
                expected_waiters,
                expected_waiters
            ));
        }

        // Warn if pool_size > max_connections (wasteful)
        if self.performance.pool_size > self.proxy.max_connections {
            warnings.push(format!(
                "pool_size ({}) exceeds max_connections ({}). \
                 Extra pool connections will never be used.",
                self.performance.pool_size, self.proxy.max_connections
            ));
        }

        Ok(warnings)
    }
}

/// Collect every valid dotted config-key path from the default configuration
/// (e.g. `"backend.password"`, `"resilience.circuit_breaker.enabled"`). Used to
/// detect unknown `SCRY_*` environment variables at load time, and by the
/// docs/code parity guardrail.
pub fn valid_config_paths() -> std::collections::HashSet<String> {
    let mut paths = std::collections::HashSet::new();
    if let Ok(value) = serde_json::to_value(Config::default()) {
        collect_paths(&value, "", &mut paths);
    }
    paths
}

fn collect_paths(
    value: &serde_json::Value,
    prefix: &str,
    out: &mut std::collections::HashSet<String>,
) {
    if let serde_json::Value::Object(map) = value {
        for (k, v) in map {
            let path = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            out.insert(path.clone());
            collect_paths(v, &path, out);
        }
    }
}

/// Return any `SCRY_*` environment variable name that does not map to a known
/// config field. Meta variables (`SCRY_CONFIG_FILE`, `SCRY_ALLOW_UNKNOWN_KEYS`)
/// and the dynamically-sized `databases` section are ignored.
fn unknown_scry_env_keys<I>(env_keys: I, valid: &std::collections::HashSet<String>) -> Vec<String>
where
    I: IntoIterator<Item = String>,
{
    let mut unknown = Vec::new();
    for key in env_keys {
        let Some(rest) = key.strip_prefix("SCRY_") else { continue };
        if rest == "CONFIG_FILE" || rest == "ALLOW_UNKNOWN_KEYS" {
            continue;
        }
        let path = rest.to_lowercase().replace("__", ".");
        // `databases` is a Vec<DatabaseConfig>; its element paths aren't in the
        // default schema, so don't strict-check that subtree.
        if path == "databases" || path.starts_with("databases.") {
            continue;
        }
        if !valid.contains(&path) {
            unknown.push(key);
        }
    }
    unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pooling_strategy_default_is_hybrid() {
        let config = Config::default();
        assert_eq!(config.performance.connection_pooling, PoolingStrategy::Hybrid);
    }

    #[test]
    fn test_pool_queue_depth_default() {
        let config = Config::default();
        assert_eq!(config.performance.pool_queue_depth, 500);
    }

    #[test]
    fn test_pool_size_default() {
        let config = Config::default();
        assert_eq!(config.performance.pool_size, 50);
    }

    #[test]
    fn test_pool_ratio_warning_threshold_default() {
        let config = Config::default();
        assert_eq!(config.performance.pool_ratio_warning_threshold, 20);
    }

    #[test]
    fn test_pool_idle_unpin_secs_default() {
        let config = Config::default();
        assert_eq!(config.performance.pool_idle_unpin_secs, 60);
    }

    #[test]
    fn test_pool_lifo_default() {
        let config = Config::default();
        assert!(config.performance.pool_lifo);
    }

    #[test]
    fn test_pooling_strategy_variants() {
        // Verify all pooling strategy variants exist and are distinct
        let strategies = [
            PoolingStrategy::Disabled,
            PoolingStrategy::Session,
            PoolingStrategy::Transaction,
            PoolingStrategy::Hybrid,
        ];
        assert_eq!(strategies.len(), 4);
        assert_ne!(PoolingStrategy::Disabled, PoolingStrategy::Session);
        assert_ne!(PoolingStrategy::Session, PoolingStrategy::Transaction);
        assert_ne!(PoolingStrategy::Transaction, PoolingStrategy::Hybrid);
    }

    #[test]
    fn test_tls_sslmode_default_is_disable() {
        let config = Config::default();
        assert_eq!(config.tls.client_tls_sslmode, TlsSslMode::Disable);
        assert_eq!(config.tls.server_tls_sslmode, TlsSslMode::Disable);
    }

    #[test]
    fn test_tls_config_defaults() {
        let config = Config::default();
        assert!(config.tls.client_tls_cert_file.is_none());
        assert!(config.tls.client_tls_key_file.is_none());
        assert!(config.tls.client_tls_ca_file.is_none());
        assert!(config.tls.server_tls_ca_file.is_none());
    }

    #[test]
    fn test_auth_type_default_is_trust() {
        let config = Config::default();
        assert_eq!(config.auth.auth_type, AuthType::Trust);
    }

    #[test]
    fn test_auth_config_defaults() {
        let config = Config::default();
        assert_eq!(config.auth.auth_type, AuthType::Trust);
        assert!(config.auth.auth_file.is_none());
        assert!(config.auth.auth_query.is_none());
    }
}

#[cfg(test)]
mod validation_tests {
    use super::*;

    /// A config that satisfies every fail-closed check in `validate()` so that
    /// pool-ratio-focused tests only exercise the (non-fatal) warnings logic.
    fn valid_base_config() -> Config {
        let mut config = Config::default();
        config.auth.allow_trust = true; // acknowledge trust mode
        config.backend.password = "test-password".to_string();
        config.publisher.anonymize_salt = Some("test-salt".to_string());
        config
    }

    #[test]
    fn test_validate_warns_on_high_ratio() {
        let mut config = valid_base_config();
        config.proxy.max_connections = 1000;
        config.performance.pool_size = 10; // 100:1 ratio, exceeds 20:1 threshold

        let warnings = config.validate().unwrap();
        assert!(!warnings.is_empty(), "Should warn on 100:1 ratio");
        assert!(warnings[0].contains("100x"), "Warning should mention 100x ratio: {}", warnings[0]);
    }

    #[test]
    fn test_validate_warns_on_small_queue() {
        let mut config = valid_base_config();
        config.proxy.max_connections = 500;
        config.performance.pool_size = 50;
        config.performance.pool_queue_depth = 50; // Too small for 450 potential waiters
        config.performance.pool_ratio_warning_threshold = 20; // 10:1 ratio is fine

        let warnings = config.validate().unwrap();
        assert!(
            warnings.iter().any(|w| w.contains("pool_queue_depth")),
            "Should warn about small queue depth: {:?}",
            warnings
        );
    }

    #[test]
    fn test_validate_warns_on_wasteful_pool() {
        let mut config = valid_base_config();
        config.proxy.max_connections = 50;
        config.performance.pool_size = 100; // Wasteful: more pool than clients

        let warnings = config.validate().unwrap();
        assert!(
            warnings.iter().any(|w| w.contains("exceeds max_connections")),
            "Should warn about wasteful pool: {:?}",
            warnings
        );
    }

    #[test]
    fn test_validate_no_warnings_for_good_config() {
        let mut config = valid_base_config();
        config.proxy.max_connections = 500;
        config.performance.pool_size = 50; // 10:1 ratio, within 20:1 threshold
        config.performance.pool_queue_depth = 500; // Adequate for 450 potential waiters
        config.performance.pool_ratio_warning_threshold = 20;

        let warnings = config.validate().unwrap();
        assert!(warnings.is_empty(), "Should have no warnings for good config: {:?}", warnings);
    }

    #[test]
    fn test_validate_disabled_when_threshold_zero() {
        let mut config = valid_base_config();
        config.proxy.max_connections = 1000;
        config.performance.pool_size = 10; // 100:1 ratio
        config.performance.pool_ratio_warning_threshold = 0; // Disable warning

        let warnings = config.validate().unwrap();
        // Should not warn about ratio when threshold is 0
        assert!(
            !warnings.iter().any(|w| w.contains("100x")),
            "Should not warn when threshold is 0: {:?}",
            warnings
        );
    }
}

#[cfg(test)]
mod secure_defaults_snapshot {
    use super::*;

    /// Serialize every security-relevant default to a stable, sorted form.
    /// Any change here means a default moved; the snapshot test forces it to be
    /// reviewed and re-approved in the same PR (P1 §5.2).
    fn snapshot() -> String {
        let c = Config::default();
        let mut lines = vec![
            format!("admin.enabled = {}", c.admin.enabled),
            format!("auth.allow_trust = {}", c.auth.allow_trust),
            format!("auth.auth_type = {:?}", c.auth.auth_type),
            format!("backend.password_is_empty = {}", c.backend.password.is_empty()),
            format!(
                "observability.unsafe_debug_logging = {}",
                c.observability.unsafe_debug_logging
            ),
            format!("publisher.allow_insecure = {}", c.publisher.allow_insecure),
            format!("publisher.anonymize = {}", c.publisher.anonymize),
            format!("publisher.parse_failure_mode = {:?}", c.publisher.parse_failure_mode),
            format!("tls.client_tls_sslmode = {:?}", c.tls.client_tls_sslmode),
            format!("tls.server_tls_sslmode = {:?}", c.tls.server_tls_sslmode),
        ];
        lines.sort();
        let mut s = lines.join("\n");
        s.push('\n');
        s
    }

    #[test]
    fn secure_defaults_are_locked() {
        let expected = include_str!("../../tests/fixtures/secure_defaults.snapshot");
        let actual = snapshot();
        assert_eq!(
            actual, expected,
            "Security-relevant defaults changed.\n\nIf this change is intentional AND \
             still secure (a default did not move toward less protection), update \
             tests/fixtures/secure_defaults.snapshot in the same PR. If a default became \
             less safe, reconsider it — this snapshot exists to catch exactly that (P1 §5.2)."
        );
    }
}

#[cfg(test)]
mod redacting_debug_tests {
    use super::*;

    #[test]
    fn debug_output_never_contains_secrets() {
        let mut config = Config::default();
        config.backend.password = "BACKEND_SECRET_PW".to_string();
        config.publisher.http_api_key = Some("API_KEY_SECRET".to_string());
        config.publisher.anonymize_salt = Some("SALT_SECRET".to_string());
        config.admin.admin_password = Some("ADMIN_SECRET_PW".to_string());

        let dbg = format!("{config:?}");

        for secret in ["BACKEND_SECRET_PW", "API_KEY_SECRET", "SALT_SECRET", "ADMIN_SECRET_PW"] {
            assert!(!dbg.contains(secret), "Debug output leaked secret {secret}");
        }
        // The redaction placeholder should be present where secrets were set.
        assert!(dbg.contains("<redacted>"), "expected redaction placeholder in: {dbg}");
        // Presence (Some/None) of optional secrets is still observable.
        assert!(dbg.contains("Some(<redacted>)"));
    }

    #[test]
    fn debug_output_shows_none_for_unset_optional_secrets() {
        let config = Config::default(); // api_key/salt/admin_password all None
        let dbg = format!("{config:?}");
        // Unset optional secrets render as None, not a redaction placeholder.
        assert!(dbg.contains("http_api_key: None"));
    }
}

#[cfg(test)]
mod latency_budget_tests {
    use super::*;

    #[test]
    fn default_budget_targets_sub_millisecond_overhead() {
        let b = Config::default().performance.latency_budget;
        assert!(b.overhead_p50_micros <= b.overhead_p95_micros);
        assert!(b.overhead_p95_micros <= b.overhead_p99_micros);
        // <1ms added-latency target (CLAUDE.md, P5).
        assert!(b.overhead_p99_micros <= 1000);
        assert!(!b.reference_workload.is_empty());
    }

    #[test]
    fn budget_is_a_known_config_path() {
        // The new nested budget fields must be discoverable so env overrides
        // like SCRY_PERFORMANCE__LATENCY_BUDGET__OVERHEAD_P99_MICROS are accepted.
        let paths = valid_config_paths();
        assert!(paths.contains("performance.latency_budget.overhead_p99_micros"));
        assert!(paths.contains("performance.latency_budget.reference_workload"));
        // The dead flat field is gone.
        assert!(!paths.contains("performance.target_latency_ms"));
    }
}

#[cfg(test)]
mod capacity_and_unknown_key_tests {
    use super::*;

    fn base() -> Config {
        let mut c = Config::default();
        c.auth.allow_trust = true;
        c.backend.password = "pw".to_string();
        c.publisher.anonymize_salt = Some("salt".to_string());
        c
    }

    #[test]
    fn rejects_zero_max_connections() {
        let mut c = base();
        c.proxy.max_connections = 0;
        assert!(c.validate().is_err(), "zero max_connections must be rejected");
    }

    #[test]
    fn rejects_zero_pool_size_when_pooling_enabled() {
        let mut c = base();
        c.performance.connection_pooling = PoolingStrategy::Transaction;
        c.performance.pool_size = 0;
        assert!(c.validate().is_err(), "zero pool_size with pooling on must be rejected");
    }

    #[test]
    fn allows_zero_pool_size_when_pooling_disabled() {
        let mut c = base();
        c.performance.connection_pooling = PoolingStrategy::Disabled;
        c.performance.pool_size = 0;
        // Pool ratio warning is skipped when pool_size == 0; this must not error.
        assert!(c.validate().is_ok(), "zero pool_size is fine when pooling is disabled");
    }

    #[test]
    fn valid_config_paths_include_known_leaves() {
        let paths = valid_config_paths();
        assert!(paths.contains("backend.password"), "missing backend.password");
        assert!(paths.contains("proxy.listen_address"), "missing proxy.listen_address");
        assert!(paths.contains("publisher.anonymize_salt"), "missing publisher.anonymize_salt");
        assert!(
            paths.contains("resilience.circuit_breaker.enabled"),
            "missing nested resilience.circuit_breaker.enabled"
        );
        // The typo'd key this pillar exists to catch is NOT a valid path.
        assert!(!paths.contains("backend.password_file"));
    }

    #[test]
    fn unknown_env_keys_are_detected() {
        let valid = valid_config_paths();
        let env = vec![
            "SCRY_BACKEND__PASSWORD".to_string(),                    // known
            "SCRY_PROXY__LISTEN_ADDRESS".to_string(),                // known
            "SCRY_RESILIENCE__CIRCUIT_BREAKER__ENABLED".to_string(), // known nested
            "SCRY_BACKEND__PASSWORD_FILE".to_string(),               // UNKNOWN (the target bug)
            "SCRY_TOTALLY__BOGUS".to_string(),                       // UNKNOWN
            "SCRY_CONFIG_FILE".to_string(),                          // meta, ignored
            "SCRY_ALLOW_UNKNOWN_KEYS".to_string(),                   // meta, ignored
            "SCRY_DATABASES__0__HOST".to_string(),                   // dynamic Vec section, ignored
            "PATH".to_string(),                                      // non-SCRY, ignored
        ];
        let unknown = unknown_scry_env_keys(env, &valid);
        assert!(unknown.contains(&"SCRY_BACKEND__PASSWORD_FILE".to_string()));
        assert!(unknown.contains(&"SCRY_TOTALLY__BOGUS".to_string()));
        assert_eq!(unknown.len(), 2, "unexpected unknowns: {unknown:?}");
    }
}

#[cfg(test)]
mod fail_closed_tests {
    use super::*;

    /// A config that satisfies every fail-closed check in `validate()`, so each
    /// negative test only needs to un-set the one condition it's exercising.
    fn fully_valid_config() -> Config {
        let mut config = Config::default();
        config.auth.auth_type = AuthType::Trust;
        config.auth.allow_trust = true;
        config.backend.password = "secret-password".to_string();
        config.publisher.enabled = true;
        config.publisher.anonymize = true;
        config.publisher.anonymize_salt = Some("some-salt".to_string());
        config.publisher.publisher_type = "debug".to_string();
        config
    }

    #[test]
    fn test_validate_ok_for_fully_valid_config() {
        let config = fully_valid_config();
        let result = config.validate();
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());
    }

    // Case 1: ScramSha256 is unsupported; must be refused, not silently
    // downgraded to trust.
    #[test]
    fn test_validate_rejects_scram_sha256_auth() {
        let mut config = fully_valid_config();
        config.auth.auth_type = AuthType::ScramSha256;
        assert!(config.validate().is_err(), "SCRAM-SHA-256 auth must be rejected (unsupported)");
    }

    // Case 2: Cert auth requires a verifying client TLS mode.
    #[test]
    fn test_validate_rejects_cert_auth_without_verifying_tls() {
        let mut config = fully_valid_config();
        config.auth.auth_type = AuthType::Cert;

        config.tls.client_tls_sslmode = TlsSslMode::Disable;
        assert!(config.validate().is_err(), "Cert auth requires a verifying TLS mode (Disable)");

        config.tls.client_tls_sslmode = TlsSslMode::Allow;
        assert!(config.validate().is_err(), "Cert auth requires a verifying TLS mode (Allow)");

        config.tls.client_tls_sslmode = TlsSslMode::Require;
        assert!(config.validate().is_err(), "Cert auth requires a verifying TLS mode (Require)");

        config.tls.client_tls_sslmode = TlsSslMode::VerifyCa;
        assert!(config.validate().is_ok(), "VerifyCa should satisfy cert auth");

        config.tls.client_tls_sslmode = TlsSslMode::VerifyFull;
        assert!(config.validate().is_ok(), "VerifyFull should satisfy cert auth");
    }

    // Case 3: MD5 auth requires an auth_file. Closes the trust-fallback-on-missing-backing
    // runtime path in server.rs by making it unreachable at startup.
    #[test]
    fn test_validate_rejects_md5_auth_without_auth_file() {
        let mut config = fully_valid_config();
        config.auth.auth_type = AuthType::Md5;
        config.auth.auth_file = None;
        assert!(config.validate().is_err(), "MD5 auth without auth_file must be rejected");
    }

    // Case 4: Trust mode disables authentication and must be explicitly acknowledged.
    #[test]
    fn test_validate_rejects_trust_without_allow_trust_ack() {
        let mut config = fully_valid_config();
        config.auth.auth_type = AuthType::Trust;
        config.auth.allow_trust = false;
        assert!(config.validate().is_err(), "Trust auth without allow_trust ack must be rejected");
    }

    // Case 5: There is no safe default backend password.
    #[test]
    fn test_validate_rejects_empty_backend_password() {
        let mut config = fully_valid_config();
        config.backend.password = String::new();
        assert!(config.validate().is_err(), "Empty backend password must be rejected");
    }

    // Case 6: Anonymization requires an explicit salt.
    #[test]
    fn test_validate_rejects_anonymize_without_salt() {
        let mut config = fully_valid_config();
        config.publisher.enabled = true;
        config.publisher.anonymize = true;
        config.publisher.anonymize_salt = None;
        assert!(config.validate().is_err(), "Anonymize without a salt must be rejected");
    }

    // Case 7: Non-HTTPS publisher endpoints require an explicit insecure acknowledgement,
    // and an http publisher_type still needs an endpoint configured at all.
    #[test]
    fn test_validate_rejects_insecure_http_publisher_endpoint() {
        let mut config = fully_valid_config();
        config.publisher.publisher_type = "http".to_string();
        config.publisher.http_endpoint = Some("http://example.com/events".to_string());
        config.publisher.allow_insecure = false;
        assert!(
            config.validate().is_err(),
            "Non-HTTPS publisher endpoint without allow_insecure must be rejected"
        );
    }

    #[test]
    fn test_validate_rejects_http_publisher_missing_endpoint() {
        let mut config = fully_valid_config();
        config.publisher.publisher_type = "http".to_string();
        config.publisher.http_endpoint = None;
        assert!(
            config.validate().is_err(),
            "http publisher_type without http_endpoint must be rejected"
        );
    }

    #[test]
    fn test_validate_accepts_https_publisher_endpoint() {
        let mut config = fully_valid_config();
        config.publisher.publisher_type = "http".to_string();
        config.publisher.http_endpoint = Some("https://example.com/events".to_string());
        config.publisher.allow_insecure = false;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_accepts_insecure_http_when_acknowledged() {
        let mut config = fully_valid_config();
        config.publisher.publisher_type = "http".to_string();
        config.publisher.http_endpoint = Some("http://example.com/events".to_string());
        config.publisher.allow_insecure = true;
        assert!(config.validate().is_ok());
    }
}

#[cfg(test)]
mod backpressure_tests {
    use super::*;

    #[test]
    fn test_backpressure_mode_default() {
        let config = Config::default();
        assert_eq!(config.performance.pool_backpressure_mode, BackpressureMode::RejectImmediate);
    }

    #[test]
    fn test_backpressure_mode_variants() {
        // Verify all backpressure mode variants exist and are distinct
        let modes = [
            BackpressureMode::RejectImmediate,
            BackpressureMode::RetryHint,
            BackpressureMode::LogAndReject,
        ];
        assert_eq!(modes.len(), 3);
        assert_ne!(BackpressureMode::RejectImmediate, BackpressureMode::RetryHint);
        assert_ne!(BackpressureMode::RetryHint, BackpressureMode::LogAndReject);
    }

    #[test]
    fn test_retry_hint_ms_default() {
        let config = Config::default();
        assert_eq!(config.performance.pool_retry_hint_ms, 200);
    }

    #[test]
    fn test_queue_saturation_warn_threshold_default() {
        let config = Config::default();
        assert!((config.performance.pool_queue_saturation_warn_threshold - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_backpressure_mode_serde_reject_immediate() {
        let json = r#"{"pool_backpressure_mode": "reject_immediate"}"#;
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        let mode: BackpressureMode =
            serde_json::from_value(value["pool_backpressure_mode"].clone()).unwrap();
        assert_eq!(mode, BackpressureMode::RejectImmediate);
    }

    #[test]
    fn test_backpressure_mode_serde_retry_hint() {
        let json = r#"{"pool_backpressure_mode": "retry_hint"}"#;
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        let mode: BackpressureMode =
            serde_json::from_value(value["pool_backpressure_mode"].clone()).unwrap();
        assert_eq!(mode, BackpressureMode::RetryHint);
    }

    #[test]
    fn test_backpressure_mode_serde_log_and_reject() {
        let json = r#"{"pool_backpressure_mode": "log_and_reject"}"#;
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        let mode: BackpressureMode =
            serde_json::from_value(value["pool_backpressure_mode"].clone()).unwrap();
        assert_eq!(mode, BackpressureMode::LogAndReject);
    }
}
