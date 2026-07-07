/// Protocol-agnostic TCP connection pooling with optional TLS
///
/// This module provides a generic connection pool that works with any
/// database protocol implementing the Protocol trait. The pool manages
/// TCP or TLS connections and delegates protocol-specific behavior (state
/// reset, health checks) to the Protocol implementation.
use crate::config::{ConnectionRetryConfig, TlsConfig, TlsSslMode};
use crate::protocol::{Protocol, ProtocolConfig};
use crate::resilience::{CircuitBreaker, RetryStrategy};
use crate::tls::{load_server_tls_config, upgrade_backend_to_tls, BackendTransport};
use anyhow::{Context, Result};
use async_trait::async_trait;
use deadpool::managed::{Manager, Pool, PoolError, QueueMode, RecycleResult, Timeouts};
use rustls::ClientConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

/// Pooled backend connection wrapper
///
/// This type wraps a pooled backend connection (TCP or TLS).
/// When dropped, the connection is automatically returned to the pool.
pub(crate) type PooledConnection = deadpool::managed::Object<BackendTransportManager>;

/// TCP connection pool for database backends
///
/// This pool is protocol-agnostic and works with any Protocol implementation.
/// It manages TCP or TLS streams and uses the Protocol trait for lifecycle hooks.
/// Includes circuit breaker and retry logic for resilience.
pub struct TcpConnectionPool {
    pool: Pool<BackendTransportManager>,
    protocol: Arc<dyn Protocol>,
    config: ProtocolConfig,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
    retry_config: Option<ConnectionRetryConfig>,
}

impl TcpConnectionPool {
    /// Create a new TCP connection pool with optional TLS
    ///
    /// # Arguments
    /// * `protocol` - Protocol implementation (Postgres, MySQL, etc.)
    /// * `config` - Protocol-agnostic connection configuration
    /// * `tls_config` - TLS configuration for backend connections
    /// * `max_size` - Maximum number of connections in the pool
    /// * `min_idle` - Minimum number of idle connections to maintain
    /// * `circuit_breaker` - Optional circuit breaker for resilience
    /// * `retry_config` - Optional retry configuration
    /// * `lifo` - Use LIFO (last-in-first-out) connection selection for better cache locality
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        protocol: Arc<dyn Protocol>,
        config: ProtocolConfig,
        tls_config: &TlsConfig,
        max_size: usize,
        min_idle: Option<usize>,
        circuit_breaker: Option<Arc<CircuitBreaker>>,
        retry_config: Option<ConnectionRetryConfig>,
        lifo: bool,
        connect_timeout_ms: u64,
    ) -> Result<Self> {
        let queue_mode = if lifo { QueueMode::Lifo } else { QueueMode::Fifo };
        let connect_timeout = Duration::from_millis(connect_timeout_ms);

        // Load TLS configuration for backend connections
        let server_tls_config = load_server_tls_config(tls_config)
            .context("Failed to load server TLS configuration")?;

        info!(
            protocol = protocol.name(),
            backend_addr = %config.backend_addr(),
            max_size = max_size,
            min_idle = ?min_idle,
            circuit_breaker_enabled = circuit_breaker.is_some(),
            retry_enabled = retry_config.is_some(),
            tls_enabled = server_tls_config.is_some(),
            sslmode = ?tls_config.server_tls_sslmode,
            lifo = lifo,
            "Creating backend connection pool"
        );

        let manager = BackendTransportManager {
            backend_addr: config.backend_addr(),
            backend_host: config.host.clone(),
            protocol: Arc::clone(&protocol),
            tls_config: server_tls_config,
            tls_sslmode: tls_config.server_tls_sslmode.clone(),
            connect_timeout,
        };

        // Bound deadpool's own create/recycle phases as a backstop to the
        // in-`create` connect timeout (P3 §4.5). `wait` is left to the
        // pool-manager wait queue, which already applies its own timeout.
        let timeouts =
            Timeouts { wait: None, create: Some(connect_timeout), recycle: Some(connect_timeout) };

        let mut builder =
            Pool::builder(manager).max_size(max_size).queue_mode(queue_mode).timeouts(timeouts);

        if let Some(min) = min_idle {
            builder = builder.runtime(deadpool::Runtime::Tokio1);
            // Note: deadpool doesn't have a direct min_idle, but we can
            // pre-warm the pool by creating connections on startup
            debug!(min_idle = min, "Pool min_idle configured");
        }

        let pool = builder
            .runtime(deadpool::Runtime::Tokio1)
            .build()
            .context("Failed to create connection pool")?;

        info!("Backend connection pool created successfully");

        Ok(Self { pool, protocol, config, circuit_breaker, retry_config })
    }

    /// Get a connection from the pool with circuit breaker and retry protection
    ///
    /// This will either:
    /// 1. Return an idle connection from the pool (after health check)
    /// 2. Create a new connection if pool not at max size
    /// 3. Wait for a connection to become available
    ///
    /// Circuit breaker and retry logic are applied if configured.
    pub async fn get(&self) -> Result<PooledConnection> {
        // 1. Check circuit breaker
        if let Some(ref cb) = self.circuit_breaker {
            cb.allow_request().map_err(|e| anyhow::anyhow!("Circuit breaker: {}", e))?;
        }

        // 2. Try to get connection (with retries if enabled)
        let result = if let Some(ref retry_config) = self.retry_config {
            if retry_config.enabled {
                self.get_with_retry(retry_config).await
            } else {
                self.pool
                    .get()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to get connection from pool: {}", e))
            }
        } else {
            self.pool
                .get()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get connection from pool: {}", e))
        };

        // 3. Record circuit breaker result
        if let Some(ref cb) = self.circuit_breaker {
            match &result {
                Ok(_) => cb.record_success(),
                Err(_) => cb.record_failure(),
            }
        }

        result
    }

    /// Get connection with retry logic.
    ///
    /// Only *transient* pool errors (a backend connect/transport failure or an
    /// acquisition timeout) are retried; *permanent* ones (a closed pool, a
    /// runtime/config error) fail fast (P3 §4.5, §5.5). Retries wrap connection
    /// acquisition only — no query bytes are ever replayed.
    async fn get_with_retry(
        &self,
        retry_config: &ConnectionRetryConfig,
    ) -> Result<PooledConnection> {
        let retry_strategy = RetryStrategy::new(retry_config.clone());

        retry_strategy
            .execute_with_classifier(|| self.pool.get(), Self::pool_error_is_retryable)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get connection: {}", e))
    }

    /// Classify a deadpool error as retryable. Transient backend/transport
    /// failures and acquisition timeouts are worth retrying; a closed pool or a
    /// runtime/hook misconfiguration is permanent and must not be retried.
    fn pool_error_is_retryable(err: &PoolError<anyhow::Error>) -> bool {
        matches!(err, PoolError::Backend(_) | PoolError::Timeout(_))
    }

    /// Get pool status information
    pub fn status(&self) -> PoolStatus {
        let status = self.pool.status();
        PoolStatus {
            size: status.size,
            available: status.available,
            max_size: status.max_size,
            protocol: self.protocol.name(),
            backend_addr: self.config.backend_addr(),
        }
    }

    /// Get the protocol being used
    pub fn protocol(&self) -> &dyn Protocol {
        self.protocol.as_ref()
    }

    /// Pre-warm the pool by creating connections up to min_idle count
    ///
    /// This should be called after pool creation but before accepting client
    /// connections. Connections are created in parallel for faster warmup.
    ///
    /// # Arguments
    /// * `count` - Number of connections to pre-create (typically pool_min_idle)
    ///
    /// # Returns
    /// The number of connections successfully created
    pub async fn warmup(&self, count: usize) -> usize {
        if count == 0 {
            return 0;
        }

        info!(target_count = count, "Warming up connection pool");

        // Create connections in parallel using join_all
        let futures: Vec<_> = (0..count)
            .map(|_| async {
                match self.pool.get().await {
                    Ok(conn) => {
                        // Connection is created and will be returned to pool when dropped
                        drop(conn);
                        true
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create warmup connection");
                        false
                    }
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        let created = results.iter().filter(|&&success| success).count();

        if created < count {
            warn!(
                created = created,
                target = count,
                "Pool warmup incomplete - some connections failed"
            );
        } else {
            info!(created = created, "Pool warmup complete");
        }

        created
    }
}

/// Pool status information for monitoring/metrics
#[derive(Debug, Clone)]
pub struct PoolStatus {
    pub size: usize,
    pub available: usize,
    pub max_size: usize,
    pub protocol: &'static str,
    pub backend_addr: String,
}

/// Manager for backend transport connections (TCP or TLS)
///
/// Implements deadpool's Manager trait to handle connection lifecycle:
/// - Creating new connections (with optional TLS upgrade)
/// - Recycling connections between uses
pub struct BackendTransportManager {
    backend_addr: String,
    backend_host: String,
    protocol: Arc<dyn Protocol>,
    tls_config: Option<Arc<ClientConfig>>,
    tls_sslmode: TlsSslMode,
    /// Upper bound on TCP connect + TLS negotiation before the attempt is
    /// abandoned, so a hung/black-holed backend cannot block a connect
    /// indefinitely (P3 §4.5).
    connect_timeout: Duration,
}

#[async_trait]
impl Manager for BackendTransportManager {
    type Type = BackendTransport;
    type Error = anyhow::Error;

    /// Create a new backend connection, optionally upgrading to TLS
    async fn create(&self) -> Result<BackendTransport, Self::Error> {
        debug!(
            backend_addr = %self.backend_addr,
            protocol = self.protocol.name(),
            sslmode = ?self.tls_sslmode,
            "Creating new backend connection"
        );

        // Establish the TCP connection and negotiate TLS under a single deadline
        // so a hung/black-holed backend cannot block indefinitely (P3 §4.5).
        let transport = tokio::time::timeout(self.connect_timeout, async {
            let stream = TcpStream::connect(&self.backend_addr)
                .await
                .context("Failed to connect to backend")?;

            // Disable Nagle's algorithm for lower latency
            stream.set_nodelay(true).context("Failed to set TCP_NODELAY on backend connection")?;

            debug!(backend_addr = %self.backend_addr, "TCP connection established");

            // Then, negotiate SSL if configured
            upgrade_backend_to_tls(
                stream,
                &self.backend_host,
                &self.tls_sslmode,
                self.tls_config.clone(),
            )
            .await
            .context("Failed to negotiate SSL with backend")
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "Timed out after {:?} connecting to backend {}",
                self.connect_timeout,
                self.backend_addr
            )
        })??;

        if transport.is_encrypted() {
            info!(backend_addr = %self.backend_addr, "Backend connection using TLS");
        } else {
            debug!(backend_addr = %self.backend_addr, "Backend connection using plain TCP");
        }

        Ok(transport)
    }

    /// Recycle a connection before returning it to the pool
    ///
    /// This is called when a connection is returned to the pool and is about
    /// to be reused by another client. We delegate to the protocol's
    /// reset_connection method to clear any session state.
    async fn recycle(
        &self,
        conn: &mut BackendTransport,
        _metrics: &deadpool::managed::Metrics,
    ) -> RecycleResult<Self::Error> {
        debug!(protocol = self.protocol.name(), "Recycling backend connection");

        // Health check and reset work differently for plain vs TLS connections
        // For plain TCP, we can use the protocol methods directly
        // For TLS, we have limited health check capability without protocol changes
        match conn {
            BackendTransport::Plain(stream) => {
                // First, check if connection is still healthy
                match self.protocol.health_check(stream).await {
                    Ok(true) => {
                        debug!("Connection health check passed");
                    }
                    Ok(false) => {
                        warn!("Connection failed health check, will be closed");
                        return Err(deadpool::managed::RecycleError::Backend(anyhow::anyhow!(
                            "Connection failed health check"
                        )));
                    }
                    Err(e) => {
                        warn!(error = %e, "Connection health check error, will be closed");
                        return Err(deadpool::managed::RecycleError::Backend(e));
                    }
                }

                // Try to reset connection state
                match self.protocol.reset_connection(stream).await {
                    Ok(true) => {
                        debug!("Connection state reset successfully");
                        Ok(())
                    }
                    Ok(false) => {
                        // Protocol doesn't support reset - close connection
                        debug!("Protocol doesn't support state reset, closing connection");
                        Err(deadpool::managed::RecycleError::Backend(anyhow::anyhow!(
                            "Protocol does not support connection reset"
                        )))
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to reset connection state, will be closed");
                        Err(deadpool::managed::RecycleError::Backend(e))
                    }
                }
            }
            BackendTransport::Tls(stream) => {
                // Health check for TLS connection
                match self.protocol.health_check(stream.as_mut()).await {
                    Ok(true) => debug!("TLS connection health check passed"),
                    Ok(false) => {
                        warn!("TLS connection failed health check, will be closed");
                        return Err(deadpool::managed::RecycleError::Backend(anyhow::anyhow!(
                            "TLS connection failed health check"
                        )));
                    }
                    Err(e) => {
                        warn!(error = %e, "TLS connection health check error, will be closed");
                        return Err(deadpool::managed::RecycleError::Backend(e));
                    }
                }

                // Reset connection state with DISCARD ALL
                match self.protocol.reset_connection(stream.as_mut()).await {
                    Ok(true) => {
                        debug!("TLS connection state reset successfully");
                        Ok(())
                    }
                    Ok(false) => {
                        debug!("TLS protocol doesn't support state reset, closing connection");
                        Err(deadpool::managed::RecycleError::Backend(anyhow::anyhow!(
                            "TLS protocol does not support connection reset"
                        )))
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to reset TLS connection state, will be closed");
                        Err(deadpool::managed::RecycleError::Backend(e))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::postgres::PostgresProtocol;

    #[test]
    fn test_pool_status() {
        let protocol = Arc::new(PostgresProtocol::new()) as Arc<dyn Protocol>;
        let config = ProtocolConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: Some("test".to_string()),
            user: Some("postgres".to_string()),
            password: Some("password".to_string()),
        };
        let tls_config = TlsConfig::default();

        let pool = TcpConnectionPool::new(
            protocol,
            config,
            &tls_config,
            10,
            Some(2),
            None, // circuit_breaker
            None, // retry_config
            true, // lifo
            5000, // connect_timeout_ms
        )
        .unwrap();
        let status = pool.status();

        assert_eq!(status.max_size, 10);
        assert_eq!(status.protocol, "postgres");
        assert_eq!(status.backend_addr, "localhost:5432");
    }

    #[tokio::test]
    async fn test_warmup_zero_count() {
        let protocol = Arc::new(PostgresProtocol::new()) as Arc<dyn Protocol>;
        let config = ProtocolConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: Some("test".to_string()),
            user: Some("postgres".to_string()),
            password: Some("password".to_string()),
        };
        let tls_config = TlsConfig::default();

        let pool = TcpConnectionPool::new(
            protocol,
            config,
            &tls_config,
            10,
            Some(0),
            None,
            None,
            true,
            5000,
        )
        .unwrap();

        // Warmup with 0 should return immediately
        let created = pool.warmup(0).await;
        assert_eq!(created, 0);
    }

    #[test]
    fn test_pool_error_classification() {
        // Permanent errors must not be retried; transient ones should.
        assert!(!TcpConnectionPool::pool_error_is_retryable(&PoolError::Closed));
        assert!(!TcpConnectionPool::pool_error_is_retryable(&PoolError::NoRuntimeSpecified));
        assert!(TcpConnectionPool::pool_error_is_retryable(&PoolError::Backend(anyhow::anyhow!(
            "connection refused"
        ))));
        assert!(TcpConnectionPool::pool_error_is_retryable(&PoolError::Timeout(
            deadpool::managed::TimeoutType::Create
        )));
    }

    /// Fault injection: when the backend is down, the per-backend breaker opens
    /// after the failure threshold and then *sheds* further requests fast with a
    /// clean error instead of repeatedly attempting to connect (P3 §4.1/§5.1).
    #[tokio::test]
    async fn test_breaker_opens_and_sheds_when_backend_down() {
        use crate::config::CircuitBreakerConfig;
        use crate::resilience::CircuitBreaker;

        // A free port with nothing listening → connection refused immediately.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let dead_port = listener.local_addr().unwrap().port();
        drop(listener);

        let protocol = Arc::new(PostgresProtocol::new()) as Arc<dyn Protocol>;
        let config = ProtocolConfig {
            host: "127.0.0.1".to_string(),
            port: dead_port,
            database: Some("test".to_string()),
            user: Some("postgres".to_string()),
            password: Some("postgres".to_string()),
        };
        let tls_config = TlsConfig::default();
        let breaker = Arc::new(CircuitBreaker::new(
            CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 2,
                success_threshold: 2,
                window_secs: 30,
                open_timeout_secs: 60,
                use_health_monitor: false,
            },
            None,
        ));

        let pool = TcpConnectionPool::new(
            protocol,
            config,
            &tls_config,
            4,
            None,
            Some(Arc::clone(&breaker)),
            None, // retry disabled so each get() is one attempt
            true,
            500,
        )
        .expect("pool");

        // The first `failure_threshold` acquisitions fail with real connection
        // errors and trip the breaker.
        for _ in 0..2 {
            assert!(pool.get().await.is_err(), "connect to dead backend should fail");
        }

        // Now the breaker is open: further requests are shed fast with a clean
        // circuit-breaker error, not another connect attempt.
        match pool.get().await {
            Ok(_) => panic!("breaker should shed the request"),
            Err(e) => {
                let msg = format!("{e:?}");
                assert!(msg.contains("Circuit breaker"), "expected breaker shed, got: {msg}");
            }
        }
    }

    /// Fault injection: a backend that black-holes the SYN must not hang the
    /// connect forever — the connect timeout must fire promptly (P3 §4.5).
    #[tokio::test]
    async fn test_connect_timeout_fires_on_unreachable_backend() {
        use std::time::Instant;

        let protocol = Arc::new(PostgresProtocol::new()) as Arc<dyn Protocol>;
        // TEST-NET-3 (203.0.113.0/24, RFC 5737) — reserved and unroutable, so
        // the SYN is dropped/rejected rather than answered.
        let config = ProtocolConfig {
            host: "203.0.113.1".to_string(),
            port: 5432,
            database: Some("test".to_string()),
            user: Some("postgres".to_string()),
            password: Some("postgres".to_string()),
        };
        let tls_config = TlsConfig::default();

        // 800ms connect budget.
        let pool =
            TcpConnectionPool::new(protocol, config, &tls_config, 2, None, None, None, true, 800)
                .expect("pool");

        let start = Instant::now();
        let result = pool.get().await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "connect to unreachable backend should fail");
        assert!(
            elapsed < Duration::from_secs(5),
            "connect must fail promptly (bounded by the timeout), took {elapsed:?}"
        );
    }
}
