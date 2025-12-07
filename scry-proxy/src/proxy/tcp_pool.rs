/// Protocol-agnostic TCP connection pooling
///
/// This module provides a generic connection pool that works with any
/// database protocol implementing the Protocol trait. The pool manages
/// raw TCP connections and delegates protocol-specific behavior (state
/// reset, health checks) to the Protocol implementation.

use crate::config::ConnectionRetryConfig;
use crate::protocol::{Protocol, ProtocolConfig};
use crate::resilience::{CircuitBreaker, RetryStrategy};
use anyhow::{Context, Result};
use async_trait::async_trait;
use deadpool::managed::{Manager, Pool, RecycleResult};
use std::sync::Arc;
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

/// Pooled TCP connection wrapper
///
/// This type wraps a pooled TCP stream connection. When dropped, the connection
/// is automatically returned to the pool for reuse.
pub(crate) type PooledConnection = deadpool::managed::Object<TcpStreamManager>;

/// TCP connection pool for database backends
///
/// This pool is protocol-agnostic and works with any Protocol implementation.
/// It manages raw TCP streams and uses the Protocol trait for lifecycle hooks.
/// Includes circuit breaker and retry logic for resilience.
pub struct TcpConnectionPool {
    pool: Pool<TcpStreamManager>,
    protocol: Arc<dyn Protocol>,
    config: ProtocolConfig,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
    retry_config: Option<ConnectionRetryConfig>,
}

impl TcpConnectionPool {
    /// Create a new TCP connection pool
    ///
    /// # Arguments
    /// * `protocol` - Protocol implementation (Postgres, MySQL, etc.)
    /// * `config` - Protocol-agnostic connection configuration
    /// * `max_size` - Maximum number of connections in the pool
    /// * `min_idle` - Minimum number of idle connections to maintain
    /// * `circuit_breaker` - Optional circuit breaker for resilience
    /// * `retry_config` - Optional retry configuration
    pub fn new(
        protocol: Arc<dyn Protocol>,
        config: ProtocolConfig,
        max_size: usize,
        min_idle: Option<usize>,
        circuit_breaker: Option<Arc<CircuitBreaker>>,
        retry_config: Option<ConnectionRetryConfig>,
    ) -> Result<Self> {
        info!(
            protocol = protocol.name(),
            backend_addr = %config.backend_addr(),
            max_size = max_size,
            min_idle = ?min_idle,
            circuit_breaker_enabled = circuit_breaker.is_some(),
            retry_enabled = retry_config.is_some(),
            "Creating TCP connection pool"
        );

        let manager = TcpStreamManager {
            backend_addr: config.backend_addr(),
            protocol: Arc::clone(&protocol),
        };

        let mut builder = Pool::builder(manager).max_size(max_size);

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

        info!("TCP connection pool created successfully");

        Ok(Self {
            pool,
            protocol,
            config,
            circuit_breaker,
            retry_config,
        })
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
            cb.allow_request()
                .map_err(|e| anyhow::anyhow!("Circuit breaker: {}", e))?;
        }

        // 2. Try to get connection (with retries if enabled)
        let result = if let Some(ref retry_config) = self.retry_config {
            if retry_config.enabled {
                self.get_with_retry(retry_config).await
            } else {
                self.pool.get().await
                    .map_err(|e| anyhow::anyhow!("Failed to get connection from pool: {}", e))
            }
        } else {
            self.pool.get().await
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

    /// Get connection with retry logic
    async fn get_with_retry(&self, retry_config: &ConnectionRetryConfig) -> Result<PooledConnection> {
        let retry_strategy = RetryStrategy::new(retry_config.clone());

        retry_strategy.execute(|| async {
            self.pool.get().await
                .map_err(|e| anyhow::anyhow!("Failed to get connection: {}", e))
        }).await
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

/// Manager for raw TCP stream connections
///
/// Implements deadpool's Manager trait to handle connection lifecycle:
/// - Creating new connections
/// - Recycling connections between uses
pub struct TcpStreamManager {
    backend_addr: String,
    protocol: Arc<dyn Protocol>,
}

#[async_trait]
impl Manager for TcpStreamManager {
    type Type = TcpStream;
    type Error = anyhow::Error;

    /// Create a new TCP connection to the backend
    async fn create(&self) -> Result<TcpStream, Self::Error> {
        debug!(
            backend_addr = %self.backend_addr,
            protocol = self.protocol.name(),
            "Creating new TCP connection"
        );

        let stream = TcpStream::connect(&self.backend_addr)
            .await
            .context("Failed to connect to backend")?;

        debug!(
            backend_addr = %self.backend_addr,
            "TCP connection established"
        );

        Ok(stream)
    }

    /// Recycle a connection before returning it to the pool
    ///
    /// This is called when a connection is returned to the pool and is about
    /// to be reused by another client. We delegate to the protocol's
    /// reset_connection method to clear any session state.
    async fn recycle(
        &self,
        conn: &mut TcpStream,
        _metrics: &deadpool::managed::Metrics,
    ) -> RecycleResult<Self::Error> {
        debug!(
            protocol = self.protocol.name(),
            "Recycling connection"
        );

        // First, check if connection is still healthy
        match self.protocol.health_check(conn).await {
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
        match self.protocol.reset_connection(conn).await {
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

        let pool = TcpConnectionPool::new(
            protocol,
            config,
            10,
            Some(2),
            None, // circuit_breaker
            None, // retry_config
        )
        .unwrap();
        let status = pool.status();

        assert_eq!(status.max_size, 10);
        assert_eq!(status.protocol, "postgres");
        assert_eq!(status.backend_addr, "localhost:5432");
    }
}
