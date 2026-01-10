use super::{
    ConnectionHandler, EventBatcher, PoolManager, PoolManagerConfig, TcpConnectionPool, WaitQueue,
};
use crate::config::{Config, PoolingStrategy, TlsSslMode};
use crate::observability::ProxyMetrics;
use crate::protocol::{Protocol, ProtocolConfig, ProtocolRegistry};
use crate::resilience::{ActiveHealthcheck, CircuitBreaker};
use crate::tls::{handle_ssl_startup, load_client_tls_config, ClientTransport, SslStartupResult};
use anyhow::{Context, Result};
use rustls::ServerConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

/// The main proxy server that accepts client connections
pub struct ProxyServer {
    config: Arc<Config>,
    listener: TcpListener,
    batcher: Arc<EventBatcher>,
    pool: Option<Arc<TcpConnectionPool>>,
    pool_manager: Option<Arc<PoolManager>>,
    metrics: Arc<ProxyMetrics>,
    tls_config: Option<Arc<ServerConfig>>,
}

impl ProxyServer {
    /// Create a new proxy server with the given configuration
    pub async fn new(
        config: Config,
        batcher: EventBatcher,
        metrics: Arc<ProxyMetrics>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(&config.proxy.listen_address)
            .await
            .context("Failed to bind proxy listener")?;

        info!(
            listen_address = %config.proxy.listen_address,
            "Proxy server listener bound"
        );

        // Get protocol implementation for the configured backend
        let protocol: Arc<dyn Protocol> = ProtocolRegistry::get(&config.backend.protocol)
            .context("Failed to get protocol implementation")?
            .into();

        info!(
            protocol = protocol.name(),
            backend = %format!("{}:{}", config.backend.host, config.backend.port),
            "Protocol initialized"
        );

        // Start active healthcheck background task if enabled
        if config.resilience.healthcheck.active_enabled {
            let healthcheck_config = config.resilience.healthcheck.clone();
            let protocol_config = ProtocolConfig {
                host: config.backend.host.clone(),
                port: config.backend.port,
                database: Some(config.backend.database.clone()),
                user: Some(config.backend.user.clone()),
                password: Some(config.backend.password.clone()),
            };

            let healthcheck = Arc::new(ActiveHealthcheck::new(
                healthcheck_config.clone(),
                Arc::clone(&protocol),
                protocol_config,
            ));

            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_secs(healthcheck_config.interval_secs));

                loop {
                    interval.tick().await;

                    match healthcheck.check().await {
                        Ok(is_healthy) => {
                            if is_healthy {
                                tracing::debug!("Active healthcheck passed");
                            } else {
                                tracing::warn!("Active healthcheck failed threshold");
                            }
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Active healthcheck error");
                        }
                    }
                }
            });

            info!(
                interval_secs = config.resilience.healthcheck.interval_secs,
                "Active healthcheck loop started"
            );
        } else {
            info!("Active healthcheck disabled");
        }

        // Create connection pool if pooling is enabled
        let pool = if config.performance.connection_pooling != PoolingStrategy::Disabled {
            info!(
                strategy = ?config.performance.connection_pooling,
                protocol = protocol.name(),
                pool_size = config.performance.pool_size,
                pool_min_idle = config.performance.pool_min_idle,
                "Creating TCP connection pool"
            );

            let protocol_config = ProtocolConfig {
                host: config.backend.host.clone(),
                port: config.backend.port,
                database: Some(config.backend.database.clone()),
                user: Some(config.backend.user.clone()),
                password: Some(config.backend.password.clone()),
            };

            // Create circuit breaker if enabled
            let circuit_breaker = if config.resilience.circuit_breaker.enabled {
                let health_monitor = if config.resilience.circuit_breaker.use_health_monitor {
                    Some(Arc::clone(metrics.health_monitor()))
                } else {
                    None
                };

                let cb = Arc::new(CircuitBreaker::new(
                    config.resilience.circuit_breaker.clone(),
                    health_monitor,
                ));

                // Store circuit breaker in metrics for observability
                metrics.set_circuit_breaker(Some(Arc::clone(&cb)));

                info!("Circuit breaker created and enabled");
                Some(cb)
            } else {
                info!("Circuit breaker disabled");
                metrics.set_circuit_breaker(None);
                None
            };

            // Pass retry config if enabled
            let retry_config = if config.resilience.connection_retry.enabled {
                info!("Connection retries enabled");
                Some(config.resilience.connection_retry.clone())
            } else {
                info!("Connection retries disabled");
                None
            };

            let pool = TcpConnectionPool::new(
                Arc::clone(&protocol),
                protocol_config,
                &config.tls,
                config.performance.pool_size,
                Some(config.performance.pool_min_idle),
                circuit_breaker,
                retry_config,
                config.performance.pool_lifo,
            )
            .context("Failed to create TCP connection pool")?;

            if config.tls.server_tls_sslmode != TlsSslMode::Disable {
                info!(
                    sslmode = ?config.tls.server_tls_sslmode,
                    "Backend TLS enabled"
                );
            }

            info!("TCP connection pool created successfully");
            Some(Arc::new(pool))
        } else {
            info!("Connection pooling disabled, using direct connections");
            None
        };

        // Create PoolManager if pooling is enabled
        let pool_manager = if let Some(ref pool) = pool {
            let wait_queue = WaitQueue::new(config.performance.pool_queue_depth);
            let pm_config = PoolManagerConfig {
                lifo: config.performance.pool_lifo,
                queue_depth: config.performance.pool_queue_depth,
                idle_unpin_secs: config.performance.pool_idle_unpin_secs,
                wait_timeout_ms: config.performance.pool_timeout_secs * 1000,
            };
            info!(
                lifo = pm_config.lifo,
                queue_depth = pm_config.queue_depth,
                idle_unpin_secs = pm_config.idle_unpin_secs,
                "Creating PoolManager"
            );
            Some(PoolManager::new(Arc::clone(pool), wait_queue, pm_config))
        } else {
            None
        };

        // Load TLS configuration for client connections
        let tls_config =
            load_client_tls_config(&config.tls).context("Failed to load TLS configuration")?;

        if tls_config.is_some() {
            info!(
                sslmode = ?config.tls.client_tls_sslmode,
                "Client TLS enabled"
            );
        } else {
            info!("Client TLS disabled");
        }

        Ok(Self {
            config: Arc::new(config),
            listener,
            batcher: Arc::new(batcher),
            pool,
            pool_manager,
            metrics,
            tls_config,
        })
    }

    /// Get the local address the server is listening on
    /// Useful for tests when binding to port 0 (OS-assigned port)
    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        self.listener.local_addr().context("Failed to get local address")
    }

    /// Get the pool manager, if pooling is enabled
    pub fn pool_manager(&self) -> Option<&Arc<PoolManager>> {
        self.pool_manager.as_ref()
    }

    /// Run the proxy server, accepting connections until shutdown signal
    pub async fn run(self) -> Result<()> {
        info!(
            listen_address = %self.config.proxy.listen_address,
            max_connections = self.config.proxy.max_connections,
            shutdown_timeout_secs = self.config.proxy.shutdown_timeout_secs,
            "Proxy server starting"
        );

        let mut connection_count = 0u64;
        let mut connection_tasks = JoinSet::new();

        // Setup shutdown signal handling
        let shutdown = async {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Received Ctrl+C signal");
                }
                Err(e) => {
                    error!(error = %e, "Failed to listen for Ctrl+C signal");
                }
            }
        };

        tokio::pin!(shutdown);

        // Accept connections until shutdown signal
        loop {
            tokio::select! {
                // Handle shutdown signal
                _ = &mut shutdown => {
                    info!("Shutdown signal received, stopping new connections");
                    break;
                }

                // Accept new connections
                accept_result = self.listener.accept() => {
                    match accept_result {
                        Ok((client_stream, client_addr)) => {
                            connection_count += 1;
                            let conn_id = connection_count;

                            info!(
                                connection_id = conn_id,
                                client_addr = %client_addr,
                                "Accepted client connection"
                            );

                            let config = Arc::clone(&self.config);
                            let batcher = Arc::clone(&self.batcher);
                            let pool_manager = self.pool_manager.clone();
                            let metrics = Arc::clone(&self.metrics);
                            let tls_config = self.tls_config.clone();

                            // Spawn a task to handle this connection and track it
                            connection_tasks.spawn(async move {
                                // Handle SSL startup handshake
                                let (transport, startup_data) = match handle_ssl_startup(
                                    client_stream,
                                    &config.tls.client_tls_sslmode,
                                    tls_config,
                                ).await {
                                    Ok(SslStartupResult::Upgraded(transport)) => {
                                        info!(
                                            connection_id = conn_id,
                                            "Connection upgraded to TLS"
                                        );
                                        (transport, Vec::new())
                                    }
                                    Ok(SslStartupResult::Declined(stream, startup_data)) => {
                                        debug!(
                                            connection_id = conn_id,
                                            "SSL declined, continuing with plain TCP"
                                        );
                                        (ClientTransport::Plain(stream), startup_data)
                                    }
                                    Ok(SslStartupResult::NoSslRequest(stream, startup_data)) => {
                                        debug!(
                                            connection_id = conn_id,
                                            "No SSL request, continuing with plain TCP"
                                        );
                                        (ClientTransport::Plain(stream), startup_data)
                                    }
                                    Err(e) => {
                                        error!(
                                            connection_id = conn_id,
                                            error = %e,
                                            "SSL startup failed"
                                        );
                                        return;
                                    }
                                };

                                let handler = ConnectionHandler::new(
                                    transport,
                                    client_addr,
                                    conn_id,
                                    config,
                                    batcher,
                                    pool_manager,
                                    metrics,
                                    startup_data,
                                );

                                if let Err(e) = handler.handle().await {
                                    error!(
                                        connection_id = conn_id,
                                        client_addr = %client_addr,
                                        error = %e,
                                        "Connection handler failed"
                                    );
                                }

                                info!(
                                    connection_id = conn_id,
                                    client_addr = %client_addr,
                                    "Connection closed"
                                );
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to accept connection");
                        }
                    }
                }
            }
        }

        // Graceful shutdown: wait for active connections to drain
        self.drain_connections(connection_tasks).await;

        // EventBatcher will be dropped here, which closes the channel
        // and triggers flush of remaining events + publisher shutdown
        info!("Proxy server shutdown complete");
        Ok(())
    }

    /// Wait for active connections to complete, with timeout
    async fn drain_connections(&self, mut connection_tasks: JoinSet<()>) {
        let active_count = connection_tasks.len();

        if active_count == 0 {
            info!("No active connections to drain");
            return;
        }

        info!(
            active_connections = active_count,
            timeout_secs = self.config.proxy.shutdown_timeout_secs,
            "Draining active connections"
        );

        let timeout = Duration::from_secs(self.config.proxy.shutdown_timeout_secs);
        let drain_start = std::time::Instant::now();

        // Wait for connections to finish, with timeout
        let drain_result = tokio::time::timeout(timeout, async {
            while let Some(result) = connection_tasks.join_next().await {
                if let Err(e) = result {
                    warn!(error = %e, "Connection task panicked during shutdown");
                }

                let remaining = connection_tasks.len();
                if remaining.is_multiple_of(10) || remaining < 10 {
                    info!(
                        remaining_connections = remaining,
                        elapsed_secs = drain_start.elapsed().as_secs(),
                        "Draining connections"
                    );
                }
            }
        })
        .await;

        match drain_result {
            Ok(_) => {
                info!(
                    elapsed_secs = drain_start.elapsed().as_secs(),
                    "All connections drained successfully"
                );
            }
            Err(_) => {
                let remaining = connection_tasks.len();
                warn!(
                    remaining_connections = remaining,
                    timeout_secs = self.config.proxy.shutdown_timeout_secs,
                    "Shutdown timeout reached, {} connections still active",
                    remaining
                );

                // Abort remaining tasks
                connection_tasks.shutdown().await;
                warn!("Forcefully closed remaining connections");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::observability::{HealthConfig, ProxyMetrics};
    use crate::proxy::EventBatcher;
    use crate::publisher::DebugLoggerPublisher;

    fn create_test_config() -> Config {
        let mut config = Config::default();
        // Use port 0 to get an available port from the OS
        config.proxy.listen_address = "127.0.0.1:0".to_string();
        // Disable active healthchecks for tests
        config.resilience.healthcheck.active_enabled = false;
        config
    }

    #[tokio::test]
    async fn test_server_creates_pool_manager() {
        // This test verifies PoolManager is created and accessible
        let config = create_test_config();
        let publisher = Arc::new(DebugLoggerPublisher::new());
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        let server = ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), metrics)
            .await
            .unwrap();

        // Server should expose pool_manager for testing
        assert!(server.pool_manager().is_some());
    }

    #[tokio::test]
    async fn test_server_no_pool_manager_when_pooling_disabled() {
        let mut config = create_test_config();
        config.performance.connection_pooling = PoolingStrategy::Disabled;
        let publisher = Arc::new(DebugLoggerPublisher::new());
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        let server = ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), metrics)
            .await
            .unwrap();

        assert!(server.pool_manager().is_none());
    }
}
