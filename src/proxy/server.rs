use super::{ConnectionHandler, EventBatcher, TcpConnectionPool};
use crate::config::{Config, PoolingStrategy};
use crate::observability::ProxyMetrics;
use crate::protocol::{Protocol, ProtocolConfig, ProtocolRegistry};
use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

/// The main proxy server that accepts client connections
pub struct ProxyServer {
    config: Arc<Config>,
    listener: TcpListener,
    batcher: Arc<EventBatcher>,
    pool: Option<Arc<TcpConnectionPool>>,
    protocol: Arc<dyn Protocol>,
    metrics: Arc<ProxyMetrics>,
}

impl ProxyServer {
    /// Create a new proxy server with the given configuration
    pub async fn new(config: Config, batcher: EventBatcher, metrics: Arc<ProxyMetrics>) -> Result<Self> {
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

            let pool = TcpConnectionPool::new(
                Arc::clone(&protocol),
                protocol_config,
                config.performance.pool_size,
                Some(config.performance.pool_min_idle),
            )
            .context("Failed to create TCP connection pool")?;

            info!("TCP connection pool created successfully");
            Some(Arc::new(pool))
        } else {
            info!("Connection pooling disabled, using direct connections");
            None
        };

        Ok(Self {
            config: Arc::new(config),
            listener,
            batcher: Arc::new(batcher),
            pool,
            protocol,
            metrics,
        })
    }

    /// Get the local address the server is listening on
    /// Useful for tests when binding to port 0 (OS-assigned port)
    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        self.listener.local_addr().context("Failed to get local address")
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
                            let pool = self.pool.clone();
                            let metrics = Arc::clone(&self.metrics);

                            // Spawn a task to handle this connection and track it
                            connection_tasks.spawn(async move {
                                let handler = ConnectionHandler::new(
                                    client_stream,
                                    client_addr,
                                    conn_id,
                                    config,
                                    batcher,
                                    pool,
                                    metrics,
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
                if remaining % 10 == 0 || remaining < 10 {
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
