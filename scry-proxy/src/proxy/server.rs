use super::{
    ConnectionHandler, EventBatcher, PoolManager, PoolManagerConfig, TcpConnectionPool, WaitQueue,
};
use crate::admin::{AdminConsole, AdminResponse, ADMIN_DATABASE};
use crate::auth::FileAuthenticator;
use crate::config::{AuthType, Config, PoolingStrategy, TlsSslMode};
use crate::observability::ProxyMetrics;
use crate::protocol::{Protocol, ProtocolConfig, ProtocolRegistry, StartupMessage};
use crate::resilience::{ActiveHealthcheck, CircuitBreaker};
use crate::tls::{handle_ssl_startup, load_client_tls_config, ClientTransport, SslStartupResult};
use anyhow::{Context, Result};
use rustls::ServerConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

/// The main proxy server that accepts client connections
pub struct ProxyServer {
    config: Arc<Config>,
    listener: TcpListener,
    batcher: Arc<EventBatcher>,
    pool_manager: Option<Arc<PoolManager>>,
    metrics: Arc<ProxyMetrics>,
    tls_config: Option<Arc<ServerConfig>>,
    authenticator: Option<Arc<FileAuthenticator>>,
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

        // Load authenticator if auth_file is configured
        let authenticator = if let Some(ref auth_file) = config.auth.auth_file {
            let auth = FileAuthenticator::from_file(auth_file)
                .context(format!("Failed to load auth file: {}", auth_file))?;
            info!(
                auth_type = ?config.auth.auth_type,
                auth_file = %auth_file,
                users = auth.len(),
                "Authentication enabled with {} users",
                auth.len()
            );
            Some(Arc::new(auth))
        } else if config.auth.auth_type != AuthType::Trust {
            warn!(
                auth_type = ?config.auth.auth_type,
                "Auth type set but no auth_file configured, falling back to trust"
            );
            None
        } else {
            info!("Authentication disabled (trust mode)");
            None
        };

        Ok(Self {
            config: Arc::new(config),
            listener,
            batcher: Arc::new(batcher),
            pool_manager,
            metrics,
            tls_config,
            authenticator,
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

    /// Get the authenticator, if authentication is enabled
    pub fn authenticator(&self) -> Option<&Arc<FileAuthenticator>> {
        self.authenticator.as_ref()
    }

    /// Run the proxy server, accepting connections until shutdown signal
    pub async fn run(self) -> Result<()> {
        info!(
            listen_address = %self.config.proxy.listen_address,
            max_connections = self.config.proxy.max_connections,
            shutdown_timeout_secs = self.config.proxy.shutdown_timeout_secs,
            "Proxy server starting"
        );

        // Spawn idle cleanup background task if pool_manager exists and idle_unpin_secs > 0
        if let Some(ref pool_manager) = self.pool_manager {
            let idle_interval = self.config.performance.pool_idle_unpin_secs;

            if idle_interval > 0 {
                let cleanup_interval_secs = std::cmp::max(1, idle_interval / 2);
                let pm = Arc::clone(pool_manager);
                tokio::spawn(async move {
                    let mut interval =
                        tokio::time::interval(Duration::from_secs(cleanup_interval_secs));
                    loop {
                        interval.tick().await;
                        let cleaned = pm.cleanup_idle();
                        if cleaned > 0 {
                            debug!(cleaned, "Cleaned up idle sticky connections");
                        }
                    }
                });

                info!(
                    interval_secs = cleanup_interval_secs,
                    idle_unpin_secs = idle_interval,
                    "Idle cleanup background task started"
                );
            }
        }

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
                            let authenticator = self.authenticator.clone();

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

                                // Check if this is an admin console connection
                                // by parsing the startup message to see if database = "pgbouncer"
                                let is_admin = if !startup_data.is_empty() {
                                    if let Some(startup) = StartupMessage::parse(&startup_data) {
                                        if let Some(db) = startup.database() {
                                            db.eq_ignore_ascii_case(ADMIN_DATABASE)
                                        } else {
                                            false
                                        }
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                };

                                if is_admin {
                                    info!(
                                        connection_id = conn_id,
                                        client_addr = %client_addr,
                                        "Routing to admin console"
                                    );

                                    if let Err(e) = handle_admin_connection(
                                        transport,
                                        pool_manager,
                                        metrics,
                                    ).await {
                                        error!(
                                            connection_id = conn_id,
                                            client_addr = %client_addr,
                                            error = %e,
                                            "Admin console connection failed"
                                        );
                                    }
                                } else {
                                    let handler = ConnectionHandler::new(
                                        transport,
                                        client_addr,
                                        conn_id,
                                        config,
                                        batcher,
                                        pool_manager,
                                        metrics,
                                        startup_data,
                                        authenticator,
                                    );

                                    if let Err(e) = handler.handle().await {
                                        error!(
                                            connection_id = conn_id,
                                            client_addr = %client_addr,
                                            error = %e,
                                            "Connection handler failed"
                                        );
                                    }
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

/// Handle an admin console connection
///
/// This handles connections to the virtual "pgbouncer" database,
/// which provides administrative commands for monitoring and controlling the proxy.
async fn handle_admin_connection(
    mut client: ClientTransport,
    pool_manager: Option<Arc<PoolManager>>,
    metrics: Arc<ProxyMetrics>,
) -> Result<()> {
    // Send AuthenticationOk
    // Format: 'R' + length(8) + auth_type(0)
    let auth_ok = [b'R', 0, 0, 0, 8, 0, 0, 0, 0];
    client.write_all(&auth_ok).await.context("Failed to send AuthenticationOk")?;

    // Send ReadyForQuery (idle state)
    // Format: 'Z' + length(5) + status('I')
    let ready_for_query = [b'Z', 0, 0, 0, 5, b'I'];
    client.write_all(&ready_for_query).await.context("Failed to send ReadyForQuery")?;

    let admin = AdminConsole::new(pool_manager, metrics);
    let mut buffer = vec![0u8; 8192];

    loop {
        let n = client.read(&mut buffer).await.context("Failed to read from admin client")?;
        if n == 0 {
            debug!("Admin client closed connection");
            break;
        }

        // Check for Query message ('Q')
        if buffer[0] == b'Q' {
            // Parse query: 'Q' + length(4) + query_string (null-terminated)
            if n >= 5 {
                let length = i32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
                if n >= 5 + length - 4 {
                    // Query string is null-terminated
                    let query_end = 5 + length - 4 - 1; // -4 for length already counted, -1 for null
                    let query = String::from_utf8_lossy(&buffer[5..query_end]).to_string();

                    debug!(query = %query, "Admin console query");

                    let response = match admin.execute(&query).await {
                        Ok(resp) => resp,
                        Err(e) => AdminResponse::Error {
                            message: e.to_string(),
                        },
                    };

                    let wire_response = response.to_wire();
                    client.write_all(&wire_response).await.context("Failed to send admin response")?;
                }
            }
        } else if buffer[0] == b'X' {
            // Terminate message
            debug!("Admin client sent Terminate");
            break;
        }
        // Ignore other message types (Sync, etc.)
    }

    info!("Admin console connection closed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthType, Config};
    use crate::observability::{HealthConfig, ProxyMetrics};
    use crate::proxy::EventBatcher;
    use crate::publisher::DebugLoggerPublisher;
    use std::io::Write;

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

    #[tokio::test]
    async fn test_server_no_authenticator_in_trust_mode() {
        let config = create_test_config();
        let publisher = Arc::new(DebugLoggerPublisher::new());
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        let server = ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), metrics)
            .await
            .unwrap();

        // Trust mode should not have an authenticator
        assert!(server.authenticator().is_none());
    }

    #[tokio::test]
    async fn test_server_loads_authenticator_from_file() {
        // Create a temp auth file
        let mut auth_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(auth_file, "\"testuser\" \"testpass\"").unwrap();
        auth_file.flush().unwrap();

        let mut config = create_test_config();
        config.auth.auth_type = AuthType::Md5;
        config.auth.auth_file = Some(auth_file.path().to_string_lossy().to_string());

        let publisher = Arc::new(DebugLoggerPublisher::new());
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        let server = ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), metrics)
            .await
            .unwrap();

        // Should have authenticator with the test user
        let auth = server.authenticator().unwrap();
        assert!(auth.has_user("testuser"));
        assert!(auth.check_password("testuser", "testpass"));
    }
}
