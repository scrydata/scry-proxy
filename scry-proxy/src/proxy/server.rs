use super::{
    ConnectionHandler, EventBatcher, PoolManager, PoolManagerConfig, TcpConnectionPool, WaitQueue,
};
use crate::admin::{AdminConsole, AdminResponse, ADMIN_DATABASE};
use crate::auth::FileAuthenticator;
use crate::config::{AuthType, Config, DatabaseConfig, PoolingStrategy, TlsSslMode};
use crate::observability::ProxyMetrics;
use crate::protocol::{Protocol, ProtocolConfig, ProtocolRegistry, StartupMessage};
use crate::resilience::{ActiveHealthcheck, CircuitBreaker};
use crate::routing::DatabaseRouter;
use crate::tls::{handle_ssl_startup, load_client_tls_config, ClientTransport, SslStartupResult};
use anyhow::{Context, Result};
use rustls::ServerConfig;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

/// The main proxy server that accepts client connections
pub struct ProxyServer {
    config: Arc<Config>,
    listener: TcpListener,
    /// Optional UNIX socket listener (Unix platforms only)
    #[cfg(unix)]
    unix_listener: Option<UnixListener>,
    batcher: Arc<EventBatcher>,
    /// Per-database pool managers. Key is database name, "*" is default.
    pool_managers: HashMap<String, Arc<PoolManager>>,
    /// Database router for multi-database support
    router: DatabaseRouter,
    metrics: Arc<ProxyMetrics>,
    tls_config: Option<Arc<ServerConfig>>,
    authenticator: std::sync::RwLock<Option<Arc<FileAuthenticator>>>,
    /// Channel to trigger config reload (e.g., on SIGHUP)
    reload_trigger: watch::Receiver<()>,
    /// Sender side of reload channel, exposed for signal handlers
    reload_sender: watch::Sender<()>,
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

        // Create database router for multi-database support
        let router = DatabaseRouter::new(
            &config.databases,
            &config.backend,
            config.performance.pool_size,
        );

        if !config.databases.is_empty() {
            info!(
                database_count = config.databases.len(),
                "Multi-database routing enabled"
            );
        }

        // Create circuit breaker if enabled (shared across all pools)
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

        // Create per-database connection pools and pool managers
        let mut pool_managers: HashMap<String, Arc<PoolManager>> = HashMap::new();

        if config.performance.connection_pooling != PoolingStrategy::Disabled {
            info!(
                strategy = ?config.performance.connection_pooling,
                protocol = protocol.name(),
                "Creating connection pools"
            );

            // Helper to create a pool manager for a database config
            let create_pool_manager = |db_config: &DatabaseConfig,
                                       protocol: &Arc<dyn Protocol>,
                                       tls_config: &crate::config::TlsConfig,
                                       perf_config: &crate::config::PerformanceConfig,
                                       circuit_breaker: &Option<Arc<CircuitBreaker>>,
                                       retry_config: &Option<crate::config::ConnectionRetryConfig>|
             -> Result<Arc<PoolManager>> {
                let pool_size = db_config.pool_size.unwrap_or(perf_config.pool_size);
                let min_idle = std::cmp::min(perf_config.pool_min_idle, pool_size);

                let protocol_config = ProtocolConfig {
                    host: db_config.host.clone(),
                    port: db_config.port,
                    database: Some(db_config.database.clone()),
                    user: Some(db_config.user.clone()),
                    password: Some(db_config.password.clone()),
                };

                let pool = TcpConnectionPool::new(
                    Arc::clone(protocol),
                    protocol_config,
                    tls_config,
                    pool_size,
                    Some(min_idle),
                    circuit_breaker.clone(),
                    retry_config.clone(),
                    perf_config.pool_lifo,
                )
                .context(format!("Failed to create pool for database '{}'", db_config.name))?;

                let wait_queue = WaitQueue::new(perf_config.pool_queue_depth);
                let pm_config = PoolManagerConfig {
                    lifo: perf_config.pool_lifo,
                    queue_depth: perf_config.pool_queue_depth,
                    idle_unpin_secs: perf_config.pool_idle_unpin_secs,
                    wait_timeout_ms: perf_config.pool_timeout_secs * 1000,
                };

                info!(
                    database = %db_config.name,
                    pool_size = pool_size,
                    host = %db_config.host,
                    port = db_config.port,
                    "Created pool for database"
                );

                Ok(PoolManager::new(Arc::new(pool), wait_queue, pm_config))
            };

            // Create pool for default backend ("*")
            let default_db_config = router.default_config();
            let default_pm = create_pool_manager(
                default_db_config,
                &protocol,
                &config.tls,
                &config.performance,
                &circuit_breaker,
                &retry_config,
            )?;
            pool_managers.insert("*".to_string(), default_pm);

            // Create pools for each configured database
            for db_config in &config.databases {
                let pm = create_pool_manager(
                    db_config,
                    &protocol,
                    &config.tls,
                    &config.performance,
                    &circuit_breaker,
                    &retry_config,
                )?;
                pool_managers.insert(db_config.name.clone(), pm);
            }

            if config.tls.server_tls_sslmode != TlsSslMode::Disable {
                info!(
                    sslmode = ?config.tls.server_tls_sslmode,
                    "Backend TLS enabled"
                );
            }

            info!(
                pool_count = pool_managers.len(),
                "Connection pools created successfully"
            );
        } else {
            info!("Connection pooling disabled, using direct connections");
        }

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

        // Create UNIX socket listener if configured (Unix platforms only)
        #[cfg(unix)]
        let unix_listener = if let Some(ref socket_path) = config.proxy.unix_socket {
            // Remove existing socket file if it exists
            if std::path::Path::new(socket_path).exists() {
                std::fs::remove_file(socket_path)
                    .context(format!("Failed to remove existing socket: {}", socket_path))?;
            }

            let listener = UnixListener::bind(socket_path)
                .context(format!("Failed to bind UNIX socket: {}", socket_path))?;

            info!(
                socket_path = %socket_path,
                "UNIX socket listener bound"
            );

            Some(listener)
        } else {
            None
        };

        #[cfg(not(unix))]
        if config.proxy.unix_socket.is_some() {
            warn!("UNIX socket configuration ignored on non-Unix platform");
        }

        // Create reload channel for SIGHUP config reload
        let (reload_sender, reload_trigger) = watch::channel(());

        Ok(Self {
            config: Arc::new(config),
            listener,
            #[cfg(unix)]
            unix_listener,
            batcher: Arc::new(batcher),
            pool_managers,
            router,
            metrics,
            tls_config,
            authenticator: std::sync::RwLock::new(authenticator),
            reload_trigger,
            reload_sender,
        })
    }

    /// Get the local address the server is listening on
    /// Useful for tests when binding to port 0 (OS-assigned port)
    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        self.listener.local_addr().context("Failed to get local address")
    }

    /// Get the default pool manager, if pooling is enabled
    pub fn pool_manager(&self) -> Option<&Arc<PoolManager>> {
        self.pool_managers.get("*")
    }

    /// Get a pool manager for a specific database
    pub fn pool_manager_for(&self, database: &str) -> Option<&Arc<PoolManager>> {
        self.pool_managers
            .get(database)
            .or_else(|| self.pool_managers.get("*"))
    }

    /// Get the database router
    pub fn router(&self) -> &DatabaseRouter {
        &self.router
    }

    /// Get the authenticator, if authentication is enabled
    pub fn authenticator(&self) -> Option<Arc<FileAuthenticator>> {
        self.authenticator.read().unwrap().clone()
    }

    /// Get the reload sender for use by signal handlers
    ///
    /// Returns a clone of the watch::Sender that can be used to trigger
    /// a config reload from external code (e.g., SIGHUP handler in main.rs).
    pub fn reload_sender(&self) -> watch::Sender<()> {
        self.reload_sender.clone()
    }

    /// Apply a config reload, updating hot-reloadable settings
    ///
    /// Currently supports reloading:
    /// - auth_file: Re-reads the userlist.txt file to update user credentials
    ///
    /// Settings that require restart (not hot-reloaded):
    /// - listen_address, unix_socket (requires re-binding)
    /// - pool_size, pool settings (requires pool recreation)
    /// - TLS certificates (requires TLS context recreation)
    pub fn apply_config_reload(&self) {
        info!("Applying config reload");

        // Reload auth file if configured
        let auth_file = self.config.auth.auth_file.as_ref();
        if let Some(auth_file) = auth_file {
            match FileAuthenticator::from_file(auth_file) {
                Ok(new_auth) => {
                    let mut auth_guard = self.authenticator.write().unwrap();
                    let user_count = new_auth.len();
                    *auth_guard = Some(Arc::new(new_auth));
                    info!(
                        auth_file = %auth_file,
                        users = user_count,
                        "Auth file reloaded successfully"
                    );
                }
                Err(e) => {
                    error!(
                        auth_file = %auth_file,
                        error = %e,
                        "Failed to reload auth file, keeping existing configuration"
                    );
                }
            }
        }

        // Future: reload other hot-reloadable config here
        // - circuit breaker thresholds
        // - publisher settings
        // - observability settings

        info!("Config reload complete");
    }

    /// Run the proxy server, accepting connections until shutdown signal
    pub async fn run(mut self) -> Result<()> {
        info!(
            listen_address = %self.config.proxy.listen_address,
            max_connections = self.config.proxy.max_connections,
            shutdown_timeout_secs = self.config.proxy.shutdown_timeout_secs,
            "Proxy server starting"
        );

        // Spawn idle cleanup background tasks for all pool managers
        let idle_interval = self.config.performance.pool_idle_unpin_secs;
        if idle_interval > 0 && !self.pool_managers.is_empty() {
            let cleanup_interval_secs = std::cmp::max(1, idle_interval / 2);

            for (db_name, pool_manager) in &self.pool_managers {
                let pm = Arc::clone(pool_manager);
                let db_name = db_name.clone();
                tokio::spawn(async move {
                    let mut interval =
                        tokio::time::interval(Duration::from_secs(cleanup_interval_secs));
                    loop {
                        interval.tick().await;
                        let cleaned = pm.cleanup_idle();
                        if cleaned > 0 {
                            debug!(database = %db_name, cleaned, "Cleaned up idle sticky connections");
                        }
                    }
                });
            }

            info!(
                interval_secs = cleanup_interval_secs,
                idle_unpin_secs = idle_interval,
                pool_count = self.pool_managers.len(),
                "Idle cleanup background tasks started"
            );
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
        #[cfg(unix)]
        self.accept_loop_with_unix(&mut shutdown, &mut connection_count, &mut connection_tasks)
            .await;

        #[cfg(not(unix))]
        self.accept_loop_tcp_only(&mut shutdown, &mut connection_count, &mut connection_tasks)
            .await;

        // Graceful shutdown: wait for active connections to drain
        self.drain_connections(connection_tasks).await;

        // EventBatcher will be dropped here, which closes the channel
        // and triggers flush of remaining events + publisher shutdown
        info!("Proxy server shutdown complete");
        Ok(())
    }

    /// Accept loop for Unix platforms (supports both TCP and UNIX sockets)
    #[cfg(unix)]
    async fn accept_loop_with_unix(
        &mut self,
        shutdown: &mut std::pin::Pin<&mut impl std::future::Future<Output = ()>>,
        connection_count: &mut u64,
        connection_tasks: &mut JoinSet<()>,
    ) {
        loop {
            // Create the UNIX accept future inside the loop
            let unix_accept = async {
                if let Some(ref listener) = self.unix_listener {
                    listener.accept().await
                } else {
                    // Never resolves if no UNIX socket configured
                    std::future::pending().await
                }
            };

            tokio::select! {
                _ = &mut *shutdown => {
                    info!("Shutdown signal received, stopping new connections");
                    break;
                }

                // Handle config reload signal (SIGHUP)
                _ = self.reload_trigger.changed() => {
                    info!("Received reload signal");
                    self.apply_config_reload();
                }

                accept_result = self.listener.accept() => {
                    match accept_result {
                        Ok((client_stream, client_addr)) => {
                            *connection_count += 1;
                            let conn_id = *connection_count;

                            info!(
                                connection_id = conn_id,
                                client_addr = %client_addr,
                                "Accepted TCP client connection"
                            );

                            self.spawn_tcp_connection_handler(
                                connection_tasks,
                                client_stream,
                                client_addr,
                                conn_id,
                            );
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to accept TCP connection");
                        }
                    }
                }

                accept_result = unix_accept => {
                    match accept_result {
                        Ok((client_stream, _addr)) => {
                            *connection_count += 1;
                            let conn_id = *connection_count;

                            info!(
                                connection_id = conn_id,
                                "Accepted UNIX socket client connection"
                            );

                            self.spawn_unix_connection_handler(
                                connection_tasks,
                                client_stream,
                                conn_id,
                            );
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to accept UNIX socket connection");
                        }
                    }
                }
            }
        }
    }

    /// Accept loop for non-Unix platforms (TCP only)
    #[cfg(not(unix))]
    async fn accept_loop_tcp_only(
        &mut self,
        shutdown: &mut std::pin::Pin<&mut impl std::future::Future<Output = ()>>,
        connection_count: &mut u64,
        connection_tasks: &mut JoinSet<()>,
    ) {
        loop {
            tokio::select! {
                _ = &mut *shutdown => {
                    info!("Shutdown signal received, stopping new connections");
                    break;
                }

                // Handle config reload signal
                _ = self.reload_trigger.changed() => {
                    info!("Received reload signal");
                    self.apply_config_reload();
                }

                accept_result = self.listener.accept() => {
                    match accept_result {
                        Ok((client_stream, client_addr)) => {
                            *connection_count += 1;
                            let conn_id = *connection_count;

                            info!(
                                connection_id = conn_id,
                                client_addr = %client_addr,
                                "Accepted TCP client connection"
                            );

                            self.spawn_tcp_connection_handler(
                                connection_tasks,
                                client_stream,
                                client_addr,
                                conn_id,
                            );
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to accept TCP connection");
                        }
                    }
                }
            }
        }
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

    /// Spawn a handler for a TCP connection
    fn spawn_tcp_connection_handler(
        &self,
        connection_tasks: &mut JoinSet<()>,
        client_stream: tokio::net::TcpStream,
        client_addr: std::net::SocketAddr,
        conn_id: u64,
    ) {
        let config = Arc::clone(&self.config);
        let batcher = Arc::clone(&self.batcher);
        let pool_managers = self.pool_managers.clone();
        let metrics = Arc::clone(&self.metrics);
        let tls_config = self.tls_config.clone();
        // Read current authenticator from RwLock (allows hot-reload via SIGHUP)
        let authenticator = self.authenticator.read().unwrap().clone();

        connection_tasks.spawn(async move {
            // Handle SSL startup handshake
            let (transport, startup_data) = match handle_ssl_startup(
                client_stream,
                &config.tls.client_tls_sslmode,
                tls_config,
            )
            .await
            {
                Ok(SslStartupResult::Upgraded(transport)) => {
                    info!(connection_id = conn_id, "Connection upgraded to TLS");
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
                    error!(connection_id = conn_id, error = %e, "SSL startup failed");
                    return;
                }
            };

            Self::handle_client_connection(
                transport,
                Some(client_addr),
                conn_id,
                config,
                batcher,
                pool_managers,
                metrics,
                startup_data,
                authenticator,
            )
            .await;
        });
    }

    /// Spawn a handler for a UNIX socket connection (Unix platforms only)
    #[cfg(unix)]
    fn spawn_unix_connection_handler(
        &self,
        connection_tasks: &mut JoinSet<()>,
        client_stream: tokio::net::UnixStream,
        conn_id: u64,
    ) {
        let config = Arc::clone(&self.config);
        let batcher = Arc::clone(&self.batcher);
        let pool_managers = self.pool_managers.clone();
        let metrics = Arc::clone(&self.metrics);
        // Read current authenticator from RwLock (allows hot-reload via SIGHUP)
        let authenticator = self.authenticator.read().unwrap().clone();

        connection_tasks.spawn(async move {
            // UNIX sockets don't use SSL, so wrap directly
            let transport = ClientTransport::Unix(client_stream);

            // For UNIX sockets, we need to read the startup message directly
            // since there's no SSL handshake
            Self::handle_client_connection(
                transport,
                None, // No socket address for UNIX sockets
                conn_id,
                config,
                batcher,
                pool_managers,
                metrics,
                Vec::new(), // Startup data will be read by connection handler
                authenticator,
            )
            .await;
        });
    }

    /// Common connection handling logic for both TCP and UNIX sockets
    async fn handle_client_connection(
        transport: ClientTransport,
        client_addr: Option<std::net::SocketAddr>,
        conn_id: u64,
        config: Arc<Config>,
        batcher: Arc<EventBatcher>,
        pool_managers: HashMap<String, Arc<PoolManager>>,
        metrics: Arc<ProxyMetrics>,
        startup_data: Vec<u8>,
        authenticator: Option<Arc<FileAuthenticator>>,
    ) {
        let addr_str = client_addr
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unix".to_string());

        // Parse startup message to get database name and check for admin
        let (database_name, is_admin) = if !startup_data.is_empty() {
            if let Some(startup) = StartupMessage::parse(&startup_data) {
                let db = startup.database().map(|s| s.to_string());
                let is_admin = db
                    .as_ref()
                    .is_some_and(|d| d.eq_ignore_ascii_case(ADMIN_DATABASE));
                (db, is_admin)
            } else {
                (None, false)
            }
        } else {
            (None, false)
        };

        // Select the appropriate pool manager for this database
        let pool_manager = database_name
            .as_ref()
            .and_then(|db| pool_managers.get(db))
            .or_else(|| pool_managers.get("*"))
            .cloned();

        if let Some(ref db) = database_name {
            debug!(
                connection_id = conn_id,
                database = %db,
                has_specific_pool = pool_managers.contains_key(db),
                "Routing connection to database"
            );
        }

        if is_admin {
            info!(
                connection_id = conn_id,
                client_addr = %addr_str,
                "Routing to admin console"
            );

            if let Err(e) = handle_admin_connection(transport, pool_manager, metrics).await {
                error!(
                    connection_id = conn_id,
                    client_addr = %addr_str,
                    error = %e,
                    "Admin console connection failed"
                );
            }
        } else {
            // Use a placeholder address for UNIX sockets
            let handler_addr =
                client_addr.unwrap_or_else(|| "0.0.0.0:0".parse().expect("valid placeholder addr"));

            let handler = ConnectionHandler::new(
                transport,
                handler_addr,
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
                    client_addr = %addr_str,
                    error = %e,
                    "Connection handler failed"
                );
            }
        }

        info!(
            connection_id = conn_id,
            client_addr = %addr_str,
            "Connection closed"
        );
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

    #[tokio::test]
    async fn test_server_creates_per_database_pools() {
        use crate::config::DatabaseConfig;

        let mut config = create_test_config();
        // Add configured databases
        config.databases = vec![
            DatabaseConfig {
                name: "app1".to_string(),
                host: "app1-host".to_string(),
                port: 5432,
                database: "app1_db".to_string(),
                user: "app1_user".to_string(),
                password: "app1_pass".to_string(),
                pool_size: Some(5),
            },
            DatabaseConfig {
                name: "app2".to_string(),
                host: "app2-host".to_string(),
                port: 5433,
                database: "app2_db".to_string(),
                user: "app2_user".to_string(),
                password: "app2_pass".to_string(),
                pool_size: None, // Uses default
            },
        ];

        let publisher = Arc::new(DebugLoggerPublisher::new());
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        let server = ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), metrics)
            .await
            .unwrap();

        // Should have default pool manager
        assert!(server.pool_manager().is_some());

        // Should route to specific pool for configured databases
        assert!(server.pool_manager_for("app1").is_some());
        assert!(server.pool_manager_for("app2").is_some());

        // Should fallback to default for unknown databases
        assert!(server.pool_manager_for("unknown").is_some());

        // Router should have the configured databases
        assert!(server.router().has_route("app1"));
        assert!(server.router().has_route("app2"));
        assert!(!server.router().has_route("unknown"));
    }

    #[tokio::test]
    async fn test_config_reload_updates_auth_file() {
        // Create initial auth file with one user
        let mut auth_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(auth_file, "\"user1\" \"pass1\"").unwrap();
        auth_file.flush().unwrap();

        let mut config = create_test_config();
        config.auth.auth_type = AuthType::Md5;
        config.auth.auth_file = Some(auth_file.path().to_string_lossy().to_string());

        let publisher = Arc::new(DebugLoggerPublisher::new());
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        let server = ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), metrics)
            .await
            .unwrap();

        // Verify initial state
        let auth = server.authenticator().unwrap();
        assert!(auth.has_user("user1"));
        assert!(auth.check_password("user1", "pass1"));
        assert!(!auth.has_user("user2"));

        // Update auth file with a new user
        let file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(auth_file.path())
            .unwrap();
        use std::io::Write as _;
        writeln!(&file, "\"user1\" \"pass1\"").unwrap();
        writeln!(&file, "\"user2\" \"pass2\"").unwrap();
        drop(file);

        // Trigger reload
        server.apply_config_reload();

        // Verify updated state
        let auth = server.authenticator().unwrap();
        assert!(auth.has_user("user1"));
        assert!(auth.has_user("user2"));
        assert!(auth.check_password("user2", "pass2"));
    }

    #[tokio::test]
    async fn test_reload_sender_can_trigger_reload() {
        // Create initial auth file
        let mut auth_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(auth_file, "\"user1\" \"pass1\"").unwrap();
        auth_file.flush().unwrap();

        let mut config = create_test_config();
        config.auth.auth_type = AuthType::Md5;
        config.auth.auth_file = Some(auth_file.path().to_string_lossy().to_string());

        let publisher = Arc::new(DebugLoggerPublisher::new());
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        let server = ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), metrics)
            .await
            .unwrap();

        // Get reload sender
        let reload_sender = server.reload_sender();

        // Update auth file
        let file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(auth_file.path())
            .unwrap();
        use std::io::Write as _;
        writeln!(&file, "\"newuser\" \"newpass\"").unwrap();
        drop(file);

        // Send reload signal (simulating SIGHUP)
        reload_sender.send(()).unwrap();

        // The reload happens asynchronously in the accept loop,
        // but for this test we just verify the sender works
        // In a full integration test, we'd run the server and verify
        assert!(reload_sender.send(()).is_ok());
    }
}
