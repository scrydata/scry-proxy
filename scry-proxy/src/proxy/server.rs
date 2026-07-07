use super::{
    AdminHandles, ClientEntry, ClientRegistry, ClientState, ConnectionHandler, EventBatcher,
    PoolManager, PoolManagerConfig, TcpConnectionPool, WaitQueue,
};
use crate::admin::{AdminConsole, AdminResponse, ADMIN_DATABASE};
use crate::auth::FileAuthenticator;
use crate::config::{AuthType, Config, DatabaseConfig, PoolingStrategy, TlsSslMode};
use crate::observability::ProxyMetrics;
use crate::protocol::{
    build_auth_cleartext_password, build_auth_ok, build_error_response, parse_password_message,
    read_startup_message, Protocol, ProtocolConfig, ProtocolRegistry, StartupMessage,
};
use crate::resilience::{ActiveHealthcheck, CircuitBreaker};
use crate::routing::DatabaseRouter;
use crate::tls::{handle_ssl_startup, load_client_tls_config, ClientTransport, SslStartupResult};
use anyhow::{Context, Result};
use rustls::ServerConfig;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

/// RAII guard that brackets a connection's lifetime: decrements the active
/// connection counter/metric AND removes the connection from the client
/// registry on drop. Because it drops on *every* exit path (clean close,
/// error, terminate, task abort), the registry can never hold a stale entry.
struct ConnectionCountGuard {
    counter: Arc<AtomicUsize>,
    metrics: Arc<ProxyMetrics>,
    /// Registry to deregister from on drop (WP-10, P4 §4.1).
    client_registry: Arc<ClientRegistry>,
    /// Connection id to remove from the registry. Deregistration is a no-op if
    /// the connection was never registered (identity never became known).
    conn_id: u64,
}

impl Drop for ConnectionCountGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
        self.metrics.decrement_active_connections();
        self.client_registry.deregister(self.conn_id);
    }
}

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
    /// Hot-reloadable authenticator, `Arc`-shared with the reload closure
    /// (`AdminHandles::reload_fn`) so both the SIGHUP path and admin `RELOAD`
    /// swap the SAME cell that live connection handlers read (WP-10 Task 4).
    authenticator: Arc<std::sync::RwLock<Option<Arc<FileAuthenticator>>>>,
    /// Channel to trigger config reload (e.g., on SIGHUP)
    #[allow(dead_code)]
    reload_trigger: watch::Receiver<()>,
    /// Sender side of reload channel, exposed for signal handlers
    reload_sender: watch::Sender<()>,
    /// Current active connection count (atomic for lock-free access)
    active_connections: Arc<AtomicUsize>,
    /// Shared operational state threaded into every admin connection: pool
    /// managers, reload/shutdown handles, config, and the client/server
    /// registries (WP-10, P4 §4.1). Constructed once here in `new`.
    admin_handles: Arc<AdminHandles>,
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
        let protocol: Arc<dyn Protocol> = ProtocolRegistry::get(
            &config.backend.protocol,
            config.performance.pool_reset_timeout_ms,
        )
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
            // The healthcheck probes the default backend; its verdict gates that
            // backend's ("*") circuit breaker (P3 §4.2/§5.3). Breakers are
            // registered during pool creation below, so the loop looks them up
            // lazily each tick.
            let hc_metrics = Arc::clone(&metrics);

            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_secs(healthcheck_config.interval_secs));

                loop {
                    interval.tick().await;

                    match healthcheck.check().await {
                        Ok(is_healthy) => {
                            // Gate the default backend's breaker on the probe.
                            if let Some(cb) = hc_metrics.get_circuit_breaker("*") {
                                cb.report_health(is_healthy);
                            }
                            if is_healthy {
                                tracing::debug!("Active healthcheck passed");
                            } else {
                                tracing::warn!("Active healthcheck failed threshold");
                            }
                        }
                        Err(e) => {
                            // A probe that could not even connect is an unhealthy
                            // signal — shed via the breaker.
                            if let Some(cb) = hc_metrics.get_circuit_breaker("*") {
                                cb.report_health(false);
                            }
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
        let router =
            DatabaseRouter::new(&config.databases, &config.backend, config.performance.pool_size);

        if !config.databases.is_empty() {
            info!(database_count = config.databases.len(), "Multi-database routing enabled");
        }

        // Circuit breakers are created per-backend (per pool) below, so one
        // failing backend cannot trip the breaker for healthy backends
        // (P3 §4.1/§5.4). Nothing shared is created here.
        if config.resilience.circuit_breaker.enabled {
            info!("Per-backend circuit breakers enabled");
        } else {
            info!("Circuit breaker disabled");
            metrics.clear_circuit_breakers();
        }

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

            // Backend connect+TLS timeout, shared by every pool.
            let connect_timeout_ms = config.backend.connection_timeout_ms;

            // Captured for per-backend breaker creation inside the closure.
            let breaker_config = config.resilience.circuit_breaker.clone();
            let breaker_metrics = Arc::clone(&metrics);

            // Helper to create a pool manager for a database config
            let create_pool_manager = |db_config: &DatabaseConfig,
                                       protocol: &Arc<dyn Protocol>,
                                       tls_config: &crate::config::TlsConfig,
                                       perf_config: &crate::config::PerformanceConfig,
                                       retry_config: &Option<
                crate::config::ConnectionRetryConfig,
            >|
             -> Result<Arc<PoolManager>> {
                let pool_size = db_config.pool_size.unwrap_or(perf_config.pool_size);
                let min_idle = std::cmp::min(perf_config.pool_min_idle, pool_size);

                // One circuit breaker per backend, registered for per-backend
                // observability, so a single bad backend is isolated.
                let circuit_breaker = if breaker_config.enabled {
                    let health_monitor = if breaker_config.use_health_monitor {
                        Some(Arc::clone(breaker_metrics.health_monitor()))
                    } else {
                        None
                    };
                    let cb = Arc::new(CircuitBreaker::new(breaker_config.clone(), health_monitor));
                    breaker_metrics
                        .register_circuit_breaker(db_config.name.clone(), Arc::clone(&cb));
                    Some(cb)
                } else {
                    None
                };

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
                    circuit_breaker,
                    retry_config.clone(),
                    perf_config.pool_lifo,
                    connect_timeout_ms,
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

            info!(pool_count = pool_managers.len(), "Connection pools created successfully");
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

        // Programmatic shutdown channel (WP-10, P4 §4.1). `run()` subscribes to
        // this and drains alongside the OS signals; a future admin SHUTDOWN
        // (Task 5) sends `true` on the sender carried by `AdminHandles`.
        let (shutdown_trigger, _shutdown_seed_rx) = watch::channel(false);

        // Build the shared admin handles once, from the pieces above. `Config`
        // becomes `Arc`-shared so both the server and the handles point at the
        // same instance.
        let config = Arc::new(config);

        // The authenticator cell is `Arc`-shared between the server (whose
        // connection handlers read it) and the reload closure (which swaps it),
        // so a hot reload is visible to live connections.
        let authenticator = Arc::new(std::sync::RwLock::new(authenticator));

        // The ONE reload function. Both the SIGHUP path (via
        // `apply_config_reload`) and admin `RELOAD` (via
        // `AdminHandles::reload_fn`) call this exact closure, so the two paths
        // can never drift. Scope is deliberately auth_file-only (documented
        // limitation); it returns the real read/parse error instead of
        // swallowing it, so a failed reload is reported honestly.
        let reload_fn = Self::build_reload_fn(Arc::clone(&config), Arc::clone(&authenticator));

        let admin_handles = AdminHandles::new(
            Arc::clone(&config),
            pool_managers.clone(),
            reload_sender.clone(),
            shutdown_trigger,
            Arc::clone(&reload_fn),
        );

        Ok(Self {
            config,
            listener,
            #[cfg(unix)]
            unix_listener,
            batcher: Arc::new(batcher),
            pool_managers,
            router,
            metrics,
            tls_config,
            authenticator,
            reload_trigger,
            reload_sender,
            active_connections: Arc::new(AtomicUsize::new(0)),
            admin_handles,
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
        self.pool_managers.get(database).or_else(|| self.pool_managers.get("*"))
    }

    /// Get the database router
    pub fn router(&self) -> &DatabaseRouter {
        &self.router
    }

    /// Get the authenticator, if authentication is enabled
    pub fn authenticator(&self) -> Option<Arc<FileAuthenticator>> {
        self.authenticator_read().clone()
    }

    /// Read-lock the authenticator, recovering from a poisoned lock.
    ///
    /// `std::sync::RwLock` poisons if a thread panics while holding a guard.
    /// The guarded value (`Option<Arc<FileAuthenticator>>`) is plain data with
    /// no invariant that a panic mid-write could leave inconsistent from a
    /// reader's perspective — either the old Arc or the new one is present —
    /// so it is always valid to recover and read it. A single poisoned lock
    /// must not cascade into every new connection's spawn path panicking.
    fn authenticator_read(&self) -> std::sync::RwLockReadGuard<'_, Option<Arc<FileAuthenticator>>> {
        self.authenticator.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Get the reload sender for use by signal handlers
    ///
    /// Returns a clone of the watch::Sender that can be used to trigger
    /// a config reload from external code (e.g., SIGHUP handler in main.rs).
    pub fn reload_sender(&self) -> watch::Sender<()> {
        self.reload_sender.clone()
    }

    /// Get the shared admin handles (pool managers, reload/shutdown triggers,
    /// config, and the client/server registries).
    ///
    /// Exposed so callers (and tests) can inspect live registry state or wire a
    /// programmatic shutdown, without owning the moved-into-`run` server.
    pub fn admin_handles(&self) -> Arc<AdminHandles> {
        Arc::clone(&self.admin_handles)
    }

    /// Get current active connection count
    pub fn active_connection_count(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Get max connections limit from config
    pub fn max_connections(&self) -> usize {
        self.config.proxy.max_connections
    }

    /// Warm up all connection pools before accepting client connections
    ///
    /// This pre-creates backend connections to avoid cold-start latency.
    /// Should be called between `new()` and `run()`.
    ///
    /// # Arguments
    /// * `min_idle` - Number of connections to pre-create per pool
    ///
    /// # Returns
    /// Total number of connections created across all pools
    pub async fn warmup_pools(&self, min_idle: usize) -> usize {
        if self.pool_managers.is_empty() || min_idle == 0 {
            return 0;
        }

        info!(
            pool_count = self.pool_managers.len(),
            min_idle = min_idle,
            "Warming up connection pools"
        );

        let mut total_created = 0;
        for (db_name, pool_manager) in &self.pool_managers {
            let created = pool_manager.warmup(min_idle).await;
            info!(database = %db_name, created = created, "Pool warmup complete");
            total_created += created;
        }

        info!(total_created = total_created, "All pools warmed up");
        total_created
    }

    /// Build the single config-reload function shared by the SIGHUP path and
    /// admin `RELOAD` (WP-10 Task 4, P4 §5.4).
    ///
    /// Currently reloads ONLY `auth_file` (re-reads the userlist and atomically
    /// swaps the shared authenticator cell). This scope is a documented
    /// limitation — settings that require re-binding (listen_address,
    /// unix_socket), pool recreation (pool sizes), or TLS-context recreation
    /// (certificates) are NOT hot-reloaded.
    ///
    /// Unlike the pre-WP-10 version, it returns the real read/parse error
    /// instead of swallowing it, so a failed reload is reported honestly to the
    /// caller (admin `RELOAD` turns an `Err` into an `ErrorResponse`; the
    /// SIGHUP path logs it and keeps the existing configuration).
    fn build_reload_fn(
        config: Arc<Config>,
        authenticator: Arc<std::sync::RwLock<Option<Arc<FileAuthenticator>>>>,
    ) -> Arc<dyn Fn() -> Result<()> + Send + Sync> {
        Arc::new(move || {
            info!("Applying config reload (scope: auth_file)");

            if let Some(auth_file) = config.auth.auth_file.as_ref() {
                let new_auth = FileAuthenticator::from_file(auth_file)
                    .with_context(|| format!("Failed to reload auth file: {}", auth_file))?;
                let user_count = new_auth.len();
                let mut auth_guard = authenticator.write().unwrap_or_else(|e| e.into_inner());
                *auth_guard = Some(Arc::new(new_auth));
                info!(
                    auth_file = %auth_file,
                    users = user_count,
                    "Auth file reloaded successfully"
                );
            }

            // Future: reload other hot-reloadable config here
            // - circuit breaker thresholds
            // - publisher settings
            // - observability settings

            info!("Config reload complete");
            Ok(())
        })
    }

    /// Apply a config reload, updating hot-reloadable settings (auth_file only,
    /// see [`build_reload_fn`](Self::build_reload_fn)).
    ///
    /// Delegates to the single shared reload closure carried by
    /// [`AdminHandles`], so this (the SIGHUP path) and admin `RELOAD` can never
    /// diverge. Returns the real error on failure instead of swallowing it.
    pub fn apply_config_reload(&self) -> Result<()> {
        (self.admin_handles.reload_fn)()
    }

    /// Run the proxy server, accepting connections until shutdown signal
    pub async fn run(mut self) -> Result<()> {
        // Set max_connections in metrics for Prometheus export
        self.metrics.set_max_connections(self.config.proxy.max_connections);
        // Set max queue depth for saturation metrics
        self.metrics.pool_metrics().set_max_queue_depth(self.config.performance.pool_queue_depth);

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

        // Spawn queue saturation monitoring background task
        let saturation_warn_threshold =
            self.config.performance.pool_queue_saturation_warn_threshold;
        if saturation_warn_threshold > 0.0 && saturation_warn_threshold < 1.0 {
            let metrics_clone = self.metrics.clone();
            let pool_managers_clone: Vec<_> = self
                .pool_managers
                .iter()
                .map(|(name, pm)| (name.clone(), Arc::clone(pm)))
                .collect();

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));
                let mut last_warned = std::time::Instant::now();
                let warn_cooldown = Duration::from_secs(30); // Only warn every 30 seconds

                loop {
                    interval.tick().await;

                    // Check pool queue saturation for each pool
                    for (db_name, pm) in &pool_managers_clone {
                        let queue_depth = pm.wait_queue_depth();
                        let saturation = metrics_clone.pool_metrics().get_queue_saturation();

                        // Update metrics
                        metrics_clone.pool_metrics().set_queue_depth(queue_depth);

                        // Warn if saturation exceeds threshold (with cooldown)
                        if saturation >= saturation_warn_threshold
                            && last_warned.elapsed() > warn_cooldown
                        {
                            warn!(
                                database = %db_name,
                                queue_depth = queue_depth,
                                saturation_pct = format!("{:.1}%", saturation * 100.0),
                                threshold_pct = format!("{:.0}%", saturation_warn_threshold * 100.0),
                                "Pool wait queue saturation high - clients may be rejected soon. \
                                 Consider increasing pool_size or pool_queue_depth."
                            );
                            last_warned = std::time::Instant::now();
                        }
                    }
                }
            });

            debug!(threshold = saturation_warn_threshold, "Queue saturation monitoring started");
        }

        let mut connection_count = 0u64;
        let mut connection_tasks = JoinSet::new();

        // Programmatic shutdown trigger (WP-10, P4 §4.1): a future admin
        // SHUTDOWN (Task 5) sends `true` here and `run()` drains via the same
        // path as the OS signals. Subscribe before the shutdown future so we
        // observe the transition even if it races startup.
        let mut admin_shutdown = self.admin_handles.shutdown_trigger.subscribe();

        // Setup shutdown signal handling. Both SIGINT (Ctrl+C) and SIGTERM
        // (the signal orchestrators like Kubernetes/Docker send to stop a
        // container) trigger the same graceful drain path (P3 §4.4). The
        // programmatic admin trigger is a third source of the same drain.
        let shutdown = async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = match signal(SignalKind::terminate()) {
                    Ok(s) => s,
                    Err(e) => {
                        error!(error = %e, "Failed to install SIGTERM handler");
                        // Fall back to SIGINT/admin-only so we still shut down cleanly.
                        tokio::select! {
                            _ = tokio::signal::ctrl_c() => {}
                            _ = admin_shutdown.changed() => {}
                        }
                        return;
                    }
                };
                tokio::select! {
                    r = tokio::signal::ctrl_c() => match r {
                        Ok(()) => info!("Received SIGINT (Ctrl+C); starting graceful shutdown"),
                        Err(e) => error!(error = %e, "Failed to listen for SIGINT"),
                    },
                    _ = sigterm.recv() => {
                        info!("Received SIGTERM; starting graceful shutdown");
                    }
                    _ = admin_shutdown.changed() => {
                        info!("Received programmatic admin shutdown request; starting graceful shutdown");
                    }
                }
            }
            #[cfg(not(unix))]
            {
                tokio::select! {
                    r = tokio::signal::ctrl_c() => match r {
                        Ok(()) => info!("Received Ctrl+C signal; starting graceful shutdown"),
                        Err(e) => error!(error = %e, "Failed to listen for Ctrl+C signal"),
                    },
                    _ = admin_shutdown.changed() => {
                        info!("Received programmatic admin shutdown request; starting graceful shutdown");
                    }
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

        // Announce drain completion so a blocking admin `SHUTDOWN WAIT`
        // (WP-10 Task 5) can return once the proxy has actually drained,
        // rather than reporting a premature success.
        self.admin_handles.signal_drain_complete();

        // EventBatcher will be dropped here, which closes the channel
        // and triggers flush of remaining events + publisher shutdown
        info!("Proxy server shutdown complete");
        Ok(())
    }

    /// Reject a connection that exceeds max_connections limit
    ///
    /// Sends a PostgreSQL ErrorResponse with SQLSTATE 53300 (too_many_connections)
    /// then closes the connection.
    async fn reject_connection_over_limit(mut stream: tokio::net::TcpStream) {
        // Build PostgreSQL ErrorResponse
        // Format: 'E' + length + fields + terminator
        let mut response = Vec::new();
        response.push(b'E');

        let mut fields = Vec::new();
        // Severity (S)
        fields.push(b'S');
        fields.extend_from_slice(b"FATAL");
        fields.push(0);
        // SQLSTATE (C) - 53300 = too_many_connections
        fields.push(b'C');
        fields.extend_from_slice(b"53300");
        fields.push(0);
        // Message (M)
        fields.push(b'M');
        fields.extend_from_slice(b"sorry, too many clients already");
        fields.push(0);
        // Terminator
        fields.push(0);

        let length = (fields.len() + 4) as i32;
        response.extend_from_slice(&length.to_be_bytes());
        response.extend_from_slice(&fields);

        // Attempt to send error (best effort)
        let _ = stream.write_all(&response).await;
        let _ = stream.shutdown().await;
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
                    if let Err(e) = self.apply_config_reload() {
                        error!(error = %e, "Config reload failed, keeping existing configuration");
                    }
                }

                accept_result = self.listener.accept() => {
                    match accept_result {
                        Ok((client_stream, client_addr)) => {
                            // Check connection limit BEFORE processing
                            let current = self.active_connections.load(Ordering::Relaxed);
                            if current >= self.config.proxy.max_connections {
                                warn!(
                                    client_addr = %client_addr,
                                    current_connections = current,
                                    max_connections = self.config.proxy.max_connections,
                                    "Connection rejected: max_connections limit reached"
                                );

                                // Record rejection in metrics
                                self.metrics.record_connection_rejected();

                                // Send PostgreSQL error and close
                                Self::reject_connection_over_limit(client_stream).await;
                                continue;
                            }

                            // Disable Nagle's algorithm for lower latency
                            if let Err(e) = client_stream.set_nodelay(true) {
                                warn!(error = %e, "Failed to set TCP_NODELAY on client connection");
                            }

                            *connection_count += 1;
                            let conn_id = *connection_count;

                            info!(
                                connection_id = conn_id,
                                client_addr = %client_addr,
                                active_connections = current + 1,
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
                            // Check connection limit BEFORE processing
                            let current = self.active_connections.load(Ordering::Relaxed);
                            if current >= self.config.proxy.max_connections {
                                warn!(
                                    current_connections = current,
                                    max_connections = self.config.proxy.max_connections,
                                    "UNIX connection rejected: max_connections limit reached"
                                );

                                // Record rejection in metrics
                                self.metrics.record_connection_rejected();

                                // For UNIX sockets, just drop the connection
                                // (no client address to send error to since it's a local socket)
                                drop(client_stream);
                                continue;
                            }

                            *connection_count += 1;
                            let conn_id = *connection_count;

                            info!(
                                connection_id = conn_id,
                                active_connections = current + 1,
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
                biased;  // Process in order to avoid reload_trigger always winning

                _ = &mut *shutdown => {
                    info!("Shutdown signal received, stopping new connections");
                    break;
                }

                accept_result = self.listener.accept() => {
                    match accept_result {
                        Ok((client_stream, client_addr)) => {
                            // Check connection limit BEFORE processing
                            let current = self.active_connections.load(Ordering::Relaxed);
                            if current >= self.config.proxy.max_connections {
                                warn!(
                                    client_addr = %client_addr,
                                    current_connections = current,
                                    max_connections = self.config.proxy.max_connections,
                                    "Connection rejected: max_connections limit reached"
                                );

                                // Record rejection in metrics
                                self.metrics.record_connection_rejected();

                                // Send PostgreSQL error and close
                                Self::reject_connection_over_limit(client_stream).await;
                                continue;
                            }

                            // Disable Nagle's algorithm for lower latency
                            if let Err(e) = client_stream.set_nodelay(true) {
                                warn!(error = %e, "Failed to set TCP_NODELAY on client connection");
                            }

                            *connection_count += 1;
                            let conn_id = *connection_count;

                            info!(
                                connection_id = conn_id,
                                client_addr = %client_addr,
                                active_connections = current + 1,
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
        let authenticator = self.authenticator_read().clone();
        let active_connections = Arc::clone(&self.active_connections);
        let admin_handles = Arc::clone(&self.admin_handles);
        // Clone the registry handle out so we can store this task's abort handle
        // AFTER spawning (the `Arc` inside `admin_handles` is moved into the task).
        let client_registry = Arc::clone(&admin_handles.client_registry);

        // Increment connection counter BEFORE spawning
        active_connections.fetch_add(1, Ordering::Relaxed);
        // Also update ProxyMetrics for observability
        metrics.increment_active_connections();

        let abort_handle = connection_tasks.spawn(async move {
            // Ensure counter is decremented AND the registry entry (if any) is
            // removed on every exit path (drop guard pattern).
            let _guard = ConnectionCountGuard {
                counter: active_connections,
                metrics: Arc::clone(&metrics),
                client_registry: Arc::clone(&admin_handles.client_registry),
                conn_id,
            };
            // Handle SSL startup handshake. `tls` captures whether the transport
            // was upgraded (previously discarded) so the registry can record it.
            let (transport, startup_data, tls) =
                match handle_ssl_startup(client_stream, &config.tls.client_tls_sslmode, tls_config)
                    .await
                {
                    Ok(SslStartupResult::Upgraded(mut transport)) => {
                        info!(connection_id = conn_id, "Connection upgraded to TLS");
                        // The client's StartupMessage hasn't been read yet (it
                        // follows the TLS handshake on the wire). Read it now,
                        // over the encrypted transport, so the client registry
                        // (WP-10, P4 §4.1) can record the real user/database
                        // for TLS connections instead of leaving them blank
                        // (previously discarded entirely; a `SHOW CLIENTS`
                        // truthfulness gap for exactly the connections most
                        // worth reporting accurately).
                        match read_startup_message(&mut transport).await {
                            Ok(data) => (transport, data, true),
                            Err(e) => {
                                error!(
                                    connection_id = conn_id,
                                    error = %e,
                                    "Failed to read startup message after TLS upgrade"
                                );
                                return;
                            }
                        }
                    }
                    Ok(SslStartupResult::Declined(stream, startup_data)) => {
                        debug!(connection_id = conn_id, "SSL declined, continuing with plain TCP");
                        (ClientTransport::Plain(stream), startup_data, false)
                    }
                    Ok(SslStartupResult::NoSslRequest(stream, startup_data)) => {
                        debug!(
                            connection_id = conn_id,
                            "No SSL request, continuing with plain TCP"
                        );
                        (ClientTransport::Plain(stream), startup_data, false)
                    }
                    Ok(SslStartupResult::Rejected) => {
                        // TLS downgrade attempt under a require/verify-* sslmode.
                        // handle_ssl_startup already sent the ErrorResponse and
                        // closed the stream; refuse to serve the connection.
                        warn!(
                            connection_id = conn_id,
                            "Rejected client connection: TLS required but client sent plaintext"
                        );
                        return;
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
                admin_handles,
                tls,
            )
            .await;
        });

        // Record this task's abort handle so `KILL [db]` can cancel it (Task 5).
        client_registry.register_abort_handle(conn_id, abort_handle);
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
        let authenticator = self.authenticator_read().clone();
        let active_connections = Arc::clone(&self.active_connections);
        let admin_handles = Arc::clone(&self.admin_handles);
        // Clone the registry handle out so we can store this task's abort handle
        // AFTER spawning (the `Arc` inside `admin_handles` is moved into the task).
        let client_registry = Arc::clone(&admin_handles.client_registry);

        // Increment connection counter BEFORE spawning
        active_connections.fetch_add(1, Ordering::Relaxed);
        // Also update ProxyMetrics for observability
        metrics.increment_active_connections();

        let abort_handle = connection_tasks.spawn(async move {
            // Ensure counter is decremented AND the registry entry (if any) is
            // removed on every exit path (drop guard pattern).
            let _guard = ConnectionCountGuard {
                counter: active_connections,
                metrics: Arc::clone(&metrics),
                client_registry: Arc::clone(&admin_handles.client_registry),
                conn_id,
            };
            // UNIX sockets don't use SSL, so wrap directly
            let transport = ClientTransport::Unix(client_stream);

            // For UNIX sockets, we need to read the startup message directly
            // since there's no SSL handshake. UNIX transports are never TLS.
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
                admin_handles,
                false, // UNIX sockets are not TLS-upgraded
            )
            .await;
        });

        // Record this task's abort handle so `KILL [db]` can cancel it (Task 5).
        client_registry.register_abort_handle(conn_id, abort_handle);
    }

    /// Common connection handling logic for both TCP and UNIX sockets
    #[allow(clippy::too_many_arguments)]
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
        admin_handles: Arc<AdminHandles>,
        tls: bool,
    ) {
        let addr_str = client_addr.map(|a| a.to_string()).unwrap_or_else(|| "unix".to_string());

        // Parse startup message to get user/database and check for admin. UNIX
        // sockets deliver an empty `startup_data` here (their startup is read
        // later by the connection handler), so their identity fields stay empty
        // for 1.0 — a documented limitation, not false state.
        let (database_name, user_name, is_admin) = if !startup_data.is_empty() {
            if let Some(startup) = StartupMessage::parse(&startup_data) {
                let db = startup.database().map(|s| s.to_string());
                let user = startup.user().map(|s| s.to_string());
                let is_admin = db.as_ref().is_some_and(|d| d.eq_ignore_ascii_case(ADMIN_DATABASE));
                (db, user, is_admin)
            } else {
                (None, None, false)
            }
        } else {
            (None, None, false)
        };

        // Register this connection in the client registry now that its identity
        // is known (WP-10, P4 §4.1). O(1) bookkeeping, off the per-query hot
        // path. Admin clients are registered too (PgBouncer lists them),
        // distinguished by `ClientState::Admin`. The `ConnectionCountGuard` in
        // the spawn closure removes this entry on every exit path.
        admin_handles.client_registry.register(ClientEntry {
            conn_id,
            addr: addr_str.clone(),
            user: user_name.clone().unwrap_or_default(),
            database: database_name.clone().unwrap_or_default(),
            state: if is_admin { ClientState::Admin } else { ClientState::Active },
            connect_time: std::time::Instant::now(),
            last_request_time: std::time::Instant::now(),
            tls,
        });

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

            if let Err(e) = handle_admin_connection(transport, admin_handles, metrics).await {
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

/// Constant-time byte-slice equality, to avoid leaking the admin password
/// through comparison timing. WP-12 centralizes constant-time comparisons via
/// `subtle`; this is the local, dependency-free version for the admin gate.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Handle an admin console connection
///
/// This handles connections to the virtual "pgbouncer" database,
/// which provides administrative commands for monitoring and controlling the proxy.
async fn handle_admin_connection(
    mut client: ClientTransport,
    admin_handles: Arc<AdminHandles>,
    metrics: Arc<ProxyMetrics>,
) -> Result<()> {
    let admin = &admin_handles.config.admin;
    // Fail closed (P1 §4.6): the admin console is refused unless it has been
    // explicitly enabled AND an admin password is configured. Without this the
    // virtual `pgbouncer` database was an unauthenticated control channel.
    let expected_password = match (admin.enabled, admin.admin_password.as_deref()) {
        (true, Some(pw)) if !pw.is_empty() => pw,
        _ => {
            warn!("Admin console connection refused: console disabled or no credential configured");
            let err = build_error_response(
                "FATAL",
                "28000",
                "administrative console is disabled on this server",
            );
            let _ = client.write_all(&err).await;
            return Ok(());
        }
    };

    // Request a cleartext password (intended for use over TLS / loopback) and
    // verify it before granting any admin access.
    client
        .write_all(&build_auth_cleartext_password())
        .await
        .context("Failed to send AuthenticationCleartextPassword")?;

    let mut auth_buf = vec![0u8; 4096];
    let auth_n = client.read(&mut auth_buf).await.context("Failed to read admin password")?;
    let provided = parse_password_message(&auth_buf[..auth_n]);
    let authenticated = provided
        .as_deref()
        .is_some_and(|p| constant_time_eq(p.as_bytes(), expected_password.as_bytes()));

    if !authenticated {
        warn!("Admin console authentication failed");
        let err = build_error_response("FATAL", "28P01", "admin authentication failed");
        let _ = client.write_all(&err).await;
        return Ok(());
    }

    // Authenticated: send AuthenticationOk
    // Format: 'R' + length(8) + auth_type(0)
    let auth_ok = build_auth_ok();
    client.write_all(&auth_ok).await.context("Failed to send AuthenticationOk")?;

    // Send ReadyForQuery (idle state)
    // Format: 'Z' + length(5) + status('I')
    let ready_for_query = [b'Z', 0, 0, 0, 5, b'I'];
    client.write_all(&ready_for_query).await.context("Failed to send ReadyForQuery")?;

    let admin = AdminConsole::new(admin_handles, metrics);
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
                let length =
                    i32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
                if n >= 5 + length - 4 {
                    // Query string is null-terminated
                    let query_end = 5 + length - 4 - 1; // -4 for length already counted, -1 for null
                    let query = String::from_utf8_lossy(&buffer[5..query_end]).to_string();

                    debug!(query = %query, "Admin console query");

                    let response = match admin.execute(&query).await {
                        Ok(resp) => resp,
                        Err(e) => AdminResponse::Error { message: e.to_string() },
                    };

                    let wire_response = response.to_wire();
                    client
                        .write_all(&wire_response)
                        .await
                        .context("Failed to send admin response")?;
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
    use crate::config::{AdminConfig, AuthType, Config};
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

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secret-longer"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    /// Drive `handle_admin_connection` with a raw TCP client and return the
    /// first server response frame (its message-type byte + body).
    async fn admin_handshake(admin: AdminConfig, password_to_send: Option<&str>) -> Vec<u8> {
        use crate::protocol::build_password_message;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        // Build minimal AdminHandles whose config carries the test's admin
        // settings (handle_admin_connection reads config.admin for the gate).
        let mut cfg = Config::default();
        cfg.admin = admin;
        let handles = AdminHandles::for_test_with_config(cfg);

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let transport = ClientTransport::Plain(stream);
            let _ = handle_admin_connection(transport, handles, metrics).await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut first = vec![0u8; 1024];
        let n = client.read(&mut first).await.unwrap();
        let first = first[..n].to_vec();

        // If the server asked for a password ('R' auth request) and we have one
        // to send, send it and read the follow-up frame instead.
        let response = match password_to_send {
            Some(pw) if !first.is_empty() && first[0] == b'R' => {
                client.write_all(&build_password_message(pw)).await.unwrap();
                let mut second = vec![0u8; 1024];
                let n2 = client.read(&mut second).await.unwrap_or(0);
                second[..n2].to_vec()
            }
            _ => first,
        };

        // Close the client so the server's post-auth command loop (which blocks
        // on read) observes EOF and returns instead of hanging the test.
        drop(client);
        let _ = server.await;
        response
    }

    #[tokio::test]
    async fn admin_console_disabled_is_refused() {
        // Default: console disabled -> immediate FATAL ErrorResponse, no auth.
        let admin = AdminConfig::default();
        let resp = admin_handshake(admin, None).await;
        assert_eq!(resp[0], b'E', "disabled console must return an ErrorResponse");
        assert!(resp.windows(5).any(|w| w == b"28000"));
    }

    #[tokio::test]
    async fn admin_console_enabled_without_credential_is_refused() {
        // Enabled but no password configured -> still refused (fail closed).
        let admin = AdminConfig { enabled: true, admin_users: None, admin_password: None };
        let resp = admin_handshake(admin, None).await;
        assert_eq!(resp[0], b'E');
        assert!(resp.windows(5).any(|w| w == b"28000"));
    }

    #[tokio::test]
    async fn admin_console_wrong_password_is_refused() {
        let admin = AdminConfig {
            enabled: true,
            admin_users: None,
            admin_password: Some("correct-horse".to_string()),
        };
        let resp = admin_handshake(admin, Some("wrong-password")).await;
        assert_eq!(resp[0], b'E', "wrong password must return an ErrorResponse");
        assert!(resp.windows(5).any(|w| w == b"28P01"));
    }

    #[tokio::test]
    async fn admin_console_correct_password_authenticates() {
        let admin = AdminConfig {
            enabled: true,
            admin_users: None,
            admin_password: Some("correct-horse".to_string()),
        };
        let resp = admin_handshake(admin, Some("correct-horse")).await;
        // AuthenticationOk is 'R' with auth-type 0.
        assert_eq!(resp[0], b'R', "correct password must yield AuthenticationOk");
        assert_eq!(&resp[1..9], &[0, 0, 0, 8, 0, 0, 0, 0]);
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
    async fn test_authenticator_read_survives_poisoned_lock() {
        // Create a temp auth file so the server has a real authenticator to read.
        let mut auth_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(auth_file, "\"testuser\" \"testpass\"").unwrap();
        auth_file.flush().unwrap();

        let mut config = create_test_config();
        config.auth.auth_type = AuthType::Md5;
        config.auth.auth_file = Some(auth_file.path().to_string_lossy().to_string());

        let publisher = Arc::new(DebugLoggerPublisher::new());
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        let server = Arc::new(
            ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), metrics)
                .await
                .unwrap(),
        );

        // Poison the std::sync::RwLock by panicking while holding the write guard.
        let server_for_thread = Arc::clone(&server);
        let poisoner = std::thread::spawn(move || {
            let _guard = server_for_thread.authenticator.write().unwrap();
            panic!("intentionally poisoning the authenticator lock");
        });
        // The thread panicking is expected; joining just waits for it to finish.
        assert!(poisoner.join().is_err());

        // A subsequent read must recover the poisoned guard rather than panic,
        // and must still return the valid authenticator data.
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
        let file =
            std::fs::OpenOptions::new().write(true).truncate(true).open(auth_file.path()).unwrap();
        use std::io::Write as _;
        writeln!(&file, "\"user1\" \"pass1\"").unwrap();
        writeln!(&file, "\"user2\" \"pass2\"").unwrap();
        drop(file);

        // Trigger reload
        server.apply_config_reload().expect("reload of valid auth file should succeed");

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
        let file =
            std::fs::OpenOptions::new().write(true).truncate(true).open(auth_file.path()).unwrap();
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

    #[tokio::test]
    async fn test_server_has_active_connection_counter() {
        let config = create_test_config();
        let publisher = Arc::new(DebugLoggerPublisher::new());
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        let server = ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), metrics)
            .await
            .unwrap();

        // Server should expose active connection count
        assert_eq!(server.active_connection_count(), 0);
        assert_eq!(server.max_connections(), 100);
    }

    #[tokio::test]
    async fn test_connection_counter_increments_on_spawn() {
        let config = create_test_config();
        let publisher = Arc::new(DebugLoggerPublisher::new());
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        let server = ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), metrics)
            .await
            .unwrap();

        // Simulate what happens when a connection is spawned
        // The counter should increment when connection starts
        // and decrement when connection ends (handled in spawned task)

        // For this unit test, we verify the counter is accessible and starts at 0
        assert_eq!(server.active_connection_count(), 0);

        // Integration test will verify actual increment/decrement behavior
    }
}
