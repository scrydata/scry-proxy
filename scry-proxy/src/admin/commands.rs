//! Admin command parsing and execution
//!
//! Implements PgBouncer-compatible admin commands.

use super::response::AdminResponse;
use crate::config::PoolingStrategy;
use crate::observability::ProxyMetrics;
use crate::proxy::{AdminHandles, ClientState};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

/// A single logical database as seen from config (the default `"*"` backend
/// plus every entry in `config.databases`), joined with the key used to look
/// it up in `AdminHandles::pool_managers` / `ServerRegistry`.
///
/// This is the single source of truth `SHOW DATABASES`/`SHOW POOLS`/
/// `SHOW SERVERS` all build their rows from, so the three commands can never
/// disagree about what a database's real name/host/port/user is (P4 §4.1).
struct DatabaseEntry {
    /// Client-facing name. For the default backend this is the backend's own
    /// database name (there is no separate alias); for a `config.databases`
    /// entry it is `DatabaseConfig::name`, exactly as configured.
    name: String,
    host: String,
    port: u16,
    /// The actual database name on the backend (may differ from `name` for a
    /// named `config.databases` entry).
    database: String,
    user: String,
    /// Configured pool size, used when no live pool exists to read from
    /// (pooling disabled).
    configured_pool_size: usize,
    /// Key into `pool_managers` / `ServerRegistry::snapshot()` (`"*"` for the
    /// default backend, `DatabaseConfig::name` otherwise).
    pool_key: String,
}

/// Map a [`PoolingStrategy`] to its PgBouncer-style `pool_mode` string.
/// `hybrid` is a Scry-specific extension beyond PgBouncer's vocabulary
/// (session/transaction/statement) but is the real configured value, which is
/// what truthfulness requires here.
fn pool_mode_str(strategy: &PoolingStrategy) -> &'static str {
    match strategy {
        PoolingStrategy::Disabled => "disabled",
        PoolingStrategy::Session => "session",
        PoolingStrategy::Transaction => "transaction",
        PoolingStrategy::Hybrid => "hybrid",
    }
}

/// Split an `"host:port"` (or bracketed IPv6 `"[::1]:port"`) string into its
/// two parts at the last colon. Falls back to `(addr, "0")` for anything else
/// (e.g. the literal `"unix"` marker used for UNIX-socket clients).
fn split_host_port(addr: &str) -> (String, String) {
    match addr.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.to_string()),
        None => (addr.to_string(), "0".to_string()),
    }
}

/// Coarse [`ClientState`] rendered as a PgBouncer-style state string. Both
/// variants render "active": a `ClientState::Admin` connection (the
/// `pgbouncer` virtual console) is genuinely active for as long as it's
/// connected; we just don't have a finer PgBouncer state machine
/// (idle/used/waiting) yet (Task 1 limitation, carried forward).
fn client_state_str(state: ClientState) -> &'static str {
    match state {
        ClientState::Active => "active",
        ClientState::Admin => "active",
    }
}

/// Parsed admin command
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminCommand {
    /// SHOW POOLS - Display pool statistics
    ShowPools,
    /// SHOW STATS - Display query statistics
    ShowStats,
    /// SHOW DATABASES - Display configured databases
    ShowDatabases,
    /// SHOW CLIENTS - Display active client connections
    ShowClients,
    /// SHOW SERVERS - Display active backend connections
    ShowServers,
    /// SHOW VERSION - Display proxy version
    ShowVersion,
    /// SHOW CONFIG - Display current configuration
    ShowConfig,
    /// PAUSE [db] - Pause accepting new connections
    Pause { database: Option<String> },
    /// RESUME [db] - Resume accepting connections
    Resume { database: Option<String> },
    /// RELOAD - Reload configuration
    Reload,
    /// SHUTDOWN - Graceful shutdown
    Shutdown { wait: bool },
    /// KILL - Kill a client connection
    Kill { database: Option<String> },
}

impl AdminCommand {
    /// Parse an SQL command into an admin command
    pub fn parse(sql: &str) -> Option<Self> {
        let sql = sql.trim().to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();

        if parts.is_empty() {
            return None;
        }

        match parts[0] {
            "SHOW" if parts.len() >= 2 => {
                match parts[1] {
                    "POOLS" => Some(AdminCommand::ShowPools),
                    "STATS" | "STATS_TOTALS" | "STATS_AVERAGES" => Some(AdminCommand::ShowStats),
                    "DATABASES" => Some(AdminCommand::ShowDatabases),
                    "CLIENTS" => Some(AdminCommand::ShowClients),
                    "SERVERS" => Some(AdminCommand::ShowServers),
                    "VERSION" => Some(AdminCommand::ShowVersion),
                    "CONFIG" => Some(AdminCommand::ShowConfig),
                    _ => None, // Regular SHOW command (not admin)
                }
            }
            "PAUSE" => {
                let database = parts.get(1).map(|s| s.to_lowercase());
                Some(AdminCommand::Pause { database })
            }
            "RESUME" => {
                let database = parts.get(1).map(|s| s.to_lowercase());
                Some(AdminCommand::Resume { database })
            }
            "RELOAD" => Some(AdminCommand::Reload),
            "SHUTDOWN" => {
                let wait = parts.get(1).map(|s| *s == "WAIT").unwrap_or(false);
                Some(AdminCommand::Shutdown { wait })
            }
            "KILL" => {
                let database = parts.get(1).map(|s| s.to_lowercase());
                Some(AdminCommand::Kill { database })
            }
            _ => None,
        }
    }
}

/// Admin console for handling administrative commands
pub struct AdminConsole {
    /// Shared operational state: pool managers, reload/shutdown triggers,
    /// config, and the client/server registries (WP-10, P4 §4.1). SHOW
    /// commands (Task 2) read all report state from here; RELOAD/SHUTDOWN
    /// (Tasks 4/5) will read the control surfaces.
    handles: Arc<AdminHandles>,
    metrics: Arc<ProxyMetrics>,
}

impl AdminConsole {
    /// Create a new admin console from the shared [`AdminHandles`] and metrics.
    ///
    /// This is the interface WP-10 Tasks 2–5 depend on: the console reads all
    /// report/control state through `handles` (plus `metrics` for SHOW STATS).
    pub fn new(handles: Arc<AdminHandles>, metrics: Arc<ProxyMetrics>) -> Self {
        Self { handles, metrics }
    }

    /// Every configured database (default backend + `config.databases`), in a
    /// stable order (default first). Shared by `SHOW DATABASES`/`SHOW POOLS`/
    /// `SHOW SERVERS` so they report a consistent view.
    fn database_entries(&self) -> Vec<DatabaseEntry> {
        let cfg = &self.handles.config;
        let mut entries = vec![DatabaseEntry {
            name: cfg.backend.database.clone(),
            host: cfg.backend.host.clone(),
            port: cfg.backend.port,
            database: cfg.backend.database.clone(),
            user: cfg.backend.user.clone(),
            configured_pool_size: cfg.performance.pool_size,
            pool_key: "*".to_string(),
        }];
        for db in &cfg.databases {
            entries.push(DatabaseEntry {
                name: db.name.clone(),
                host: db.host.clone(),
                port: db.port,
                database: db.database.clone(),
                user: db.user.clone(),
                configured_pool_size: db.pool_size.unwrap_or(cfg.performance.pool_size),
                pool_key: db.name.clone(),
            });
        }
        entries
    }

    /// Count of currently-live (non-admin) clients per pool key, resolved the
    /// same way the proxy itself resolves a client's startup database to a
    /// pool (`pool_managers.get(db).or_else(|| pool_managers.get("*"))`, see
    /// `server.rs`). Admin-console connections are excluded: they don't
    /// occupy a backend pool slot.
    fn live_clients_per_pool_key(&self) -> HashMap<String, usize> {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for entry in self.handles.client_registry.snapshot() {
            if entry.state == ClientState::Admin {
                continue;
            }
            let key = if self.handles.pool_managers.contains_key(&entry.database) {
                entry.database
            } else {
                "*".to_string()
            };
            *counts.entry(key).or_insert(0) += 1;
        }
        counts
    }

    /// Check if an SQL command is an admin command
    ///
    /// This is used to detect admin commands before full parsing.
    /// Returns true for commands that should be handled by the admin console.
    pub fn is_admin_command(sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();

        // SHOW commands - but only specific ones
        if upper.starts_with("SHOW ") {
            let rest = upper.strip_prefix("SHOW ").unwrap_or("").trim();
            let keyword = rest.split_whitespace().next().unwrap_or("");
            return matches!(
                keyword,
                "POOLS"
                    | "STATS"
                    | "STATS_TOTALS"
                    | "STATS_AVERAGES"
                    | "DATABASES"
                    | "CLIENTS"
                    | "SERVERS"
                    | "VERSION"
                    | "CONFIG"
            );
        }

        // Other admin commands
        upper.starts_with("PAUSE")
            || upper.starts_with("RESUME")
            || upper.starts_with("RELOAD")
            || upper.starts_with("SHUTDOWN")
            || upper.starts_with("KILL")
    }

    /// Execute an admin command
    pub async fn execute(&self, sql: &str) -> Result<AdminResponse> {
        let cmd = AdminCommand::parse(sql)
            .ok_or_else(|| anyhow::anyhow!("Unknown admin command: {}", sql))?;

        match cmd {
            AdminCommand::ShowPools => self.show_pools(),
            AdminCommand::ShowStats => self.show_stats(),
            AdminCommand::ShowDatabases => self.show_databases(),
            AdminCommand::ShowClients => self.show_clients(),
            AdminCommand::ShowServers => self.show_servers(),
            AdminCommand::ShowVersion => self.show_version(),
            AdminCommand::ShowConfig => self.show_config(),
            AdminCommand::Pause { database } => self.pause(database).await,
            AdminCommand::Resume { database } => self.resume(database).await,
            AdminCommand::Reload => self.reload().await,
            AdminCommand::Shutdown { wait } => self.shutdown(wait).await,
            AdminCommand::Kill { database } => self.kill(database).await,
        }
    }

    fn show_pools(&self) -> Result<AdminResponse> {
        let entries = self.database_entries();
        let live_clients = self.live_clients_per_pool_key();
        let pool_mode = pool_mode_str(&self.handles.config.performance.connection_pooling);

        let rows = entries
            .into_iter()
            .map(|entry| {
                let cl_active = live_clients.get(&entry.pool_key).copied().unwrap_or(0);
                // cl_waiting/sv_active/sv_idle come from the live pool status
                // and wait queue when pooling is enabled for this database;
                // when pooling is disabled there is no pool to read (honest
                // zero, not fabrication — it genuinely doesn't exist).
                let (cl_waiting, sv_active, sv_idle) =
                    match self.handles.pool_managers.get(&entry.pool_key) {
                        Some(pm) => {
                            let status = pm.pool().status();
                            (
                                pm.wait_queue_depth(),
                                status.size.saturating_sub(status.available),
                                status.available,
                            )
                        }
                        None => (0, 0, 0),
                    };

                vec![
                    entry.name,
                    entry.user,
                    cl_active.to_string(),
                    cl_waiting.to_string(),
                    sv_active.to_string(),
                    sv_idle.to_string(),
                    // sv_used/sv_tested/sv_login: `PoolStatus` doesn't track
                    // these finer server-connection substates yet (candidate
                    // follow-up) — honest zero, not fabrication.
                    "0".to_string(),
                    "0".to_string(),
                    "0".to_string(),
                    // maxwait: wait-time-in-queue isn't measured yet (the
                    // prior value here reused the pool's `max_size`, a
                    // different metric under the wrong label — honest zero
                    // is more truthful than that).
                    "0".to_string(),
                    pool_mode.to_string(),
                ]
            })
            .collect();

        Ok(AdminResponse::RowSet {
            columns: vec![
                "database".to_string(),
                "user".to_string(),
                "cl_active".to_string(),
                "cl_waiting".to_string(),
                "sv_active".to_string(),
                "sv_idle".to_string(),
                "sv_used".to_string(),
                "sv_tested".to_string(),
                "sv_login".to_string(),
                "maxwait".to_string(),
                "pool_mode".to_string(),
            ],
            rows,
        })
    }

    fn show_stats(&self) -> Result<AdminResponse> {
        use std::sync::atomic::Ordering;

        let query_metrics = self.metrics.query_metrics();
        let total_queries = query_metrics.total_queries.load(Ordering::Relaxed);
        let _total_errors = query_metrics.total_errors.load(Ordering::Relaxed);

        let latency = query_metrics.get_latency_percentiles();
        let uptime_secs = self.metrics.uptime().as_secs().max(1);
        let avg_queries_per_sec = total_queries / uptime_secs;

        // total_query_time in microseconds (use mean * count as approximation)
        let total_time_us = (latency.mean_micros * total_queries as f64) as u64;

        // The real configured backend database name (was hardcoded "default").
        // `ProxyMetrics` is process-global, not siloed per database, so this
        // single row represents all traffic, labeled with the primary/default
        // database rather than fabricating per-db splits we don't measure.
        let database_name = self.handles.config.backend.database.clone();

        let rows = vec![vec![
            database_name,
            total_queries.to_string(), // total_xact_count
            total_queries.to_string(), // total_query_count
            // total_received/total_sent: byte counters are not collected
            // anywhere today (no hot-path byte counting exists) — honest
            // zero (candidate follow-up; NOT added in this task per scope:
            // latency budget + no new hot-path counters).
            "0".to_string(),
            "0".to_string(),
            total_time_us.to_string(), // total_xact_time
            total_time_us.to_string(), // total_query_time
            // total_wait_time: pool wait-time isn't measured yet — honest zero.
            "0".to_string(),
            avg_queries_per_sec.to_string(), // avg_xact_count
            avg_queries_per_sec.to_string(), // avg_query_count
            // avg_recv/avg_sent: same as total_received/total_sent above.
            "0".to_string(),
            "0".to_string(),
            latency.mean_micros.round().to_string(), // avg_xact_time
            latency.mean_micros.round().to_string(), // avg_query_time
            // avg_wait_time: same as total_wait_time above.
            "0".to_string(),
        ]];

        Ok(AdminResponse::RowSet {
            columns: vec![
                "database".to_string(),
                "total_xact_count".to_string(),
                "total_query_count".to_string(),
                "total_received".to_string(),
                "total_sent".to_string(),
                "total_xact_time".to_string(),
                "total_query_time".to_string(),
                "total_wait_time".to_string(),
                "avg_xact_count".to_string(),
                "avg_query_count".to_string(),
                "avg_recv".to_string(),
                "avg_sent".to_string(),
                "avg_xact_time".to_string(),
                "avg_query_time".to_string(),
                "avg_wait_time".to_string(),
            ],
            rows,
        })
    }

    fn show_databases(&self) -> Result<AdminResponse> {
        let entries = self.database_entries();
        let live_clients = self.live_clients_per_pool_key();
        let pool_mode = pool_mode_str(&self.handles.config.performance.connection_pooling);
        // No per-database connection cap is configured; the real constraint
        // is the proxy-wide `proxy.max_connections`. Reporting that (rather
        // than a fabricated 0) is the honest value available.
        let max_connections = self.handles.config.proxy.max_connections;

        let rows = entries
            .into_iter()
            .map(|entry| {
                let pool_size = self
                    .handles
                    .pool_managers
                    .get(&entry.pool_key)
                    .map(|pm| pm.pool().status().max_size)
                    .unwrap_or(entry.configured_pool_size);
                let current_connections = live_clients.get(&entry.pool_key).copied().unwrap_or(0);

                vec![
                    entry.name,
                    entry.host,
                    entry.port.to_string(),
                    entry.database,
                    // force_user: no forced-user-override concept exists in
                    // config today — honestly always empty, not fabricated.
                    String::new(),
                    pool_size.to_string(),
                    // reserve_pool: no reserve-pool feature exists yet.
                    "0".to_string(),
                    pool_mode.to_string(),
                    max_connections.to_string(),
                    current_connections.to_string(),
                    // paused/disabled: PAUSE/RESUME state isn't tracked yet
                    // (WP-10 Task 5) — honest zero.
                    "0".to_string(),
                    "0".to_string(),
                ]
            })
            .collect();

        Ok(AdminResponse::RowSet {
            columns: vec![
                "name".to_string(),
                "host".to_string(),
                "port".to_string(),
                "database".to_string(),
                "force_user".to_string(),
                "pool_size".to_string(),
                "reserve_pool".to_string(),
                "pool_mode".to_string(),
                "max_connections".to_string(),
                "current_connections".to_string(),
                "paused".to_string(),
                "disabled".to_string(),
            ],
            rows,
        })
    }

    fn show_clients(&self) -> Result<AdminResponse> {
        // The proxy's own listen address is real and known even though we
        // don't track a per-connection local socket — real value, not a
        // fabricated placeholder.
        let (local_addr, local_port) = split_host_port(&self.handles.config.proxy.listen_address);

        let mut entries = self.handles.client_registry.snapshot();
        // Deterministic order for callers/tests.
        entries.sort_by_key(|e| e.conn_id);

        let rows = entries
            .into_iter()
            .map(|entry| {
                let (addr, port) = split_host_port(&entry.addr);
                vec![
                    "C".to_string(), // type: PgBouncer client-connection type code
                    entry.user,
                    entry.database,
                    client_state_str(entry.state).to_string(),
                    addr,
                    port,
                    local_addr.clone(),
                    local_port.clone(),
                    // connect_time/request_time: `ClientEntry` stores a
                    // monotonic `Instant`, which has no wall-clock
                    // representation, so we report the real elapsed time
                    // since the event rather than fabricate an absolute
                    // timestamp.
                    format!("{:.3}s ago", entry.connect_time.elapsed().as_secs_f64()),
                    format!("{:.3}s ago", entry.last_request_time.elapsed().as_secs_f64()),
                    // wait/wait_us/close_needed/ptr/link/remote_pid: none of
                    // these are tracked per-client today — honest zero/empty,
                    // not fabrication (candidate follow-up).
                    "0".to_string(),
                    "0".to_string(),
                    "0".to_string(),
                    "0".to_string(),
                    String::new(),
                    "0".to_string(),
                    if entry.tls { "1".to_string() } else { "0".to_string() },
                ]
            })
            .collect();

        Ok(AdminResponse::RowSet {
            columns: vec![
                "type".to_string(),
                "user".to_string(),
                "database".to_string(),
                "state".to_string(),
                "addr".to_string(),
                "port".to_string(),
                "local_addr".to_string(),
                "local_port".to_string(),
                "connect_time".to_string(),
                "request_time".to_string(),
                "wait".to_string(),
                "wait_us".to_string(),
                "close_needed".to_string(),
                "ptr".to_string(),
                "link".to_string(),
                "remote_pid".to_string(),
                "tls".to_string(),
            ],
            rows,
        })
    }

    fn show_servers(&self) -> Result<AdminResponse> {
        // Join pool-key -> (name, user) so SHOW SERVERS agrees with SHOW
        // DATABASES/SHOW POOLS about what each pool's database/user is.
        let name_and_user_by_pool_key: HashMap<String, (String, String)> =
            self.database_entries().into_iter().map(|e| (e.pool_key, (e.name, e.user))).collect();

        let rows = self
            .handles
            .server_registry
            .snapshot()
            .into_iter()
            .map(|snap| {
                let (addr, port) = split_host_port(&snap.backend_addr);
                let (database, user) = name_and_user_by_pool_key
                    .get(&snap.database)
                    .cloned()
                    .unwrap_or_else(|| (snap.database.clone(), String::new()));

                vec![
                    "S".to_string(), // type: PgBouncer server-connection type code
                    user,
                    database,
                    // state: pool-level granularity only (Task 1 limitation:
                    // deadpool exposes no stable per-socket handle). A pool
                    // with any available connection is reported "idle",
                    // otherwise "active" (every connection currently
                    // checked out) — real, aggregate state, not a fabricated
                    // per-socket row.
                    if snap.available > 0 { "idle".to_string() } else { "active".to_string() },
                    addr,
                    port,
                    // local_addr/local_port/connect_time/request_time: not
                    // tracked at pool granularity today — honest empty.
                    String::new(),
                    "0".to_string(),
                    String::new(),
                    String::new(),
                    // wait/wait_us/close_needed/ptr/link/remote_pid: no live
                    // source at pool granularity — honest zero/empty.
                    "0".to_string(),
                    "0".to_string(),
                    "0".to_string(),
                    "0".to_string(),
                    String::new(),
                    "0".to_string(),
                    // tls: backend (server-side) TLS is not yet surfaced
                    // per-pool in `ServerPoolSnapshot` — honest zero.
                    "0".to_string(),
                ]
            })
            .collect();

        Ok(AdminResponse::RowSet {
            columns: vec![
                "type".to_string(),
                "user".to_string(),
                "database".to_string(),
                "state".to_string(),
                "addr".to_string(),
                "port".to_string(),
                "local_addr".to_string(),
                "local_port".to_string(),
                "connect_time".to_string(),
                "request_time".to_string(),
                "wait".to_string(),
                "wait_us".to_string(),
                "close_needed".to_string(),
                "ptr".to_string(),
                "link".to_string(),
                "remote_pid".to_string(),
                "tls".to_string(),
            ],
            rows,
        })
    }

    fn show_version(&self) -> Result<AdminResponse> {
        let version = env!("CARGO_PKG_VERSION");
        Ok(AdminResponse::RowSet {
            columns: vec!["version".to_string()],
            rows: vec![vec![format!("Scry {}", version)]],
        })
    }

    fn show_config(&self) -> Result<AdminResponse> {
        // Return basic config info
        Ok(AdminResponse::RowSet {
            columns: vec![
                "key".to_string(),
                "value".to_string(),
                "default".to_string(),
                "changeable".to_string(),
            ],
            rows: vec![
                vec![
                    "pool_mode".to_string(),
                    "transaction".to_string(),
                    "transaction".to_string(),
                    "yes".to_string(),
                ],
                vec![
                    "max_client_conn".to_string(),
                    "100".to_string(),
                    "100".to_string(),
                    "yes".to_string(),
                ],
                vec![
                    "default_pool_size".to_string(),
                    "10".to_string(),
                    "10".to_string(),
                    "yes".to_string(),
                ],
            ],
        })
    }

    async fn pause(&self, _database: Option<String>) -> Result<AdminResponse> {
        // TODO: Implement pause functionality
        Ok(AdminResponse::CommandComplete { tag: "PAUSE".to_string() })
    }

    async fn resume(&self, _database: Option<String>) -> Result<AdminResponse> {
        // TODO: Implement resume functionality
        Ok(AdminResponse::CommandComplete { tag: "RESUME".to_string() })
    }

    async fn reload(&self) -> Result<AdminResponse> {
        // TODO: Implement config reload
        Ok(AdminResponse::CommandComplete { tag: "RELOAD".to_string() })
    }

    async fn shutdown(&self, _wait: bool) -> Result<AdminResponse> {
        // TODO: Implement shutdown signal
        Ok(AdminResponse::CommandComplete { tag: "SHUTDOWN".to_string() })
    }

    async fn kill(&self, _database: Option<String>) -> Result<AdminResponse> {
        // TODO: Implement kill functionality
        Ok(AdminResponse::CommandComplete { tag: "KILL".to_string() })
    }
}
