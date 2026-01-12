//! Admin command parsing and execution
//!
//! Implements PgBouncer-compatible admin commands.

use super::response::AdminResponse;
use crate::observability::ProxyMetrics;
use crate::proxy::PoolManager;
use anyhow::Result;
use std::sync::Arc;

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
    pool_manager: Option<Arc<PoolManager>>,
    metrics: Arc<ProxyMetrics>,
}

impl AdminConsole {
    /// Create a new admin console
    pub fn new(pool_manager: Option<Arc<PoolManager>>, metrics: Arc<ProxyMetrics>) -> Self {
        Self {
            pool_manager,
            metrics,
        }
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
                "POOLS" | "STATS" | "STATS_TOTALS" | "STATS_AVERAGES"
                | "DATABASES" | "CLIENTS" | "SERVERS" | "VERSION" | "CONFIG"
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
        let mut rows = Vec::new();

        if let Some(ref pm) = self.pool_manager {
            let status = pm.pool().status();
            rows.push(vec![
                "default".to_string(),        // database
                "scry".to_string(),           // user
                status.size.to_string(),      // cl_active (approximate)
                "0".to_string(),              // cl_waiting
                status.available.to_string(), // sv_active
                "0".to_string(),              // sv_idle
                "0".to_string(),              // sv_used
                "0".to_string(),              // sv_tested
                "0".to_string(),              // sv_login
                status.max_size.to_string(),  // maxwait
                "transaction".to_string(),    // pool_mode
            ]);
        }

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

        let rows = vec![vec![
            "default".to_string(),                    // database
            total_queries.to_string(),                // total_xact_count
            total_queries.to_string(),                // total_query_count
            "0".to_string(),                          // total_received
            "0".to_string(),                          // total_sent
            total_time_us.to_string(),                // total_xact_time
            total_time_us.to_string(),                // total_query_time
            "0".to_string(),                          // total_wait_time
            avg_queries_per_sec.to_string(),          // avg_xact_count
            avg_queries_per_sec.to_string(),          // avg_query_count
            "0".to_string(),                          // avg_recv
            "0".to_string(),                          // avg_sent
            latency.mean_micros.round().to_string(),  // avg_xact_time
            latency.mean_micros.round().to_string(),  // avg_query_time
            "0".to_string(),                          // avg_wait_time
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
        // For now, just show the default database
        let rows = vec![vec![
            "default".to_string(),  // name
            "localhost".to_string(), // host
            "5432".to_string(),     // port
            "postgres".to_string(), // database
            "".to_string(),         // force_user
            "10".to_string(),       // pool_size
            "0".to_string(),        // reserve_pool
            "transaction".to_string(), // pool_mode
            "0".to_string(),        // max_connections
            "0".to_string(),        // current_connections
            "0".to_string(),        // paused
            "0".to_string(),        // disabled
        ]];

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
        // Return empty for now - would need connection tracking
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
            rows: vec![],
        })
    }

    fn show_servers(&self) -> Result<AdminResponse> {
        // Return empty for now - would need backend connection tracking
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
            rows: vec![],
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
                vec!["pool_mode".to_string(), "transaction".to_string(), "transaction".to_string(), "yes".to_string()],
                vec!["max_client_conn".to_string(), "100".to_string(), "100".to_string(), "yes".to_string()],
                vec!["default_pool_size".to_string(), "10".to_string(), "10".to_string(), "yes".to_string()],
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
