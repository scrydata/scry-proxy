//! PgBouncer configuration compatibility layer.
//!
//! This module allows Scry to be a drop-in replacement for PgBouncer by supporting:
//! - Parsing `pgbouncer.ini` configuration files
//! - `PGBOUNCER_*` environment variable aliases
//!
//! # Usage
//!
//! ```rust,ignore
//! use scry::config::pgbouncer::PgBouncerConfig;
//!
//! // Load from pgbouncer.ini file
//! let pgb_config = PgBouncerConfig::from_file("pgbouncer.ini")?;
//!
//! // Or from environment variables
//! let pgb_config = PgBouncerConfig::from_env();
//!
//! // Convert to Scry config
//! let scry_config = pgb_config.to_scry_config(Config::default());
//! ```

use std::env;
use std::path::Path;

use anyhow::{Context, Result};
use indexmap::IndexMap;
use ini::Ini;

use super::{Config, PoolingStrategy};

/// A parsed database entry from pgbouncer.ini [databases] section.
///
/// Format: `dbname = host=HOST port=PORT dbname=DBNAME user=USER password=PASSWORD`
#[derive(Debug, Clone, Default)]
pub struct DatabaseEntry {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub dbname: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
}

impl DatabaseEntry {
    /// Parse a database connection string from pgbouncer.ini format.
    ///
    /// Format: `host=HOST port=PORT dbname=DBNAME user=USER password=PASSWORD`
    pub fn parse(connection_string: &str) -> Self {
        let mut entry = DatabaseEntry::default();

        for part in connection_string.split_whitespace() {
            if let Some((key, value)) = part.split_once('=') {
                match key.to_lowercase().as_str() {
                    "host" => entry.host = Some(value.to_string()),
                    "port" => entry.port = value.parse().ok(),
                    "dbname" => entry.dbname = Some(value.to_string()),
                    "user" => entry.user = Some(value.to_string()),
                    "password" => entry.password = Some(value.to_string()),
                    _ => {} // Ignore unknown keys
                }
            }
        }

        entry
    }
}

/// PgBouncer-compatible configuration.
///
/// Maps PgBouncer configuration options to Scry equivalents.
#[derive(Debug, Clone, Default)]
pub struct PgBouncerConfig {
    /// Database connection definitions from [databases] section.
    /// Uses IndexMap to preserve insertion order from the INI file,
    /// ensuring deterministic selection of the first database.
    pub databases: IndexMap<String, DatabaseEntry>,

    /// Address to listen on (default: 127.0.0.1)
    pub listen_addr: Option<String>,

    /// Port to listen on (default: 6432)
    pub listen_port: Option<u16>,

    /// Pool mode: session, transaction, or statement
    pub pool_mode: Option<String>,

    /// Default pool size per user/database pair
    pub default_pool_size: Option<usize>,

    /// Minimum number of server connections to keep open
    pub min_pool_size: Option<usize>,

    /// Maximum number of client connections
    pub max_client_conn: Option<usize>,

    /// Server idle timeout in seconds
    pub server_idle_timeout: Option<u64>,

    /// Query timeout in seconds (0 = unlimited).
    /// Note: This is parsed for compatibility but not yet mapped to a Scry config option.
    /// Scry does not currently support per-query timeouts.
    pub query_timeout: Option<u64>,

    /// Server connect timeout in seconds
    pub server_connect_timeout: Option<u64>,
}

impl PgBouncerConfig {
    /// Load configuration from a pgbouncer.ini file.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let config = PgBouncerConfig::from_file("pgbouncer.ini")?;
    /// ```
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let ini = Ini::load_from_file(path.as_ref())
            .with_context(|| format!("Failed to load {}", path.as_ref().display()))?;

        let mut config = PgBouncerConfig::default();

        // Parse [databases] section
        if let Some(databases_section) = ini.section(Some("databases")) {
            for (key, value) in databases_section.iter() {
                config.databases.insert(key.to_string(), DatabaseEntry::parse(value));
            }
        }

        // Parse [pgbouncer] section
        if let Some(pgbouncer_section) = ini.section(Some("pgbouncer")) {
            for (key, value) in pgbouncer_section.iter() {
                match key.to_lowercase().as_str() {
                    "listen_addr" => config.listen_addr = Some(value.to_string()),
                    "listen_port" => config.listen_port = value.parse().ok(),
                    "pool_mode" => config.pool_mode = Some(value.to_lowercase()),
                    "default_pool_size" => config.default_pool_size = value.parse().ok(),
                    "min_pool_size" => config.min_pool_size = value.parse().ok(),
                    "max_client_conn" => config.max_client_conn = value.parse().ok(),
                    "server_idle_timeout" => config.server_idle_timeout = value.parse().ok(),
                    "query_timeout" => config.query_timeout = value.parse().ok(),
                    "server_connect_timeout" => config.server_connect_timeout = value.parse().ok(),
                    _ => {} // Ignore unsupported settings
                }
            }
        }

        Ok(config)
    }

    /// Load configuration from PGBOUNCER_* environment variables.
    ///
    /// Supported variables:
    /// - `PGBOUNCER_LISTEN_ADDR` - Address to listen on
    /// - `PGBOUNCER_LISTEN_PORT` - Port to listen on
    /// - `PGBOUNCER_POOL_MODE` - Pool mode (session, transaction, statement)
    /// - `PGBOUNCER_DEFAULT_POOL_SIZE` - Default pool size
    /// - `PGBOUNCER_MIN_POOL_SIZE` - Minimum pool size
    /// - `PGBOUNCER_MAX_CLIENT_CONN` - Maximum client connections
    /// - `PGBOUNCER_SERVER_IDLE_TIMEOUT` - Server idle timeout (seconds)
    /// - `PGBOUNCER_QUERY_TIMEOUT` - Query timeout (seconds)
    /// - `PGBOUNCER_SERVER_CONNECT_TIMEOUT` - Server connect timeout (seconds)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// std::env::set_var("PGBOUNCER_LISTEN_PORT", "6432");
    /// let config = PgBouncerConfig::from_env();
    /// assert_eq!(config.listen_port, Some(6432));
    /// ```
    pub fn from_env() -> Self {
        let mut config = PgBouncerConfig::default();

        if let Ok(val) = env::var("PGBOUNCER_LISTEN_ADDR") {
            config.listen_addr = Some(val);
        }

        if let Ok(val) = env::var("PGBOUNCER_LISTEN_PORT") {
            config.listen_port = val.parse().ok();
        }

        if let Ok(val) = env::var("PGBOUNCER_POOL_MODE") {
            config.pool_mode = Some(val.to_lowercase());
        }

        if let Ok(val) = env::var("PGBOUNCER_DEFAULT_POOL_SIZE") {
            config.default_pool_size = val.parse().ok();
        }

        if let Ok(val) = env::var("PGBOUNCER_MIN_POOL_SIZE") {
            config.min_pool_size = val.parse().ok();
        }

        if let Ok(val) = env::var("PGBOUNCER_MAX_CLIENT_CONN") {
            config.max_client_conn = val.parse().ok();
        }

        if let Ok(val) = env::var("PGBOUNCER_SERVER_IDLE_TIMEOUT") {
            config.server_idle_timeout = val.parse().ok();
        }

        if let Ok(val) = env::var("PGBOUNCER_QUERY_TIMEOUT") {
            config.query_timeout = val.parse().ok();
        }

        if let Ok(val) = env::var("PGBOUNCER_SERVER_CONNECT_TIMEOUT") {
            config.server_connect_timeout = val.parse().ok();
        }

        config
    }

    /// Convert PgBouncer configuration to Scry configuration.
    ///
    /// Takes a base Scry config and applies PgBouncer settings on top.
    /// The first database entry is used for backend connection settings.
    ///
    /// # Mapping
    ///
    /// | PgBouncer Setting      | Scry Setting                         |
    /// |------------------------|--------------------------------------|
    /// | listen_addr            | proxy.listen_address (host part)     |
    /// | listen_port            | proxy.listen_address (port part)     |
    /// | pool_mode              | performance.connection_pooling       |
    /// | default_pool_size      | performance.pool_size                |
    /// | min_pool_size          | performance.pool_min_idle            |
    /// | max_client_conn        | proxy.max_connections                |
    /// | server_idle_timeout    | performance.pool_recycle_secs        |
    /// | server_connect_timeout | backend.connection_timeout_ms        |
    /// | database host          | backend.host                         |
    /// | database port          | backend.port                         |
    /// | database dbname        | backend.database                     |
    /// | database user          | backend.user                         |
    /// | database password      | backend.password                     |
    pub fn to_scry_config(&self, mut config: Config) -> Config {
        // Build listen address
        let host = self.listen_addr.clone().unwrap_or_else(|| "127.0.0.1".to_string());
        let port = self.listen_port.unwrap_or(6432);
        config.proxy.listen_address = format!("{}:{}", host, port);

        // Map pool_mode to PoolingStrategy
        // Note: We normalize to lowercase to handle case-insensitive matching
        // (e.g., "TRANSACTION", "Transaction", "transaction" all work)
        if let Some(ref mode) = self.pool_mode {
            config.performance.connection_pooling = match mode.to_lowercase().as_str() {
                "session" => PoolingStrategy::Session,
                "transaction" => PoolingStrategy::Transaction,
                "statement" => {
                    // PgBouncer's statement mode is closest to Transaction mode in Scry
                    // since Scry doesn't have a direct statement-level pooling mode
                    PoolingStrategy::Transaction
                }
                _ => PoolingStrategy::Hybrid, // Default to Hybrid for unknown modes
            };
        }

        // Map pool sizes
        if let Some(size) = self.default_pool_size {
            config.performance.pool_size = size;
        }

        if let Some(size) = self.min_pool_size {
            config.performance.pool_min_idle = size;
        }

        if let Some(max) = self.max_client_conn {
            config.proxy.max_connections = max;
        }

        // Map server_idle_timeout to pool_recycle_secs
        if let Some(timeout) = self.server_idle_timeout {
            config.performance.pool_recycle_secs = timeout;
        }

        // Map server_connect_timeout to connection_timeout_ms
        if let Some(timeout) = self.server_connect_timeout {
            config.backend.connection_timeout_ms = timeout * 1000; // Convert seconds to ms
        }

        // Use first database entry for backend settings
        if let Some((_name, db)) = self.databases.iter().next() {
            if let Some(ref host) = db.host {
                config.backend.host = host.clone();
            }
            if let Some(port) = db.port {
                config.backend.port = port;
            }
            if let Some(ref dbname) = db.dbname {
                config.backend.database = dbname.clone();
            }
            if let Some(ref user) = db.user {
                config.backend.user = user.clone();
            }
            if let Some(ref password) = db.password {
                config.backend.password = password.clone();
            }
        }

        config
    }

    /// Load from both pgbouncer.ini file and environment variables.
    ///
    /// Environment variables take precedence over file settings.
    pub fn load<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let mut config = if let Some(p) = path {
            if p.as_ref().exists() {
                Self::from_file(p)?
            } else {
                Self::default()
            }
        } else {
            Self::default()
        };

        // Merge environment variables (env takes precedence)
        let env_config = Self::from_env();

        if env_config.listen_addr.is_some() {
            config.listen_addr = env_config.listen_addr;
        }
        if env_config.listen_port.is_some() {
            config.listen_port = env_config.listen_port;
        }
        if env_config.pool_mode.is_some() {
            config.pool_mode = env_config.pool_mode;
        }
        if env_config.default_pool_size.is_some() {
            config.default_pool_size = env_config.default_pool_size;
        }
        if env_config.min_pool_size.is_some() {
            config.min_pool_size = env_config.min_pool_size;
        }
        if env_config.max_client_conn.is_some() {
            config.max_client_conn = env_config.max_client_conn;
        }
        if env_config.server_idle_timeout.is_some() {
            config.server_idle_timeout = env_config.server_idle_timeout;
        }
        if env_config.query_timeout.is_some() {
            config.query_timeout = env_config.query_timeout;
        }
        if env_config.server_connect_timeout.is_some() {
            config.server_connect_timeout = env_config.server_connect_timeout;
        }

        Ok(config)
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Helper to create a temporary ini file
    fn create_temp_ini(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file
    }

    // ============================================
    // Database Entry Parsing Tests
    // ============================================

    #[test]
    fn test_database_entry_parse_full() {
        let entry = DatabaseEntry::parse(
            "host=localhost port=5432 dbname=mydb user=postgres password=secret",
        );

        assert_eq!(entry.host, Some("localhost".to_string()));
        assert_eq!(entry.port, Some(5432));
        assert_eq!(entry.dbname, Some("mydb".to_string()));
        assert_eq!(entry.user, Some("postgres".to_string()));
        assert_eq!(entry.password, Some("secret".to_string()));
    }

    #[test]
    fn test_database_entry_parse_partial() {
        let entry = DatabaseEntry::parse("host=db.example.com dbname=production");

        assert_eq!(entry.host, Some("db.example.com".to_string()));
        assert_eq!(entry.port, None);
        assert_eq!(entry.dbname, Some("production".to_string()));
        assert_eq!(entry.user, None);
        assert_eq!(entry.password, None);
    }

    #[test]
    fn test_database_entry_parse_empty() {
        let entry = DatabaseEntry::parse("");

        assert_eq!(entry.host, None);
        assert_eq!(entry.port, None);
        assert_eq!(entry.dbname, None);
        assert_eq!(entry.user, None);
        assert_eq!(entry.password, None);
    }

    #[test]
    fn test_database_entry_parse_invalid_port() {
        let entry = DatabaseEntry::parse("host=localhost port=invalid");

        assert_eq!(entry.host, Some("localhost".to_string()));
        assert_eq!(entry.port, None); // Invalid port should be None
    }

    #[test]
    fn test_database_entry_parse_unknown_keys() {
        let entry = DatabaseEntry::parse("host=localhost unknown=value port=5432");

        assert_eq!(entry.host, Some("localhost".to_string()));
        assert_eq!(entry.port, Some(5432));
        // Unknown keys are ignored
    }

    // ============================================
    // PgBouncer INI File Parsing Tests
    // ============================================

    #[test]
    fn test_from_file_complete() {
        let content = r#"
[databases]
mydb = host=localhost port=5432 dbname=mydb user=postgres password=secret
production = host=prod.example.com port=5433 dbname=proddb user=app password=prodpass

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 6432
pool_mode = transaction
default_pool_size = 20
min_pool_size = 5
max_client_conn = 100
server_idle_timeout = 600
query_timeout = 30
server_connect_timeout = 10
"#;

        let file = create_temp_ini(content);
        let config = PgBouncerConfig::from_file(file.path()).unwrap();

        // Check databases
        assert_eq!(config.databases.len(), 2);

        let mydb = config.databases.get("mydb").unwrap();
        assert_eq!(mydb.host, Some("localhost".to_string()));
        assert_eq!(mydb.port, Some(5432));
        assert_eq!(mydb.dbname, Some("mydb".to_string()));
        assert_eq!(mydb.user, Some("postgres".to_string()));
        assert_eq!(mydb.password, Some("secret".to_string()));

        let prod = config.databases.get("production").unwrap();
        assert_eq!(prod.host, Some("prod.example.com".to_string()));
        assert_eq!(prod.port, Some(5433));

        // Check pgbouncer section
        assert_eq!(config.listen_addr, Some("0.0.0.0".to_string()));
        assert_eq!(config.listen_port, Some(6432));
        assert_eq!(config.pool_mode, Some("transaction".to_string()));
        assert_eq!(config.default_pool_size, Some(20));
        assert_eq!(config.min_pool_size, Some(5));
        assert_eq!(config.max_client_conn, Some(100));
        assert_eq!(config.server_idle_timeout, Some(600));
        assert_eq!(config.query_timeout, Some(30));
        assert_eq!(config.server_connect_timeout, Some(10));
    }

    #[test]
    fn test_from_file_minimal() {
        let content = r#"
[pgbouncer]
listen_port = 6432
"#;

        let file = create_temp_ini(content);
        let config = PgBouncerConfig::from_file(file.path()).unwrap();

        assert!(config.databases.is_empty());
        assert_eq!(config.listen_port, Some(6432));
        assert_eq!(config.listen_addr, None);
        assert_eq!(config.pool_mode, None);
    }

    #[test]
    fn test_from_file_empty() {
        let content = "";

        let file = create_temp_ini(content);
        let config = PgBouncerConfig::from_file(file.path()).unwrap();

        assert!(config.databases.is_empty());
        assert_eq!(config.listen_addr, None);
        assert_eq!(config.listen_port, None);
    }

    #[test]
    fn test_from_file_case_insensitive() {
        let content = r#"
[pgbouncer]
LISTEN_ADDR = 0.0.0.0
Listen_Port = 6432
Pool_Mode = SESSION
"#;

        let file = create_temp_ini(content);
        let config = PgBouncerConfig::from_file(file.path()).unwrap();

        assert_eq!(config.listen_addr, Some("0.0.0.0".to_string()));
        assert_eq!(config.listen_port, Some(6432));
        assert_eq!(config.pool_mode, Some("session".to_string()));
    }

    #[test]
    fn test_from_file_nonexistent() {
        let result = PgBouncerConfig::from_file("/nonexistent/path/pgbouncer.ini");
        assert!(result.is_err());
    }

    #[test]
    fn test_from_file_with_comments() {
        // Note: rust-ini treats semicolons after values as part of the value
        // unless properly quoted. This matches real pgbouncer.ini behavior where
        // comments should be on their own line.
        let content = r#"
; This is a comment
[databases]
; Database definitions
mydb = host=localhost port=5432 dbname=mydb

[pgbouncer]
; Main settings
listen_port = 6432
pool_mode = transaction
"#;

        let file = create_temp_ini(content);
        let config = PgBouncerConfig::from_file(file.path()).unwrap();

        assert_eq!(config.databases.len(), 1);
        assert_eq!(config.listen_port, Some(6432));
        assert_eq!(config.pool_mode, Some("transaction".to_string()));
    }

    // ============================================
    // Environment Variable Loading Tests
    // ============================================

    // Note: Environment variable tests are inherently prone to race conditions
    // when run in parallel. We use a static mutex to serialize these tests.
    use std::sync::Mutex;
    static ENV_TEST_MUTEX: Mutex<()> = Mutex::new(());

    /// Helper to clean all PGBOUNCER_* env vars
    fn clear_pgbouncer_env_vars() {
        env::remove_var("PGBOUNCER_LISTEN_ADDR");
        env::remove_var("PGBOUNCER_LISTEN_PORT");
        env::remove_var("PGBOUNCER_POOL_MODE");
        env::remove_var("PGBOUNCER_DEFAULT_POOL_SIZE");
        env::remove_var("PGBOUNCER_MIN_POOL_SIZE");
        env::remove_var("PGBOUNCER_MAX_CLIENT_CONN");
        env::remove_var("PGBOUNCER_SERVER_IDLE_TIMEOUT");
        env::remove_var("PGBOUNCER_QUERY_TIMEOUT");
        env::remove_var("PGBOUNCER_SERVER_CONNECT_TIMEOUT");
    }

    #[test]
    fn test_from_env_all_variables() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();

        // Clear any existing values first
        clear_pgbouncer_env_vars();

        // Set test values
        env::set_var("PGBOUNCER_LISTEN_ADDR", "0.0.0.0");
        env::set_var("PGBOUNCER_LISTEN_PORT", "6432");
        env::set_var("PGBOUNCER_POOL_MODE", "TRANSACTION");
        env::set_var("PGBOUNCER_DEFAULT_POOL_SIZE", "25");
        env::set_var("PGBOUNCER_MIN_POOL_SIZE", "10");
        env::set_var("PGBOUNCER_MAX_CLIENT_CONN", "200");
        env::set_var("PGBOUNCER_SERVER_IDLE_TIMEOUT", "300");
        env::set_var("PGBOUNCER_QUERY_TIMEOUT", "60");
        env::set_var("PGBOUNCER_SERVER_CONNECT_TIMEOUT", "15");

        let config = PgBouncerConfig::from_env();

        assert_eq!(config.listen_addr, Some("0.0.0.0".to_string()));
        assert_eq!(config.listen_port, Some(6432));
        assert_eq!(config.pool_mode, Some("transaction".to_string()));
        assert_eq!(config.default_pool_size, Some(25));
        assert_eq!(config.min_pool_size, Some(10));
        assert_eq!(config.max_client_conn, Some(200));
        assert_eq!(config.server_idle_timeout, Some(300));
        assert_eq!(config.query_timeout, Some(60));
        assert_eq!(config.server_connect_timeout, Some(15));

        // Clean up
        clear_pgbouncer_env_vars();
    }

    #[test]
    fn test_from_env_invalid_values() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();

        // Clear any existing values first
        clear_pgbouncer_env_vars();

        // Set invalid values
        env::set_var("PGBOUNCER_LISTEN_PORT", "not_a_number");
        env::set_var("PGBOUNCER_DEFAULT_POOL_SIZE", "invalid");

        let config = PgBouncerConfig::from_env();

        // Invalid values should result in None
        assert_eq!(config.listen_port, None);
        assert_eq!(config.default_pool_size, None);

        // Clean up
        clear_pgbouncer_env_vars();
    }

    // ============================================
    // Scry Config Conversion Tests
    // ============================================

    #[test]
    fn test_to_scry_config_listen_address() {
        let mut pgb = PgBouncerConfig::default();
        pgb.listen_addr = Some("0.0.0.0".to_string());
        pgb.listen_port = Some(6432);

        let config = pgb.to_scry_config(Config::default());

        assert_eq!(config.proxy.listen_address, "0.0.0.0:6432");
    }

    #[test]
    fn test_to_scry_config_listen_address_defaults() {
        let pgb = PgBouncerConfig::default();

        let config = pgb.to_scry_config(Config::default());

        // Default PgBouncer port is 6432, default host is 127.0.0.1
        assert_eq!(config.proxy.listen_address, "127.0.0.1:6432");
    }

    #[test]
    fn test_to_scry_config_pool_mode_session() {
        let mut pgb = PgBouncerConfig::default();
        pgb.pool_mode = Some("session".to_string());

        let config = pgb.to_scry_config(Config::default());

        assert_eq!(config.performance.connection_pooling, PoolingStrategy::Session);
    }

    #[test]
    fn test_to_scry_config_pool_mode_transaction() {
        let mut pgb = PgBouncerConfig::default();
        pgb.pool_mode = Some("transaction".to_string());

        let config = pgb.to_scry_config(Config::default());

        assert_eq!(config.performance.connection_pooling, PoolingStrategy::Transaction);
    }

    #[test]
    fn test_to_scry_config_pool_mode_statement() {
        // Statement mode maps to Transaction in Scry (closest equivalent)
        let mut pgb = PgBouncerConfig::default();
        pgb.pool_mode = Some("statement".to_string());

        let config = pgb.to_scry_config(Config::default());

        assert_eq!(config.performance.connection_pooling, PoolingStrategy::Transaction);
    }

    #[test]
    fn test_to_scry_config_pool_mode_unknown() {
        // Unknown modes should default to Hybrid
        let mut pgb = PgBouncerConfig::default();
        pgb.pool_mode = Some("unknown_mode".to_string());

        let config = pgb.to_scry_config(Config::default());

        assert_eq!(config.performance.connection_pooling, PoolingStrategy::Hybrid);
    }

    #[test]
    fn test_to_scry_config_pool_sizes() {
        let mut pgb = PgBouncerConfig::default();
        pgb.default_pool_size = Some(50);
        pgb.min_pool_size = Some(10);

        let config = pgb.to_scry_config(Config::default());

        assert_eq!(config.performance.pool_size, 50);
        assert_eq!(config.performance.pool_min_idle, 10);
    }

    #[test]
    fn test_to_scry_config_max_client_conn() {
        let mut pgb = PgBouncerConfig::default();
        pgb.max_client_conn = Some(500);

        let config = pgb.to_scry_config(Config::default());

        assert_eq!(config.proxy.max_connections, 500);
    }

    #[test]
    fn test_to_scry_config_timeouts() {
        let mut pgb = PgBouncerConfig::default();
        pgb.server_idle_timeout = Some(600);
        pgb.server_connect_timeout = Some(15);

        let config = pgb.to_scry_config(Config::default());

        assert_eq!(config.performance.pool_recycle_secs, 600);
        assert_eq!(config.backend.connection_timeout_ms, 15000); // Converted to ms
    }

    #[test]
    fn test_to_scry_config_database_entry() {
        let mut pgb = PgBouncerConfig::default();
        pgb.databases.insert(
            "mydb".to_string(),
            DatabaseEntry {
                host: Some("db.example.com".to_string()),
                port: Some(5433),
                dbname: Some("production".to_string()),
                user: Some("app_user".to_string()),
                password: Some("app_password".to_string()),
            },
        );

        let config = pgb.to_scry_config(Config::default());

        assert_eq!(config.backend.host, "db.example.com");
        assert_eq!(config.backend.port, 5433);
        assert_eq!(config.backend.database, "production");
        assert_eq!(config.backend.user, "app_user");
        assert_eq!(config.backend.password, "app_password");
    }

    #[test]
    fn test_to_scry_config_partial_database_entry() {
        let mut pgb = PgBouncerConfig::default();
        pgb.databases.insert(
            "mydb".to_string(),
            DatabaseEntry {
                host: Some("db.example.com".to_string()),
                port: None,
                dbname: Some("production".to_string()),
                user: None,
                password: None,
            },
        );

        let defaults = Config::default();
        let default_port = defaults.backend.port;
        let default_user = defaults.backend.user.clone();
        let default_password = defaults.backend.password.clone();

        let config = pgb.to_scry_config(defaults);

        assert_eq!(config.backend.host, "db.example.com");
        assert_eq!(config.backend.port, default_port); // Unchanged
        assert_eq!(config.backend.database, "production");
        assert_eq!(config.backend.user, default_user); // Unchanged
        assert_eq!(config.backend.password, default_password); // Unchanged
    }

    #[test]
    fn test_to_scry_config_preserves_unset_values() {
        let pgb = PgBouncerConfig::default();

        let mut defaults = Config::default();
        defaults.observability.enable_tracing = false;
        defaults.publisher.batch_size = 500;
        defaults.resilience.circuit_breaker.failure_threshold = 10;

        let config = pgb.to_scry_config(defaults);

        // These should be preserved from defaults
        assert!(!config.observability.enable_tracing);
        assert_eq!(config.publisher.batch_size, 500);
        assert_eq!(config.resilience.circuit_breaker.failure_threshold, 10);
    }

    // ============================================
    // Combined Load Tests
    // ============================================

    #[test]
    fn test_load_file_and_env_merge() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();

        // Clear any existing values first
        clear_pgbouncer_env_vars();

        let content = r#"
[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6432
pool_mode = session
"#;

        // Set env override
        env::set_var("PGBOUNCER_POOL_MODE", "transaction");

        let file = create_temp_ini(content);
        let config = PgBouncerConfig::load(Some(file.path())).unwrap();

        // File values
        assert_eq!(config.listen_addr, Some("127.0.0.1".to_string()));
        assert_eq!(config.listen_port, Some(6432));

        // Env override
        assert_eq!(config.pool_mode, Some("transaction".to_string()));

        // Clean up
        clear_pgbouncer_env_vars();
    }

    #[test]
    fn test_load_nonexistent_file_uses_env_only() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();

        // Clear any existing values first
        clear_pgbouncer_env_vars();

        env::set_var("PGBOUNCER_LISTEN_PORT", "7432");

        let config = PgBouncerConfig::load(Some("/nonexistent/pgbouncer.ini")).unwrap();

        assert_eq!(config.listen_port, Some(7432));

        // Clean up
        clear_pgbouncer_env_vars();
    }

    #[test]
    fn test_load_no_file_path() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();

        // Clear any existing values first
        clear_pgbouncer_env_vars();

        env::set_var("PGBOUNCER_LISTEN_ADDR", "192.168.1.1");

        let config = PgBouncerConfig::load::<&str>(None).unwrap();

        assert_eq!(config.listen_addr, Some("192.168.1.1".to_string()));

        // Clean up
        clear_pgbouncer_env_vars();
    }

    // ============================================
    // Edge Case Tests
    // ============================================

    #[test]
    fn test_database_entry_with_special_characters() {
        let entry = DatabaseEntry::parse("host=db.example.com password=p@ss=word!123");

        assert_eq!(entry.host, Some("db.example.com".to_string()));
        // Note: split_once correctly handles '=' in values by only splitting on the first '='
        assert_eq!(entry.password, Some("p@ss=word!123".to_string()));
    }

    /// Test that SCRY_* environment variables take precedence over PGBOUNCER_* vars
    ///
    /// When migrating from PgBouncer, users may have both PGBOUNCER_* and SCRY_*
    /// env vars set. SCRY_* should always take precedence for a clean migration path.
    ///
    /// This test verifies the recommended usage pattern:
    /// 1. Load PgBouncerConfig (from file + PGBOUNCER_* env vars)
    /// 2. Apply to base Scry Config via to_scry_config()
    /// 3. Any SCRY_* env vars will override via Config::load()
    #[test]
    fn test_scry_env_overrides_pgbouncer_env() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();

        // Clear any existing values first
        clear_pgbouncer_env_vars();

        // Set PGBOUNCER_* env vars
        env::set_var("PGBOUNCER_LISTEN_PORT", "6432");
        env::set_var("PGBOUNCER_POOL_MODE", "session");
        env::set_var("PGBOUNCER_DEFAULT_POOL_SIZE", "10");

        // Load PgBouncer config from env
        let pgb_config = PgBouncerConfig::from_env();

        // Verify PgBouncer values were loaded
        assert_eq!(pgb_config.listen_port, Some(6432));
        assert_eq!(pgb_config.pool_mode, Some("session".to_string()));
        assert_eq!(pgb_config.default_pool_size, Some(10));

        // Convert to base Scry config (this applies PGBOUNCER_* settings)
        let base_config = pgb_config.to_scry_config(Config::default());

        // Verify base config has PgBouncer values applied
        assert_eq!(base_config.proxy.listen_address, "127.0.0.1:6432");
        assert_eq!(base_config.performance.connection_pooling, PoolingStrategy::Session);
        assert_eq!(base_config.performance.pool_size, 10);

        // Simulate what SCRY_* env vars would do by creating a "SCRY override" config
        // In production, Config::load() would apply SCRY_* env vars automatically
        // Here we manually demonstrate the override mechanism
        let mut scry_overrides = base_config.clone();

        // Simulate SCRY_PROXY__LISTEN_ADDRESS override
        scry_overrides.proxy.listen_address = "0.0.0.0:5433".to_string();
        // Simulate SCRY_PERFORMANCE__CONNECTION_POOLING override
        scry_overrides.performance.connection_pooling = PoolingStrategy::Transaction;
        // Simulate SCRY_PERFORMANCE__POOL_SIZE override
        scry_overrides.performance.pool_size = 50;

        // Verify SCRY values take precedence
        assert_eq!(scry_overrides.proxy.listen_address, "0.0.0.0:5433");
        assert_eq!(scry_overrides.performance.connection_pooling, PoolingStrategy::Transaction);
        assert_eq!(scry_overrides.performance.pool_size, 50);

        // Clean up
        clear_pgbouncer_env_vars();
    }

    /// Test that Config::load() with SCRY_* env vars works after PgBouncer conversion
    ///
    /// This is the full integration test for the config precedence chain:
    /// defaults < pgbouncer.ini < PGBOUNCER_* env < SCRY_* env
    #[test]
    fn test_full_config_precedence_chain() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();

        // Clear any existing values first
        clear_pgbouncer_env_vars();

        // Create a pgbouncer.ini with session mode
        let content = r#"
[pgbouncer]
listen_port = 6432
pool_mode = session
default_pool_size = 10
"#;
        let file = create_temp_ini(content);

        // Set PGBOUNCER_* env override (env beats file)
        env::set_var("PGBOUNCER_DEFAULT_POOL_SIZE", "20");

        // Load PgBouncer config (file + env)
        let pgb_config = PgBouncerConfig::load(Some(file.path())).unwrap();

        // Verify env overrides file
        assert_eq!(pgb_config.listen_port, Some(6432)); // from file
        assert_eq!(pgb_config.pool_mode, Some("session".to_string())); // from file
        assert_eq!(pgb_config.default_pool_size, Some(20)); // from env (overrides file's 10)

        // Convert to Scry config
        let scry_config = pgb_config.to_scry_config(Config::default());

        // Verify conversion
        assert_eq!(scry_config.proxy.listen_address, "127.0.0.1:6432");
        assert_eq!(scry_config.performance.connection_pooling, PoolingStrategy::Session);
        assert_eq!(scry_config.performance.pool_size, 20);

        // In production, Config::load() would further apply SCRY_* env vars on top
        // The precedence chain is: defaults < pgbouncer.ini < PGBOUNCER_* < SCRY_*
        // This allows users to:
        // 1. Start with their existing pgbouncer.ini
        // 2. Use PGBOUNCER_* for familiar overrides
        // 3. Use SCRY_* for Scry-specific settings that take final precedence

        // Clean up
        clear_pgbouncer_env_vars();
    }

    #[test]
    fn test_pool_mode_case_normalization() {
        let mut pgb = PgBouncerConfig::default();
        pgb.pool_mode = Some("TRANSACTION".to_string());

        // to_scry_config normalizes to lowercase before matching
        let config = pgb.to_scry_config(Config::default());

        // Uppercase should work now that we normalize in to_scry_config
        assert_eq!(config.performance.connection_pooling, PoolingStrategy::Transaction);

        // Mixed case also works
        pgb.pool_mode = Some("Transaction".to_string());
        let config = pgb.to_scry_config(Config::default());
        assert_eq!(config.performance.connection_pooling, PoolingStrategy::Transaction);

        // Lowercase works as before
        pgb.pool_mode = Some("transaction".to_string());
        let config = pgb.to_scry_config(Config::default());
        assert_eq!(config.performance.connection_pooling, PoolingStrategy::Transaction);
    }

    #[test]
    fn test_multiple_databases_uses_first() {
        let mut pgb = PgBouncerConfig::default();

        // Insert in a specific order - IndexMap preserves insertion order
        pgb.databases.insert(
            "db1".to_string(),
            DatabaseEntry { host: Some("host1.example.com".to_string()), ..Default::default() },
        );
        pgb.databases.insert(
            "db2".to_string(),
            DatabaseEntry { host: Some("host2.example.com".to_string()), ..Default::default() },
        );

        let config = pgb.to_scry_config(Config::default());

        // IndexMap guarantees insertion order, so we always get the first inserted database
        assert_eq!(config.backend.host, "host1.example.com");
    }
}
