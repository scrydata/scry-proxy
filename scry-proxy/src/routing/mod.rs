//! Database routing for multi-database support
//!
//! This module provides routing logic to direct client connections
//! to the appropriate backend database based on the database name
//! specified in the PostgreSQL startup message.

use crate::config::{BackendConfig, DatabaseConfig};
use std::collections::HashMap;

/// Routes client connections to the appropriate backend database.
///
/// Maintains a mapping of logical database names to their backend configurations.
/// If a client connects with a database name that matches an entry in the routing
/// table, they are directed to that specific backend. Otherwise, they fall back
/// to the default backend configuration.
#[derive(Debug)]
pub struct DatabaseRouter {
    /// Map of logical database names to their configurations
    routes: HashMap<String, DatabaseConfig>,
    /// Default backend configuration (used when no specific route matches)
    default: DatabaseConfig,
}

impl DatabaseRouter {
    /// Create a new router from the list of configured databases and default backend.
    ///
    /// # Arguments
    /// * `databases` - List of database-specific configurations
    /// * `default_backend` - The default backend configuration to use as fallback
    /// * `default_pool_size` - Default pool size from performance config
    pub fn new(
        databases: &[DatabaseConfig],
        default_backend: &BackendConfig,
        default_pool_size: usize,
    ) -> Self {
        let mut routes = HashMap::new();

        // Add all configured databases to the routing table
        for db in databases {
            routes.insert(db.name.clone(), db.clone());
        }

        // Create a DatabaseConfig from the default BackendConfig
        let default = DatabaseConfig {
            name: "*".to_string(), // Special name for default
            host: default_backend.host.clone(),
            port: default_backend.port,
            database: default_backend.database.clone(),
            user: default_backend.user.clone(),
            password: default_backend.password.clone(),
            pool_size: Some(default_pool_size),
        };

        Self { routes, default }
    }

    /// Route a database name to its configuration.
    ///
    /// Returns the specific database configuration if one matches,
    /// otherwise returns the default configuration.
    pub fn route(&self, database_name: &str) -> &DatabaseConfig {
        self.routes.get(database_name).unwrap_or(&self.default)
    }

    /// Check if a specific route exists for the given database name.
    pub fn has_route(&self, database_name: &str) -> bool {
        self.routes.contains_key(database_name)
    }

    /// Get all configured database names (excluding default).
    pub fn database_names(&self) -> impl Iterator<Item = &str> {
        self.routes.keys().map(|s| s.as_str())
    }

    /// Get the number of configured routes (excluding default).
    pub fn route_count(&self) -> usize {
        self.routes.len()
    }

    /// Get the default configuration.
    pub fn default_config(&self) -> &DatabaseConfig {
        &self.default
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_backend() -> BackendConfig {
        BackendConfig {
            protocol: crate::config::DatabaseProtocol::Postgres,
            host: "default-host".to_string(),
            port: 5432,
            database: "default_db".to_string(),
            user: "default_user".to_string(),
            password: "default_pass".to_string(),
            password_file: None,
            pool_size: 10,
            connection_timeout_ms: 5000,
        }
    }

    #[test]
    fn test_router_with_no_databases() {
        let backend = create_test_backend();
        let router = DatabaseRouter::new(&[], &backend, 50);

        // Should return default for any database
        let config = router.route("any_database");
        assert_eq!(config.host, "default-host");
        assert_eq!(config.database, "default_db");
        assert_eq!(config.name, "*");
    }

    #[test]
    fn test_router_routes_to_specific_database() {
        let backend = create_test_backend();
        let databases = vec![
            DatabaseConfig {
                name: "myapp".to_string(),
                host: "myapp-host".to_string(),
                port: 5433,
                database: "myapp_db".to_string(),
                user: "myapp_user".to_string(),
                password: "myapp_pass".to_string(),
                pool_size: Some(20),
            },
            DatabaseConfig {
                name: "analytics".to_string(),
                host: "analytics-host".to_string(),
                port: 5434,
                database: "analytics_db".to_string(),
                user: "analytics_user".to_string(),
                password: "analytics_pass".to_string(),
                pool_size: None,
            },
        ];

        let router = DatabaseRouter::new(&databases, &backend, 50);

        // Should route to specific database
        let config = router.route("myapp");
        assert_eq!(config.host, "myapp-host");
        assert_eq!(config.port, 5433);
        assert_eq!(config.database, "myapp_db");
        assert_eq!(config.pool_size, Some(20));

        // Should route to another specific database
        let config = router.route("analytics");
        assert_eq!(config.host, "analytics-host");
        assert_eq!(config.database, "analytics_db");

        // Should fall back to default for unknown database
        let config = router.route("unknown");
        assert_eq!(config.host, "default-host");
        assert_eq!(config.name, "*");
    }

    #[test]
    fn test_has_route() {
        let backend = create_test_backend();
        let databases = vec![DatabaseConfig {
            name: "myapp".to_string(),
            host: "myapp-host".to_string(),
            port: 5433,
            database: "myapp_db".to_string(),
            user: "myapp_user".to_string(),
            password: "myapp_pass".to_string(),
            pool_size: Some(20),
        }];

        let router = DatabaseRouter::new(&databases, &backend, 50);

        assert!(router.has_route("myapp"));
        assert!(!router.has_route("unknown"));
    }

    #[test]
    fn test_database_names() {
        let backend = create_test_backend();
        let databases = vec![
            DatabaseConfig {
                name: "db1".to_string(),
                host: "host1".to_string(),
                port: 5432,
                database: "db1".to_string(),
                user: "user".to_string(),
                password: "pass".to_string(),
                pool_size: None,
            },
            DatabaseConfig {
                name: "db2".to_string(),
                host: "host2".to_string(),
                port: 5432,
                database: "db2".to_string(),
                user: "user".to_string(),
                password: "pass".to_string(),
                pool_size: None,
            },
        ];

        let router = DatabaseRouter::new(&databases, &backend, 50);

        let names: Vec<_> = router.database_names().collect();
        assert_eq!(router.route_count(), 2);
        assert!(names.contains(&"db1"));
        assert!(names.contains(&"db2"));
    }
}
