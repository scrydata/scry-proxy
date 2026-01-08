/// Protocol abstraction for database wire protocols
///
/// This trait defines the interface that database-specific implementations
/// must provide. It allows the proxy to support multiple database systems
/// (Postgres, MySQL, MongoDB, CockroachDB, etc.) with the same core pooling
/// and forwarding logic.
use anyhow::Result;
use async_trait::async_trait;
use tokio::net::TcpStream;

/// Database protocol handler trait
///
/// Each database protocol (Postgres, MySQL, etc.) implements this trait
/// to provide protocol-specific behavior like state reset, health checking,
/// and message extraction.
#[async_trait]
pub trait Protocol: Send + Sync + 'static {
    /// Get the protocol name (e.g., "postgres", "mysql", "mongodb")
    fn name(&self) -> &'static str;

    /// Default port for this protocol
    fn default_port(&self) -> u16;

    /// Reset connection state between client sessions
    ///
    /// This is called when a pooled connection is about to be reused.
    /// Implementations should:
    /// - Clear session state (temp tables, variables, etc.)
    /// - Reset transaction state
    /// - Clear prepared statements if needed
    ///
    /// Strategies:
    /// - Postgres: Send "DISCARD ALL" command
    /// - MySQL: Send "RESET CONNECTION" command
    /// - MongoDB: Close and recreate (stateless protocol)
    /// - Generic: Return Ok(false) to close/recreate connection
    ///
    /// Returns:
    /// - Ok(true) if reset succeeded (connection can be reused)
    /// - Ok(false) if reset not supported (connection will be closed)
    /// - Err(_) if reset failed (connection will be closed)
    async fn reset_connection(&self, stream: &mut TcpStream) -> Result<bool>;

    /// Health check a connection
    ///
    /// Verify the connection is still alive and responsive.
    /// This is used for pool maintenance and pre-flight checks.
    ///
    /// Strategies:
    /// - Postgres: Can use TCP keepalive or simple query
    /// - MySQL: Send ping command
    /// - MongoDB: Send ping command
    /// - Generic: Check TCP socket is readable
    ///
    /// Returns:
    /// - Ok(true) if connection is healthy
    /// - Ok(false) or Err(_) if connection is dead
    async fn health_check(&self, stream: &mut TcpStream) -> Result<bool>;

    /// Extract query information from client-to-backend messages
    ///
    /// This is used for observability - extracting SQL queries, commands,
    /// etc. from the wire protocol for logging and analysis.
    ///
    /// Returns Some(query_string) if a query was found, None otherwise.
    fn extract_query(&self, data: &[u8]) -> Option<String>;

    /// Check if a backend-to-client message indicates query completion
    ///
    /// Used to measure query timing - detect when backend has finished
    /// processing a query.
    fn is_query_complete(&self, data: &[u8]) -> bool;

    /// Extract error information from backend-to-client messages
    ///
    /// Returns Some(error_message) if an error was detected, None otherwise.
    fn extract_error(&self, data: &[u8]) -> Option<String>;
}

/// Protocol configuration
///
/// This struct holds protocol-agnostic connection settings.
/// Protocol-specific implementations can extend this with their own config.
#[derive(Debug, Clone)]
pub struct ProtocolConfig {
    pub host: String,
    pub port: u16,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
}

impl ProtocolConfig {
    pub fn backend_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Registry for protocol implementations
///
/// Allows runtime selection of protocol based on configuration.
pub struct ProtocolRegistry;

impl ProtocolRegistry {
    /// Get protocol implementation by database protocol type
    ///
    /// This creates the appropriate Protocol implementation based on the
    /// DatabaseProtocol enum value from the configuration.
    pub fn get(protocol: &crate::config::DatabaseProtocol) -> Result<Box<dyn Protocol>> {
        use crate::config::DatabaseProtocol;

        match protocol {
            DatabaseProtocol::Postgres => {
                Ok(Box::new(crate::protocol::postgres::PostgresProtocol::new()))
            } // Future protocol support - uncomment when implementing:
              // DatabaseProtocol::Mysql => {
              //     Err(anyhow::anyhow!("MySQL protocol not yet implemented"))
              // }
              // DatabaseProtocol::Mongodb => {
              //     Err(anyhow::anyhow!("MongoDB protocol not yet implemented"))
              // }
        }
    }
}
