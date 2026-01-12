//! Integration tests for the admin console
//!
//! Tests the PgBouncer-compatible admin interface.

use scry::admin::{AdminCommand, AdminConsole, ADMIN_DATABASE};
use scry::observability::{HealthConfig, ProxyMetrics};
use scry::protocol::StartupMessage;
use std::sync::Arc;

#[test]
fn test_admin_database_constant() {
    assert_eq!(ADMIN_DATABASE, "pgbouncer");
}

#[test]
fn test_startup_message_detects_admin_database() {
    // Build a startup message for the "pgbouncer" database
    let startup_bytes = StartupMessage::build("admin", "pgbouncer", &[]);

    let parsed = StartupMessage::parse(&startup_bytes).unwrap();
    assert_eq!(parsed.user(), Some("admin"));
    assert_eq!(parsed.database(), Some("pgbouncer"));

    // Check case-insensitive match
    let db = parsed.database().unwrap();
    assert!(db.eq_ignore_ascii_case(ADMIN_DATABASE));
}

#[test]
fn test_startup_message_regular_database() {
    // Build a startup message for a regular database
    let startup_bytes = StartupMessage::build("user", "myapp", &[]);

    let parsed = StartupMessage::parse(&startup_bytes).unwrap();
    assert_eq!(parsed.database(), Some("myapp"));

    // Should not match admin database
    let db = parsed.database().unwrap();
    assert!(!db.eq_ignore_ascii_case(ADMIN_DATABASE));
}

#[tokio::test]
async fn test_admin_console_show_version() {
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let admin = AdminConsole::new(None, metrics);

    let response = admin.execute("SHOW VERSION").await.unwrap();

    // Check it returns a row set with version info
    match response {
        scry::admin::AdminResponse::RowSet { columns, rows } => {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "version");
            assert_eq!(rows.len(), 1);
            assert!(rows[0][0].starts_with("Scry "));
        }
        _ => panic!("Expected RowSet response"),
    }
}

#[tokio::test]
async fn test_admin_console_show_pools() {
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let admin = AdminConsole::new(None, metrics);

    let response = admin.execute("SHOW POOLS").await.unwrap();

    // Check it returns a row set with pool columns
    match response {
        scry::admin::AdminResponse::RowSet { columns, rows: _ } => {
            assert!(columns.contains(&"database".to_string()));
            assert!(columns.contains(&"cl_active".to_string()));
            assert!(columns.contains(&"sv_active".to_string()));
            assert!(columns.contains(&"pool_mode".to_string()));
        }
        _ => panic!("Expected RowSet response"),
    }
}

#[tokio::test]
async fn test_admin_console_show_stats() {
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let admin = AdminConsole::new(None, metrics);

    let response = admin.execute("SHOW STATS").await.unwrap();

    // Check it returns a row set with stats columns
    match response {
        scry::admin::AdminResponse::RowSet { columns, rows } => {
            assert!(columns.contains(&"database".to_string()));
            assert!(columns.contains(&"total_query_count".to_string()));
            assert!(columns.contains(&"avg_query_time".to_string()));
            assert_eq!(rows.len(), 1); // One row for "default" database
        }
        _ => panic!("Expected RowSet response"),
    }
}

#[tokio::test]
async fn test_admin_console_show_databases() {
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let admin = AdminConsole::new(None, metrics);

    let response = admin.execute("SHOW DATABASES").await.unwrap();

    match response {
        scry::admin::AdminResponse::RowSet { columns, rows: _ } => {
            assert!(columns.contains(&"name".to_string()));
            assert!(columns.contains(&"host".to_string()));
            assert!(columns.contains(&"port".to_string()));
            assert!(columns.contains(&"pool_mode".to_string()));
        }
        _ => panic!("Expected RowSet response"),
    }
}

#[tokio::test]
async fn test_admin_console_pause() {
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let admin = AdminConsole::new(None, metrics);

    let response = admin.execute("PAUSE").await.unwrap();

    match response {
        scry::admin::AdminResponse::CommandComplete { tag } => {
            assert_eq!(tag, "PAUSE");
        }
        _ => panic!("Expected CommandComplete response"),
    }
}

#[tokio::test]
async fn test_admin_console_resume() {
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let admin = AdminConsole::new(None, metrics);

    let response = admin.execute("RESUME").await.unwrap();

    match response {
        scry::admin::AdminResponse::CommandComplete { tag } => {
            assert_eq!(tag, "RESUME");
        }
        _ => panic!("Expected CommandComplete response"),
    }
}

#[tokio::test]
async fn test_admin_console_reload() {
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let admin = AdminConsole::new(None, metrics);

    let response = admin.execute("RELOAD").await.unwrap();

    match response {
        scry::admin::AdminResponse::CommandComplete { tag } => {
            assert_eq!(tag, "RELOAD");
        }
        _ => panic!("Expected CommandComplete response"),
    }
}

#[tokio::test]
async fn test_admin_console_unknown_command() {
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let admin = AdminConsole::new(None, metrics);

    // This should be parsed as unknown
    let response = admin.execute("SELECT * FROM users").await;

    assert!(response.is_err());
}

#[test]
fn test_admin_command_parsing() {
    // Test various command formats
    assert_eq!(AdminCommand::parse("SHOW POOLS"), Some(AdminCommand::ShowPools));
    assert_eq!(AdminCommand::parse("show pools"), Some(AdminCommand::ShowPools));
    assert_eq!(AdminCommand::parse("  SHOW  POOLS  "), Some(AdminCommand::ShowPools));

    assert_eq!(AdminCommand::parse("PAUSE"), Some(AdminCommand::Pause { database: None }));
    assert_eq!(
        AdminCommand::parse("PAUSE mydb"),
        Some(AdminCommand::Pause { database: Some("mydb".to_string()) })
    );

    assert_eq!(AdminCommand::parse("RESUME"), Some(AdminCommand::Resume { database: None }));
    assert_eq!(
        AdminCommand::parse("RESUME mydb"),
        Some(AdminCommand::Resume { database: Some("mydb".to_string()) })
    );

    assert_eq!(AdminCommand::parse("RELOAD"), Some(AdminCommand::Reload));

    assert_eq!(AdminCommand::parse("SHUTDOWN"), Some(AdminCommand::Shutdown { wait: false }));
    assert_eq!(
        AdminCommand::parse("SHUTDOWN WAIT"),
        Some(AdminCommand::Shutdown { wait: true })
    );

    // Regular SQL should not be parsed as admin command
    assert_eq!(AdminCommand::parse("SELECT 1"), None);
    assert_eq!(AdminCommand::parse("INSERT INTO foo VALUES (1)"), None);
}

#[test]
fn test_wire_protocol_response() {
    use scry::admin::AdminResponse;

    // Test RowSet wire encoding
    let response = AdminResponse::RowSet {
        columns: vec!["name".to_string(), "value".to_string()],
        rows: vec![vec!["test".to_string(), "123".to_string()]],
    };

    let wire = response.to_wire();

    // Should contain message types in order: T (RowDescription), D (DataRow), C (CommandComplete), Z (ReadyForQuery)
    assert!(wire.iter().any(|&b| b == b'T'));
    assert!(wire.iter().any(|&b| b == b'D'));
    assert!(wire.iter().any(|&b| b == b'C'));
    assert!(wire.iter().any(|&b| b == b'Z'));

    // Test CommandComplete wire encoding
    let response = AdminResponse::CommandComplete { tag: "PAUSE".to_string() };
    let wire = response.to_wire();

    assert!(wire.iter().any(|&b| b == b'C'));
    assert!(wire.iter().any(|&b| b == b'Z'));

    // Test Error wire encoding
    let response = AdminResponse::Error { message: "Unknown command".to_string() };
    let wire = response.to_wire();

    assert!(wire.iter().any(|&b| b == b'E'));
    assert!(wire.iter().any(|&b| b == b'Z'));
}
