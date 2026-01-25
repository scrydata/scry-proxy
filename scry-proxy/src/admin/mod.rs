//! Admin console for PgBouncer-compatible management interface
//!
//! This module implements a virtual database "pgbouncer" that provides
//! administrative commands for monitoring and controlling the proxy.
//!
//! Supported commands:
//! - SHOW POOLS - Display pool statistics
//! - SHOW STATS - Display query statistics
//! - SHOW DATABASES - Display configured databases
//! - SHOW CLIENTS - Display active client connections
//! - SHOW SERVERS - Display active backend connections
//! - SHOW VERSION - Display proxy version
//! - PAUSE [db] - Pause accepting new connections
//! - RESUME [db] - Resume accepting connections
//! - RELOAD - Reload configuration
//! - SHUTDOWN - Graceful shutdown

mod commands;
mod response;

pub use commands::{AdminCommand, AdminConsole};
pub use response::AdminResponse;

/// Virtual database name for admin console (PgBouncer compatible)
pub const ADMIN_DATABASE: &str = "pgbouncer";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_admin_command_show_pools() {
        assert!(AdminConsole::is_admin_command("SHOW POOLS"));
        assert!(AdminConsole::is_admin_command("show pools"));
        assert!(AdminConsole::is_admin_command("  SHOW POOLS  "));
    }

    #[test]
    fn test_detect_admin_command_show_stats() {
        assert!(AdminConsole::is_admin_command("SHOW STATS"));
        assert!(AdminConsole::is_admin_command("SHOW STATS_TOTALS"));
    }

    #[test]
    fn test_detect_admin_command_show_databases() {
        assert!(AdminConsole::is_admin_command("SHOW DATABASES"));
    }

    #[test]
    fn test_detect_admin_command_show_version() {
        assert!(AdminConsole::is_admin_command("SHOW VERSION"));
    }

    #[test]
    fn test_detect_admin_command_pause_resume() {
        assert!(AdminConsole::is_admin_command("PAUSE"));
        assert!(AdminConsole::is_admin_command("PAUSE mydb"));
        assert!(AdminConsole::is_admin_command("RESUME"));
        assert!(AdminConsole::is_admin_command("RESUME mydb"));
    }

    #[test]
    fn test_detect_admin_command_reload() {
        assert!(AdminConsole::is_admin_command("RELOAD"));
    }

    #[test]
    fn test_detect_admin_command_shutdown() {
        assert!(AdminConsole::is_admin_command("SHUTDOWN"));
        assert!(AdminConsole::is_admin_command("SHUTDOWN WAIT"));
    }

    #[test]
    fn test_detect_admin_command_not_admin() {
        assert!(!AdminConsole::is_admin_command("SELECT * FROM users"));
        assert!(!AdminConsole::is_admin_command("INSERT INTO logs VALUES (1)"));
        assert!(!AdminConsole::is_admin_command("SHOW search_path")); // Regular SHOW
    }

    #[test]
    fn test_parse_admin_command() {
        assert_eq!(AdminCommand::parse("SHOW POOLS"), Some(AdminCommand::ShowPools));
        assert_eq!(AdminCommand::parse("SHOW STATS"), Some(AdminCommand::ShowStats));
        assert_eq!(AdminCommand::parse("SHOW DATABASES"), Some(AdminCommand::ShowDatabases));
        assert_eq!(AdminCommand::parse("SHOW CLIENTS"), Some(AdminCommand::ShowClients));
        assert_eq!(AdminCommand::parse("SHOW SERVERS"), Some(AdminCommand::ShowServers));
        assert_eq!(AdminCommand::parse("SHOW VERSION"), Some(AdminCommand::ShowVersion));
        assert_eq!(AdminCommand::parse("PAUSE"), Some(AdminCommand::Pause { database: None }));
        assert_eq!(
            AdminCommand::parse("PAUSE mydb"),
            Some(AdminCommand::Pause { database: Some("mydb".to_string()) })
        );
        assert_eq!(AdminCommand::parse("RESUME"), Some(AdminCommand::Resume { database: None }));
        assert_eq!(AdminCommand::parse("RELOAD"), Some(AdminCommand::Reload));
        assert_eq!(AdminCommand::parse("SHUTDOWN"), Some(AdminCommand::Shutdown { wait: false }));
        assert_eq!(
            AdminCommand::parse("SHUTDOWN WAIT"),
            Some(AdminCommand::Shutdown { wait: true })
        );
    }
}
