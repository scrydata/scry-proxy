/// Active healthcheck implementation
///
/// Provides periodic active healthchecks to the backend database.
/// Complements the passive healthchecks that happen during connection recycling.
use crate::config::HealthcheckConfig;
use crate::protocol::{Protocol, ProtocolConfig};
use anyhow::Result;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tracing::{debug, warn};

/// Active healthcheck executor
pub struct ActiveHealthcheck {
    config: HealthcheckConfig,
    protocol: Arc<dyn Protocol>,
    protocol_config: ProtocolConfig,

    // State
    consecutive_failures: AtomicU32,
    is_healthy: AtomicBool,
}

impl ActiveHealthcheck {
    /// Create a new active healthcheck
    pub fn new(
        config: HealthcheckConfig,
        protocol: Arc<dyn Protocol>,
        protocol_config: ProtocolConfig,
    ) -> Self {
        Self {
            config,
            protocol,
            protocol_config,
            consecutive_failures: AtomicU32::new(0),
            is_healthy: AtomicBool::new(true),
        }
    }

    /// Run a single healthcheck
    ///
    /// Creates a temporary connection to the backend and runs a protocol-specific
    /// health check. Updates the health status based on the result.
    pub async fn check(&self) -> Result<bool> {
        if !self.config.active_enabled {
            return Ok(true);
        }

        debug!("Running active healthcheck");

        let timeout = Duration::from_millis(self.config.timeout_ms);

        let check_result = tokio::time::timeout(timeout, async {
            // Create temporary connection
            let backend_addr = self.protocol_config.backend_addr();
            let mut stream = TcpStream::connect(&backend_addr).await?;

            // Run protocol-specific health check
            self.protocol.health_check(&mut stream).await
        })
        .await;

        // Update state based on result
        let is_healthy = match check_result {
            Ok(Ok(true)) => {
                debug!("Active healthcheck passed");
                self.consecutive_failures.store(0, Ordering::Relaxed);
                true
            }
            Ok(Ok(false)) | Ok(Err(_)) | Err(_) => {
                let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;

                if failures >= self.config.failure_threshold {
                    warn!(
                        consecutive_failures = failures,
                        threshold = self.config.failure_threshold,
                        "Active healthcheck failed threshold reached"
                    );
                    false
                } else {
                    warn!(
                        consecutive_failures = failures,
                        threshold = self.config.failure_threshold,
                        "Active healthcheck failed"
                    );
                    // Still healthy until threshold
                    self.is_healthy.load(Ordering::Relaxed)
                }
            }
        };

        self.is_healthy.store(is_healthy, Ordering::Relaxed);
        Ok(is_healthy)
    }

    /// Get current health status
    pub fn is_healthy(&self) -> bool {
        self.is_healthy.load(Ordering::Relaxed)
    }

    /// Get consecutive failure count
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::postgres::PostgresProtocol;

    #[test]
    fn test_healthcheck_creation() {
        let config = HealthcheckConfig {
            active_enabled: true,
            interval_secs: 30,
            timeout_ms: 1000,
            failure_threshold: 3,
        };

        let protocol = Arc::new(PostgresProtocol::new()) as Arc<dyn Protocol>;
        let protocol_config = ProtocolConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: Some("test".to_string()),
            user: Some("postgres".to_string()),
            password: Some("password".to_string()),
        };

        let healthcheck = ActiveHealthcheck::new(config, protocol, protocol_config);

        assert!(healthcheck.is_healthy());
        assert_eq!(healthcheck.consecutive_failures(), 0);
    }

    #[test]
    fn test_healthcheck_disabled() {
        let config = HealthcheckConfig {
            active_enabled: false,
            interval_secs: 30,
            timeout_ms: 1000,
            failure_threshold: 3,
        };

        let protocol = Arc::new(PostgresProtocol::new()) as Arc<dyn Protocol>;
        let protocol_config = ProtocolConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: Some("test".to_string()),
            user: Some("postgres".to_string()),
            password: Some("password".to_string()),
        };

        let healthcheck = ActiveHealthcheck::new(config, protocol, protocol_config);

        assert!(healthcheck.is_healthy());
    }
}
