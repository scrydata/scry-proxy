/// Lock-free circuit breaker implementation using atomic operations
///
/// Implements a three-state circuit breaker (Closed, Open, HalfOpen) that protects
/// backend connections from cascading failures. The circuit breaker uses atomic
/// operations for state management to avoid lock contention and maintain <1ms latency.
///
/// State transitions:
/// - Closed → Open: consecutive_failures >= failure_threshold OR (use_health_monitor && HealthStatus::Unhealthy)
/// - Open → HalfOpen: After open_timeout_secs elapsed
/// - HalfOpen → Closed: consecutive_successes >= success_threshold
/// - HalfOpen → Open: Any failure in half-open state
use crate::config::CircuitBreakerConfig;
use crate::observability::health::{HealthMonitor, HealthStatus};
use crate::resilience::errors::CircuitBreakerError;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CircuitState {
    /// Closed: Normal operation, requests pass through
    Closed = 0,

    /// Open: Circuit is open, failing fast without hitting backend
    Open = 1,

    /// HalfOpen: Testing if backend has recovered
    HalfOpen = 2,
}

impl From<u8> for CircuitState {
    fn from(value: u8) -> Self {
        match value {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed, // Default to closed for invalid values
        }
    }
}

impl CircuitState {
    pub fn as_str(&self) -> &'static str {
        match self {
            CircuitState::Closed => "closed",
            CircuitState::Open => "open",
            CircuitState::HalfOpen => "half_open",
        }
    }
}

/// Circuit breaker metrics for observability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerMetrics {
    pub state: String,
    pub consecutive_failures: u32,
    pub consecutive_successes: u32,
    pub failure_count_in_window: u32,
    pub requests_allowed: u64,
    pub requests_rejected: u64,
    pub last_state_change_secs: u64,
}

/// Lock-free circuit breaker using atomic operations
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,

    // Atomic state (lock-free)
    state: AtomicU8, // 0=Closed, 1=Open, 2=HalfOpen

    // Metrics (lock-free)
    consecutive_failures: AtomicU32,
    consecutive_successes: AtomicU32,
    failure_count_in_window: AtomicU32,
    requests_allowed: AtomicU64,
    requests_rejected: AtomicU64,

    // Timestamps (atomic u64 for unix timestamp)
    window_start: AtomicU64,
    open_timestamp: AtomicU64,
    last_state_change: AtomicU64,

    // Health monitor integration (optional)
    health_monitor: Option<Arc<HealthMonitor>>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(config: CircuitBreakerConfig, health_monitor: Option<Arc<HealthMonitor>>) -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        info!(
            enabled = config.enabled,
            failure_threshold = config.failure_threshold,
            success_threshold = config.success_threshold,
            use_health_monitor = config.use_health_monitor,
            "Creating circuit breaker"
        );

        Self {
            config,
            state: AtomicU8::new(CircuitState::Closed as u8),
            consecutive_failures: AtomicU32::new(0),
            consecutive_successes: AtomicU32::new(0),
            failure_count_in_window: AtomicU32::new(0),
            requests_allowed: AtomicU64::new(0),
            requests_rejected: AtomicU64::new(0),
            window_start: AtomicU64::new(now),
            open_timestamp: AtomicU64::new(0),
            last_state_change: AtomicU64::new(now),
            health_monitor,
        }
    }

    /// Check if request should be allowed
    ///
    /// Returns Ok(()) if allowed, Err if circuit is open
    pub fn allow_request(&self) -> Result<(), CircuitBreakerError> {
        if !self.config.enabled {
            return Ok(());
        }

        let state = self.get_state();

        match state {
            CircuitState::Closed => {
                self.requests_allowed.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }

            CircuitState::Open => {
                // Check if enough time has passed to try half-open
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let open_ts = self.open_timestamp.load(Ordering::Relaxed);

                if now - open_ts >= self.config.open_timeout_secs {
                    // Attempt transition to half-open
                    if self.try_transition_to_half_open() {
                        self.requests_allowed.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    } else {
                        // Another thread beat us to it, reject this request
                        self.requests_rejected.fetch_add(1, Ordering::Relaxed);
                        Err(CircuitBreakerError::CircuitOpen)
                    }
                } else {
                    self.requests_rejected.fetch_add(1, Ordering::Relaxed);
                    Err(CircuitBreakerError::CircuitOpen)
                }
            }

            CircuitState::HalfOpen => {
                // In half-open, allow limited requests
                // Use success count as a gate (allow if < threshold)
                let successes = self.consecutive_successes.load(Ordering::Relaxed);
                if successes < self.config.success_threshold {
                    self.requests_allowed.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                } else {
                    // Already have enough successes, close circuit
                    self.transition_to_closed();
                    self.requests_allowed.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            }
        }
    }

    /// Record successful operation
    pub fn record_success(&self) {
        if !self.config.enabled {
            return;
        }

        let state = self.get_state();

        match state {
            CircuitState::Closed => {
                // Reset failure counters
                self.consecutive_failures.store(0, Ordering::Relaxed);
            }

            CircuitState::HalfOpen => {
                let successes = self.consecutive_successes.fetch_add(1, Ordering::Relaxed) + 1;

                debug!(
                    successes = successes,
                    threshold = self.config.success_threshold,
                    "Circuit breaker recorded success in half-open state"
                );

                if successes >= self.config.success_threshold {
                    self.transition_to_closed();
                }
            }

            CircuitState::Open => {
                // Shouldn't happen, but reset if we get success while open
                warn!("Unexpected success while circuit breaker is open, closing circuit");
                self.transition_to_closed();
            }
        }
    }

    /// Record failed operation
    pub fn record_failure(&self) {
        if !self.config.enabled {
            return;
        }

        self.reset_window_if_needed();

        let state = self.get_state();

        match state {
            CircuitState::Closed => {
                let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
                self.failure_count_in_window.fetch_add(1, Ordering::Relaxed);

                debug!(
                    consecutive_failures = failures,
                    threshold = self.config.failure_threshold,
                    "Circuit breaker recorded failure"
                );

                // Check if we should open circuit based on failure threshold
                if failures >= self.config.failure_threshold {
                    warn!(
                        consecutive_failures = failures,
                        "Circuit breaker failure threshold reached, opening circuit"
                    );
                    self.transition_to_open();
                }
                // Also check health monitor if enabled
                else if self.config.use_health_monitor {
                    if let Some(monitor) = &self.health_monitor {
                        let status = monitor.get_status();
                        if status == HealthStatus::Unhealthy {
                            warn!(
                                health_status = ?status,
                                consecutive_failures = failures,
                                "Health monitor reports unhealthy, opening circuit"
                            );
                            self.transition_to_open();
                        }
                    }
                }
            }

            CircuitState::HalfOpen => {
                // Single failure in half-open -> back to open
                warn!("Failure in half-open state, reopening circuit");
                self.transition_to_open();
            }

            CircuitState::Open => {
                // Already open, just update failure count
                self.failure_count_in_window.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Get current metrics
    pub fn get_metrics(&self) -> CircuitBreakerMetrics {
        let state = self.get_state();
        CircuitBreakerMetrics {
            state: state.as_str().to_string(),
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
            consecutive_successes: self.consecutive_successes.load(Ordering::Relaxed),
            failure_count_in_window: self.failure_count_in_window.load(Ordering::Relaxed),
            requests_allowed: self.requests_allowed.load(Ordering::Relaxed),
            requests_rejected: self.requests_rejected.load(Ordering::Relaxed),
            last_state_change_secs: self.last_state_change.load(Ordering::Relaxed),
        }
    }

    /// Get current state
    pub fn get_state(&self) -> CircuitState {
        let state_val = self.state.load(Ordering::Relaxed);
        CircuitState::from(state_val)
    }

    /// Gate the breaker on an external active-healthcheck probe (P3 §4.2/§5.3).
    ///
    /// An unhealthy probe opens the breaker *proactively* — traffic is shed
    /// before clients ever hit a failure. A healthy probe lets an already-open
    /// breaker begin recovering (half-open) immediately, so a backend that comes
    /// back is not held down for the full open timeout.
    pub fn report_health(&self, healthy: bool) {
        match (healthy, self.get_state()) {
            (false, state) if state != CircuitState::Open => {
                warn!("Active healthcheck reports backend unhealthy; opening circuit");
                self.transition_to_open();
            }
            (true, CircuitState::Open) => {
                // The probe reached the backend — allow a recovery attempt.
                self.try_transition_to_half_open();
            }
            _ => {}
        }
    }

    // Private helper methods

    fn transition_to_open(&self) {
        info!("Circuit breaker transitioning to OPEN");
        self.state.store(CircuitState::Open as u8, Ordering::Relaxed);

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        self.open_timestamp.store(now, Ordering::Relaxed);
        self.last_state_change.store(now, Ordering::Relaxed);

        self.consecutive_successes.store(0, Ordering::Relaxed);
    }

    fn transition_to_closed(&self) {
        info!("Circuit breaker transitioning to CLOSED");
        self.state.store(CircuitState::Closed as u8, Ordering::Relaxed);

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        self.last_state_change.store(now, Ordering::Relaxed);

        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.consecutive_successes.store(0, Ordering::Relaxed);
    }

    fn try_transition_to_half_open(&self) -> bool {
        // Try CAS from Open to HalfOpen
        let result = self.state.compare_exchange(
            CircuitState::Open as u8,
            CircuitState::HalfOpen as u8,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );

        if result.is_ok() {
            info!("Circuit breaker transitioning to HALF-OPEN");
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            self.last_state_change.store(now, Ordering::Relaxed);
            self.consecutive_successes.store(0, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    fn reset_window_if_needed(&self) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let window_start = self.window_start.load(Ordering::Relaxed);

        if now - window_start >= self.config.window_secs {
            // New window
            self.window_start.store(now, Ordering::Relaxed);
            self.failure_count_in_window.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_report_health_gates_the_breaker() {
        let config = CircuitBreakerConfig {
            enabled: true,
            // High threshold so failure-driven logic doesn't interfere.
            failure_threshold: 100,
            success_threshold: 2,
            window_secs: 30,
            open_timeout_secs: 60,
            use_health_monitor: false,
        };
        let cb = CircuitBreaker::new(config, None);
        assert_eq!(cb.get_state(), CircuitState::Closed);

        // A healthy probe on a closed breaker is a no-op.
        cb.report_health(true);
        assert_eq!(cb.get_state(), CircuitState::Closed);

        // An unhealthy probe opens the breaker proactively and sheds traffic.
        cb.report_health(false);
        assert_eq!(cb.get_state(), CircuitState::Open);
        assert!(cb.allow_request().is_err(), "open breaker must shed requests");

        // A healthy probe on an open breaker starts recovery (half-open).
        cb.report_health(true);
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_circuit_breaker_creation() {
        let config = CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 5,
            success_threshold: 2,
            window_secs: 30,
            open_timeout_secs: 60,
            use_health_monitor: false,
        };

        let cb = CircuitBreaker::new(config, None);
        assert_eq!(cb.get_state(), CircuitState::Closed);

        let metrics = cb.get_metrics();
        assert_eq!(metrics.state, "closed");
        assert_eq!(metrics.consecutive_failures, 0);
        assert_eq!(metrics.requests_allowed, 0);
        assert_eq!(metrics.requests_rejected, 0);
    }

    #[test]
    fn test_circuit_breaker_disabled() {
        let config = CircuitBreakerConfig {
            enabled: false,
            failure_threshold: 1,
            success_threshold: 1,
            window_secs: 30,
            open_timeout_secs: 60,
            use_health_monitor: false,
        };

        let cb = CircuitBreaker::new(config, None);

        // Should allow requests even after failures
        assert!(cb.allow_request().is_ok());
        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        assert!(cb.allow_request().is_ok());

        // State should remain closed
        assert_eq!(cb.get_state(), CircuitState::Closed);
    }

    #[test]
    fn test_transition_closed_to_open() {
        let config = CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 3,
            success_threshold: 2,
            window_secs: 30,
            open_timeout_secs: 60,
            use_health_monitor: false,
        };

        let cb = CircuitBreaker::new(config, None);

        // Initially closed
        assert_eq!(cb.get_state(), CircuitState::Closed);
        assert!(cb.allow_request().is_ok());

        // Record failures
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Closed);

        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Closed);

        // Third failure should open circuit
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Open);

        // Requests should be rejected
        assert!(cb.allow_request().is_err());

        let metrics = cb.get_metrics();
        assert_eq!(metrics.state, "open");
        assert!(metrics.requests_rejected > 0);
    }

    #[test]
    fn test_transition_open_to_half_open_to_closed() {
        let config = CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 2,
            success_threshold: 2,
            window_secs: 30,
            open_timeout_secs: 1, // Short timeout for testing
            use_health_monitor: false,
        };

        let cb = CircuitBreaker::new(config, None);

        // Open the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Open);

        // Wait for timeout
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Should transition to half-open
        assert!(cb.allow_request().is_ok());
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);

        // Record successes
        cb.record_success();
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);

        cb.record_success();
        // Should now be closed
        assert_eq!(cb.get_state(), CircuitState::Closed);
    }

    #[test]
    fn test_transition_half_open_to_open_on_failure() {
        let config = CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 2,
            success_threshold: 2,
            window_secs: 30,
            open_timeout_secs: 1,
            use_health_monitor: false,
        };

        let cb = CircuitBreaker::new(config, None);

        // Open the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Open);

        // Wait for timeout and transition to half-open
        std::thread::sleep(std::time::Duration::from_secs(2));
        assert!(cb.allow_request().is_ok());
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);

        // Single failure should reopen circuit
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Open);
    }

    #[test]
    fn test_success_resets_failures_in_closed_state() {
        let config = CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 5,
            success_threshold: 2,
            window_secs: 30,
            open_timeout_secs: 60,
            use_health_monitor: false,
        };

        let cb = CircuitBreaker::new(config, None);

        // Record some failures
        cb.record_failure();
        cb.record_failure();

        let metrics = cb.get_metrics();
        assert_eq!(metrics.consecutive_failures, 2);

        // Success should reset failure counter
        cb.record_success();

        let metrics = cb.get_metrics();
        assert_eq!(metrics.consecutive_failures, 0);
        assert_eq!(cb.get_state(), CircuitState::Closed);
    }
}
