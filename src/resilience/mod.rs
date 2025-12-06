/// Resilience features for the Scry proxy
///
/// This module provides circuit breaking, connection retries, and active healthchecks
/// to improve the resilience and reliability of the proxy.
///
/// All features are:
/// - Configurable via 12-factor environment variables
/// - Independently disableable
/// - Designed for <1ms latency overhead

pub mod circuit_breaker;
pub mod errors;
pub mod healthcheck;
pub mod retry;

pub use circuit_breaker::{CircuitBreaker, CircuitBreakerMetrics, CircuitState};
pub use errors::{CircuitBreakerError, RetryError};
pub use healthcheck::ActiveHealthcheck;
pub use retry::RetryStrategy;
