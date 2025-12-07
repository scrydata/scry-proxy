/// Error types for resilience features

#[derive(Debug, thiserror::Error)]
pub enum CircuitBreakerError {
    #[error("Circuit breaker is open, failing fast")]
    CircuitOpen,
}

#[derive(Debug, thiserror::Error)]
pub enum RetryError {
    #[error("All retry attempts exhausted: {0}")]
    RetriesExhausted(String),
}
