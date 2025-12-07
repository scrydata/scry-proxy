/// Retry strategy with exponential backoff and jitter
///
/// Provides configurable retry logic for connection failures with:
/// - Exponential backoff
/// - Random jitter to prevent thundering herd
/// - Configurable max attempts and backoff caps

use crate::config::ConnectionRetryConfig;
use std::future::Future;
use std::time::Duration;
use tracing::{debug, warn};

/// Retry strategy implementation
pub struct RetryStrategy {
    config: ConnectionRetryConfig,
}

impl RetryStrategy {
    /// Create a new retry strategy
    pub fn new(config: ConnectionRetryConfig) -> Self {
        Self { config }
    }

    /// Execute an operation with retry logic
    ///
    /// Retries the operation up to max_attempts times with exponential backoff.
    /// Returns the successful result or the last error encountered.
    pub async fn execute<F, Fut, T, E>(&self, operation: F) -> Result<T, E>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        if !self.config.enabled {
            return operation().await;
        }

        let mut attempt = 0;
        let mut backoff_ms = self.config.initial_backoff_ms;

        loop {
            attempt += 1;

            match operation().await {
                Ok(result) => {
                    if attempt > 1 {
                        debug!(
                            attempt = attempt,
                            "Operation succeeded after retries"
                        );
                    }
                    return Ok(result);
                }
                Err(e) => {
                    if attempt >= self.config.max_attempts {
                        warn!(
                            attempts = attempt,
                            error = %e,
                            "Operation failed after all retry attempts"
                        );
                        return Err(e);
                    }

                    warn!(
                        attempt = attempt,
                        max_attempts = self.config.max_attempts,
                        error = %e,
                        backoff_ms = backoff_ms,
                        "Operation failed, retrying"
                    );

                    // Apply jitter
                    let jitter = self.calculate_jitter(backoff_ms);
                    let delay_ms = backoff_ms + jitter;

                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                    // Exponential backoff
                    backoff_ms = ((backoff_ms as f64) * self.config.backoff_multiplier) as u64;
                    backoff_ms = backoff_ms.min(self.config.max_backoff_ms);
                }
            }
        }
    }

    fn calculate_jitter(&self, base_ms: u64) -> u64 {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let jitter_range = (base_ms as f64 * self.config.jitter_factor) as u64;
        if jitter_range > 0 {
            rng.gen_range(0..=jitter_range)
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_disabled() {
        let config = ConnectionRetryConfig {
            enabled: false,
            max_attempts: 3,
            initial_backoff_ms: 50,
            max_backoff_ms: 5000,
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        };

        let strategy = RetryStrategy::new(config);

        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let result: Result<(), &str> = strategy
            .execute(|| {
                let count = call_count_clone.clone();
                async move {
                    count.fetch_add(1, Ordering::Relaxed);
                    Err("always fails")
                }
            })
            .await;

        // Should only call once since retries are disabled
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_retry_succeeds_on_second_attempt() {
        let config = ConnectionRetryConfig {
            enabled: true,
            max_attempts: 3,
            initial_backoff_ms: 10, // Short for testing
            max_backoff_ms: 100,
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        };

        let strategy = RetryStrategy::new(config);

        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let result: Result<String, &str> = strategy
            .execute(|| {
                let count = call_count_clone.clone();
                async move {
                    let current = count.fetch_add(1, Ordering::Relaxed) + 1;
                    if current < 2 {
                        Err("transient error")
                    } else {
                        Ok("success".to_string())
                    }
                }
            })
            .await;

        // Should call twice (first fails, second succeeds)
        assert_eq!(call_count.load(Ordering::Relaxed), 2);
        assert_eq!(result.unwrap(), "success");
    }

    #[tokio::test]
    async fn test_retry_exhausts_attempts() {
        let config = ConnectionRetryConfig {
            enabled: true,
            max_attempts: 3,
            initial_backoff_ms: 10,
            max_backoff_ms: 100,
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        };

        let strategy = RetryStrategy::new(config);

        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let result: Result<(), &str> = strategy
            .execute(|| {
                let count = call_count_clone.clone();
                async move {
                    count.fetch_add(1, Ordering::Relaxed);
                    Err("persistent error")
                }
            })
            .await;

        // Should call max_attempts times
        assert_eq!(call_count.load(Ordering::Relaxed), 3);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "persistent error");
    }

    #[test]
    fn test_jitter_calculation() {
        let config = ConnectionRetryConfig {
            enabled: true,
            max_attempts: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 5000,
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        };

        let strategy = RetryStrategy::new(config);

        // Jitter should be 0-10ms for base 100ms
        for _ in 0..10 {
            let jitter = strategy.calculate_jitter(100);
            assert!(jitter <= 10, "jitter {} should be <= 10", jitter);
        }
    }

    #[test]
    fn test_backoff_calculation() {
        let config = ConnectionRetryConfig {
            enabled: true,
            max_attempts: 5,
            initial_backoff_ms: 50,
            max_backoff_ms: 500,
            backoff_multiplier: 2.0,
            jitter_factor: 0.0, // No jitter for predictable testing
        };

        // Backoff progression: 50, 100, 200, 400, 500 (capped)
        let expected_backoffs = vec![50, 100, 200, 400, 500];

        let mut backoff = config.initial_backoff_ms;
        for (i, expected) in expected_backoffs.iter().enumerate() {
            assert_eq!(backoff, *expected, "Backoff at attempt {}", i);

            // Calculate next backoff
            backoff = ((backoff as f64) * config.backoff_multiplier) as u64;
            backoff = backoff.min(config.max_backoff_ms);
        }
    }
}
