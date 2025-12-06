/// Health monitoring with predictive warnings
///
/// Uses Exponential Moving Average (EMA) to track baseline behavior and
/// detect anomalies like error rate spikes, latency spikes, and pool saturation.
///
/// Innovation: Most proxies only expose current metrics. This module predicts
/// problems before they become critical by comparing current behavior to baselines.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Health monitoring configuration
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Factor for error rate spike detection (default: 3.0)
    /// Warning triggered if: current_error_rate > baseline * factor
    pub error_rate_spike_factor: f64,

    /// Factor for latency spike detection (default: 2.0)
    /// Warning triggered if: current_p99 > baseline_p99 * factor
    pub latency_spike_factor: f64,

    /// Pool saturation threshold (default: 0.95 = 95%)
    /// Warning triggered if: pool_utilization > threshold
    pub pool_saturation_threshold: f64,

    /// EMA alpha (smoothing factor, default: 0.1)
    /// Higher = more weight to recent samples, Lower = smoother baseline
    pub ema_alpha: f64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            error_rate_spike_factor: 3.0,
            latency_spike_factor: 2.0,
            pool_saturation_threshold: 0.95,
            ema_alpha: 0.1,
        }
    }
}

/// Health monitor with baseline tracking and anomaly detection
pub struct HealthMonitor {
    config: HealthConfig,
    warnings: RwLock<Vec<HealthWarning>>,
    baseline: RwLock<Baseline>,
    last_check: AtomicU64, // Unix timestamp in seconds
}

impl HealthMonitor {
    /// Create a new health monitor
    pub fn new(config: HealthConfig) -> Self {
        let ema_alpha = config.ema_alpha;
        Self {
            config,
            warnings: RwLock::new(Vec::new()),
            baseline: RwLock::new(Baseline::new(ema_alpha)),
            last_check: AtomicU64::new(0),
        }
    }

    /// Check health and update warnings
    ///
    /// This should be called periodically (e.g., every 10 seconds) from a background task.
    /// It compares current metrics against baselines and generates warnings.
    pub fn check_and_update(&self, current: &HealthSnapshot) {
        let mut warnings = Vec::new();

        // Update baseline with current sample
        let mut baseline = self.baseline.write();
        baseline.update(current);

        // Check for error rate spike
        if current.error_rate > 0.0 && baseline.avg_error_rate > 0.0 {
            let spike_factor = current.error_rate / baseline.avg_error_rate;
            if spike_factor >= self.config.error_rate_spike_factor {
                warnings.push(HealthWarning::ErrorRateSpike {
                    current: current.error_rate,
                    baseline: baseline.avg_error_rate,
                    factor: spike_factor,
                });
            }
        }

        // Check for latency spike
        if current.latency_p99_ms > 0.0 && baseline.avg_latency_p99 > 0.0 {
            let spike_factor = current.latency_p99_ms / baseline.avg_latency_p99;
            if spike_factor >= self.config.latency_spike_factor {
                warnings.push(HealthWarning::LatencySpike {
                    current_p99_ms: current.latency_p99_ms,
                    baseline_p99_ms: baseline.avg_latency_p99,
                    factor: spike_factor,
                });
            }
        }

        // Check for pool saturation
        if current.pool_utilization >= self.config.pool_saturation_threshold {
            warnings.push(HealthWarning::PoolSaturation {
                utilization: current.pool_utilization,
                threshold: self.config.pool_saturation_threshold,
            });
        }

        // Check for pool starvation (no available connections + queries waiting)
        if current.pool_available == 0 && current.active_connections > current.pool_size {
            let wait_queue = current.active_connections - current.pool_size;
            warnings.push(HealthWarning::PoolStarvation {
                available: current.pool_available,
                wait_queue,
            });
        }

        // Update warnings
        *self.warnings.write() = warnings;

        // Update last check timestamp
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_check.store(now, Ordering::Relaxed);
    }

    /// Get current warnings
    pub fn get_warnings(&self) -> Vec<HealthWarning> {
        self.warnings.read().clone()
    }

    /// Get overall health status
    pub fn get_status(&self) -> HealthStatus {
        let warnings = self.warnings.read();

        if warnings.is_empty() {
            HealthStatus::Healthy
        } else {
            // Check for critical warnings
            let has_critical = warnings.iter().any(|w| match w {
                HealthWarning::PoolStarvation { .. } => true,
                HealthWarning::PoolSaturation { utilization, .. } => *utilization >= 0.99,
                HealthWarning::ErrorRateSpike { factor, .. } => *factor >= 5.0,
                _ => false,
            });

            if has_critical {
                HealthStatus::Unhealthy
            } else {
                HealthStatus::Degraded
            }
        }
    }

    /// Get baseline information
    pub fn get_baseline(&self) -> BaselineSnapshot {
        let baseline = self.baseline.read();
        BaselineSnapshot {
            avg_error_rate: baseline.avg_error_rate,
            avg_latency_p99_ms: baseline.avg_latency_p99,
            avg_pool_utilization: baseline.avg_pool_utilization,
            sample_count: baseline.sample_count,
        }
    }
}

/// Baseline tracking using Exponential Moving Average
struct Baseline {
    avg_error_rate: f64,
    avg_latency_p99: f64,
    avg_pool_utilization: f64,
    alpha: f64, // EMA smoothing factor
    sample_count: u64,
}

impl Baseline {
    fn new(alpha: f64) -> Self {
        Self {
            avg_error_rate: 0.0,
            avg_latency_p99: 0.0,
            avg_pool_utilization: 0.0,
            alpha,
            sample_count: 0,
        }
    }

    /// Update baseline with new sample using EMA
    ///
    /// EMA formula: new_avg = alpha * new_sample + (1 - alpha) * old_avg
    fn update(&mut self, sample: &HealthSnapshot) {
        if self.sample_count == 0 {
            // First sample - use it as initial baseline
            self.avg_error_rate = sample.error_rate;
            self.avg_latency_p99 = sample.latency_p99_ms;
            self.avg_pool_utilization = sample.pool_utilization;
        } else {
            // Apply EMA
            self.avg_error_rate =
                self.alpha * sample.error_rate + (1.0 - self.alpha) * self.avg_error_rate;

            self.avg_latency_p99 =
                self.alpha * sample.latency_p99_ms + (1.0 - self.alpha) * self.avg_latency_p99;

            self.avg_pool_utilization = self.alpha * sample.pool_utilization
                + (1.0 - self.alpha) * self.avg_pool_utilization;
        }

        self.sample_count += 1;
    }
}

/// Current health snapshot (input to health monitor)
#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    pub error_rate: f64,        // Errors / total queries
    pub latency_p99_ms: f64,    // 99th percentile latency in milliseconds
    pub pool_utilization: f64,  // (pool_size - available) / pool_size
    pub pool_size: usize,       // Current pool size
    pub pool_available: usize,  // Available connections
    pub active_connections: usize, // Active client connections
}

/// Baseline information for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineSnapshot {
    pub avg_error_rate: f64,
    pub avg_latency_p99_ms: f64,
    pub avg_pool_utilization: f64,
    pub sample_count: u64,
}

/// Health warning types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HealthWarning {
    /// Error rate significantly higher than baseline
    ErrorRateSpike {
        current: f64,
        baseline: f64,
        factor: f64,
    },

    /// P99 latency significantly higher than baseline
    LatencySpike {
        current_p99_ms: f64,
        baseline_p99_ms: f64,
        factor: f64,
    },

    /// Pool utilization above threshold
    PoolSaturation { utilization: f64, threshold: f64 },

    /// No available connections and queries waiting
    PoolStarvation { available: usize, wait_queue: usize },
}

/// Overall health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_baseline_initialization() {
        let mut baseline = Baseline::new(0.1);

        let sample = HealthSnapshot {
            error_rate: 0.01,
            latency_p99_ms: 5.0,
            pool_utilization: 0.5,
            pool_size: 10,
            pool_available: 5,
            active_connections: 5,
        };

        baseline.update(&sample);

        // First sample should set the baseline
        assert_eq!(baseline.avg_error_rate, 0.01);
        assert_eq!(baseline.avg_latency_p99, 5.0);
        assert_eq!(baseline.avg_pool_utilization, 0.5);
        assert_eq!(baseline.sample_count, 1);
    }

    #[test]
    fn test_baseline_ema() {
        let mut baseline = Baseline::new(0.1);

        // First sample
        baseline.update(&HealthSnapshot {
            error_rate: 0.01,
            latency_p99_ms: 5.0,
            pool_utilization: 0.5,
            pool_size: 10,
            pool_available: 5,
            active_connections: 5,
        });

        // Second sample with different values
        baseline.update(&HealthSnapshot {
            error_rate: 0.02,
            latency_p99_ms: 10.0,
            pool_utilization: 0.7,
            pool_size: 10,
            pool_available: 3,
            active_connections: 7,
        });

        // EMA should be between first and second sample
        // new_avg = 0.1 * new_sample + 0.9 * old_avg
        let expected_error = 0.1 * 0.02 + 0.9 * 0.01; // = 0.011
        assert!((baseline.avg_error_rate - expected_error).abs() < 0.0001);

        let expected_latency = 0.1 * 10.0 + 0.9 * 5.0; // = 5.5
        assert!((baseline.avg_latency_p99 - expected_latency).abs() < 0.0001);
    }

    #[test]
    fn test_error_rate_spike_detection() {
        let config = HealthConfig {
            error_rate_spike_factor: 3.0,
            ..Default::default()
        };
        let monitor = HealthMonitor::new(config);

        // Establish baseline
        monitor.check_and_update(&HealthSnapshot {
            error_rate: 0.01,
            latency_p99_ms: 5.0,
            pool_utilization: 0.5,
            pool_size: 10,
            pool_available: 5,
            active_connections: 5,
        });

        assert!(monitor.get_warnings().is_empty());

        // Spike: 3x baseline
        monitor.check_and_update(&HealthSnapshot {
            error_rate: 0.03,
            latency_p99_ms: 5.0,
            pool_utilization: 0.5,
            pool_size: 10,
            pool_available: 5,
            active_connections: 5,
        });

        let warnings = monitor.get_warnings();
        assert_eq!(warnings.len(), 1);

        match &warnings[0] {
            HealthWarning::ErrorRateSpike { current, baseline, factor } => {
                assert_eq!(*current, 0.03);
                assert!(factor >= &3.0);
            }
            _ => panic!("Expected ErrorRateSpike warning"),
        }
    }

    #[test]
    fn test_pool_saturation_detection() {
        let config = HealthConfig {
            pool_saturation_threshold: 0.95,
            ..Default::default()
        };
        let monitor = HealthMonitor::new(config);

        // Pool 96% utilized (above threshold)
        monitor.check_and_update(&HealthSnapshot {
            error_rate: 0.0,
            latency_p99_ms: 5.0,
            pool_utilization: 0.96,
            pool_size: 10,
            pool_available: 0,
            active_connections: 10,
        });

        let warnings = monitor.get_warnings();
        assert_eq!(warnings.len(), 1);

        match &warnings[0] {
            HealthWarning::PoolSaturation { utilization, threshold } => {
                assert_eq!(*utilization, 0.96);
                assert_eq!(*threshold, 0.95);
            }
            _ => panic!("Expected PoolSaturation warning"),
        }
    }

    #[test]
    fn test_health_status() {
        let monitor = HealthMonitor::new(HealthConfig::default());

        // Initially healthy (no warnings)
        assert_eq!(monitor.get_status(), HealthStatus::Healthy);

        // Add minor warning
        monitor.check_and_update(&HealthSnapshot {
            error_rate: 0.0,
            latency_p99_ms: 5.0,
            pool_utilization: 0.96, // Saturation warning
            pool_size: 10,
            pool_available: 0,
            active_connections: 10,
        });

        assert_eq!(monitor.get_status(), HealthStatus::Degraded);
    }
}
