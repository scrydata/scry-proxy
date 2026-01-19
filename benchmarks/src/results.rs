//! Benchmark results and histogram management.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Latency percentiles in microseconds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50: u64,
    pub p75: u64,
    pub p90: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
    pub min: u64,
    pub mean: f64,
    pub stddev: f64,
}

impl LatencyPercentiles {
    pub fn from_histogram(hist: &Histogram<u64>) -> Self {
        Self {
            p50: hist.value_at_quantile(0.50),
            p75: hist.value_at_quantile(0.75),
            p90: hist.value_at_quantile(0.90),
            p95: hist.value_at_quantile(0.95),
            p99: hist.value_at_quantile(0.99),
            p999: hist.value_at_quantile(0.999),
            max: hist.max(),
            min: hist.min(),
            mean: hist.mean(),
            stddev: hist.stdev(),
        }
    }
}

/// Resource usage snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub cpu_percent: f32,
    pub memory_mb: f64,
    pub sample_count: usize,
}

/// Full benchmark results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResults {
    pub label: String,
    pub proxy: String,
    pub config: BenchmarkConfig,
    pub timestamp: String,
    pub duration_secs: f64,
    pub total_queries: u64,
    pub successful_queries: u64,
    pub failed_queries: u64,
    pub throughput_qps: f64,
    pub latency_us: LatencyPercentiles,
    pub resource_usage: Option<ResourceUsage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    pub connections: usize,
    pub target_queries: usize,
    pub anonymize: Option<bool>,
    pub events_enabled: Option<bool>,
}

/// Thread-safe histogram wrapper.
pub struct LatencyHistogram {
    hist: parking_lot::Mutex<Histogram<u64>>,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        // Record latencies from 1us to 60 seconds with 3 significant figures
        let hist = Histogram::new_with_bounds(1, 60_000_000, 3)
            .expect("Failed to create histogram");
        Self { hist: parking_lot::Mutex::new(hist) }
    }

    pub fn record(&self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        let micros = micros.max(1); // Minimum 1us
        let mut hist = self.hist.lock();
        let _ = hist.record(micros);
    }

    pub fn percentiles(&self) -> LatencyPercentiles {
        let hist = self.hist.lock();
        LatencyPercentiles::from_histogram(&hist)
    }

    pub fn count(&self) -> u64 {
        self.hist.lock().len()
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}
