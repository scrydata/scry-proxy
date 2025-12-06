/// Core metrics infrastructure using HDR histograms
///
/// ProxyMetrics is the central singleton that tracks all proxy observability data:
/// - Query latencies (total, queue, pool acquire, backend) with percentiles
/// - Connection pool state
/// - Hot data patterns (value fingerprints)
/// - Health baselines and warnings
///
/// Design goals:
/// - <300ns overhead per query
/// - ~150KB memory footprint
/// - Lock-free counters, read-write locks only for histograms

use super::health::{HealthConfig, HealthMonitor, HealthSnapshot};
use super::hot_data::HotDataTracker;
use super::timeline::{QueryTimeline, TimelinePhases};
use hdrhistogram::Histogram;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Main proxy metrics singleton
///
/// This is created once at startup and passed to all components.
/// All methods are thread-safe and designed for concurrent access.
#[derive(Clone)]
pub struct ProxyMetrics {
    query: Arc<QueryMetrics>,
    pool: Arc<PoolMetrics>,
    hot_data: Arc<HotDataTracker>,
    health: Arc<HealthMonitor>,
    start_time: Instant,
    active_connections: Arc<AtomicUsize>,
    circuit_breaker: Arc<RwLock<Option<Arc<crate::resilience::CircuitBreaker>>>>,
}

impl ProxyMetrics {
    /// Create a new ProxyMetrics instance
    ///
    /// # Arguments
    /// * `hot_data_top_k` - Number of top fingerprints to track (default: 100)
    /// * `health_config` - Health monitoring configuration
    pub fn new(hot_data_top_k: usize, health_config: HealthConfig) -> Self {
        Self {
            query: Arc::new(QueryMetrics::new()),
            pool: Arc::new(PoolMetrics::new()),
            hot_data: Arc::new(HotDataTracker::new(hot_data_top_k, 0.99)),
            health: Arc::new(HealthMonitor::new(health_config)),
            start_time: Instant::now(),
            active_connections: Arc::new(AtomicUsize::new(0)),
            circuit_breaker: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the circuit breaker (called from server setup)
    pub fn set_circuit_breaker(&self, cb: Option<Arc<crate::resilience::CircuitBreaker>>) {
        *self.circuit_breaker.write() = cb;
    }

    /// Record a completed query with timeline breakdown
    ///
    /// This is the main entry point for query metrics. Called from ConnectionHandler
    /// when a query completes (success or error).
    ///
    /// Cost: ~200-300ns per call (within budget)
    pub fn record_query(&self, timeline: &QueryTimeline, success: bool) {
        // Get phase durations in microseconds
        let phases = timeline.phase_durations_micros();

        // Record in histograms (HDR histogram record is ~100-200ns)
        self.query.record_latency(phases);

        // Update counters (atomic increment is ~1-5ns)
        if success {
            self.query.total_queries.fetch_add(1, Ordering::Relaxed);
        } else {
            self.query.total_queries.fetch_add(1, Ordering::Relaxed);
            self.query.total_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record hot data access (value fingerprints from anonymized queries)
    ///
    /// Cost: ~50-100ns per fingerprint
    pub fn record_hot_data(&self, fingerprints: &[String]) {
        for fp in fingerprints {
            self.hot_data.record_access(fp);
        }
    }

    /// Update pool metrics (called periodically from background task)
    pub fn update_pool_metrics(&self, size: usize, available: usize, max_size: usize) {
        self.pool.update(size, available, max_size);
    }

    /// Increment active connection counter
    pub fn increment_active_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active connection counter
    pub fn decrement_active_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get current active connection count
    pub fn get_active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Run health check (called periodically from background task)
    pub fn run_health_check(&self) {
        let snapshot = self.create_health_snapshot();
        self.health.check_and_update(&snapshot);
    }

    /// Create a health snapshot from current metrics
    fn create_health_snapshot(&self) -> HealthSnapshot {
        let total = self.query.total_queries.load(Ordering::Relaxed) as f64;
        let errors = self.query.total_errors.load(Ordering::Relaxed) as f64;
        let error_rate = if total > 0.0 { errors / total } else { 0.0 };

        let latency_p99_ms = self.query.latency_p99_ms();

        let pool_status = self.pool.get_status();
        let pool_utilization = if pool_status.max_size > 0 {
            (pool_status.size - pool_status.available) as f64 / pool_status.max_size as f64
        } else {
            0.0
        };

        HealthSnapshot {
            error_rate,
            latency_p99_ms,
            pool_utilization,
            pool_size: pool_status.size,
            pool_available: pool_status.available,
            active_connections: self.get_active_connections(),
        }
    }

    // Accessors for each component
    pub fn query_metrics(&self) -> &Arc<QueryMetrics> {
        &self.query
    }

    pub fn pool_metrics(&self) -> &Arc<PoolMetrics> {
        &self.pool
    }

    pub fn hot_data(&self) -> &Arc<HotDataTracker> {
        &self.hot_data
    }

    pub fn health_monitor(&self) -> &Arc<HealthMonitor> {
        &self.health
    }

    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get circuit breaker metrics if circuit breaker is enabled
    pub fn circuit_breaker_metrics(&self) -> Option<crate::resilience::CircuitBreakerMetrics> {
        self.circuit_breaker
            .read()
            .as_ref()
            .map(|cb| cb.get_metrics())
    }
}

/// Query latency metrics with HDR histograms
pub struct QueryMetrics {
    // Histograms for percentile tracking
    latency_histogram: RwLock<Histogram<u64>>,
    queue_time_histogram: RwLock<Histogram<u64>>,
    pool_acquire_histogram: RwLock<Histogram<u64>>,
    backend_time_histogram: RwLock<Histogram<u64>>,

    // Atomic counters
    pub total_queries: AtomicU64,
    pub total_errors: AtomicU64,
}

impl QueryMetrics {
    fn new() -> Self {
        Self {
            // HDR histogram: 3 significant figures, auto-resize up to 3.6 billion microseconds (1 hour)
            latency_histogram: RwLock::new(
                Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3).unwrap(),
            ),
            queue_time_histogram: RwLock::new(
                Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3).unwrap(),
            ),
            pool_acquire_histogram: RwLock::new(
                Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3).unwrap(),
            ),
            backend_time_histogram: RwLock::new(
                Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3).unwrap(),
            ),
            total_queries: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
        }
    }

    /// Record latency measurements from timeline phases
    fn record_latency(&self, phases: TimelinePhases) {
        // Record total latency (always present)
        let _ = self.latency_histogram.write().record(phases.total_micros);

        // Record phase breakdowns if available
        if let Some(queue_micros) = phases.queue_time_micros {
            let _ = self.queue_time_histogram.write().record(queue_micros);
        }
        if let Some(pool_micros) = phases.pool_acquire_micros {
            let _ = self.pool_acquire_histogram.write().record(pool_micros);
        }
        if let Some(backend_micros) = phases.backend_micros {
            let _ = self.backend_time_histogram.write().record(backend_micros);
        }
    }

    /// Get latency percentiles
    pub fn get_latency_percentiles(&self) -> LatencyPercentiles {
        let hist = self.latency_histogram.read();
        LatencyPercentiles {
            p50_micros: hist.value_at_quantile(0.50),
            p90_micros: hist.value_at_quantile(0.90),
            p95_micros: hist.value_at_quantile(0.95),
            p99_micros: hist.value_at_quantile(0.99),
            p999_micros: hist.value_at_quantile(0.999),
            max_micros: hist.max(),
            mean_micros: hist.mean(),
        }
    }

    /// Get queue time percentiles
    pub fn get_queue_percentiles(&self) -> LatencyPercentiles {
        let hist = self.queue_time_histogram.read();
        LatencyPercentiles {
            p50_micros: hist.value_at_quantile(0.50),
            p90_micros: hist.value_at_quantile(0.90),
            p95_micros: hist.value_at_quantile(0.95),
            p99_micros: hist.value_at_quantile(0.99),
            p999_micros: hist.value_at_quantile(0.999),
            max_micros: hist.max(),
            mean_micros: hist.mean(),
        }
    }

    /// Get pool acquire time percentiles
    pub fn get_pool_acquire_percentiles(&self) -> LatencyPercentiles {
        let hist = self.pool_acquire_histogram.read();
        LatencyPercentiles {
            p50_micros: hist.value_at_quantile(0.50),
            p90_micros: hist.value_at_quantile(0.90),
            p95_micros: hist.value_at_quantile(0.95),
            p99_micros: hist.value_at_quantile(0.99),
            p999_micros: hist.value_at_quantile(0.999),
            max_micros: hist.max(),
            mean_micros: hist.mean(),
        }
    }

    /// Get backend execution time percentiles
    pub fn get_backend_percentiles(&self) -> LatencyPercentiles {
        let hist = self.backend_time_histogram.read();
        LatencyPercentiles {
            p50_micros: hist.value_at_quantile(0.50),
            p90_micros: hist.value_at_quantile(0.90),
            p95_micros: hist.value_at_quantile(0.95),
            p99_micros: hist.value_at_quantile(0.99),
            p999_micros: hist.value_at_quantile(0.999),
            max_micros: hist.max(),
            mean_micros: hist.mean(),
        }
    }

    /// Get p99 latency in milliseconds (for health monitoring)
    fn latency_p99_ms(&self) -> f64 {
        let hist = self.latency_histogram.read();
        hist.value_at_quantile(0.99) as f64 / 1000.0
    }

    /// Get snapshot of all histograms (for Prometheus export)
    pub fn snapshot_histograms(&self) -> HistogramSnapshots {
        HistogramSnapshots {
            latency: self.latency_histogram.read().clone(),
            queue_time: self.queue_time_histogram.read().clone(),
            pool_acquire: self.pool_acquire_histogram.read().clone(),
            backend_time: self.backend_time_histogram.read().clone(),
        }
    }
}

/// Pool metrics
pub struct PoolMetrics {
    size: AtomicUsize,
    available: AtomicUsize,
    max_size: AtomicUsize,
}

impl PoolMetrics {
    fn new() -> Self {
        Self {
            size: AtomicUsize::new(0),
            available: AtomicUsize::new(0),
            max_size: AtomicUsize::new(0),
        }
    }

    fn update(&self, size: usize, available: usize, max_size: usize) {
        self.size.store(size, Ordering::Relaxed);
        self.available.store(available, Ordering::Relaxed);
        self.max_size.store(max_size, Ordering::Relaxed);
    }

    pub fn get_status(&self) -> PoolStatus {
        PoolStatus {
            size: self.size.load(Ordering::Relaxed),
            available: self.available.load(Ordering::Relaxed),
            max_size: self.max_size.load(Ordering::Relaxed),
        }
    }
}

/// Latency percentiles in microseconds
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50_micros: u64,
    pub p90_micros: u64,
    pub p95_micros: u64,
    pub p99_micros: u64,
    pub p999_micros: u64,
    pub max_micros: u64,
    pub mean_micros: f64,
}

impl LatencyPercentiles {
    /// Convert to milliseconds for JSON output
    pub fn to_millis(&self) -> LatencyPercentilesMs {
        LatencyPercentilesMs {
            p50: self.p50_micros as f64 / 1000.0,
            p90: self.p90_micros as f64 / 1000.0,
            p95: self.p95_micros as f64 / 1000.0,
            p99: self.p99_micros as f64 / 1000.0,
            p999: self.p999_micros as f64 / 1000.0,
            max: self.max_micros as f64 / 1000.0,
            mean: self.mean_micros / 1000.0,
        }
    }
}

/// Latency percentiles in milliseconds (for JSON API)
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LatencyPercentilesMs {
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
    pub mean: f64,
}

/// Pool status snapshot
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PoolStatus {
    pub size: usize,
    pub available: usize,
    pub max_size: usize,
}

impl PoolStatus {
    pub fn utilization(&self) -> f64 {
        if self.max_size == 0 {
            0.0
        } else {
            (self.size - self.available) as f64 / self.max_size as f64
        }
    }
}

/// Histogram snapshots for Prometheus export
pub struct HistogramSnapshots {
    pub latency: Histogram<u64>,
    pub queue_time: Histogram<u64>,
    pub pool_acquire: Histogram<u64>,
    pub backend_time: Histogram<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::timeline::QueryTimeline;
    use std::thread;
    use std::time::Duration as StdDuration;

    #[test]
    fn test_metrics_initialization() {
        let metrics = ProxyMetrics::new(100, HealthConfig::default());
        assert_eq!(metrics.query_metrics().total_queries.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.query_metrics().total_errors.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.get_active_connections(), 0);
    }

    #[test]
    fn test_record_query() {
        let metrics = ProxyMetrics::new(100, HealthConfig::default());

        let mut timeline = QueryTimeline::new();
        timeline.mark_pool_acquire_start();
        thread::sleep(StdDuration::from_micros(100));
        timeline.mark_pool_acquire_end();
        timeline.mark_backend_start();
        thread::sleep(StdDuration::from_micros(500));
        timeline.mark_backend_end();

        metrics.record_query(&timeline, true);

        assert_eq!(metrics.query_metrics().total_queries.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.query_metrics().total_errors.load(Ordering::Relaxed), 0);

        let percentiles = metrics.query_metrics().get_latency_percentiles();
        assert!(percentiles.p99_micros > 0);
    }

    #[test]
    fn test_record_error() {
        let metrics = ProxyMetrics::new(100, HealthConfig::default());

        let timeline = QueryTimeline::new();
        metrics.record_query(&timeline, false); // Error

        assert_eq!(metrics.query_metrics().total_queries.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.query_metrics().total_errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_percentile_calculation() {
        let metrics = ProxyMetrics::new(100, HealthConfig::default());

        // Record 100 queries with varying latencies
        for i in 0..100 {
            let mut timeline = QueryTimeline::new();
            timeline.mark_backend_start();
            thread::sleep(StdDuration::from_micros(i * 10));
            timeline.mark_backend_end();
            metrics.record_query(&timeline, true);
        }

        let percentiles = metrics.query_metrics().get_latency_percentiles();

        // p50 should be around 500 microseconds (50th query * 10)
        // p99 should be around 990 microseconds (99th query * 10)
        assert!(percentiles.p50_micros > 0);
        assert!(percentiles.p99_micros > percentiles.p50_micros);
        assert!(percentiles.max_micros >= percentiles.p99_micros);
    }

    #[test]
    fn test_pool_metrics() {
        let metrics = ProxyMetrics::new(100, HealthConfig::default());

        metrics.update_pool_metrics(10, 5, 10);

        let status = metrics.pool_metrics().get_status();
        assert_eq!(status.size, 10);
        assert_eq!(status.available, 5);
        assert_eq!(status.max_size, 10);
        assert_eq!(status.utilization(), 0.5);
    }

    #[test]
    fn test_active_connections() {
        let metrics = ProxyMetrics::new(100, HealthConfig::default());

        metrics.increment_active_connections();
        assert_eq!(metrics.get_active_connections(), 1);

        metrics.increment_active_connections();
        assert_eq!(metrics.get_active_connections(), 2);

        metrics.decrement_active_connections();
        assert_eq!(metrics.get_active_connections(), 1);
    }

    #[test]
    fn test_hot_data_integration() {
        let metrics = ProxyMetrics::new(10, HealthConfig::default());

        let fingerprints = vec![
            "blake3:abc123".to_string(),
            "blake3:abc123".to_string(),
            "blake3:def456".to_string(),
        ];

        metrics.record_hot_data(&fingerprints);

        let top_k = metrics.hot_data().get_top_k();
        assert!(top_k.len() >= 2);

        // "abc123" should have count >= 2
        let abc_entry = top_k.iter().find(|e| e.fingerprint == "blake3:abc123");
        assert!(abc_entry.is_some());
        assert!(abc_entry.unwrap().access_count >= 2);
    }
}
