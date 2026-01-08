/// Query execution timeline tracking
///
/// Tracks the different phases of query execution to identify bottlenecks:
/// 1. Queue time - Time waiting before pool acquisition
/// 2. Pool acquire - Time to get a connection from the pool
/// 3. Backend execution - Time spent executing on the backend database
///
/// This enables the /debug/timeline endpoint to show phase-by-phase breakdowns.
use std::time::{Duration, Instant};

/// Timeline tracking for a single query execution
#[derive(Debug, Clone)]
pub struct QueryTimeline {
    /// When the request was received (implicit start)
    received_at: Instant,

    /// When we started trying to acquire a pool connection
    pool_acquire_start: Option<Instant>,

    /// When we successfully acquired a pool connection
    pool_acquire_end: Option<Instant>,

    /// When backend execution started (query sent to database)
    backend_start: Option<Instant>,

    /// When backend execution completed (response received)
    backend_end: Option<Instant>,
}

impl QueryTimeline {
    /// Create a new timeline, starting the clock
    pub fn new() -> Self {
        Self {
            received_at: Instant::now(),
            pool_acquire_start: None,
            pool_acquire_end: None,
            backend_start: None,
            backend_end: None,
        }
    }

    /// Mark the start of pool connection acquisition
    pub fn mark_pool_acquire_start(&mut self) {
        self.pool_acquire_start = Some(Instant::now());
    }

    /// Mark the end of pool connection acquisition (connection obtained)
    pub fn mark_pool_acquire_end(&mut self) {
        self.pool_acquire_end = Some(Instant::now());
    }

    /// Mark the start of backend execution (query sent to database)
    pub fn mark_backend_start(&mut self) {
        self.backend_start = Some(Instant::now());
    }

    /// Mark the end of backend execution (response received from database)
    pub fn mark_backend_end(&mut self) {
        self.backend_end = Some(Instant::now());
    }

    /// Calculate time spent waiting in queue before pool acquisition
    pub fn queue_time(&self) -> Option<Duration> {
        self.pool_acquire_start.map(|start| start.duration_since(self.received_at))
    }

    /// Calculate time spent acquiring a connection from the pool
    pub fn pool_acquire_time(&self) -> Option<Duration> {
        match (self.pool_acquire_start, self.pool_acquire_end) {
            (Some(start), Some(end)) => Some(end.duration_since(start)),
            _ => None,
        }
    }

    /// Calculate time spent executing on the backend database
    pub fn backend_time(&self) -> Option<Duration> {
        match (self.backend_start, self.backend_end) {
            (Some(start), Some(end)) => Some(end.duration_since(start)),
            _ => None,
        }
    }

    /// Calculate total time from request received to completion
    pub fn total_time(&self) -> Duration {
        self.backend_end.unwrap_or_else(Instant::now).duration_since(self.received_at)
    }

    /// Get all phase durations in microseconds (for histogram recording)
    pub fn phase_durations_micros(&self) -> TimelinePhases {
        TimelinePhases {
            queue_time_micros: self.queue_time().map(|d| d.as_micros() as u64),
            pool_acquire_micros: self.pool_acquire_time().map(|d| d.as_micros() as u64),
            backend_micros: self.backend_time().map(|d| d.as_micros() as u64),
            total_micros: self.total_time().as_micros() as u64,
        }
    }
}

impl Default for QueryTimeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Phase durations in microseconds (ready for histogram recording)
#[derive(Debug, Clone, Copy)]
pub struct TimelinePhases {
    pub queue_time_micros: Option<u64>,
    pub pool_acquire_micros: Option<u64>,
    pub backend_micros: Option<u64>,
    pub total_micros: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_timeline_phases() {
        let mut timeline = QueryTimeline::new();

        // Simulate queue time
        sleep(Duration::from_millis(5));
        timeline.mark_pool_acquire_start();

        // Simulate pool acquisition
        sleep(Duration::from_millis(10));
        timeline.mark_pool_acquire_end();

        // Simulate backend execution
        timeline.mark_backend_start();
        sleep(Duration::from_millis(20));
        timeline.mark_backend_end();

        // Verify all phases are tracked
        assert!(timeline.queue_time().is_some());
        assert!(timeline.pool_acquire_time().is_some());
        assert!(timeline.backend_time().is_some());

        // Verify durations are reasonable (within tolerance)
        let queue = timeline.queue_time().unwrap();
        let pool = timeline.pool_acquire_time().unwrap();
        let backend = timeline.backend_time().unwrap();
        let total = timeline.total_time();

        assert!(queue.as_millis() >= 5 && queue.as_millis() < 15);
        assert!(pool.as_millis() >= 10 && pool.as_millis() < 20);
        assert!(backend.as_millis() >= 20 && backend.as_millis() < 30);

        // Total should be approximately sum of phases
        let sum = queue + pool + backend;
        let diff = total.abs_diff(sum);
        assert!(diff.as_millis() < 5, "Total time should approximate sum of phases");
    }

    #[test]
    fn test_timeline_phases_micros() {
        let mut timeline = QueryTimeline::new();

        timeline.mark_pool_acquire_start();
        sleep(Duration::from_micros(500));
        timeline.mark_pool_acquire_end();

        timeline.mark_backend_start();
        sleep(Duration::from_micros(1000));
        timeline.mark_backend_end();

        let phases = timeline.phase_durations_micros();

        // All durations should be in microseconds
        assert!(phases.queue_time_micros.is_some());
        assert!(phases.pool_acquire_micros.is_some());
        assert!(phases.backend_micros.is_some());
        assert!(phases.total_micros > 0);

        // Pool acquire should be >= 500 microseconds
        assert!(phases.pool_acquire_micros.unwrap() >= 500);

        // Backend should be >= 1000 microseconds
        assert!(phases.backend_micros.unwrap() >= 1000);
    }

    #[test]
    fn test_incomplete_timeline() {
        let mut timeline = QueryTimeline::new();

        // Only mark some phases
        timeline.mark_pool_acquire_start();
        timeline.mark_backend_start();
        timeline.mark_backend_end();

        // Queue time should work (pool acquire start is set)
        assert!(timeline.queue_time().is_some());

        // Pool acquire time should be None (end not set)
        assert!(timeline.pool_acquire_time().is_none());

        // Backend time should work
        assert!(timeline.backend_time().is_some());

        // Total time always works
        assert!(timeline.total_time().as_nanos() > 0);
    }
}
