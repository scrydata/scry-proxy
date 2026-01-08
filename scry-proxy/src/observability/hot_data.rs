/// Hot data detection using Count-Min Sketch and Top-K heap
///
/// Tracks which value fingerprints are accessed most frequently to identify
/// "hot" data patterns without storing actual query values (preserving privacy).
///
/// Algorithm:
/// 1. Count-Min Sketch: Probabilistic frequency counter with <1% error
/// 2. Top-K Heap: Maintains the K most frequently accessed fingerprints
/// 3. Temporal Decay: Recent accesses weighted more heavily than old ones
use ahash::RandomState;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

/// Hot data tracker combining Count-Min Sketch and Top-K heap
pub struct HotDataTracker {
    sketch: RwLock<CountMinSketch>,
    top_k: RwLock<TopKHeap>,
    decay_factor: f64,
}

impl HotDataTracker {
    /// Create a new hot data tracker
    ///
    /// # Arguments
    /// * `k` - Number of top fingerprints to track (default: 100)
    /// * `decay_factor` - Temporal decay factor 0-1 (default: 0.99 = 1% decay per update)
    pub fn new(k: usize, decay_factor: f64) -> Self {
        Self {
            sketch: RwLock::new(CountMinSketch::new(2048, 4)),
            top_k: RwLock::new(TopKHeap::new(k)),
            decay_factor,
        }
    }

    /// Record an access to a value fingerprint
    ///
    /// This is called for each query that has value_fingerprints.
    /// Cost: O(depth) for sketch update + O(log k) for heap update = ~50-100ns
    pub fn record_access(&self, fingerprint: &str) {
        // Update Count-Min Sketch (lock-free, atomic increments)
        let estimated_count = self.sketch.read().increment(fingerprint);

        // Update Top-K heap with write lock
        let mut top_k = self.top_k.write();
        top_k.update(fingerprint.to_string(), estimated_count);
    }

    /// Get the top K most frequently accessed fingerprints
    pub fn get_top_k(&self) -> Vec<HotDataEntry> {
        self.top_k.read().get_top_k()
    }

    /// Get total number of unique fingerprints tracked
    pub fn unique_fingerprints(&self) -> usize {
        self.top_k.read().unique_count()
    }

    /// Apply temporal decay to all counts (called periodically)
    ///
    /// This ensures recent accesses are weighted more heavily.
    /// Should be called every few seconds from a background task.
    pub fn apply_decay(&self) {
        // Decay the sketch
        self.sketch.write().apply_decay(self.decay_factor);

        // Decay the top-k heap
        self.top_k.write().apply_decay(self.decay_factor);
    }
}

/// Count-Min Sketch for probabilistic frequency counting
///
/// Memory: width * depth * 8 bytes = 2048 * 4 * 8 = 64 KB
/// Error bound: ε = e / width ≈ 0.13% with width=2048
struct CountMinSketch {
    width: usize,
    depth: usize,
    table: Vec<Vec<AtomicU64>>,
    hash_builders: Vec<RandomState>,
}

impl CountMinSketch {
    /// Create a new Count-Min Sketch
    ///
    /// # Arguments
    /// * `width` - Number of buckets per row (larger = less error)
    /// * `depth` - Number of hash functions (larger = less error)
    fn new(width: usize, depth: usize) -> Self {
        let mut table = Vec::with_capacity(depth);
        for _ in 0..depth {
            let row: Vec<AtomicU64> = (0..width).map(|_| AtomicU64::new(0)).collect();
            table.push(row);
        }

        // Create independent hash functions
        let hash_builders: Vec<RandomState> = (0..depth).map(|_| RandomState::new()).collect();

        Self { width, depth, table, hash_builders }
    }

    /// Increment count for a fingerprint, return estimated count
    ///
    /// Lock-free: Uses atomic operations only. Safe for concurrent access.
    fn increment(&self, fingerprint: &str) -> u64 {
        let mut min_count = u64::MAX;

        // Hash with each function and increment corresponding bucket
        for i in 0..self.depth {
            let hash = self.hash(fingerprint, i);
            let bucket = (hash % self.width as u64) as usize;

            // Atomic increment
            let new_count = self.table[i][bucket].fetch_add(1, Ordering::Relaxed) + 1;

            // Track minimum (Count-Min Sketch estimate)
            min_count = min_count.min(new_count);
        }

        min_count
    }

    /// Get estimated count for a fingerprint (minimum across all hash functions)
    #[cfg(test)]
    fn estimate(&self, fingerprint: &str) -> u64 {
        let mut min_count = u64::MAX;

        for i in 0..self.depth {
            let hash = self.hash(fingerprint, i);
            let bucket = (hash % self.width as u64) as usize;
            let count = self.table[i][bucket].load(Ordering::Relaxed);
            min_count = min_count.min(count);
        }

        min_count
    }

    /// Apply temporal decay to all counts
    fn apply_decay(&mut self, decay_factor: f64) {
        for row in &self.table {
            for bucket in row {
                let current = bucket.load(Ordering::Relaxed);
                let decayed = (current as f64 * decay_factor) as u64;
                bucket.store(decayed, Ordering::Relaxed);
            }
        }
    }

    /// Hash a fingerprint with the i-th hash function
    fn hash(&self, fingerprint: &str, hash_index: usize) -> u64 {
        
        
        self.hash_builders[hash_index].hash_one(fingerprint)
    }
}

/// Top-K heap for tracking most frequently accessed fingerprints
///
/// Uses a min-heap: smallest count is at the root.
/// When heap is full, we only insert if new count > min count.
struct TopKHeap {
    k: usize,
    heap: BinaryHeap<Reverse<HotDataEntry>>,
    fingerprint_to_count: HashMap<String, u64>,
}

impl TopKHeap {
    fn new(k: usize) -> Self {
        Self {
            k,
            heap: BinaryHeap::with_capacity(k + 1), // +1 for overflow before pop
            fingerprint_to_count: HashMap::with_capacity(k),
        }
    }

    /// Update or insert a fingerprint with its count
    fn update(&mut self, fingerprint: String, count: u64) {
        // Check if already in top-k
        if let Some(existing_count) = self.fingerprint_to_count.get_mut(&fingerprint) {
            // Update count
            *existing_count = count;

            // Rebuild heap (O(n log n) but n=k is small, typically 100)
            // Alternative: Use a more complex structure like indexed priority queue
            self.rebuild_heap();
        } else if self.fingerprint_to_count.len() < self.k {
            // Heap not full yet, just insert
            self.fingerprint_to_count.insert(fingerprint.clone(), count);
            self.heap.push(Reverse(HotDataEntry { fingerprint, access_count: count }));
        } else {
            // Heap is full, check if this count beats the minimum
            if let Some(Reverse(min_entry)) = self.heap.peek() {
                if count > min_entry.access_count {
                    // Remove minimum
                    if let Some(Reverse(removed)) = self.heap.pop() {
                        self.fingerprint_to_count.remove(&removed.fingerprint);
                    }

                    // Insert new entry
                    self.fingerprint_to_count.insert(fingerprint.clone(), count);
                    self.heap.push(Reverse(HotDataEntry { fingerprint, access_count: count }));
                }
            }
        }
    }

    /// Rebuild the heap from the fingerprint_to_count map
    fn rebuild_heap(&mut self) {
        self.heap.clear();
        for (fingerprint, count) in &self.fingerprint_to_count {
            self.heap.push(Reverse(HotDataEntry {
                fingerprint: fingerprint.clone(),
                access_count: *count,
            }));
        }
    }

    /// Get top K fingerprints sorted by count (descending)
    fn get_top_k(&self) -> Vec<HotDataEntry> {
        let mut entries: Vec<HotDataEntry> = self
            .fingerprint_to_count
            .iter()
            .map(|(fp, count)| HotDataEntry { fingerprint: fp.clone(), access_count: *count })
            .collect();

        // Sort descending by access count
        entries.sort_by(|a, b| b.access_count.cmp(&a.access_count));

        entries
    }

    /// Apply temporal decay to all counts
    fn apply_decay(&mut self, decay_factor: f64) {
        for count in self.fingerprint_to_count.values_mut() {
            *count = (*count as f64 * decay_factor) as u64;
        }
        self.rebuild_heap();
    }

    fn unique_count(&self) -> usize {
        self.fingerprint_to_count.len()
    }
}

/// Entry in the hot data top-K list
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct HotDataEntry {
    pub fingerprint: String,
    pub access_count: u64,
}

// Implement Ord for min-heap semantics
impl Ord for HotDataEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.access_count.cmp(&other.access_count)
    }
}

impl PartialOrd for HotDataEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_min_sketch_basic() {
        let sketch = CountMinSketch::new(1024, 4);

        // Increment same fingerprint multiple times
        let fp = "blake3:abc123";
        for _ in 0..10 {
            sketch.increment(fp);
        }

        let estimate = sketch.estimate(fp);
        assert_eq!(estimate, 10, "Estimate should be exact for single fingerprint");
    }

    #[test]
    fn test_count_min_sketch_multiple_fingerprints() {
        let sketch = CountMinSketch::new(2048, 4);

        // Increment different fingerprints
        for i in 0..100 {
            let fp = format!("blake3:fp{}", i);
            sketch.increment(&fp);
        }

        // Each should have count ~1 (may have collisions but should be close)
        let estimate = sketch.estimate("blake3:fp42");
        assert!((1..=5).contains(&estimate), "Estimate should be close to 1");
    }

    #[test]
    fn test_top_k_heap() {
        let mut heap = TopKHeap::new(5);

        // Insert 10 items, only top 5 should be kept
        for i in 0..10 {
            heap.update(format!("fp{}", i), i as u64);
        }

        let top_k = heap.get_top_k();
        assert_eq!(top_k.len(), 5, "Should keep only top 5");

        // Verify they're the highest counts (5-9)
        assert!(top_k[0].access_count >= 5);
        assert_eq!(top_k[0].access_count, 9); // Highest
    }

    #[test]
    fn test_hot_data_tracker() {
        let tracker = HotDataTracker::new(10, 0.99);

        // Record accesses to different fingerprints
        tracker.record_access("blake3:popular");
        tracker.record_access("blake3:popular");
        tracker.record_access("blake3:popular");
        tracker.record_access("blake3:rare");

        let top_k = tracker.get_top_k();

        // Should have 2 fingerprints
        assert!(top_k.len() >= 2);

        // "popular" should have highest count
        let popular_entry = top_k.iter().find(|e| e.fingerprint == "blake3:popular");
        assert!(popular_entry.is_some());
        assert!(popular_entry.unwrap().access_count >= 3);
    }

    #[test]
    fn test_temporal_decay() {
        let tracker = HotDataTracker::new(10, 0.5); // 50% decay

        tracker.record_access("blake3:test");
        tracker.record_access("blake3:test");

        let before = tracker.get_top_k();
        let count_before = before[0].access_count;

        // Apply decay
        tracker.apply_decay();

        let after = tracker.get_top_k();
        let count_after = after[0].access_count;

        // Count should be roughly halved
        assert!(count_after < count_before);
        assert!(count_after >= count_before / 2 - 1 && count_after <= count_before / 2 + 1);
    }
}
