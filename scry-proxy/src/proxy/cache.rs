//! Prepared statement cache for tracking extended query protocol state.

use scry_protocol::ParamValue;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

/// Cached prepared statement from Parse message
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub query: String,
    pub param_oids: Vec<u32>,
}

/// Pending execution state from Bind message
#[derive(Debug)]
pub struct PendingExecution {
    pub query: String,
    pub params: Vec<ParamValue>,
    pub params_incomplete: bool,
    pub started_at: Instant,
}

/// Per-connection cache for prepared statements and pending executions.
/// Uses LRU eviction when max_size is reached.
pub struct PreparedStatementCache {
    /// Statement name → prepared statement
    statements: HashMap<String, PreparedStatement>,
    /// LRU order: oldest at front
    lru_order: VecDeque<String>,
    /// Maximum cached statements
    max_size: usize,
    /// Portal name → pending execution
    pending: HashMap<String, PendingExecution>,
}

impl PreparedStatementCache {
    /// Create a new cache with the given maximum size.
    pub fn new(max_size: usize) -> Self {
        Self {
            statements: HashMap::new(),
            lru_order: VecDeque::new(),
            max_size,
            pending: HashMap::new(),
        }
    }

    /// Insert a prepared statement, evicting oldest if at capacity.
    pub fn insert_statement(&mut self, name: String, stmt: PreparedStatement) {
        // Remove existing entry if present
        if self.statements.contains_key(&name) {
            self.lru_order.retain(|n| n != &name);
        }

        // Evict oldest if at capacity
        while self.statements.len() >= self.max_size {
            if let Some(oldest) = self.lru_order.pop_front() {
                self.statements.remove(&oldest);
            } else {
                break;
            }
        }

        self.statements.insert(name.clone(), stmt);
        self.lru_order.push_back(name);
    }

    /// Get a prepared statement by name, updating LRU order.
    pub fn get_statement(&mut self, name: &str) -> Option<&PreparedStatement> {
        if self.statements.contains_key(name) {
            // Move to back (most recently used)
            self.lru_order.retain(|n| n != name);
            self.lru_order.push_back(name.to_string());
            self.statements.get(name)
        } else {
            None
        }
    }

    /// Remove a statement by name.
    pub fn remove_statement(&mut self, name: &str) {
        self.statements.remove(name);
        self.lru_order.retain(|n| n != name);
    }

    /// Set pending execution for a portal.
    pub fn set_pending(&mut self, portal: String, pending: PendingExecution) {
        self.pending.insert(portal, pending);
    }

    /// Take pending execution for a portal.
    pub fn take_pending(&mut self, portal: &str) -> Option<PendingExecution> {
        self.pending.remove(portal)
    }

    /// Whether a query is currently pending (in flight) for a portal.
    pub fn has_pending(&self, portal: &str) -> bool {
        self.pending.contains_key(portal)
    }

    /// Clear pending execution for a portal.
    pub fn clear_pending(&mut self, portal: &str) {
        self.pending.remove(portal);
    }

    /// Clear all state (for DISCARD ALL).
    pub fn clear(&mut self) {
        self.statements.clear();
        self.lru_order.clear();
        self.pending.clear();
    }

    /// Number of cached statements.
    pub fn len(&self) -> usize {
        self.statements.len()
    }

    /// Whether cache is empty.
    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_stmt(query: &str) -> PreparedStatement {
        PreparedStatement { query: query.to_string(), param_oids: vec![] }
    }

    #[test]
    fn test_insert_and_get() {
        let mut cache = PreparedStatementCache::new(10);
        cache.insert_statement("s1".into(), make_stmt("SELECT 1"));

        let stmt = cache.get_statement("s1");
        assert!(stmt.is_some());
        assert_eq!(stmt.unwrap().query, "SELECT 1");
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = PreparedStatementCache::new(2);
        cache.insert_statement("a".into(), make_stmt("SELECT a"));
        cache.insert_statement("b".into(), make_stmt("SELECT b"));
        cache.insert_statement("c".into(), make_stmt("SELECT c")); // evicts "a"

        assert!(cache.get_statement("a").is_none());
        assert!(cache.get_statement("b").is_some());
        assert!(cache.get_statement("c").is_some());
    }

    #[test]
    fn test_lru_access_updates_order() {
        let mut cache = PreparedStatementCache::new(2);
        cache.insert_statement("a".into(), make_stmt("SELECT a"));
        cache.insert_statement("b".into(), make_stmt("SELECT b"));

        // Access "a" to make it most recently used
        cache.get_statement("a");

        // Insert "c" - should evict "b" not "a"
        cache.insert_statement("c".into(), make_stmt("SELECT c"));

        assert!(cache.get_statement("a").is_some());
        assert!(cache.get_statement("b").is_none());
        assert!(cache.get_statement("c").is_some());
    }

    #[test]
    fn test_remove_statement() {
        let mut cache = PreparedStatementCache::new(10);
        cache.insert_statement("s1".into(), make_stmt("SELECT 1"));
        cache.remove_statement("s1");

        assert!(cache.get_statement("s1").is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_pending_execution() {
        let mut cache = PreparedStatementCache::new(10);

        cache.set_pending(
            "".into(),
            PendingExecution {
                query: "SELECT 1".into(),
                params: vec![],
                params_incomplete: false,
                started_at: Instant::now(),
            },
        );

        let pending = cache.take_pending("");
        assert!(pending.is_some());
        assert_eq!(pending.unwrap().query, "SELECT 1");

        // Should be gone after take
        assert!(cache.take_pending("").is_none());
    }

    #[test]
    fn test_clear() {
        let mut cache = PreparedStatementCache::new(10);
        cache.insert_statement("s1".into(), make_stmt("SELECT 1"));
        cache.set_pending(
            "p1".into(),
            PendingExecution {
                query: "SELECT 1".into(),
                params: vec![],
                params_incomplete: false,
                started_at: Instant::now(),
            },
        );

        cache.clear();

        assert!(cache.get_statement("s1").is_none());
        assert!(cache.take_pending("p1").is_none());
        assert!(cache.is_empty());
    }
}
