// scry-proxy/src/proxy/pool_manager.rs

//! Pool Manager with LIFO and Sticky Selection
//!
//! This module provides a `PoolManager` that wraps `TcpConnectionPool` and adds:
//! - LIFO connection selection for better cache locality
//! - Client-to-backend sticky mapping for hybrid pooling mode
//! - Integration with `WaitQueue` for bounded waiting when pool is exhausted
//!
//! The pool manager supports different pooling strategies:
//! - Session pooling: One client per backend connection
//! - Transaction pooling: Connections released after each transaction
//! - Hybrid mode: Connections stick to clients when they have pinned state

use crate::proxy::connection_state::PinReason;
use crate::proxy::tcp_pool::{PooledConnection, TcpConnectionPool};
use crate::proxy::wait_queue::{QueueFullError, WaitQueue};
use crate::tls::BackendTransport;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, trace, warn};

/// Errors that can occur when acquiring a connection
#[derive(Debug, Error)]
pub enum AcquireError {
    #[error("pool error: {0}")]
    PoolError(#[from] anyhow::Error),

    #[error("queue full: {0}")]
    QueueFull(#[from] QueueFullError),

    #[error("wait timeout")]
    WaitTimeout,
}

/// Configuration for the pool manager
#[derive(Debug, Clone)]
pub struct PoolManagerConfig {
    /// Use LIFO selection for better cache locality
    pub lifo: bool,
    /// Maximum queue depth for waiting clients
    pub queue_depth: usize,
    /// Seconds before unpinning idle connections (0 = never)
    pub idle_unpin_secs: u64,
    /// Timeout for waiting in queue (milliseconds, 0 = no timeout)
    pub wait_timeout_ms: u64,
}

impl Default for PoolManagerConfig {
    fn default() -> Self {
        Self {
            lifo: true,
            queue_depth: 100,
            idle_unpin_secs: 300,    // 5 minutes
            wait_timeout_ms: 30_000, // 30 seconds
        }
    }
}

/// Information about a sticky connection
#[derive(Debug, Clone)]
pub struct StickyConnectionInfo {
    /// Reasons why the connection is pinned
    pub pin_reasons: Vec<PinReason>,
    /// When the connection was last used
    pub last_used: Instant,
    /// Unique identifier for this sticky binding
    pub binding_id: u64,
}

impl StickyConnectionInfo {
    fn new(pin_reasons: Vec<PinReason>, binding_id: u64) -> Self {
        Self { pin_reasons, last_used: Instant::now(), binding_id }
    }

    fn touch(&mut self) {
        self.last_used = Instant::now();
    }
}

/// A managed connection from the pool manager
///
/// This wrapper tracks whether the connection is pinned and should be
/// returned to a sticky mapping rather than the general pool.
pub struct ManagedConnection {
    /// The underlying pooled connection (Option for take semantics)
    connection: Option<PooledConnection>,
    /// Client ID this connection belongs to
    client_id: u64,
    /// Whether this connection is pinned to the client
    is_pinned: bool,
    /// Binding ID for sticky connections (used internally)
    #[allow(dead_code)]
    binding_id: Option<u64>,
}

impl ManagedConnection {
    /// Get a reference to the underlying backend transport (TCP or TLS)
    pub fn stream(&self) -> &BackendTransport {
        self.connection.as_ref().expect("connection taken")
    }

    /// Get a mutable reference to the underlying backend transport (TCP or TLS)
    pub fn stream_mut(&mut self) -> &mut BackendTransport {
        self.connection.as_mut().expect("connection taken")
    }

    /// Check if this connection is pinned
    pub fn is_pinned(&self) -> bool {
        self.is_pinned
    }

    /// Get the client ID
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    /// Take the underlying connection (for returning to pool)
    fn take_connection(&mut self) -> Option<PooledConnection> {
        self.connection.take()
    }
}

/// Pool Manager with LIFO and sticky selection
///
/// Wraps `TcpConnectionPool` to provide:
/// - LIFO connection selection (optional, for cache locality)
/// - Sticky client-to-backend mapping for hybrid pooling mode
/// - Integration with `WaitQueue` for bounded waiting
pub struct PoolManager {
    /// Underlying connection pool
    pool: Arc<TcpConnectionPool>,
    /// Sticky mapping: client_id -> (connection, info)
    /// Uses RwLock<HashMap> for concurrent access
    sticky_map: RwLock<HashMap<u64, (PooledConnection, StickyConnectionInfo)>>,
    /// Wait queue for bounded waiting when pool exhausted
    wait_queue: Arc<WaitQueue>,
    /// Configuration
    config: PoolManagerConfig,
    /// Counter for generating unique binding IDs
    next_binding_id: AtomicU64,
}

impl PoolManager {
    /// Create a new pool manager
    pub fn new(
        pool: Arc<TcpConnectionPool>,
        wait_queue: Arc<WaitQueue>,
        config: PoolManagerConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            pool,
            sticky_map: RwLock::new(HashMap::new()),
            wait_queue,
            config,
            next_binding_id: AtomicU64::new(1),
        })
    }

    /// Acquire a connection for a client
    ///
    /// If `needs_sticky` is true and the client has a sticky mapping,
    /// returns that connection. Otherwise gets from pool (with LIFO if configured).
    /// If pool is exhausted, uses WaitQueue (returns error if queue full).
    pub async fn acquire(
        self: &Arc<Self>,
        client_id: u64,
        needs_sticky: bool,
    ) -> Result<ManagedConnection, AcquireError> {
        // 1. Check sticky mapping first if needed
        if needs_sticky {
            if let Some(conn) = self.get_sticky_connection(client_id) {
                debug!(client_id, "Returning sticky connection");
                return Ok(conn);
            }
        }

        // 2. Try to get from pool
        match self.pool.get().await {
            Ok(pooled_conn) => {
                trace!(client_id, "Got connection from pool");
                Ok(ManagedConnection {
                    connection: Some(pooled_conn),
                    client_id,
                    is_pinned: false,
                    binding_id: None,
                })
            }
            Err(e) => {
                // 3. Pool exhausted, try wait queue
                debug!(client_id, error = %e, "Pool exhausted, trying wait queue");
                self.wait_for_connection(client_id).await
            }
        }
    }

    /// Release a connection back to the pool
    ///
    /// If the connection is pinned, it's kept in the sticky mapping.
    /// Otherwise, it's returned to the pool.
    pub fn release(&self, mut conn: ManagedConnection) {
        let client_id = conn.client_id;

        if conn.is_pinned {
            // Keep in sticky map - update last_used time
            if let Some(pooled) = conn.take_connection() {
                let mut map = self.sticky_map.write();
                if let Some((_, ref mut info)) = map.get_mut(&client_id) {
                    info.touch();
                } else {
                    // Connection was pinned but not in map - this indicates a logic error
                    // where is_pinned was set without adding to sticky_map, or the entry
                    // was removed by another thread. Log a warning and recover by adding
                    // the connection to the sticky map.
                    warn!(
                        client_id,
                        "Pinned connection not found in sticky map - recovering by adding entry"
                    );
                    let binding_id = self.next_binding_id.fetch_add(1, Ordering::Relaxed);
                    map.insert(client_id, (pooled, StickyConnectionInfo::new(vec![], binding_id)));
                }
            }
            trace!(client_id, "Released pinned connection back to sticky map");
        } else {
            // Return to pool (drop the ManagedConnection which drops PooledConnection)
            // PooledConnection's Drop impl handles returning to deadpool
            trace!(client_id, "Released connection back to pool");
        }

        // Notify one waiter that a connection might be available
        self.wait_queue.notify_one();
    }

    /// Pin a client's connection with a specific reason
    ///
    /// This keeps the connection sticky to the client until unpinned.
    pub fn pin(&self, client_id: u64, reason: PinReason, conn: ManagedConnection) {
        if let Some(pooled) = {
            let mut conn = conn;
            conn.take_connection()
        } {
            let mut map = self.sticky_map.write();
            if let Some((_, ref mut info)) = map.get_mut(&client_id) {
                // Already sticky, add reason
                if !info.pin_reasons.contains(&reason) {
                    info.pin_reasons.push(reason);
                }
                info.touch();
            } else {
                // New sticky binding
                let binding_id = self.next_binding_id.fetch_add(1, Ordering::Relaxed);
                let info = StickyConnectionInfo::new(vec![reason], binding_id);
                map.insert(client_id, (pooled, info));
            }
            debug!(client_id, ?reason, "Pinned connection");
        }
    }

    /// Unpin a client's connection
    ///
    /// Removes the sticky mapping and returns the connection to the pool.
    pub fn unpin(&self, client_id: u64) {
        let entry = {
            let mut map = self.sticky_map.write();
            map.remove(&client_id)
        };

        if entry.is_some() {
            debug!(client_id, "Unpinned connection, returning to pool");
            // Connection (PooledConnection) is dropped here, returning to pool
            self.wait_queue.notify_one();
        }
    }

    /// Get sticky connection info for a client (without taking the connection)
    pub fn get_sticky_info(&self, client_id: u64) -> Option<StickyConnectionInfo> {
        let map = self.sticky_map.read();
        map.get(&client_id).map(|(_, info)| info.clone())
    }

    /// Check if a client has a sticky connection
    pub fn has_sticky(&self, client_id: u64) -> bool {
        let map = self.sticky_map.read();
        map.contains_key(&client_id)
    }

    /// Get the number of sticky connections
    pub fn sticky_count(&self) -> usize {
        let map = self.sticky_map.read();
        map.len()
    }

    /// Clean up idle sticky connections
    ///
    /// Returns connections that have been idle longer than `idle_unpin_secs` to the pool.
    pub fn cleanup_idle(&self) -> usize {
        if self.config.idle_unpin_secs == 0 {
            return 0;
        }

        let idle_threshold = Duration::from_secs(self.config.idle_unpin_secs);
        let now = Instant::now();
        let mut cleaned = 0;

        let to_remove: Vec<u64> = {
            let map = self.sticky_map.read();
            map.iter()
                .filter(|(_, (_, info))| now.duration_since(info.last_used) > idle_threshold)
                .map(|(k, _)| *k)
                .collect()
        };

        for client_id in to_remove {
            let mut map = self.sticky_map.write();
            if let Some((_, info)) = map.get(&client_id) {
                if now.duration_since(info.last_used) > idle_threshold {
                    map.remove(&client_id);
                    cleaned += 1;
                    debug!(client_id, "Cleaned up idle sticky connection");
                }
            }
        }

        if cleaned > 0 {
            // Notify waiters since connections were returned
            for _ in 0..cleaned {
                self.wait_queue.notify_one();
            }
        }

        cleaned
    }

    /// Get the underlying pool for status queries
    pub fn pool(&self) -> &TcpConnectionPool {
        &self.pool
    }

    /// Pre-warm the underlying connection pool
    ///
    /// # Arguments
    /// * `count` - Number of connections to pre-create
    ///
    /// # Returns
    /// The number of connections successfully created
    pub async fn warmup(&self, count: usize) -> usize {
        self.pool.warmup(count).await
    }

    /// Get the wait queue depth
    pub fn wait_queue_depth(&self) -> usize {
        self.wait_queue.depth()
    }

    /// Take a sticky connection for a client
    ///
    /// This removes the connection from the sticky map. The caller must either:
    /// 1. Call `release()` with `is_pinned = true` to put it back
    /// 2. Call `release()` with `is_pinned = false` to return to pool
    /// 3. Call `pin()` to re-establish the sticky mapping
    fn get_sticky_connection(&self, client_id: u64) -> Option<ManagedConnection> {
        let mut map = self.sticky_map.write();
        if let Some((pooled_conn, info)) = map.remove(&client_id) {
            let binding_id = info.binding_id;
            trace!(client_id, binding_id, "Taking sticky connection");

            Some(ManagedConnection {
                connection: Some(pooled_conn),
                client_id,
                is_pinned: true, // Mark as pinned so release knows to re-sticky
                binding_id: Some(binding_id),
            })
        } else {
            None
        }
    }

    // Internal: Wait for a connection to become available
    async fn wait_for_connection(&self, client_id: u64) -> Result<ManagedConnection, AcquireError> {
        // Try to enqueue
        let mut waiter = self.wait_queue.enqueue().await?;

        // Wait with timeout
        let timeout = if self.config.wait_timeout_ms > 0 {
            Some(Duration::from_millis(self.config.wait_timeout_ms))
        } else {
            None
        };

        let wait_result = if let Some(timeout_duration) = timeout {
            tokio::time::timeout(timeout_duration, waiter.wait()).await
        } else {
            waiter.wait().await;
            Ok(())
        };

        match wait_result {
            Ok(()) => {
                // We were notified, try to get connection again
                match self.pool.get().await {
                    Ok(pooled_conn) => {
                        trace!(client_id, "Got connection after waiting");
                        Ok(ManagedConnection {
                            connection: Some(pooled_conn),
                            client_id,
                            is_pinned: false,
                            binding_id: None,
                        })
                    }
                    Err(e) => {
                        warn!(client_id, error = %e, "Failed to get connection after wait");
                        Err(AcquireError::PoolError(e))
                    }
                }
            }
            Err(_) => {
                debug!(client_id, "Wait timeout");
                Err(AcquireError::WaitTimeout)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TlsConfig;
    use crate::protocol::postgres::PostgresProtocol;
    use crate::protocol::{Protocol, ProtocolConfig};

    fn create_test_pool() -> Arc<TcpConnectionPool> {
        create_test_pool_with_lifo(true)
    }

    fn create_test_pool_with_lifo(lifo: bool) -> Arc<TcpConnectionPool> {
        let protocol = Arc::new(PostgresProtocol::new()) as Arc<dyn Protocol>;
        let config = ProtocolConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: Some("test".to_string()),
            user: Some("postgres".to_string()),
            password: Some("password".to_string()),
        };
        let tls_config = TlsConfig::default();

        Arc::new(
            TcpConnectionPool::new(
                protocol,
                config,
                &tls_config,
                10,   // max_size
                None, // min_idle
                None, // circuit_breaker
                None, // retry_config
                lifo, // lifo
            )
            .expect("Failed to create pool"),
        )
    }

    #[test]
    fn test_pool_manager_creation() {
        let pool = create_test_pool();
        let wait_queue = WaitQueue::new(100);
        let config = PoolManagerConfig::default();

        let manager = PoolManager::new(pool, wait_queue, config);

        assert_eq!(manager.sticky_count(), 0);
        assert_eq!(manager.wait_queue_depth(), 0);
    }

    #[test]
    fn test_config_defaults() {
        let config = PoolManagerConfig::default();

        assert!(config.lifo);
        assert_eq!(config.queue_depth, 100);
        assert_eq!(config.idle_unpin_secs, 300);
        assert_eq!(config.wait_timeout_ms, 30_000);
    }

    #[test]
    fn test_sticky_info_creation() {
        let info = StickyConnectionInfo::new(vec![PinReason::PreparedStatement], 1);

        assert_eq!(info.pin_reasons.len(), 1);
        assert_eq!(info.binding_id, 1);
    }

    #[test]
    fn test_sticky_info_touch() {
        let mut info = StickyConnectionInfo::new(vec![], 1);
        let initial_time = info.last_used;

        std::thread::sleep(std::time::Duration::from_millis(10));
        info.touch();

        assert!(info.last_used > initial_time);
    }

    #[test]
    fn test_has_sticky_initially_false() {
        let pool = create_test_pool();
        let wait_queue = WaitQueue::new(100);
        let config = PoolManagerConfig::default();

        let manager = PoolManager::new(pool, wait_queue, config);

        assert!(!manager.has_sticky(12345));
    }

    #[test]
    fn test_managed_connection_is_pinned() {
        // Test ManagedConnection properties
        let conn = ManagedConnection {
            connection: None,
            client_id: 42,
            is_pinned: true,
            binding_id: Some(1),
        };

        assert!(conn.is_pinned());
        assert_eq!(conn.client_id(), 42);
    }

    #[test]
    fn test_managed_connection_not_pinned() {
        let conn = ManagedConnection {
            connection: None,
            client_id: 99,
            is_pinned: false,
            binding_id: None,
        };

        assert!(!conn.is_pinned());
        assert_eq!(conn.client_id(), 99);
    }

    #[test]
    fn test_cleanup_idle_with_zero_timeout() {
        let pool = create_test_pool();
        let wait_queue = WaitQueue::new(100);
        let config = PoolManagerConfig {
            idle_unpin_secs: 0, // Disabled
            ..Default::default()
        };

        let manager = PoolManager::new(pool, wait_queue, config);

        // Should return 0 when disabled
        assert_eq!(manager.cleanup_idle(), 0);
    }

    #[test]
    fn test_acquire_error_display() {
        let err = AcquireError::WaitTimeout;
        assert_eq!(format!("{}", err), "wait timeout");

        let err = AcquireError::QueueFull(QueueFullError);
        assert!(format!("{}", err).contains("queue full"));
    }

    #[test]
    fn test_get_sticky_info_none() {
        let pool = create_test_pool();
        let wait_queue = WaitQueue::new(100);
        let config = PoolManagerConfig::default();

        let manager = PoolManager::new(pool, wait_queue, config);

        assert!(manager.get_sticky_info(99999).is_none());
    }

    #[tokio::test]
    async fn test_queue_full_error() {
        let pool = create_test_pool();
        let wait_queue = WaitQueue::new(1); // Very small queue
        let config = PoolManagerConfig::default();

        let _manager = PoolManager::new(pool, wait_queue.clone(), config);

        // Fill the queue
        let _waiter1 = wait_queue.enqueue().await.unwrap();

        // Now try to wait - should get queue full
        let result = wait_queue.enqueue().await;
        assert!(result.is_err());
    }

    // Note: Tests that require actual TCP connections would be integration tests.
    // The following tests verify the logic without needing real connections.

    #[test]
    fn test_unpin_nonexistent_client() {
        let pool = create_test_pool();
        let wait_queue = WaitQueue::new(100);
        let config = PoolManagerConfig::default();

        let manager = PoolManager::new(pool, wait_queue, config);

        // Should not panic
        manager.unpin(99999);
        assert!(!manager.has_sticky(99999));
    }

    #[test]
    fn test_pool_access() {
        let pool = create_test_pool();
        let wait_queue = WaitQueue::new(100);
        let config = PoolManagerConfig::default();

        let manager = PoolManager::new(pool, wait_queue, config);

        let status = manager.pool().status();
        assert_eq!(status.max_size, 10);
    }

    #[test]
    fn test_wait_queue_depth_initially_zero() {
        let pool = create_test_pool();
        let wait_queue = WaitQueue::new(100);
        let config = PoolManagerConfig::default();

        let manager = PoolManager::new(pool, wait_queue, config);

        assert_eq!(manager.wait_queue_depth(), 0);
    }

    #[tokio::test]
    async fn test_wait_queue_depth_increases() {
        let pool = create_test_pool();
        let wait_queue = WaitQueue::new(100);
        let config = PoolManagerConfig::default();

        let _manager = PoolManager::new(pool, wait_queue.clone(), config);

        // Add a waiter directly to the queue
        let _waiter = wait_queue.enqueue().await.unwrap();

        assert_eq!(wait_queue.depth(), 1);
    }

    #[test]
    fn test_sticky_connection_info_clone() {
        let info = StickyConnectionInfo::new(
            vec![PinReason::PreparedStatement, PinReason::SessionVariable],
            42,
        );

        let cloned = info.clone();

        assert_eq!(cloned.pin_reasons.len(), 2);
        assert_eq!(cloned.binding_id, 42);
    }

    #[test]
    fn test_config_custom_values() {
        let config = PoolManagerConfig {
            lifo: false,
            queue_depth: 50,
            idle_unpin_secs: 600,
            wait_timeout_ms: 5000,
        };

        assert!(!config.lifo);
        assert_eq!(config.queue_depth, 50);
        assert_eq!(config.idle_unpin_secs, 600);
        assert_eq!(config.wait_timeout_ms, 5000);
    }
}
