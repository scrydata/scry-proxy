mod cache;
mod connection;
mod connection_state;
mod event_batcher;
mod mode_enforcer;
mod pool_manager;
mod server;
mod tcp_pool;
mod transaction;
mod wait_queue;

pub use cache::{PendingExecution, PreparedStatement, PreparedStatementCache};
pub use connection::ConnectionHandler;
pub use connection_state::{ConnectionState, PinReason, PreparedStatementInfo, ReplayableState};
pub use event_batcher::EventBatcher;
pub use mode_enforcer::{ModeEnforcer, PoolingMode};
pub use pool_manager::{
    AcquireError, ConnectionTaken, ManagedConnection, PoolManager, PoolManagerConfig,
    StickyConnectionInfo,
};
pub use server::ProxyServer;
pub use tcp_pool::{PoolStatus, TcpConnectionPool};
pub use transaction::{TransactionState, TransactionTracker};
pub use wait_queue::{QueueFullError, WaitQueue, Waiter};

use crate::config::Config;
use crate::observability::ProxyMetrics;
use crate::publisher::EventPublisher;
use anyhow::Result;
use std::sync::Arc;

/// Start the proxy server with the given configuration and event publisher
pub async fn start_proxy(
    config: Config,
    publisher: Arc<dyn EventPublisher>,
    metrics: Arc<ProxyMetrics>,
) -> Result<()> {
    let batcher = EventBatcher::new(
        publisher,
        config.publisher.batch_size,
        config.publisher.flush_interval_ms,
        config.publisher.max_queue_size,
    );

    let server = ProxyServer::new(config.clone(), batcher, metrics).await?;

    // Warm up connection pools before accepting connections
    let min_idle = config.performance.pool_min_idle;
    if min_idle > 0 {
        server.warmup_pools(min_idle).await;
    }

    // Setup SIGHUP handler for config reload (Unix only)
    #[cfg(unix)]
    {
        let reload_sender = server.reload_sender();
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};

            let mut sighup = match signal(SignalKind::hangup()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to setup SIGHUP handler");
                    return;
                }
            };

            loop {
                sighup.recv().await;
                tracing::info!("Received SIGHUP, triggering config reload");
                if reload_sender.send(()).is_err() {
                    tracing::warn!("Failed to send reload signal, server may have shutdown");
                    break;
                }
            }
        });
        tracing::info!("SIGHUP handler registered for config reload");
    }

    server.run().await
}
