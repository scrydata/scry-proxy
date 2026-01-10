mod cache;
mod connection;
mod connection_state;
mod event_batcher;
mod mode_enforcer;
mod pool_manager;
mod server;
mod state_replayer;
mod tcp_pool;
mod transaction;
mod wait_queue;

pub use cache::{PendingExecution, PreparedStatement, PreparedStatementCache};
pub use connection::ConnectionHandler;
pub use connection_state::{ConnectionState, PinReason, PreparedStatementInfo, ReplayableState};
pub use event_batcher::EventBatcher;
pub use mode_enforcer::{ModeEnforcer, PoolingMode};
pub use pool_manager::{
    AcquireError, ManagedConnection, PoolManager, PoolManagerConfig, StickyConnectionInfo,
};
pub use server::ProxyServer;
pub use state_replayer::{ReplayError, ReplayResult, StateReplayer};
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

    let server = ProxyServer::new(config, batcher, metrics).await?;
    server.run().await
}
