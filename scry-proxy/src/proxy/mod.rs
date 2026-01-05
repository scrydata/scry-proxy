mod cache;
mod connection;
mod event_batcher;
mod server;
mod tcp_pool;

pub use cache::{PreparedStatementCache, PreparedStatement, PendingExecution};
pub use connection::ConnectionHandler;
pub use event_batcher::EventBatcher;
pub use server::ProxyServer;
pub use tcp_pool::{TcpConnectionPool, PoolStatus};
pub(crate) use tcp_pool::PooledConnection;

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
