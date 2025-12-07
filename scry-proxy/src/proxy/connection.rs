use super::{EventBatcher, TcpConnectionPool};
use crate::config::Config;
use crate::observability::{ProxyMetrics, QueryTimeline};
use crate::protocol::{MessageExtractor, QueryAnonymizer};
use crate::publisher::QueryEventBuilder;
use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument, warn};

/// Handles a single client connection, forwarding messages to/from the backend
pub struct ConnectionHandler {
    client_stream: TcpStream,
    client_addr: SocketAddr,
    connection_id: u64,
    config: Arc<Config>,
    batcher: Arc<EventBatcher>,
    pool: Option<Arc<TcpConnectionPool>>,
    metrics: Arc<ProxyMetrics>,
}

impl ConnectionHandler {
    pub fn new(
        client_stream: TcpStream,
        client_addr: SocketAddr,
        connection_id: u64,
        config: Arc<Config>,
        batcher: Arc<EventBatcher>,
        pool: Option<Arc<TcpConnectionPool>>,
        metrics: Arc<ProxyMetrics>,
    ) -> Self {
        Self { client_stream, client_addr, connection_id, config, batcher, pool, metrics }
    }

    /// Build a QueryEventBuilder with anonymization if enabled
    /// Returns (builder, value_fingerprints) for hot data tracking
    fn build_query_event(
        query: String,
        connection_id: u64,
        database: String,
        anonymize: bool,
    ) -> (QueryEventBuilder, Vec<String>) {
        let mut builder = QueryEventBuilder::new(query.clone());
        builder = builder
            .connection_id(connection_id.to_string())
            .database(database);

        let mut fingerprints = Vec::new();

        // Apply anonymization if enabled
        if anonymize {
            let anonymizer = QueryAnonymizer::new();
            if let Some(anon) = anonymizer.anonymize(&query) {
                fingerprints = anon.value_fingerprints.clone();
                builder = builder
                    .normalized_query(anon.normalized_query)
                    .value_fingerprints(anon.value_fingerprints);
            }
        }

        (builder, fingerprints)
    }

    /// Handle the connection, forwarding messages until completion
    #[instrument(skip(self), fields(connection_id = self.connection_id, client_addr = %self.client_addr))]
    pub async fn handle(self) -> Result<()> {
        info!("Starting connection handler");

        // Get backend connection - either from pool or create direct connection
        let backend_addr = format!("{}:{}", self.config.backend.host, self.config.backend.port);

        // Try to get connection from pool if available
        if let Some(ref pool) = self.pool {
            info!(
                backend_addr = %backend_addr,
                connection_id = self.connection_id,
                "Getting backend connection from pool"
            );
            let pooled_conn = pool.get().await.context("Failed to get connection from pool")?;
            info!(backend_addr = %backend_addr, "Using pooled backend connection");

            // Use pooled connection - keep wrapper alive for entire session
            return self.handle_with_pooled_backend(pooled_conn).await;
        } else {
            info!(backend_addr = %backend_addr, "Creating direct backend connection");
            let backend_stream = TcpStream::connect(&backend_addr).await.context("Failed to connect to backend")?;

            // Use direct connection
            return self.handle_with_owned_backend(backend_stream).await;
        }
    }

    /// Handle connection with a pooled backend connection
    async fn handle_with_pooled_backend(
        mut self,
        mut backend_conn: super::PooledConnection,
    ) -> Result<()> {
        // For pooled connections, we use the wrapper which implements Deref/DerefMut
        // We'll use a manual forwarding loop instead of split() to keep the wrapper alive

        let connection_id = self.connection_id;
        let database = self.config.backend.database.clone();
        let batcher = Arc::clone(&self.batcher);
        let anonymize = self.config.publisher.anonymize;
        let metrics = Arc::clone(&self.metrics);

        let extractor = MessageExtractor::new();
        let current_query: Arc<Mutex<Option<(String, Instant, QueryTimeline)>>> = Arc::new(Mutex::new(None));

        let mut client_buffer = vec![0u8; self.config.performance.buffer_size];
        let mut backend_buffer = vec![0u8; self.config.performance.buffer_size];

        loop {
            tokio::select! {
                // Client -> Backend
                result = self.client_stream.read(&mut client_buffer) => {
                    match result {
                        Ok(0) => {
                            debug!("Client closed connection");
                            break;
                        }
                        Ok(n) => {
                            let data = &client_buffer[..n];

                            // Try to extract query
                            if let Some(query) = extractor.extract_query(data) {
                                debug!(query = %query, "Extracted query from client");
                                let mut timeline = QueryTimeline::new();
                                timeline.mark_backend_start();
                                *current_query.lock().await = Some((query, Instant::now(), timeline));
                            }

                            // Forward to backend (using DerefMut to get &mut TcpStream)
                            backend_conn.write_all(data).await.context("Failed to write to backend")?;
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to read from client");
                            break;
                        }
                    }
                }

                // Backend -> Client
                result = backend_conn.read(&mut backend_buffer) => {
                    match result {
                        Ok(0) => {
                            debug!("Backend closed connection");
                            break;
                        }
                        Ok(n) => {
                            let data = &backend_buffer[..n];

                            // Check for error response
                            if let Some(error_msg) = extractor.extract_error(data) {
                                let mut query_guard = current_query.lock().await;
                                if let Some((query, start_time, mut timeline)) = query_guard.take() {
                                    timeline.mark_backend_end();
                                    let duration = start_time.elapsed();
                                    warn!(query = %query, error = %error_msg, duration_ms = duration.as_millis(), "Query failed");

                                    let (builder, fingerprints) = Self::build_query_event(query, connection_id, database.clone(), anonymize);
                                    let event = builder
                                        .duration(duration)
                                        .success(false)
                                        .error(error_msg)
                                        .build();

                                    if let Err(e) = batcher.send_event(event) {
                                        warn!(error = %e, "Failed to send event to batcher");
                                    }

                                    // Record metrics
                                    metrics.record_query(&timeline, false);
                                    if !fingerprints.is_empty() {
                                        metrics.record_hot_data(&fingerprints);
                                    }
                                }
                            }
                            // Check for query completion
                            else if extractor.is_query_complete(data) {
                                let mut query_guard = current_query.lock().await;
                                if let Some((query, start_time, mut timeline)) = query_guard.take() {
                                    timeline.mark_backend_end();
                                    let duration = start_time.elapsed();
                                    debug!(query = %query, duration_ms = duration.as_millis(), "Query completed successfully");

                                    let (builder, fingerprints) = Self::build_query_event(query, connection_id, database.clone(), anonymize);
                                    let event = builder
                                        .duration(duration)
                                        .success(true)
                                        .build();

                                    if let Err(e) = batcher.send_event(event) {
                                        warn!(error = %e, "Failed to send event to batcher");
                                    }

                                    // Record metrics
                                    metrics.record_query(&timeline, true);
                                    if !fingerprints.is_empty() {
                                        metrics.record_hot_data(&fingerprints);
                                    }
                                }
                            }

                            // Forward to client
                            self.client_stream.write_all(data).await.context("Failed to write to client")?;
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to read from backend");
                            break;
                        }
                    }
                }
            }
        }

        info!("Connection handler completed (pooled)");
        Ok(())
    }

    /// Handle connection with an owned backend TCP stream
    async fn handle_with_owned_backend(mut self, mut backend_stream: TcpStream) -> Result<()> {
        // For owned connections, we can use split() as before
        let (mut client_read, mut client_write) = self.client_stream.split();
        let (mut backend_read, mut backend_write) = backend_stream.split();

        let connection_id = self.connection_id;
        let database = self.config.backend.database.clone();
        let batcher_clone = Arc::clone(&self.batcher);
        let config_clone = Arc::clone(&self.config);
        let anonymize = self.config.publisher.anonymize;
        let metrics = Arc::clone(&self.metrics);

        // Track query timing and timeline - shared between both async tasks
        let current_query: Arc<Mutex<Option<(String, Instant, QueryTimeline)>>> = Arc::new(Mutex::new(None));

        // Client -> Backend forwarding with query extraction
        let query_tracker = Arc::clone(&current_query);
        let client_to_backend = async move {
            let mut buffer = vec![0u8; config_clone.performance.buffer_size];
            let extractor = MessageExtractor::new();

            loop {
                match client_read.read(&mut buffer).await {
                    Ok(0) => {
                        debug!("Client closed connection");
                        break;
                    }
                    Ok(n) => {
                        let data = &buffer[..n];

                        // Try to extract query information
                        if let Some(query) = extractor.extract_query(data) {
                            debug!(query = %query, "Extracted query from client");
                            let mut timeline = QueryTimeline::new();
                            timeline.mark_backend_start(); // Start backend execution timing
                            *query_tracker.lock().await = Some((query, Instant::now(), timeline));
                        }

                        // Forward to backend
                        if let Err(e) = backend_write.write_all(data).await {
                            error!(error = %e, "Failed to write to backend");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to read from client");
                        break;
                    }
                }
            }
        };

        // Backend -> Client forwarding with response detection
        let query_tracker = Arc::clone(&current_query);
        let backend_to_client = async move {
            let mut buffer = vec![0u8; self.config.performance.buffer_size];
            let extractor = MessageExtractor::new();

            loop {
                match backend_read.read(&mut buffer).await {
                    Ok(0) => {
                        debug!("Backend closed connection");
                        break;
                    }
                    Ok(n) => {
                        let data = &buffer[..n];

                        // Check for error response first
                        if let Some(error_msg) = extractor.extract_error(data) {
                            let mut query_guard = query_tracker.lock().await;
                            if let Some((query, start_time, mut timeline)) = query_guard.take() {
                                timeline.mark_backend_end(); // Mark backend completion
                                let duration = start_time.elapsed();
                                warn!(
                                    query = %query,
                                    error = %error_msg,
                                    duration_ms = duration.as_millis(),
                                    "Query failed"
                                );

                                // Create and send error event
                                let (builder, fingerprints) = Self::build_query_event(
                                    query,
                                    connection_id,
                                    database.clone(),
                                    anonymize,
                                );
                                let event = builder
                                    .duration(duration)
                                    .success(false)
                                    .error(error_msg)
                                    .build();

                                if let Err(e) = batcher_clone.send_event(event) {
                                    warn!(error = %e, "Failed to send event to batcher");
                                }

                                // Record metrics
                                metrics.record_query(&timeline, false);
                                if !fingerprints.is_empty() {
                                    metrics.record_hot_data(&fingerprints);
                                }
                            }
                        }
                        // Check if this is a successful query completion
                        else if extractor.is_query_complete(data) {
                            let mut query_guard = query_tracker.lock().await;
                            if let Some((query, start_time, mut timeline)) = query_guard.take() {
                                timeline.mark_backend_end(); // Mark backend completion
                                let duration = start_time.elapsed();
                                debug!(
                                    query = %query,
                                    duration_ms = duration.as_millis(),
                                    "Query completed successfully"
                                );

                                // Create and send success event
                                let (builder, fingerprints) = Self::build_query_event(
                                    query,
                                    connection_id,
                                    database.clone(),
                                    anonymize,
                                );
                                let event = builder
                                    .duration(duration)
                                    .success(true)
                                    .build();

                                if let Err(e) = batcher_clone.send_event(event) {
                                    warn!(error = %e, "Failed to send event to batcher");
                                }

                                // Record metrics
                                metrics.record_query(&timeline, true);
                                if !fingerprints.is_empty() {
                                    metrics.record_hot_data(&fingerprints);
                                }
                            }
                        }

                        // Forward to client
                        if let Err(e) = client_write.write_all(data).await {
                            error!(error = %e, "Failed to write to client");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to read from backend");
                        break;
                    }
                }
            }
        };

        // Run both directions concurrently
        tokio::select! {
            _ = client_to_backend => {
                debug!("Client to backend forwarding completed");
            }
            _ = backend_to_client => {
                debug!("Backend to client forwarding completed");
            }
        }

        info!("Connection handler completed");
        Ok(())
    }
}
