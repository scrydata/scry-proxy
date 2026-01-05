use super::{EventBatcher, TcpConnectionPool, PreparedStatementCache, PreparedStatement, PendingExecution};
use crate::config::Config;
use crate::observability::{ProxyMetrics, QueryTimeline};
use crate::protocol::{MessageExtractor, QueryAnonymizer, Message, decode_params};
use crate::publisher::QueryEventBuilder;
use anyhow::{Context, Result};
use scry_protocol::ParamValue;
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
        let mut stmt_cache = PreparedStatementCache::new(self.config.protocol.max_prepared_statements);

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

                            // Process ALL protocol messages in buffer
                            for msg in extractor.extract_messages(data) {
                                match msg {
                                    Message::Parse { name, query, param_oids } => {
                                        debug!(name = %name, query = %query, "Cached prepared statement");
                                        stmt_cache.insert_statement(name.clone(), PreparedStatement {
                                            query: query.clone(),
                                            param_oids,
                                        });
                                        // Set pending with empty params for Parse errors
                                        // Will be overwritten by Bind if Parse succeeds
                                        stmt_cache.set_pending(String::new(), PendingExecution {
                                            query,
                                            params: vec![],
                                            params_incomplete: true,
                                            started_at: Instant::now(),
                                        });
                                    }
                                    Message::Bind { portal, statement, format_codes, params_raw } => {
                                        let (query, params, incomplete) = match stmt_cache.get_statement(&statement) {
                                            Some(stmt) => {
                                                let params = decode_params(&params_raw, &format_codes, &stmt.param_oids);
                                                (stmt.query.clone(), params, false)
                                            }
                                            None => {
                                                warn!(statement = %statement, "Statement not in cache");
                                                let params: Vec<ParamValue> = params_raw.iter()
                                                    .map(|p| match p {
                                                        Some(data) => ParamValue::Unknown { oid: 0, data: data.clone() },
                                                        None => ParamValue::Null,
                                                    })
                                                    .collect();
                                                (format!("[unknown: {}]", statement), params, true)
                                            }
                                        };

                                        stmt_cache.set_pending(portal, PendingExecution {
                                            query,
                                            params,
                                            params_incomplete: incomplete,
                                            started_at: Instant::now(),
                                        });
                                    }
                                    Message::Query { query } => {
                                        debug!(query = %query, "Simple query");
                                        stmt_cache.set_pending(String::new(), PendingExecution {
                                            query,
                                            params: vec![],
                                            params_incomplete: false,
                                            started_at: Instant::now(),
                                        });
                                    }
                                    Message::Close { kind, name } => {
                                        match kind {
                                            'S' => stmt_cache.remove_statement(&name),
                                            'P' => stmt_cache.clear_pending(&name),
                                            _ => {}
                                        }
                                    }
                                    _ => {}
                                }
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
                                // Try unnamed portal first (simple query), then named
                                if let Some(pending) = stmt_cache.take_pending("") {
                                    let duration = pending.started_at.elapsed();
                                    warn!(query = %pending.query, error = %error_msg, duration_ms = duration.as_millis(), "Query failed");

                                    let (builder, fingerprints) = Self::build_query_event(pending.query, connection_id, database.clone(), anonymize);
                                    let event = builder
                                        .params(pending.params)
                                        .params_incomplete(pending.params_incomplete)
                                        .duration(duration)
                                        .success(false)
                                        .error(error_msg)
                                        .build();

                                    if let Err(e) = batcher.send_event(event) {
                                        warn!(error = %e, "Failed to send event to batcher");
                                    }

                                    metrics.record_query(&QueryTimeline::new(), false);
                                    if !fingerprints.is_empty() {
                                        metrics.record_hot_data(&fingerprints);
                                    }
                                }
                            }
                            // Check for query completion
                            else if extractor.is_query_complete(data) {
                                if let Some(pending) = stmt_cache.take_pending("") {
                                    let duration = pending.started_at.elapsed();
                                    debug!(query = %pending.query, duration_ms = duration.as_millis(), "Query completed successfully");

                                    let (builder, fingerprints) = Self::build_query_event(pending.query, connection_id, database.clone(), anonymize);
                                    let event = builder
                                        .params(pending.params)
                                        .params_incomplete(pending.params_incomplete)
                                        .duration(duration)
                                        .success(true)
                                        .build();

                                    if let Err(e) = batcher.send_event(event) {
                                        warn!(error = %e, "Failed to send event to batcher");
                                    }

                                    metrics.record_query(&QueryTimeline::new(), true);
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
        let max_stmts = self.config.protocol.max_prepared_statements;

        // Shared prepared statement cache between both async tasks
        let stmt_cache: Arc<Mutex<PreparedStatementCache>> = Arc::new(Mutex::new(PreparedStatementCache::new(max_stmts)));

        // Client -> Backend forwarding with message extraction
        let cache_writer = Arc::clone(&stmt_cache);
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

                        // Process ALL protocol messages in buffer
                        let messages = extractor.extract_messages(data);
                        if !messages.is_empty() {
                            let mut cache = cache_writer.lock().await;
                            for msg in messages {
                                match msg {
                                    Message::Parse { name, query, param_oids } => {
                                        debug!(name = %name, query = %query, "Cached prepared statement");
                                        cache.insert_statement(name.clone(), PreparedStatement {
                                            query: query.clone(),
                                            param_oids,
                                        });
                                        // Set pending with empty params for Parse errors
                                        // Will be overwritten by Bind if Parse succeeds
                                        cache.set_pending(String::new(), PendingExecution {
                                            query,
                                            params: vec![],
                                            params_incomplete: true,
                                            started_at: Instant::now(),
                                        });
                                    }
                                    Message::Bind { portal, statement, format_codes, params_raw } => {
                                        let (query, params, incomplete) = match cache.get_statement(&statement) {
                                            Some(stmt) => {
                                                let params = decode_params(&params_raw, &format_codes, &stmt.param_oids);
                                                (stmt.query.clone(), params, false)
                                            }
                                            None => {
                                                warn!(statement = %statement, "Statement not in cache");
                                                let params: Vec<ParamValue> = params_raw.iter()
                                                    .map(|p| match p {
                                                        Some(data) => ParamValue::Unknown { oid: 0, data: data.clone() },
                                                        None => ParamValue::Null,
                                                    })
                                                    .collect();
                                                (format!("[unknown: {}]", statement), params, true)
                                            }
                                        };

                                        cache.set_pending(portal, PendingExecution {
                                            query,
                                            params,
                                            params_incomplete: incomplete,
                                            started_at: Instant::now(),
                                        });
                                    }
                                    Message::Query { query } => {
                                        debug!(query = %query, "Simple query");
                                        cache.set_pending(String::new(), PendingExecution {
                                            query,
                                            params: vec![],
                                            params_incomplete: false,
                                            started_at: Instant::now(),
                                        });
                                    }
                                    Message::Close { kind, name } => {
                                        match kind {
                                            'S' => cache.remove_statement(&name),
                                            'P' => cache.clear_pending(&name),
                                            _ => {}
                                        }
                                    }
                                    _ => {}
                                }
                            }
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
        let cache_reader = Arc::clone(&stmt_cache);
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
                            let mut cache = cache_reader.lock().await;
                            if let Some(pending) = cache.take_pending("") {
                                let duration = pending.started_at.elapsed();
                                warn!(
                                    query = %pending.query,
                                    error = %error_msg,
                                    duration_ms = duration.as_millis(),
                                    "Query failed"
                                );

                                let (builder, fingerprints) = Self::build_query_event(
                                    pending.query,
                                    connection_id,
                                    database.clone(),
                                    anonymize,
                                );
                                let event = builder
                                    .params(pending.params)
                                    .params_incomplete(pending.params_incomplete)
                                    .duration(duration)
                                    .success(false)
                                    .error(error_msg)
                                    .build();

                                if let Err(e) = batcher_clone.send_event(event) {
                                    warn!(error = %e, "Failed to send event to batcher");
                                }

                                metrics.record_query(&QueryTimeline::new(), false);
                                if !fingerprints.is_empty() {
                                    metrics.record_hot_data(&fingerprints);
                                }
                            }
                        }
                        // Check if this is a successful query completion
                        else if extractor.is_query_complete(data) {
                            let mut cache = cache_reader.lock().await;
                            if let Some(pending) = cache.take_pending("") {
                                let duration = pending.started_at.elapsed();
                                debug!(
                                    query = %pending.query,
                                    duration_ms = duration.as_millis(),
                                    "Query completed successfully"
                                );

                                let (builder, fingerprints) = Self::build_query_event(
                                    pending.query,
                                    connection_id,
                                    database.clone(),
                                    anonymize,
                                );
                                let event = builder
                                    .params(pending.params)
                                    .params_incomplete(pending.params_incomplete)
                                    .duration(duration)
                                    .success(true)
                                    .build();

                                if let Err(e) = batcher_clone.send_event(event) {
                                    warn!(error = %e, "Failed to send event to batcher");
                                }

                                metrics.record_query(&QueryTimeline::new(), true);
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
