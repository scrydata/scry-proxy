use super::{
    EventBatcher, TcpConnectionPool, PreparedStatementCache, PreparedStatement, PendingExecution,
    TransactionTracker, ConnectionState, ModeEnforcer, PoolingMode,
};
use crate::config::{Config, PoolingStrategy};
use crate::observability::{ProxyMetrics, QueryTimeline};
use crate::protocol::{MessageExtractor, QueryAnonymizer, Message, decode_params, CommandDetector, DetectedCommand};
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

    /// Convert PoolingStrategy from config to PoolingMode for enforcement
    fn pooling_mode(strategy: &PoolingStrategy) -> PoolingMode {
        match strategy {
            PoolingStrategy::Disabled | PoolingStrategy::Session => PoolingMode::Session,
            PoolingStrategy::Transaction => PoolingMode::Transaction,
            PoolingStrategy::Hybrid => PoolingMode::Hybrid,
        }
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

        // Transaction pooling tracking components
        let pooling_mode = Self::pooling_mode(&self.config.performance.connection_pooling);
        let mode_enforcer = ModeEnforcer::new(pooling_mode);
        let mut txn_tracker = TransactionTracker::new();
        let mut conn_state = ConnectionState::new(self.config.protocol.max_prepared_statements);

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
                            let mut should_forward = true;

                            // Process ALL protocol messages in buffer
                            for msg in extractor.extract_messages(data) {
                                match msg {
                                    Message::Parse { ref name, ref query, ref param_oids } => {
                                        // Validate command against pooling mode
                                        if let Err(err_msg) = mode_enforcer.validate(query, txn_tracker.is_in_transaction()) {
                                            warn!(query = %query, error = %err_msg, "Command rejected by pooling mode");
                                            let error_response = ModeEnforcer::build_error_response(&err_msg);
                                            self.client_stream.write_all(&error_response).await.context("Failed to send error to client")?;
                                            // Send ReadyForQuery to complete the error cycle
                                            let ready_for_query = Self::build_ready_for_query(txn_tracker.state());
                                            self.client_stream.write_all(&ready_for_query).await.context("Failed to send ReadyForQuery")?;
                                            should_forward = false;
                                            break;
                                        }

                                        debug!(name = %name, query = %query, "Cached prepared statement");
                                        stmt_cache.insert_statement(name.clone(), PreparedStatement {
                                            query: query.clone(),
                                            param_oids: param_oids.clone(),
                                        });
                                        // Set pending with empty params for Parse errors
                                        // Will be overwritten by Bind if Parse succeeds
                                        stmt_cache.set_pending(String::new(), PendingExecution {
                                            query: query.clone(),
                                            params: vec![],
                                            params_incomplete: true,
                                            started_at: Instant::now(),
                                        });

                                        // Track prepared statement in connection state (for pinning)
                                        //
                                        // KNOWN LIMITATION: State tracking happens before backend confirmation.
                                        // If Parse fails at the backend, we'll incorrectly think we have a prepared
                                        // statement that doesn't exist. This is safe but suboptimal - the connection
                                        // stays pinned when it doesn't need to be. The prepared statement will be
                                        // re-prepared on the next attempt, and the phantom entry doesn't cause issues.
                                        // A full fix would require tracking "pending" state changes and only applying
                                        // them when ParseComplete is received, but the complexity isn't justified
                                        // given the minimal practical impact.
                                        conn_state.add_prepared_statement(
                                            name.clone(),
                                            query.clone(),
                                            param_oids.clone(),
                                        );
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
                                    Message::Query { ref query } => {
                                        // Validate command against pooling mode
                                        if let Err(err_msg) = mode_enforcer.validate(query, txn_tracker.is_in_transaction()) {
                                            warn!(query = %query, error = %err_msg, "Command rejected by pooling mode");
                                            let error_response = ModeEnforcer::build_error_response(&err_msg);
                                            self.client_stream.write_all(&error_response).await.context("Failed to send error to client")?;
                                            // Send ReadyForQuery to complete the error cycle
                                            let ready_for_query = Self::build_ready_for_query(txn_tracker.state());
                                            self.client_stream.write_all(&ready_for_query).await.context("Failed to send ReadyForQuery")?;
                                            should_forward = false;
                                            break;
                                        }

                                        debug!(query = %query, "Simple query");
                                        stmt_cache.set_pending(String::new(), PendingExecution {
                                            query: query.clone(),
                                            params: vec![],
                                            params_incomplete: false,
                                            started_at: Instant::now(),
                                        });

                                        // Update connection state based on detected command
                                        //
                                        // KNOWN LIMITATION: State tracking happens before backend confirmation.
                                        // If a SET/CREATE TEMP TABLE/etc. command fails at the backend, we'll
                                        // incorrectly track state that doesn't exist. This results in conservative
                                        // behavior - the connection stays pinned when it might not need to be.
                                        // This is safe (no data corruption or incorrect behavior) but suboptimal.
                                        // A full fix would require tracking "pending" state and only applying
                                        // changes on CommandComplete, discarding on ErrorResponse. Given the
                                        // minimal practical impact, we accept this limitation.
                                        Self::update_connection_state(&mut conn_state, query);
                                    }
                                    Message::Close { kind, ref name } => {
                                        match kind {
                                            'S' => {
                                                stmt_cache.remove_statement(name);
                                                conn_state.remove_prepared_statement(name);
                                            }
                                            'P' => stmt_cache.clear_pending(name),
                                            _ => {}
                                        }
                                    }
                                    _ => {}
                                }
                            }

                            // Forward to backend (using DerefMut to get &mut TcpStream) if not rejected
                            if should_forward {
                                backend_conn.write_all(data).await.context("Failed to write to backend")?;
                            }
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

                            // Track transaction state from ReadyForQuery messages
                            if let Some(status) = extractor.extract_ready_for_query(data) {
                                let was_in_transaction = txn_tracker.is_in_transaction();
                                txn_tracker.update_from_ready_for_query(status);

                                // Log transaction boundary for debugging
                                if was_in_transaction && txn_tracker.is_idle() {
                                    debug!(
                                        connection_id = connection_id,
                                        is_pinned = conn_state.is_pinned(),
                                        has_unsafe_state = conn_state.has_unsafe_state(),
                                        "Transaction ended - connection could be released"
                                    );
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

        info!(
            connection_id = connection_id,
            is_pinned = conn_state.is_pinned(),
            has_unsafe_state = conn_state.has_unsafe_state(),
            "Connection handler completed (pooled)"
        );
        Ok(())
    }

    /// Build a ReadyForQuery message with the given transaction state
    fn build_ready_for_query(state: super::TransactionState) -> Vec<u8> {
        let status = match state {
            super::TransactionState::Idle => b'I',
            super::TransactionState::InTransaction => b'T',
            super::TransactionState::InError => b'E',
        };
        // ReadyForQuery: 'Z' + length(5) + status
        vec![b'Z', 0, 0, 0, 5, status]
    }

    /// Update connection state based on detected SQL command
    fn update_connection_state(conn_state: &mut ConnectionState, query: &str) {
        if let Some(cmd) = CommandDetector::detect(query) {
            match cmd {
                DetectedCommand::Set { name, value } => {
                    conn_state.add_session_variable(name, value);
                }
                DetectedCommand::Reset { name } => {
                    conn_state.remove_session_variable(&name);
                }
                DetectedCommand::ResetAll => {
                    conn_state.clear_session_variables();
                }
                DetectedCommand::CreateTempTable { name } => {
                    conn_state.add_temp_table(name);
                }
                DetectedCommand::DropTable { name } => {
                    // Only remove if it's a known temp table
                    conn_state.remove_temp_table(&name);
                }
                DetectedCommand::DeclareCursor { name, .. } => {
                    conn_state.add_cursor(name);
                }
                DetectedCommand::CloseCursor { name } => {
                    conn_state.remove_cursor(&name);
                }
                DetectedCommand::AdvisoryLock { key } => {
                    if let Some(k) = key {
                        conn_state.add_advisory_lock(k);
                    }
                }
                DetectedCommand::AdvisoryUnlock { key } => {
                    if let Some(k) = key {
                        conn_state.remove_advisory_lock(k);
                    }
                }
                DetectedCommand::DiscardAll => {
                    conn_state.clear_all();
                }
                DetectedCommand::Deallocate { name } => {
                    conn_state.remove_prepared_statement(&name);
                }
                DetectedCommand::DeallocateAll => {
                    conn_state.clear_prepared_statements();
                }
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PoolingStrategy;

    #[test]
    fn test_pooling_mode_conversion_disabled() {
        let mode = ConnectionHandler::pooling_mode(&PoolingStrategy::Disabled);
        assert_eq!(mode, PoolingMode::Session);
    }

    #[test]
    fn test_pooling_mode_conversion_session() {
        let mode = ConnectionHandler::pooling_mode(&PoolingStrategy::Session);
        assert_eq!(mode, PoolingMode::Session);
    }

    #[test]
    fn test_pooling_mode_conversion_transaction() {
        let mode = ConnectionHandler::pooling_mode(&PoolingStrategy::Transaction);
        assert_eq!(mode, PoolingMode::Transaction);
    }

    #[test]
    fn test_pooling_mode_conversion_hybrid() {
        let mode = ConnectionHandler::pooling_mode(&PoolingStrategy::Hybrid);
        assert_eq!(mode, PoolingMode::Hybrid);
    }

    #[test]
    fn test_build_ready_for_query_idle() {
        let msg = ConnectionHandler::build_ready_for_query(super::super::TransactionState::Idle);
        assert_eq!(msg, vec![b'Z', 0, 0, 0, 5, b'I']);
    }

    #[test]
    fn test_build_ready_for_query_in_transaction() {
        let msg = ConnectionHandler::build_ready_for_query(super::super::TransactionState::InTransaction);
        assert_eq!(msg, vec![b'Z', 0, 0, 0, 5, b'T']);
    }

    #[test]
    fn test_build_ready_for_query_in_error() {
        let msg = ConnectionHandler::build_ready_for_query(super::super::TransactionState::InError);
        assert_eq!(msg, vec![b'Z', 0, 0, 0, 5, b'E']);
    }

    #[test]
    fn test_update_connection_state_set() {
        let mut conn_state = ConnectionState::new(100);
        ConnectionHandler::update_connection_state(&mut conn_state, "SET timezone = 'UTC'");
        assert!(conn_state.is_pinned());
    }

    #[test]
    fn test_update_connection_state_reset() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_session_variable("timezone".to_string(), "UTC".to_string());
        assert!(conn_state.is_pinned());

        ConnectionHandler::update_connection_state(&mut conn_state, "RESET timezone");
        assert!(!conn_state.is_pinned());
    }

    #[test]
    fn test_update_connection_state_reset_all() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_session_variable("timezone".to_string(), "UTC".to_string());
        conn_state.add_session_variable("search_path".to_string(), "public".to_string());
        assert!(conn_state.is_pinned());

        ConnectionHandler::update_connection_state(&mut conn_state, "RESET ALL");
        assert!(!conn_state.is_pinned());
    }

    #[test]
    fn test_update_connection_state_create_temp_table() {
        let mut conn_state = ConnectionState::new(100);
        ConnectionHandler::update_connection_state(&mut conn_state, "CREATE TEMP TABLE tmp_users (id int)");
        assert!(conn_state.is_pinned());
        assert!(conn_state.has_unsafe_state());
    }

    #[test]
    fn test_update_connection_state_declare_cursor() {
        let mut conn_state = ConnectionState::new(100);
        ConnectionHandler::update_connection_state(&mut conn_state, "DECLARE my_cursor CURSOR FOR SELECT 1");
        assert!(conn_state.is_pinned());
        assert!(conn_state.has_unsafe_state());
    }

    #[test]
    fn test_update_connection_state_close_cursor() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_cursor("my_cursor".to_string());
        assert!(conn_state.has_unsafe_state());

        ConnectionHandler::update_connection_state(&mut conn_state, "CLOSE my_cursor");
        assert!(!conn_state.has_unsafe_state());
    }

    #[test]
    fn test_update_connection_state_advisory_lock() {
        let mut conn_state = ConnectionState::new(100);
        ConnectionHandler::update_connection_state(&mut conn_state, "SELECT pg_advisory_lock(12345)");
        assert!(conn_state.is_pinned());
        assert!(conn_state.has_unsafe_state());
    }

    #[test]
    fn test_update_connection_state_advisory_unlock() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_advisory_lock(12345);
        assert!(conn_state.has_unsafe_state());

        ConnectionHandler::update_connection_state(&mut conn_state, "SELECT pg_advisory_unlock(12345)");
        assert!(!conn_state.has_unsafe_state());
    }

    #[test]
    fn test_update_connection_state_discard_all() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_session_variable("tz".to_string(), "UTC".to_string());
        conn_state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        conn_state.add_temp_table("tmp".to_string());
        assert!(conn_state.is_pinned());
        assert!(conn_state.has_unsafe_state());

        ConnectionHandler::update_connection_state(&mut conn_state, "DISCARD ALL");
        assert!(!conn_state.is_pinned());
        assert!(!conn_state.has_unsafe_state());
    }

    #[test]
    fn test_update_connection_state_deallocate() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        assert!(conn_state.is_pinned());

        ConnectionHandler::update_connection_state(&mut conn_state, "DEALLOCATE stmt1");
        assert!(!conn_state.is_pinned());
    }

    #[test]
    fn test_update_connection_state_deallocate_all() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        conn_state.add_prepared_statement("stmt2".to_string(), "SELECT 2".to_string(), vec![]);
        assert!(conn_state.is_pinned());

        ConnectionHandler::update_connection_state(&mut conn_state, "DEALLOCATE ALL");
        assert!(!conn_state.is_pinned());
    }

    #[test]
    fn test_update_connection_state_regular_query_no_effect() {
        let mut conn_state = ConnectionState::new(100);
        ConnectionHandler::update_connection_state(&mut conn_state, "SELECT * FROM users");
        assert!(!conn_state.is_pinned());
    }

    #[test]
    fn test_update_connection_state_drop_table() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_temp_table("tmp_users".to_string());
        assert!(conn_state.has_unsafe_state());

        ConnectionHandler::update_connection_state(&mut conn_state, "DROP TABLE tmp_users");
        assert!(!conn_state.has_unsafe_state());
    }
}
