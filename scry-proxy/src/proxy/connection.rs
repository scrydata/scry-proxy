use super::{
    AcquireError, ConnectionState, EventBatcher, ModeEnforcer, PendingExecution, PoolManager,
    PoolingMode, PreparedStatement, PreparedStatementCache, StateReplayer, TransactionTracker,
};
use crate::auth::{Authenticator, FileAuthenticator};
use crate::config::{BackpressureMode, Config, ParseFailureMode, PoolingStrategy};
use crate::observability::{ProxyMetrics, QueryTimeline};
use crate::protocol::{decode_params, Message, MessageExtractor, QueryAnonymizer};
use crate::publisher::{QueryEvent, QueryEventBuilder};
use crate::tls::ClientTransport;
use anyhow::{Context, Result};
use parking_lot::Mutex;
use scry_protocol::ParamValue;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, instrument, warn};

/// Placeholder substituted for a query the anonymizer could not parse when
/// `ParseFailureMode::Redact` is in effect. Fixed and value-free.
const REDACTED_QUERY: &str = "<redacted: unparseable>";

/// Resolved anonymization policy for a connection.
///
/// Centralizes every privacy-sensitive transform (P1 §4.4) so that no
/// event-construction site can accidentally ship raw query text, raw
/// parameter values, or a literal-echoing error message:
/// - the event `query` is the normalized (never raw) form when enabled;
/// - a query the parser rejects is dropped or hard-redacted per
///   [`ParseFailureMode`], never shipped raw;
/// - parameters are replaced with type-only shapes;
/// - the error field is scrubbed to severity + SQLSTATE.
#[derive(Clone)]
struct AnonymizationSettings {
    enabled: bool,
    anonymizer: Arc<QueryAnonymizer>,
    parse_failure: ParseFailureMode,
}

impl AnonymizationSettings {
    fn from_config(config: &Config) -> Self {
        // `Config::validate()` guarantees a salt is present when `anonymize` is
        // enabled; fall back to the default only for the disabled path.
        let anonymizer = match &config.publisher.anonymize_salt {
            Some(salt) => QueryAnonymizer::with_salt(salt.clone().into_bytes()),
            None => QueryAnonymizer::new(),
        };
        Self {
            enabled: config.publisher.anonymize,
            anonymizer: Arc::new(anonymizer),
            parse_failure: config.publisher.parse_failure_mode.clone(),
        }
    }

    /// Choose the error field for an event: the scrubbed severity+SQLSTATE form
    /// when anonymizing, otherwise the full message (consistent with shipping
    /// the raw query when anonymization is off).
    fn error_field(
        &self,
        extractor: &MessageExtractor,
        data: &[u8],
        full_error: String,
    ) -> Option<String> {
        if self.enabled {
            extractor.extract_error_scrubbed(data)
        } else {
            Some(full_error)
        }
    }

    /// Replace parameter values with type-only shapes when anonymizing, so no
    /// raw literal (PII) is ever published. When disabled, params pass through.
    fn redact_params(&self, params: Vec<ParamValue>) -> Vec<ParamValue> {
        if !self.enabled {
            return params;
        }
        params.iter().map(redact_param).collect()
    }

    /// Build a fully-formed [`QueryEvent`] under the anonymization policy.
    ///
    /// Returns `None` when the event must be dropped entirely
    /// (`ParseFailureMode::Drop` on a query the parser rejected). The returned
    /// fingerprints are for hot-data metrics.
    #[allow(clippy::too_many_arguments)]
    fn build_event(
        &self,
        query: &str,
        params: Vec<ParamValue>,
        params_incomplete: bool,
        duration: Duration,
        success: bool,
        error: Option<String>,
        connection_id: &str,
        database: &str,
    ) -> Option<(QueryEvent, Vec<String>)> {
        let (query_text, normalized, fingerprints) = if !self.enabled {
            // Anonymization disabled: raw query is expected behavior.
            (query.to_string(), None, Vec::new())
        } else {
            match self.anonymizer.anonymize(query) {
                Some(anon) => {
                    // Never ship raw: the event query IS the normalized form.
                    (
                        anon.normalized_query.clone(),
                        Some(anon.normalized_query),
                        anon.value_fingerprints,
                    )
                }
                None => match self.parse_failure {
                    // Fail closed: a query we cannot parse is never shipped raw.
                    ParseFailureMode::Drop => return None,
                    ParseFailureMode::Redact => {
                        (REDACTED_QUERY.to_string(), Some(REDACTED_QUERY.to_string()), Vec::new())
                    }
                },
            }
        };

        let mut builder = QueryEventBuilder::new(query_text)
            .connection_id(connection_id)
            .database(database)
            .params(self.redact_params(params))
            .params_incomplete(params_incomplete)
            .duration(duration)
            .success(success);
        if let Some(nq) = normalized {
            builder = builder.normalized_query(nq);
        }
        if !fingerprints.is_empty() {
            builder = builder.value_fingerprints(fingerprints.clone());
        }
        if let Some(err) = error {
            builder = builder.error(err);
        }
        Some((builder.build(), fingerprints))
    }
}

/// Replace a single parameter value with a type-preserving, value-free shape.
///
/// Keeps the variant (and OID for `Unknown`) so downstream analytics still see
/// the parameter's type, but strips every value so no literal/PII leaks.
/// Recurses into composite/array/range shapes.
fn redact_param(p: &ParamValue) -> ParamValue {
    match p {
        ParamValue::Null => ParamValue::Null,
        ParamValue::Bool(_) => ParamValue::Bool(false),
        ParamValue::Int16(_) => ParamValue::Int16(0),
        ParamValue::Int32(_) => ParamValue::Int32(0),
        ParamValue::Int64(_) => ParamValue::Int64(0),
        ParamValue::Float32(_) => ParamValue::Float32(0.0),
        ParamValue::Float64(_) => ParamValue::Float64(0.0),
        ParamValue::Numeric(_) => ParamValue::Numeric(String::new()),
        ParamValue::Text(_) => ParamValue::Text(String::new()),
        ParamValue::Bytes(_) => ParamValue::Bytes(Vec::new()),
        ParamValue::Date(_) => ParamValue::Date(0),
        ParamValue::Time(_) => ParamValue::Time(0),
        ParamValue::Timestamp(_) => ParamValue::Timestamp(0),
        ParamValue::TimestampTz(_) => ParamValue::TimestampTz(0),
        ParamValue::Interval { .. } => ParamValue::Interval { months: 0, days: 0, microseconds: 0 },
        ParamValue::Uuid(_) => ParamValue::Uuid([0u8; 16]),
        ParamValue::Json(_) => ParamValue::Json(String::new()),
        ParamValue::Array { elements, dimensions } => ParamValue::Array {
            elements: elements.iter().map(redact_param).collect(),
            dimensions: dimensions.clone(),
        },
        ParamValue::Range { lower, upper, lower_inc, upper_inc } => ParamValue::Range {
            lower: lower.as_ref().map(|b| Box::new(redact_param(b))),
            upper: upper.as_ref().map(|b| Box::new(redact_param(b))),
            lower_inc: *lower_inc,
            upper_inc: *upper_inc,
        },
        ParamValue::Composite { fields } => {
            ParamValue::Composite { fields: fields.iter().map(redact_param).collect() }
        }
        // Preserve the OID (type identity) but drop the raw payload bytes.
        ParamValue::Unknown { oid, .. } => ParamValue::Unknown { oid: *oid, data: Vec::new() },
    }
}

/// Handles a single client connection, forwarding messages to/from the backend
pub struct ConnectionHandler {
    client_stream: ClientTransport,
    client_addr: SocketAddr,
    connection_id: u64,
    config: Arc<Config>,
    batcher: Arc<EventBatcher>,
    pool_manager: Option<Arc<PoolManager>>,
    metrics: Arc<ProxyMetrics>,
    startup_data: Vec<u8>,
    authenticator: Option<Arc<FileAuthenticator>>,
}

impl ConnectionHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_stream: ClientTransport,
        client_addr: SocketAddr,
        connection_id: u64,
        config: Arc<Config>,
        batcher: Arc<EventBatcher>,
        pool_manager: Option<Arc<PoolManager>>,
        metrics: Arc<ProxyMetrics>,
        startup_data: Vec<u8>,
        authenticator: Option<Arc<FileAuthenticator>>,
    ) -> Self {
        Self {
            client_stream,
            client_addr,
            connection_id,
            config,
            batcher,
            pool_manager,
            metrics,
            startup_data,
            authenticator,
        }
    }

    /// Convert PoolingStrategy from config to PoolingMode for enforcement
    fn pooling_mode(strategy: &PoolingStrategy) -> PoolingMode {
        match strategy {
            PoolingStrategy::Disabled | PoolingStrategy::Session => PoolingMode::Session,
            PoolingStrategy::Transaction => PoolingMode::Transaction,
            PoolingStrategy::Hybrid => PoolingMode::Hybrid,
        }
    }

    /// Determine if connection should be released after transaction
    ///
    /// Returns true if the connection should be released back to the pool,
    /// allowing other clients to use it. The decision depends on:
    /// - Pooling strategy (transaction mode always releases, session mode never does)
    /// - Connection state (pinned connections with unsafe state are not released)
    fn should_release_connection(strategy: &PoolingStrategy, conn_state: &ConnectionState) -> bool {
        match strategy {
            // Disabled or Session mode: never release until client disconnects
            PoolingStrategy::Disabled | PoolingStrategy::Session => false,
            // Transaction mode: always release after transaction (strict mode)
            PoolingStrategy::Transaction => true,
            // Hybrid mode: release only if connection is not pinned
            PoolingStrategy::Hybrid => !conn_state.is_pinned(),
        }
    }

    // Query-event construction now lives on `AnonymizationSettings`
    // (see below): it resolves the query text, params, and error under the
    // fail-closed anonymization policy so no call site can accidentally ship
    // raw data.

    /// Build PostgreSQL ErrorResponse for queue full condition
    ///
    /// Returns a properly formatted PostgreSQL wire protocol error message
    /// with SQLSTATE 53300 (too_many_connections) and a retry hint.
    fn build_queue_full_error(retry_hint_ms: u64) -> Vec<u8> {
        let mut response = Vec::new();
        response.push(b'E'); // ErrorResponse

        let mut fields = Vec::new();

        // Severity (S)
        fields.push(b'S');
        fields.extend_from_slice(b"ERROR");
        fields.push(0);

        // SQLSTATE (C) - 53300 = too_many_connections
        fields.push(b'C');
        fields.extend_from_slice(b"53300");
        fields.push(0);

        // Message (M)
        fields.push(b'M');
        fields.extend_from_slice(b"connection pool queue is full");
        fields.push(0);

        // Hint (H) with retry suggestion
        fields.push(b'H');
        let hint = format!("Server is under load. Please retry in {}ms.", retry_hint_ms);
        fields.extend_from_slice(hint.as_bytes());
        fields.push(0);

        // Terminator
        fields.push(0);

        // Length (including self, 4 bytes)
        let length = (fields.len() + 4) as i32;
        response.extend_from_slice(&length.to_be_bytes());
        response.extend_from_slice(&fields);

        response
    }

    /// Build PostgreSQL ErrorResponse for wait timeout condition
    fn build_wait_timeout_error() -> Vec<u8> {
        let mut response = Vec::new();
        response.push(b'E'); // ErrorResponse

        let mut fields = Vec::new();

        // Severity (S)
        fields.push(b'S');
        fields.extend_from_slice(b"ERROR");
        fields.push(0);

        // SQLSTATE (C) - 53300 = too_many_connections
        fields.push(b'C');
        fields.extend_from_slice(b"53300");
        fields.push(0);

        // Message (M)
        fields.push(b'M');
        fields.extend_from_slice(b"timeout waiting for connection from pool");
        fields.push(0);

        // Hint (H)
        fields.push(b'H');
        fields.extend_from_slice(b"Server is under load. Please retry later.");
        fields.push(0);

        // Terminator
        fields.push(0);

        // Length (including self, 4 bytes)
        let length = (fields.len() + 4) as i32;
        response.extend_from_slice(&length.to_be_bytes());
        response.extend_from_slice(&fields);

        response
    }

    /// Handle the connection, forwarding messages until completion
    #[instrument(skip(self), fields(connection_id = self.connection_id, client_addr = %self.client_addr))]
    pub async fn handle(mut self) -> Result<()> {
        info!("Starting connection handler");

        // Get backend connection - either from pool manager or create direct connection
        let backend_addr = format!("{}:{}", self.config.backend.host, self.config.backend.port);

        // Try to get connection from pool manager if available
        if let Some(pool_manager) = self.pool_manager.clone() {
            info!(
                backend_addr = %backend_addr,
                connection_id = self.connection_id,
                "Acquiring backend connection from PoolManager"
            );

            // Check if we need sticky connection (e.g., client has prior state)
            let needs_sticky = pool_manager.has_sticky(self.connection_id);

            let managed_conn = match pool_manager.acquire(self.connection_id, needs_sticky).await {
                Ok(conn) => conn,
                Err(e) => {
                    // Handle pool acquire errors with proper backpressure behavior
                    return self.handle_acquire_error(e).await;
                }
            };

            info!(
                backend_addr = %backend_addr,
                is_pinned = managed_conn.is_pinned(),
                "Using managed connection"
            );

            // Use managed connection with proper pool integration
            return self.handle_with_managed_connection(managed_conn, &pool_manager).await;
        } else {
            info!(backend_addr = %backend_addr, "Creating direct backend connection");
            let backend_stream =
                TcpStream::connect(&backend_addr).await.context("Failed to connect to backend")?;

            // Disable Nagle's algorithm for lower latency
            backend_stream
                .set_nodelay(true)
                .context("Failed to set TCP_NODELAY on backend connection")?;

            // Use direct connection
            return self.handle_with_owned_backend(backend_stream).await;
        }
    }

    /// Handle pool acquire errors with proper backpressure behavior
    ///
    /// Depending on the configured backpressure mode, this method will:
    /// - RejectImmediate: Close the connection silently
    /// - RetryHint: Send a PostgreSQL error with retry suggestion
    /// - LogAndReject: Log the rejection and close the connection
    async fn handle_acquire_error(&mut self, error: AcquireError) -> Result<()> {
        // Record rejection metric
        self.metrics.pool_metrics().record_queue_rejected();

        let backpressure_mode = &self.config.performance.pool_backpressure_mode;
        let retry_hint_ms = self.config.performance.pool_retry_hint_ms;

        match error {
            AcquireError::QueueFull(_) => match backpressure_mode {
                BackpressureMode::RejectImmediate => {
                    debug!(
                        connection_id = self.connection_id,
                        "Queue full, rejecting connection (reject_immediate mode)"
                    );
                }
                BackpressureMode::RetryHint => {
                    debug!(
                        connection_id = self.connection_id,
                        retry_hint_ms = retry_hint_ms,
                        "Queue full, sending error with retry hint"
                    );
                    let error_msg = Self::build_queue_full_error(retry_hint_ms);
                    let _ = self.client_stream.write_all(&error_msg).await;
                }
                BackpressureMode::LogAndReject => {
                    warn!(
                        connection_id = self.connection_id,
                        "Connection rejected: pool queue full"
                    );
                }
            },
            AcquireError::WaitTimeout => match backpressure_mode {
                BackpressureMode::RejectImmediate => {
                    debug!(
                        connection_id = self.connection_id,
                        "Wait timeout, rejecting connection"
                    );
                }
                BackpressureMode::RetryHint => {
                    debug!(
                        connection_id = self.connection_id,
                        "Wait timeout, sending error with retry hint"
                    );
                    let error_msg = Self::build_wait_timeout_error();
                    let _ = self.client_stream.write_all(&error_msg).await;
                }
                BackpressureMode::LogAndReject => {
                    warn!(
                        connection_id = self.connection_id,
                        "Connection rejected: timeout waiting for pool connection"
                    );
                }
            },
            AcquireError::PoolError(e) => {
                // Pool errors are unexpected, always log them
                error!(
                    connection_id = self.connection_id,
                    error = %e,
                    "Pool error while acquiring connection"
                );
                return Err(e.context("Failed to acquire connection from pool"));
            }
        }

        // For queue full and wait timeout, return Ok to close connection gracefully
        Ok(())
    }

    /// Perform the startup/authentication handshake
    ///
    /// This method:
    /// 1. Authenticates the client (if auth enabled)
    /// 2. Forwards the startup message to the backend (with backend credentials)
    /// 3. Handles backend authentication (MD5, SCRAM, etc.)
    /// 4. Forwards remaining startup messages to the client
    ///
    /// After this completes, the connection is ready for query forwarding.
    async fn perform_startup_handshake(
        &mut self,
        backend_stream: &mut (impl AsyncWriteExt + AsyncReadExt + Unpin),
    ) -> Result<()> {
        let connection_id = self.connection_id;
        debug!(connection_id, "Starting handshake");

        // Create authenticator for client auth
        let authenticator =
            Authenticator::new(Arc::clone(&self.config), self.authenticator.clone());

        // Perform client authentication and get startup bytes for backend
        debug!(
            connection_id,
            startup_data_len = self.startup_data.len(),
            "Starting client authentication"
        );
        let auth_result = authenticator
            .authenticate(&mut self.client_stream, &self.startup_data)
            .await
            .context("Client authentication failed")?;
        debug!(connection_id, username = %auth_result.username, "Client authenticated");

        info!(
            connection_id = connection_id,
            username = %auth_result.username,
            database = ?auth_result.database,
            "Client authenticated successfully"
        );

        // Forward startup to backend
        debug!(
            connection_id,
            bytes = auth_result.startup_bytes.len(),
            "Forwarding startup to backend"
        );
        backend_stream
            .write_all(&auth_result.startup_bytes)
            .await
            .context("Failed to forward startup to backend")?;

        // Handle backend authentication using BackendAuthenticator
        let backend_auth = crate::auth::BackendAuthenticator::new(
            self.config.backend.user.clone(),
            self.config.backend.password.clone(),
        );

        debug!(connection_id, "Handling backend authentication");
        let remaining_data = backend_auth
            .authenticate(backend_stream, &[])
            .await
            .context("Backend authentication failed")?;

        // Forward AuthenticationOk to client
        let auth_ok = crate::protocol::build_auth_ok();
        self.client_stream
            .write_all(&auth_ok)
            .await
            .context("Failed to send AuthenticationOk to client")?;

        // Forward any remaining data and continue reading until ReadyForQuery
        let mut pending = remaining_data;
        let mut backend_buffer = vec![0u8; 8192];
        let extractor = MessageExtractor::new();

        loop {
            // Check pending data first
            if !pending.is_empty() {
                // Forward to client
                self.client_stream
                    .write_all(&pending)
                    .await
                    .context("Failed to forward startup data to client")?;

                // Check for ReadyForQuery using proper message framing
                // (not raw byte search which could false-positive on binary data)
                if extractor.contains_ready_for_query(&pending) {
                    debug!(connection_id, "Backend startup complete (ReadyForQuery received)");
                    break;
                }
                pending.clear();
            }

            // Read more from backend
            let n = backend_stream
                .read(&mut backend_buffer)
                .await
                .context("Failed to read backend startup data")?;

            if n == 0 {
                anyhow::bail!("Backend closed connection during startup");
            }

            let data = &backend_buffer[..n];

            // Forward to client
            self.client_stream
                .write_all(data)
                .await
                .context("Failed to forward startup data to client")?;

            // Check for ReadyForQuery using proper message framing
            if extractor.contains_ready_for_query(data) {
                debug!(connection_id, "Backend startup complete (ReadyForQuery received)");
                break;
            }
        }

        debug!(connection_id, "Handshake complete");
        Ok(())
    }

    /// Handle connection with a managed connection from PoolManager
    ///
    /// This method integrates with the PoolManager for proper connection lifecycle:
    /// - Tracks transaction state for connection release decisions
    /// - Handles sticky connections for clients with pinned state
    /// - Releases connections back to the pool on transaction boundaries (transaction mode)
    /// - Re-acquires connections when needed for subsequent queries
    async fn handle_with_managed_connection(
        mut self,
        mut managed_conn: super::ManagedConnection,
        pool_manager: &Arc<PoolManager>,
    ) -> Result<()> {
        let connection_id = self.connection_id;
        // Pre-format connection_id once to avoid repeated u64::to_string() calls
        let connection_id_str: Arc<str> = Arc::from(connection_id.to_string());
        // Use Arc<str> for database to avoid repeated String clones
        let database: Arc<str> = Arc::from(self.config.backend.database.as_str());
        let batcher = Arc::clone(&self.batcher);
        let anon_settings = AnonymizationSettings::from_config(&self.config);
        let metrics = Arc::clone(&self.metrics);

        let extractor = MessageExtractor::new();
        let mut stmt_cache =
            PreparedStatementCache::new(self.config.protocol.max_prepared_statements);

        // Transaction pooling tracking components
        let pooling_strategy = self.config.performance.connection_pooling.clone();
        let pooling_mode = Self::pooling_mode(&pooling_strategy);
        let mode_enforcer = ModeEnforcer::new(pooling_mode);
        let mut txn_tracker = TransactionTracker::new();
        let mut conn_state = ConnectionState::new(self.config.protocol.max_prepared_statements);

        let mut client_buffer = vec![0u8; self.config.performance.buffer_size];
        let mut backend_buffer = vec![0u8; self.config.performance.buffer_size];

        // Perform authentication and startup handshake
        self.perform_startup_handshake(managed_conn.stream_mut())
            .await
            .context("Startup handshake failed")?;

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
                                            warn!(query = %crate::observability::loggable(query), error = %err_msg, "Command rejected by pooling mode");
                                            let error_response = ModeEnforcer::build_error_response(&err_msg);
                                            self.client_stream.write_all(&error_response).await.context("Failed to send error to client")?;
                                            // Send ReadyForQuery to complete the error cycle
                                            let ready_for_query = Self::build_ready_for_query(txn_tracker.state());
                                            self.client_stream.write_all(&ready_for_query).await.context("Failed to send ReadyForQuery")?;
                                            should_forward = false;
                                            break;
                                        }

                                        debug!(name = %name, query = %crate::observability::loggable(query), "Cached prepared statement");
                                        stmt_cache.insert_statement(name.clone(), PreparedStatement {
                                            query: query.clone(),
                                            param_oids: param_oids.clone(),
                                        });
                                        stmt_cache.set_pending(String::new(), PendingExecution {
                                            query: query.clone(),
                                            params: vec![],
                                            params_incomplete: true,
                                            started_at: Instant::now(),
                                        });

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
                                            warn!(query = %crate::observability::loggable(query), error = %err_msg, "Command rejected by pooling mode");
                                            let error_response = ModeEnforcer::build_error_response(&err_msg);
                                            self.client_stream.write_all(&error_response).await.context("Failed to send error to client")?;
                                            let ready_for_query = Self::build_ready_for_query(txn_tracker.state());
                                            self.client_stream.write_all(&ready_for_query).await.context("Failed to send ReadyForQuery")?;
                                            should_forward = false;
                                            break;
                                        }

                                        debug!(query = %crate::observability::loggable(query), "Simple query");
                                        stmt_cache.set_pending(String::new(), PendingExecution {
                                            query: query.clone(),
                                            params: vec![],
                                            params_incomplete: false,
                                            started_at: Instant::now(),
                                        });

                                        conn_state.apply_query(query);
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

                            // Forward to backend if not rejected
                            if should_forward {
                                managed_conn.stream_mut().write_all(data).await.context("Failed to write to backend")?;
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to read from client");
                            break;
                        }
                    }
                }

                // Backend -> Client
                result = managed_conn.stream_mut().read(&mut backend_buffer) => {
                    match result {
                        Ok(0) => {
                            debug!("Backend closed connection");
                            break;
                        }
                        Ok(n) => {
                            let data = &backend_buffer[..n];

                            // Check for error response
                            if let Some(error_msg) = extractor.extract_error(data) {
                                if let Some(pending) = stmt_cache.take_pending("") {
                                    let duration = pending.started_at.elapsed();
                                    warn!(query = %crate::observability::loggable(&pending.query), error = %crate::observability::loggable(&error_msg), duration_ms = duration.as_millis(), "Query failed");

                                    let error_field = anon_settings.error_field(&extractor, data, error_msg);
                                    if let Some((event, fingerprints)) = anon_settings.build_event(
                                        &pending.query,
                                        pending.params,
                                        pending.params_incomplete,
                                        duration,
                                        false,
                                        error_field,
                                        &connection_id_str,
                                        &database,
                                    ) {
                                        if let Err(e) = batcher.send_event(event) {
                                            warn!(error = %e, "Failed to send event to batcher");
                                        }
                                        if !fingerprints.is_empty() {
                                            metrics.record_hot_data(&fingerprints);
                                        }
                                    }
                                    metrics.record_query(&QueryTimeline::for_completed(pending.started_at), false);
                                }
                            }
                            // Check for query completion
                            else if extractor.is_query_complete(data) {
                                if let Some(pending) = stmt_cache.take_pending("") {
                                    let duration = pending.started_at.elapsed();
                                    debug!(query = %crate::observability::loggable(&pending.query), duration_ms = duration.as_millis(), "Query completed successfully");

                                    if let Some((event, fingerprints)) = anon_settings.build_event(
                                        &pending.query,
                                        pending.params,
                                        pending.params_incomplete,
                                        duration,
                                        true,
                                        None,
                                        &connection_id_str,
                                        &database,
                                    ) {
                                        if let Err(e) = batcher.send_event(event) {
                                            warn!(error = %e, "Failed to send event to batcher");
                                        }
                                        if !fingerprints.is_empty() {
                                            metrics.record_hot_data(&fingerprints);
                                        }
                                    }
                                    metrics.record_query(&QueryTimeline::for_completed(pending.started_at), true);
                                }
                            }

                            // Track transaction state from ReadyForQuery messages
                            if let Some(status) = extractor.extract_ready_for_query(data) {
                                let was_in_transaction = txn_tracker.is_in_transaction();
                                txn_tracker.update_from_ready_for_query(status);

                                // Determine if we should release the connection back to the pool
                                // We release when a transaction completes (was_in_transaction && now idle)
                                //
                                // Note: We do NOT release for auto-commit queries (queries outside transactions)
                                // because releasing triggers DISCARD ALL which clears prepared statements.
                                // Clients using extended protocol (like tokio-postgres) cache prepared
                                // statement names and would break if we released mid-session.
                                //
                                // The connection will be released when the client disconnects (handler exits).
                                let just_finished_transaction = was_in_transaction && txn_tracker.is_idle();

                                let should_release = Self::should_release_connection(&pooling_strategy, &conn_state);

                                if just_finished_transaction && should_release {
                                    debug!(
                                        connection_id = connection_id,
                                        is_pinned = conn_state.is_pinned(),
                                        "Transaction complete, releasing connection to pool"
                                    );

                                    // Forward to client BEFORE releasing connection
                                    // This ensures client receives the ReadyForQuery
                                    self.client_stream.write_all(data).await.context("Failed to write to client")?;

                                    // Release current connection back to pool
                                    pool_manager.release(managed_conn);

                                    // Re-acquire connection lazily - spawn a brief yield to allow
                                    // the released connection to return to the pool
                                    tokio::task::yield_now().await;

                                    // Now re-acquire connection for next query
                                    managed_conn = pool_manager
                                        .acquire(connection_id, conn_state.is_pinned())
                                        .await
                                        .context("Failed to re-acquire connection from pool")?;

                                    debug!(
                                        connection_id = connection_id,
                                        new_is_pinned = managed_conn.is_pinned(),
                                        "Re-acquired connection from pool"
                                    );

                                    // State replay: if client has state but got a fresh connection,
                                    // replay the state to the new connection
                                    if conn_state.is_pinned() && !managed_conn.is_pinned() && !conn_state.has_unsafe_state() {
                                        let replay_state = conn_state.get_replayable_state();
                                        if !replay_state.prepared_statements.is_empty() || !replay_state.session_variables.is_empty() {
                                            debug!(
                                                connection_id = connection_id,
                                                prepared_statements = replay_state.prepared_statements.len(),
                                                session_variables = replay_state.session_variables.len(),
                                                "Replaying state to new connection"
                                            );

                                            let replayer = StateReplayer::new();
                                            match replayer.replay(&replay_state, managed_conn.stream_mut()).await {
                                                Ok(result) => {
                                                    if result.is_success() {
                                                        debug!(
                                                            connection_id = connection_id,
                                                            prepared_statements_replayed = result.prepared_statements_replayed,
                                                            session_variables_replayed = result.session_variables_replayed,
                                                            "State replay completed successfully"
                                                        );
                                                    } else {
                                                        warn!(
                                                            connection_id = connection_id,
                                                            errors = ?result.errors,
                                                            "State replay had errors"
                                                        );
                                                    }
                                                }
                                                Err(e) => {
                                                    warn!(
                                                        connection_id = connection_id,
                                                        error = %e,
                                                        "State replay failed"
                                                    );
                                                    // Clear client state since replay failed
                                                    conn_state.clear_all();
                                                }
                                            }
                                        }
                                    }

                                    // Continue to next iteration (data already sent to client)
                                    continue;
                                } else if was_in_transaction && txn_tracker.is_idle() {
                                    debug!(
                                        connection_id = connection_id,
                                        is_pinned = conn_state.is_pinned(),
                                        has_unsafe_state = conn_state.has_unsafe_state(),
                                        "Transaction ended but connection NOT released (session/pinned)"
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

        // Release connection back to pool manager when handler exits
        pool_manager.release(managed_conn);

        info!(
            connection_id = connection_id,
            is_pinned = conn_state.is_pinned(),
            has_unsafe_state = conn_state.has_unsafe_state(),
            "Connection handler completed (managed)"
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
    /// Handle connection with an owned backend TCP stream
    async fn handle_with_owned_backend(mut self, mut backend_stream: TcpStream) -> Result<()> {
        // Perform authentication and startup handshake
        self.perform_startup_handshake(&mut backend_stream)
            .await
            .context("Startup handshake failed")?;

        // For owned connections, we can use split() - works with ClientTransport via tokio::io::split
        let (mut client_read, mut client_write) = tokio::io::split(self.client_stream);
        let (mut backend_read, mut backend_write) = backend_stream.split();

        let connection_id = self.connection_id;
        // Pre-format connection_id once to avoid repeated u64::to_string() calls
        let connection_id_str: Arc<str> = Arc::from(connection_id.to_string());
        // Use Arc<str> for database to avoid repeated String clones
        let database: Arc<str> = Arc::from(self.config.backend.database.as_str());
        let batcher_clone = Arc::clone(&self.batcher);
        let config_clone = Arc::clone(&self.config);
        let anon_settings = AnonymizationSettings::from_config(&self.config);
        let metrics = Arc::clone(&self.metrics);
        let max_stmts = self.config.protocol.max_prepared_statements;

        // Shared prepared statement cache between both async tasks
        let stmt_cache: Arc<Mutex<PreparedStatementCache>> =
            Arc::new(Mutex::new(PreparedStatementCache::new(max_stmts)));

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
                            let mut cache = cache_writer.lock();
                            for msg in messages {
                                match msg {
                                    Message::Parse { name, query, param_oids } => {
                                        debug!(name = %name, query = %crate::observability::loggable(&query), "Cached prepared statement");
                                        cache.insert_statement(
                                            name.clone(),
                                            PreparedStatement { query: query.clone(), param_oids },
                                        );
                                        // Set pending with empty params for Parse errors
                                        // Will be overwritten by Bind if Parse succeeds
                                        cache.set_pending(
                                            String::new(),
                                            PendingExecution {
                                                query,
                                                params: vec![],
                                                params_incomplete: true,
                                                started_at: Instant::now(),
                                            },
                                        );
                                    }
                                    Message::Bind {
                                        portal,
                                        statement,
                                        format_codes,
                                        params_raw,
                                    } => {
                                        let (query, params, incomplete) = match cache
                                            .get_statement(&statement)
                                        {
                                            Some(stmt) => {
                                                let params = decode_params(
                                                    &params_raw,
                                                    &format_codes,
                                                    &stmt.param_oids,
                                                );
                                                (stmt.query.clone(), params, false)
                                            }
                                            None => {
                                                warn!(statement = %statement, "Statement not in cache");
                                                let params: Vec<ParamValue> = params_raw
                                                    .iter()
                                                    .map(|p| match p {
                                                        Some(data) => ParamValue::Unknown {
                                                            oid: 0,
                                                            data: data.clone(),
                                                        },
                                                        None => ParamValue::Null,
                                                    })
                                                    .collect();
                                                (format!("[unknown: {}]", statement), params, true)
                                            }
                                        };

                                        cache.set_pending(
                                            portal,
                                            PendingExecution {
                                                query,
                                                params,
                                                params_incomplete: incomplete,
                                                started_at: Instant::now(),
                                            },
                                        );
                                    }
                                    Message::Query { query } => {
                                        debug!(query = %crate::observability::loggable(&query), "Simple query");
                                        cache.set_pending(
                                            String::new(),
                                            PendingExecution {
                                                query,
                                                params: vec![],
                                                params_incomplete: false,
                                                started_at: Instant::now(),
                                            },
                                        );
                                    }
                                    Message::Close { kind, name } => match kind {
                                        'S' => cache.remove_statement(&name),
                                        'P' => cache.clear_pending(&name),
                                        _ => {}
                                    },
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
                            let mut cache = cache_reader.lock();
                            if let Some(pending) = cache.take_pending("") {
                                let duration = pending.started_at.elapsed();
                                warn!(
                                    query = %crate::observability::loggable(&pending.query),
                                    error = %crate::observability::loggable(&error_msg),
                                    duration_ms = duration.as_millis(),
                                    "Query failed"
                                );

                                let error_field =
                                    anon_settings.error_field(&extractor, data, error_msg);
                                if let Some((event, fingerprints)) = anon_settings.build_event(
                                    &pending.query,
                                    pending.params,
                                    pending.params_incomplete,
                                    duration,
                                    false,
                                    error_field,
                                    &connection_id_str,
                                    &database,
                                ) {
                                    if let Err(e) = batcher_clone.send_event(event) {
                                        warn!(error = %e, "Failed to send event to batcher");
                                    }
                                    if !fingerprints.is_empty() {
                                        metrics.record_hot_data(&fingerprints);
                                    }
                                }
                                metrics.record_query(
                                    &QueryTimeline::for_completed(pending.started_at),
                                    false,
                                );
                            }
                        }
                        // Check if this is a successful query completion
                        else if extractor.is_query_complete(data) {
                            let mut cache = cache_reader.lock();
                            if let Some(pending) = cache.take_pending("") {
                                let duration = pending.started_at.elapsed();
                                debug!(
                                    query = %crate::observability::loggable(&pending.query),
                                    duration_ms = duration.as_millis(),
                                    "Query completed successfully"
                                );

                                if let Some((event, fingerprints)) = anon_settings.build_event(
                                    &pending.query,
                                    pending.params,
                                    pending.params_incomplete,
                                    duration,
                                    true,
                                    None,
                                    &connection_id_str,
                                    &database,
                                ) {
                                    if let Err(e) = batcher_clone.send_event(event) {
                                        warn!(error = %e, "Failed to send event to batcher");
                                    }
                                    if !fingerprints.is_empty() {
                                        metrics.record_hot_data(&fingerprints);
                                    }
                                }
                                metrics.record_query(
                                    &QueryTimeline::for_completed(pending.started_at),
                                    true,
                                );
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
mod anonymization_tests {
    use super::*;

    fn enabled_settings(parse_failure: ParseFailureMode) -> AnonymizationSettings {
        AnonymizationSettings {
            enabled: true,
            anonymizer: Arc::new(QueryAnonymizer::with_salt(b"unit-test-salt".to_vec())),
            parse_failure,
        }
    }

    fn disabled_settings() -> AnonymizationSettings {
        AnonymizationSettings {
            enabled: false,
            anonymizer: Arc::new(QueryAnonymizer::new()),
            parse_failure: ParseFailureMode::Redact,
        }
    }

    #[test]
    fn enabled_event_query_is_normalized_never_raw() {
        let settings = enabled_settings(ParseFailureMode::Redact);
        let raw = "SELECT * FROM users WHERE email = 'bob@example.com' AND id = 42";
        let (event, fingerprints) = settings
            .build_event(raw, vec![], false, Duration::from_millis(1), true, None, "c1", "db")
            .expect("event should be produced");

        // The literal must never appear in the shipped query or normalized form.
        assert!(!event.query.contains("bob@example.com"), "query leaked literal: {}", event.query);
        assert!(!event.query.contains("42 "), "query leaked literal: {}", event.query);
        assert_eq!(event.query, event.normalized_query.clone().unwrap());
        assert!(
            event.query.contains('?'),
            "normalized query should use placeholders: {}",
            event.query
        );
        // Two literals → two fingerprints.
        assert_eq!(fingerprints.len(), 2);
    }

    #[test]
    fn disabled_event_ships_raw_query() {
        let settings = disabled_settings();
        let raw = "SELECT * FROM users WHERE email = 'bob@example.com'";
        let (event, _fps) = settings
            .build_event(raw, vec![], false, Duration::from_millis(1), true, None, "c1", "db")
            .expect("event should be produced");
        assert_eq!(event.query, raw);
        assert!(event.normalized_query.is_none());
    }

    #[test]
    fn parse_failure_redact_hides_raw_query() {
        let settings = enabled_settings(ParseFailureMode::Redact);
        // Unparseable / vendor syntax that carries a secret literal.
        let raw = "CREATE ROLE admin PASSWORD 'super-secret-pw' NOSUPERUSER GIBBERISH";
        let (event, fps) = settings
            .build_event(raw, vec![], false, Duration::from_millis(1), false, None, "c1", "db")
            .expect("redact mode still produces an event");
        assert_eq!(event.query, REDACTED_QUERY);
        assert!(!event.query.contains("super-secret-pw"));
        assert!(fps.is_empty());
    }

    #[test]
    fn parse_failure_drop_drops_event() {
        let settings = enabled_settings(ParseFailureMode::Drop);
        let raw = "CREATE ROLE admin PASSWORD 'super-secret-pw' GIBBERISH";
        let result = settings.build_event(
            raw,
            vec![],
            false,
            Duration::from_millis(1),
            false,
            None,
            "c1",
            "db",
        );
        assert!(result.is_none(), "drop mode must drop the event entirely");
    }

    #[test]
    fn params_are_redacted_when_enabled() {
        let settings = enabled_settings(ParseFailureMode::Redact);
        let params = vec![
            ParamValue::Text("bob@example.com".to_string()),
            ParamValue::Int32(31337),
            ParamValue::Json(r#"{"ssn":"123-45-6789"}"#.to_string()),
        ];
        let (event, _fps) = settings
            .build_event(
                "SELECT * FROM users WHERE id = $1",
                params,
                false,
                Duration::from_millis(1),
                true,
                None,
                "c1",
                "db",
            )
            .expect("event");
        // Same arity, but no raw values survive.
        assert_eq!(event.params.len(), 3);
        assert_eq!(event.params[0], ParamValue::Text(String::new()));
        assert_eq!(event.params[1], ParamValue::Int32(0));
        assert_eq!(event.params[2], ParamValue::Json(String::new()));
    }

    #[test]
    fn params_pass_through_when_disabled() {
        let settings = disabled_settings();
        let params = vec![ParamValue::Text("keep-me".to_string()), ParamValue::Int32(7)];
        let redacted = settings.redact_params(params.clone());
        assert_eq!(redacted, params);
    }

    #[test]
    fn redact_param_recurses_into_composites() {
        let nested = ParamValue::Array {
            elements: vec![
                ParamValue::Text("secret".to_string()),
                ParamValue::Composite { fields: vec![ParamValue::Int64(999)] },
            ],
            dimensions: vec![2],
        };
        let redacted = redact_param(&nested);
        match redacted {
            ParamValue::Array { elements, dimensions } => {
                assert_eq!(dimensions, vec![2]);
                assert_eq!(elements[0], ParamValue::Text(String::new()));
                match &elements[1] {
                    ParamValue::Composite { fields } => {
                        assert_eq!(fields[0], ParamValue::Int64(0));
                    }
                    other => panic!("expected composite, got {other:?}"),
                }
            }
            other => panic!("expected array, got {other:?}"),
        }
    }
}

/// The crown-jewel guardrail (P1 §5.3): with anonymization enabled, no produced
/// event and no gated log line may contain any input literal or parameter value.
#[cfg(test)]
mod anonymization_fuzz {
    use super::*;
    use crate::observability::{loggable, set_unsafe_debug_logging};
    use proptest::prelude::*;

    fn enabled(mode: ParseFailureMode) -> AnonymizationSettings {
        AnonymizationSettings {
            enabled: true,
            anonymizer: Arc::new(QueryAnonymizer::with_salt(b"fuzz-salt".to_vec())),
            parse_failure: mode,
        }
    }

    fn build(
        settings: &AnonymizationSettings,
        query: &str,
        params: Vec<ParamValue>,
    ) -> Option<QueryEvent> {
        settings
            .build_event(query, params, false, Duration::from_millis(1), true, None, "conn", "db")
            .map(|(event, _fps)| event)
    }

    /// Serialized event JSON — the exact bytes that would be published.
    fn event_json(
        settings: &AnonymizationSettings,
        query: &str,
        params: Vec<ParamValue>,
    ) -> Option<String> {
        build(settings, query, params).map(|e| serde_json::to_string(&e).expect("serialize event"))
    }

    proptest! {
        // High-entropy sentinel literals cannot collide with SQL keywords/idents,
        // so their absence from the event JSON is a clean signal of redaction.
        #[test]
        fn parameterized_event_never_leaks_literals(
            strlit in "SEC_[A-Za-z0-9]{8,24}",
            numlit in 100_000_000i64..9_999_999_999,
        ) {
            let settings = enabled(ParseFailureMode::Redact);
            let query = format!(
                "SELECT * FROM accounts WHERE token = '{strlit}' AND balance = {numlit}"
            );
            let params = vec![
                ParamValue::Text(strlit.clone()),
                ParamValue::Int64(numlit),
                ParamValue::Json(format!("{{\"secret\":\"{strlit}\"}}")),
            ];
            let event = build(&settings, &query, params).expect("redact mode keeps the event");
            let json = serde_json::to_string(&event).unwrap();
            // High-entropy string literal must not appear anywhere in the payload.
            prop_assert!(!json.contains(&strlit), "event leaked string literal {strlit}: {json}");
            // The numeric literal is checked against the query text fields only
            // (a raw digit-string could otherwise coincidentally match the event
            // timestamp/UUID in the full JSON — a false positive, not a leak).
            let numstr = numlit.to_string();
            prop_assert!(!event.query.contains(&numstr), "query leaked numeric literal {numstr}: {}", event.query);
            if let Some(nq) = &event.normalized_query {
                prop_assert!(!nq.contains(&numstr), "normalized_query leaked numeric literal {numstr}: {nq}");
            }
        }

        // With the flag off, the log redactor must never echo a literal.
        #[test]
        fn loggable_never_leaks_literal_when_disabled(lit in "SEC_[A-Za-z0-9]{8,24}") {
            set_unsafe_debug_logging(false);
            prop_assert!(!loggable(&lit).contains(&lit));
        }
    }

    /// Explicit adversarial corpus: DDL echoing secrets, unparseable vendor
    /// syntax, and multi-literal statements. Under both parse-failure modes,
    /// neither the event nor the log redactor may surface any listed secret.
    #[test]
    fn adversarial_corpus_never_leaks() {
        set_unsafe_debug_logging(false);
        // (query, secrets that must never appear anywhere)
        let corpus: &[(&str, &[&str])] = &[
            ("CREATE ROLE deploy PASSWORD 'super-secret-pw'", &["super-secret-pw"]),
            ("SELECT * FROM patients WHERE ssn = '123-45-6789'", &["123-45-6789"]),
            (
                "INSERT INTO t (a, b) VALUES ('alice@example.com', 'hunter2')",
                &["alice@example.com", "hunter2"],
            ),
            (
                "UPDATE users SET pw = 'secretA' WHERE email = 'secretB@x.io'",
                &["secretA", "secretB@x.io"],
            ),
            ("$$ totally @@ unparseable ## vendor 'embedded-secret' syntax", &["embedded-secret"]),
        ];

        for mode in [ParseFailureMode::Redact, ParseFailureMode::Drop] {
            let settings = enabled(mode);
            for (query, secrets) in corpus {
                // Event guarantee (only present in Redact mode; Drop yields None).
                if let Some(json) = event_json(&settings, query, vec![]) {
                    for secret in *secrets {
                        assert!(
                            !json.contains(secret),
                            "event leaked '{secret}' for query {query:?}: {json}"
                        );
                    }
                }
                // Log guarantee: the redactor never echoes a secret with the flag off.
                for secret in *secrets {
                    assert!(!loggable(secret).contains(secret));
                }
            }
        }
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

    // Tests for should_release_connection()

    #[test]
    fn test_should_release_connection_disabled_mode() {
        let conn_state = ConnectionState::new(100);
        // Disabled mode should never release
        assert!(!ConnectionHandler::should_release_connection(
            &PoolingStrategy::Disabled,
            &conn_state
        ));
    }

    #[test]
    fn test_should_release_connection_session_mode() {
        let conn_state = ConnectionState::new(100);
        // Session mode should never release
        assert!(!ConnectionHandler::should_release_connection(
            &PoolingStrategy::Session,
            &conn_state
        ));
    }

    #[test]
    fn test_should_release_connection_transaction_mode() {
        let conn_state = ConnectionState::new(100);
        // Transaction mode should always release
        assert!(ConnectionHandler::should_release_connection(
            &PoolingStrategy::Transaction,
            &conn_state
        ));
    }

    #[test]
    fn test_should_release_connection_transaction_mode_with_pinned_state() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        // Transaction mode should still release even with pinned state (strict mode)
        assert!(ConnectionHandler::should_release_connection(
            &PoolingStrategy::Transaction,
            &conn_state
        ));
    }

    #[test]
    fn test_should_release_connection_hybrid_mode_unpinned() {
        let conn_state = ConnectionState::new(100);
        // Hybrid mode should release when not pinned
        assert!(ConnectionHandler::should_release_connection(
            &PoolingStrategy::Hybrid,
            &conn_state
        ));
    }

    #[test]
    fn test_should_release_connection_hybrid_mode_pinned() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        // Hybrid mode should NOT release when pinned
        assert!(!ConnectionHandler::should_release_connection(
            &PoolingStrategy::Hybrid,
            &conn_state
        ));
    }

    #[test]
    fn test_should_release_connection_hybrid_mode_with_session_variable() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_session_variable("timezone".to_string(), "UTC".to_string());
        // Hybrid mode should NOT release when connection has session variables
        assert!(!ConnectionHandler::should_release_connection(
            &PoolingStrategy::Hybrid,
            &conn_state
        ));
    }

    #[test]
    fn test_should_release_connection_hybrid_mode_with_temp_table() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_temp_table("tmp_users".to_string());
        // Hybrid mode should NOT release when connection has temp tables
        assert!(!ConnectionHandler::should_release_connection(
            &PoolingStrategy::Hybrid,
            &conn_state
        ));
    }

    #[test]
    fn test_build_ready_for_query_idle() {
        let msg = ConnectionHandler::build_ready_for_query(super::super::TransactionState::Idle);
        assert_eq!(msg, vec![b'Z', 0, 0, 0, 5, b'I']);
    }

    #[test]
    fn test_build_ready_for_query_in_transaction() {
        let msg =
            ConnectionHandler::build_ready_for_query(super::super::TransactionState::InTransaction);
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
        conn_state.apply_query("SET timezone = 'UTC'");
        assert!(conn_state.is_pinned());
    }

    #[test]
    fn test_update_connection_state_reset() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_session_variable("timezone".to_string(), "UTC".to_string());
        assert!(conn_state.is_pinned());

        conn_state.apply_query("RESET timezone");
        assert!(!conn_state.is_pinned());
    }

    #[test]
    fn test_update_connection_state_reset_all() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_session_variable("timezone".to_string(), "UTC".to_string());
        conn_state.add_session_variable("search_path".to_string(), "public".to_string());
        assert!(conn_state.is_pinned());

        conn_state.apply_query("RESET ALL");
        assert!(!conn_state.is_pinned());
    }

    #[test]
    fn test_update_connection_state_create_temp_table() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.apply_query("CREATE TEMP TABLE tmp_users (id int)");
        assert!(conn_state.is_pinned());
        assert!(conn_state.has_unsafe_state());
    }

    #[test]
    fn test_update_connection_state_declare_cursor() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.apply_query("DECLARE my_cursor CURSOR FOR SELECT 1");
        assert!(conn_state.is_pinned());
        assert!(conn_state.has_unsafe_state());
    }

    #[test]
    fn test_update_connection_state_close_cursor() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_cursor("my_cursor".to_string());
        assert!(conn_state.has_unsafe_state());

        conn_state.apply_query("CLOSE my_cursor");
        assert!(!conn_state.has_unsafe_state());
    }

    #[test]
    fn test_update_connection_state_advisory_lock() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.apply_query("SELECT pg_advisory_lock(12345)");
        assert!(conn_state.is_pinned());
        assert!(conn_state.has_unsafe_state());
    }

    #[test]
    fn test_update_connection_state_advisory_unlock() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_advisory_lock(12345);
        assert!(conn_state.has_unsafe_state());

        conn_state.apply_query("SELECT pg_advisory_unlock(12345)");
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

        conn_state.apply_query("DISCARD ALL");
        assert!(!conn_state.is_pinned());
        assert!(!conn_state.has_unsafe_state());
    }

    #[test]
    fn test_update_connection_state_deallocate() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        assert!(conn_state.is_pinned());

        conn_state.apply_query("DEALLOCATE stmt1");
        assert!(!conn_state.is_pinned());
    }

    #[test]
    fn test_update_connection_state_deallocate_all() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        conn_state.add_prepared_statement("stmt2".to_string(), "SELECT 2".to_string(), vec![]);
        assert!(conn_state.is_pinned());

        conn_state.apply_query("DEALLOCATE ALL");
        assert!(!conn_state.is_pinned());
    }

    #[test]
    fn test_update_connection_state_regular_query_no_effect() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.apply_query("SELECT * FROM users");
        assert!(!conn_state.is_pinned());
    }

    #[test]
    fn test_update_connection_state_drop_table() {
        let mut conn_state = ConnectionState::new(100);
        conn_state.add_temp_table("tmp_users".to_string());
        assert!(conn_state.has_unsafe_state());

        conn_state.apply_query("DROP TABLE tmp_users");
        assert!(!conn_state.has_unsafe_state());
    }

    // Tests for queue full error message format

    #[test]
    fn test_build_queue_full_error_format() {
        let error = ConnectionHandler::build_queue_full_error(200);

        // Should start with 'E' (ErrorResponse)
        assert_eq!(error[0], b'E');

        // Length is bytes 1-4 (big-endian i32)
        let length = i32::from_be_bytes([error[1], error[2], error[3], error[4]]);
        assert_eq!(length as usize, error.len() - 1); // Length includes itself but not the 'E'

        // Should contain SQLSTATE 53300
        let error_str = String::from_utf8_lossy(&error);
        assert!(error_str.contains("53300"), "Should contain SQLSTATE 53300");

        // Should contain queue full message
        assert!(
            error_str.contains("connection pool queue is full"),
            "Should contain queue full message"
        );

        // Should contain retry hint with the specified ms
        assert!(error_str.contains("200ms"), "Should contain retry hint with 200ms");
    }

    #[test]
    fn test_build_queue_full_error_different_retry_hint() {
        let error = ConnectionHandler::build_queue_full_error(500);
        let error_str = String::from_utf8_lossy(&error);
        assert!(error_str.contains("500ms"), "Should contain retry hint with 500ms");
    }

    #[test]
    fn test_build_wait_timeout_error_format() {
        let error = ConnectionHandler::build_wait_timeout_error();

        // Should start with 'E' (ErrorResponse)
        assert_eq!(error[0], b'E');

        // Should contain SQLSTATE 53300
        let error_str = String::from_utf8_lossy(&error);
        assert!(error_str.contains("53300"), "Should contain SQLSTATE 53300");

        // Should contain timeout message
        assert!(
            error_str.contains("timeout waiting for connection"),
            "Should contain timeout message"
        );
    }
}
