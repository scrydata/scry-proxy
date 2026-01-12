# PgBouncer Parity Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Complete all remaining features to make Scry a drop-in PgBouncer replacement with enhanced observability.

**Architecture:** Integrate existing PoolManager with ConnectionHandler for dynamic connection release, add authentication passthrough via auth_file parsing, implement admin console as a virtual database interceptor, and add missing operational features (UNIX sockets, online reload, multi-database routing).

**Tech Stack:** Rust, Tokio, existing deadpool integration, rustls for TLS, tokio::signal for SIGHUP handling.

---

## Current State Summary

**Already Implemented:**
- `TransactionTracker` - tracks transaction state via ReadyForQuery
- `ConnectionState` - tracks pinnable state (prepared stmts, SET vars, temp tables, cursors, locks)
- `ModeEnforcer` - validates commands in transaction mode
- `CommandDetector` - parses SQL for state-changing commands
- `StateReplayer` - replays safe state to new connections
- `PoolManager` - LIFO/sticky selection, wait queue, idle cleanup
- `WaitQueue` - bounded FIFO queue for pool exhaustion
- `PgBouncerConfig` - parses pgbouncer.ini and PGBOUNCER_* env vars
- TLS support (client and backend)
- Circuit breaker, retries, healthchecks

**Missing Integration:**
- ConnectionHandler uses TcpConnectionPool directly, not PoolManager
- No connection release after transaction end
- No state replay on reconnection

**Missing Features:**
- Authentication passthrough (auth_type, auth_file)
- Admin console (SHOW, PAUSE, RESUME, RELOAD)
- Multi-database routing
- UNIX socket listening
- SIGHUP config reload

---

## Phase 1: PoolManager Integration (Critical Path)

### Task 1.1: Update ProxyServer to create PoolManager

**Files:**
- Modify: `scry-proxy/src/proxy/server.rs`

**Step 1: Write the failing test**

```rust
// Add to scry-proxy/src/proxy/server.rs tests module

#[tokio::test]
async fn test_server_creates_pool_manager() {
    // This test verifies PoolManager is created and accessible
    let config = create_test_config();
    let publisher = Arc::new(DebugLoggerPublisher::new());
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

    let server = ProxyServer::new(config,
        EventBatcher::new(publisher, 10, 100, 1000),
        metrics).await.unwrap();

    // Server should expose pool_manager for testing
    assert!(server.pool_manager().is_some());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry --lib server::tests::test_server_creates_pool_manager`
Expected: FAIL with "no method named `pool_manager`"

**Step 3: Update ProxyServer struct**

```rust
// In scry-proxy/src/proxy/server.rs

use super::{PoolManager, PoolManagerConfig, WaitQueue};

pub struct ProxyServer {
    listener: TcpListener,
    config: Arc<Config>,
    batcher: Arc<EventBatcher>,
    pool: Option<Arc<TcpConnectionPool>>,
    pool_manager: Option<Arc<PoolManager>>,  // Add this
    metrics: Arc<ProxyMetrics>,
    tls_acceptor: Option<Arc<TlsAcceptor>>,
    shutdown: watch::Receiver<bool>,
    shutdown_sender: watch::Sender<bool>,
}

impl ProxyServer {
    pub async fn new(/* ... */) -> Result<Self> {
        // ... existing pool creation ...

        // Create PoolManager if pooling enabled
        let pool_manager = if let Some(ref pool) = pool {
            let wait_queue = WaitQueue::new(config.performance.pool_queue_depth);
            let pm_config = PoolManagerConfig {
                lifo: config.performance.pool_lifo,
                queue_depth: config.performance.pool_queue_depth,
                idle_unpin_secs: config.performance.pool_idle_unpin_secs,
                wait_timeout_ms: config.performance.pool_timeout_secs * 1000,
            };
            Some(PoolManager::new(Arc::clone(pool), wait_queue, pm_config))
        } else {
            None
        };

        Ok(Self {
            // ... existing fields ...
            pool_manager,
        })
    }

    pub fn pool_manager(&self) -> Option<&Arc<PoolManager>> {
        self.pool_manager.as_ref()
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p scry --lib server::tests::test_server_creates_pool_manager`
Expected: PASS

**Step 5: Commit**

```bash
git add scry-proxy/src/proxy/server.rs
git commit -m "feat(pool): create PoolManager in ProxyServer"
```

---

### Task 1.2: Pass PoolManager to ConnectionHandler

**Files:**
- Modify: `scry-proxy/src/proxy/server.rs`
- Modify: `scry-proxy/src/proxy/connection.rs`

**Step 1: Update ConnectionHandler constructor**

```rust
// In scry-proxy/src/proxy/connection.rs

pub struct ConnectionHandler {
    client_stream: ClientTransport,
    client_addr: SocketAddr,
    connection_id: u64,
    config: Arc<Config>,
    batcher: Arc<EventBatcher>,
    pool_manager: Option<Arc<PoolManager>>,  // Change from pool: Option<Arc<TcpConnectionPool>>
    metrics: Arc<ProxyMetrics>,
    startup_data: Vec<u8>,
}

impl ConnectionHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_stream: ClientTransport,
        client_addr: SocketAddr,
        connection_id: u64,
        config: Arc<Config>,
        batcher: Arc<EventBatcher>,
        pool_manager: Option<Arc<PoolManager>>,  // Change parameter type
        metrics: Arc<ProxyMetrics>,
        startup_data: Vec<u8>,
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
        }
    }
}
```

**Step 2: Update server.rs to pass PoolManager**

```rust
// In ProxyServer::run() where ConnectionHandler is created

let handler = ConnectionHandler::new(
    client_transport,
    addr,
    connection_id,
    Arc::clone(&self.config),
    Arc::clone(&self.batcher),
    self.pool_manager.clone(),  // Pass PoolManager instead of pool
    Arc::clone(&self.metrics),
    startup_buffer,
);
```

**Step 3: Run tests**

Run: `cargo test -p scry --lib`
Expected: PASS (with updates to handle signature change)

**Step 4: Commit**

```bash
git add scry-proxy/src/proxy/server.rs scry-proxy/src/proxy/connection.rs
git commit -m "refactor(pool): pass PoolManager to ConnectionHandler"
```

---

### Task 1.3: Implement connection acquisition via PoolManager

**Files:**
- Modify: `scry-proxy/src/proxy/connection.rs`

**Step 1: Update handle() to use PoolManager.acquire()**

```rust
// In ConnectionHandler::handle()

pub async fn handle(self) -> Result<()> {
    info!("Starting connection handler");

    let backend_addr = format!("{}:{}", self.config.backend.host, self.config.backend.port);

    // Get connection via PoolManager if available
    if let Some(ref pool_manager) = self.pool_manager {
        info!(
            backend_addr = %backend_addr,
            connection_id = self.connection_id,
            "Acquiring backend connection from PoolManager"
        );

        // Check if we need sticky connection (e.g., client has prior state)
        let needs_sticky = pool_manager.has_sticky(self.connection_id);

        let managed_conn = pool_manager
            .acquire(self.connection_id, needs_sticky)
            .await
            .context("Failed to acquire connection from pool")?;

        info!(backend_addr = %backend_addr, is_pinned = managed_conn.is_pinned(), "Using managed connection");

        return self.handle_with_managed_connection(managed_conn, pool_manager).await;
    } else {
        // Fallback to direct connection
        info!(backend_addr = %backend_addr, "Creating direct backend connection");
        let backend_stream = TcpStream::connect(&backend_addr)
            .await
            .context("Failed to connect to backend")?;
        return self.handle_with_owned_backend(backend_stream).await;
    }
}
```

**Step 2: Create handle_with_managed_connection method**

This is the key integration point. See Task 1.4 for full implementation.

**Step 3: Commit**

```bash
git add scry-proxy/src/proxy/connection.rs
git commit -m "feat(pool): acquire connections via PoolManager"
```

---

### Task 1.4: Implement transaction-based connection release

**Files:**
- Modify: `scry-proxy/src/proxy/connection.rs`

**Step 1: Write integration test**

```rust
// Add to scry-proxy/tests/pooling_integration_test.rs

#[tokio::test]
async fn test_connection_released_after_transaction() {
    // Setup with pool_size = 1
    let docker = Cli::default();
    let postgres = docker.run(Postgres::default());
    let port = postgres.get_host_port_ipv4(5432);

    let mut config = create_test_config("127.0.0.1".to_string(), port);
    config.performance.connection_pooling = PoolingStrategy::Transaction;
    config.performance.pool_size = 1;

    // Start proxy
    let (server, addr) = start_test_proxy(config).await;

    // Client 1: BEGIN, INSERT, COMMIT (releases connection)
    let mut client1 = connect_to_proxy(addr).await;
    send_query(&mut client1, "BEGIN").await;
    send_query(&mut client1, "SELECT 1").await;
    send_query(&mut client1, "COMMIT").await;

    // Client 2 should immediately get connection (not wait)
    let start = Instant::now();
    let mut client2 = connect_to_proxy(addr).await;
    let response = send_query(&mut client2, "SELECT 2").await;

    // Should complete quickly (connection was released)
    assert!(start.elapsed() < Duration::from_millis(100));
    assert!(response_is_success(&response));
}
```

**Step 2: Implement handle_with_managed_connection**

```rust
// In scry-proxy/src/proxy/connection.rs

async fn handle_with_managed_connection(
    mut self,
    mut managed_conn: ManagedConnection,
    pool_manager: &Arc<PoolManager>,
) -> Result<()> {
    let connection_id = self.connection_id;
    let database = self.config.backend.database.clone();
    let batcher = Arc::clone(&self.batcher);
    let anonymize = self.config.publisher.anonymize;
    let metrics = Arc::clone(&self.metrics);
    let pooling_strategy = self.config.performance.connection_pooling.clone();

    let extractor = MessageExtractor::new();
    let mut stmt_cache = PreparedStatementCache::new(self.config.protocol.max_prepared_statements);

    // Transaction pooling tracking
    let pooling_mode = Self::pooling_mode(&pooling_strategy);
    let mode_enforcer = ModeEnforcer::new(pooling_mode);
    let mut txn_tracker = TransactionTracker::new();
    let mut conn_state = ConnectionState::new(self.config.protocol.max_prepared_statements);

    let mut client_buffer = vec![0u8; self.config.performance.buffer_size];
    let mut backend_buffer = vec![0u8; self.config.performance.buffer_size];

    // Forward any buffered startup data
    if !self.startup_data.is_empty() {
        managed_conn.stream_mut()
            .write_all(&self.startup_data)
            .await
            .context("Failed to forward startup data")?;
    }

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

                        // Process messages (same as existing code)
                        for msg in extractor.extract_messages(data) {
                            // ... existing message processing ...
                            // (Parse, Bind, Query validation and state tracking)
                        }

                        if should_forward {
                            managed_conn.stream_mut()
                                .write_all(data)
                                .await
                                .context("Failed to write to backend")?;
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

                        // ... existing response processing (error/success events) ...

                        // KEY INTEGRATION: Track transaction state and release connection
                        if let Some(status) = extractor.extract_ready_for_query(data) {
                            let was_in_transaction = txn_tracker.is_in_transaction();
                            txn_tracker.update_from_ready_for_query(status);

                            // Check if we should release connection
                            if was_in_transaction && txn_tracker.is_idle() {
                                if Self::should_release_connection(
                                    &pooling_strategy,
                                    &conn_state,
                                ) {
                                    debug!(
                                        connection_id,
                                        "Transaction complete, releasing connection to pool"
                                    );
                                    // Release and re-acquire for next query
                                    pool_manager.release(managed_conn);

                                    // For next query, acquire new connection
                                    managed_conn = pool_manager
                                        .acquire(connection_id, conn_state.is_pinned())
                                        .await
                                        .context("Failed to re-acquire connection")?;
                                }
                            }
                        }

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

    // Release connection on handler exit
    pool_manager.release(managed_conn);

    info!(
        connection_id,
        is_pinned = conn_state.is_pinned(),
        "Connection handler completed"
    );
    Ok(())
}

/// Determine if connection should be released after transaction
fn should_release_connection(
    strategy: &PoolingStrategy,
    conn_state: &ConnectionState,
) -> bool {
    match strategy {
        PoolingStrategy::Disabled | PoolingStrategy::Session => false,
        PoolingStrategy::Transaction => true,  // Always release in transaction mode
        PoolingStrategy::Hybrid => !conn_state.is_pinned(),  // Release if not pinned
    }
}
```

**Step 3: Run integration test**

Run: `cargo test -p scry --test pooling_integration_test`
Expected: PASS

**Step 4: Commit**

```bash
git add scry-proxy/src/proxy/connection.rs scry-proxy/tests/pooling_integration_test.rs
git commit -m "feat(pool): release connections after transaction completion"
```

---

### Task 1.5: Add state replay on reconnection

**Files:**
- Modify: `scry-proxy/src/proxy/connection.rs`

**Step 1: Write test for state replay**

```rust
#[tokio::test]
async fn test_prepared_statement_replay_on_reconnection() {
    // Pool size 1, transaction mode
    // Client 1: PREPARE, BEGIN, EXECUTE, COMMIT (releases connection)
    // Client 2: Takes the connection
    // Client 1: EXECUTE again - should work via re-preparation
}
```

**Step 2: Integrate StateReplayer**

```rust
// After acquiring a new connection in handle_with_managed_connection

if !managed_conn.is_pinned() && conn_state.is_pinned() {
    // We have state but got a fresh connection - need to replay
    let replay_state = conn_state.get_replayable_state();
    if !replay_state.is_empty() {
        debug!(connection_id, "Replaying state to new connection");

        let replayer = StateReplayer::new();
        let result = replayer
            .replay(managed_conn.stream_mut(), &replay_state)
            .await;

        if let Err(e) = result {
            warn!(error = %e, "State replay failed");
            // State replay failed - need to propagate error to client
            // or clear state and continue
        }
    }
}
```

**Step 3: Commit**

```bash
git add scry-proxy/src/proxy/connection.rs
git commit -m "feat(pool): replay state on reconnection"
```

---

### Task 1.6: Start idle cleanup background task

**Files:**
- Modify: `scry-proxy/src/proxy/server.rs`

**Step 1: Add idle cleanup task**

```rust
// In ProxyServer::run()

// Spawn idle cleanup task
if let Some(ref pool_manager) = self.pool_manager {
    let pm = Arc::clone(pool_manager);
    let idle_interval = self.config.performance.pool_idle_unpin_secs;

    if idle_interval > 0 {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(idle_interval / 2));
            loop {
                interval.tick().await;
                let cleaned = pm.cleanup_idle();
                if cleaned > 0 {
                    debug!(cleaned, "Cleaned up idle sticky connections");
                }
            }
        });
    }
}
```

**Step 2: Commit**

```bash
git add scry-proxy/src/proxy/server.rs
git commit -m "feat(pool): add idle connection cleanup task"
```

---

## Phase 2: Authentication Passthrough

### Task 2.1: Define auth_file format and config

**Files:**
- Create: `scry-proxy/src/auth/mod.rs`
- Modify: `scry-proxy/src/config/mod.rs`
- Modify: `scry-proxy/src/lib.rs`

**Step 1: Add auth config options**

```rust
// In scry-proxy/src/config/mod.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Authentication type: trust, md5, scram-sha-256, cert
    #[serde(default = "default_auth_type")]
    pub auth_type: String,

    /// Path to userlist.txt file (PgBouncer format)
    pub auth_file: Option<String>,

    /// Path to auth_query database (for runtime auth)
    pub auth_query: Option<String>,
}

fn default_auth_type() -> String {
    "trust".to_string()
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            auth_type: default_auth_type(),
            auth_file: None,
            auth_query: None,
        }
    }
}
```

**Step 2: Create auth module**

```rust
// scry-proxy/src/auth/mod.rs

mod file_auth;
mod types;

pub use file_auth::FileAuthenticator;
pub use types::{AuthType, AuthResult, UserCredentials};
```

**Step 3: Commit**

```bash
git add scry-proxy/src/auth/mod.rs scry-proxy/src/config/mod.rs scry-proxy/src/lib.rs
git commit -m "feat(auth): add auth configuration types"
```

---

### Task 2.2: Parse userlist.txt (auth_file)

**Files:**
- Create: `scry-proxy/src/auth/file_auth.rs`
- Create: `scry-proxy/src/auth/types.rs`

**Step 1: Write failing test**

```rust
// In scry-proxy/src/auth/file_auth.rs

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_userlist_plain() {
        let content = r#"
"postgres" "password123"
"admin" "adminpass"
"#;
        let auth = FileAuthenticator::from_string(content).unwrap();

        assert!(auth.check_password("postgres", "password123"));
        assert!(!auth.check_password("postgres", "wrong"));
        assert!(auth.check_password("admin", "adminpass"));
    }

    #[test]
    fn test_parse_userlist_md5() {
        // MD5 format: "user" "md5<32 hex chars>"
        let content = r#"
"postgres" "md5e8a48653851e28c69d0506508fb27fc5"
"#;
        let auth = FileAuthenticator::from_string(content).unwrap();

        // MD5 = md5(password + username) for postgres wire protocol
        assert!(auth.check_password_md5("postgres", "password"));
    }
}
```

**Step 2: Implement FileAuthenticator**

```rust
// scry-proxy/src/auth/file_auth.rs

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use anyhow::{Context, Result};
use md5::{Md5, Digest};

pub struct FileAuthenticator {
    users: HashMap<String, PasswordEntry>,
}

enum PasswordEntry {
    Plain(String),
    Md5(String),  // The 32-char hex after "md5"
    ScramSha256(String),
}

impl FileAuthenticator {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read auth file: {}", path.as_ref().display()))?;
        Self::from_string(&content)
    }

    pub fn from_string(content: &str) -> Result<Self> {
        let mut users = HashMap::new();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with(';') || line.starts_with('#') {
                continue;
            }

            // Parse: "username" "password"
            if let Some((user, pass)) = Self::parse_line(line) {
                let entry = if pass.starts_with("md5") && pass.len() == 35 {
                    PasswordEntry::Md5(pass[3..].to_string())
                } else if pass.starts_with("SCRAM-SHA-256$") {
                    PasswordEntry::ScramSha256(pass.to_string())
                } else {
                    PasswordEntry::Plain(pass)
                };
                users.insert(user, entry);
            }
        }

        Ok(Self { users })
    }

    fn parse_line(line: &str) -> Option<(String, String)> {
        // Format: "username" "password"
        let mut parts = line.split('"').filter(|s| !s.trim().is_empty());
        let user = parts.next()?.to_string();
        let pass = parts.next()?.to_string();
        Some((user, pass))
    }

    pub fn check_password(&self, username: &str, password: &str) -> bool {
        match self.users.get(username) {
            Some(PasswordEntry::Plain(stored)) => stored == password,
            Some(PasswordEntry::Md5(stored_hash)) => {
                // Compute md5(password + username)
                let mut hasher = Md5::new();
                hasher.update(password.as_bytes());
                hasher.update(username.as_bytes());
                let hash = format!("{:x}", hasher.finalize());
                &hash == stored_hash
            }
            _ => false,
        }
    }

    pub fn get_password(&self, username: &str) -> Option<&str> {
        match self.users.get(username) {
            Some(PasswordEntry::Plain(pass)) => Some(pass),
            _ => None,
        }
    }
}
```

**Step 3: Run tests**

Run: `cargo test -p scry --lib auth::file_auth::tests`
Expected: PASS

**Step 4: Commit**

```bash
git add scry-proxy/src/auth/
git commit -m "feat(auth): implement userlist.txt parser"
```

---

### Task 2.3: Integrate auth into startup handshake

**Files:**
- Modify: `scry-proxy/src/proxy/server.rs`

This task integrates authentication into the PostgreSQL startup message flow.

**Step 1: Add auth to ProxyServer**

```rust
// In ProxyServer struct
auth: Option<Arc<FileAuthenticator>>,

// In ProxyServer::new()
let auth = if let Some(ref auth_file) = config.auth.auth_file {
    Some(Arc::new(FileAuthenticator::from_file(auth_file)?))
} else {
    None
};
```

**Step 2: Modify startup handshake**

The startup handshake in `handle_connection()` needs to:
1. Parse StartupMessage for username
2. Send AuthenticationMD5Password (or CleartextPassword)
3. Receive PasswordMessage from client
4. Verify against auth_file
5. Forward to backend with backend credentials

**Step 3: Commit**

```bash
git add scry-proxy/src/proxy/server.rs
git commit -m "feat(auth): integrate authentication into startup"
```

---

## Phase 3: Admin Console

### Task 3.1: Define admin database detector

**Files:**
- Create: `scry-proxy/src/admin/mod.rs`
- Create: `scry-proxy/src/admin/commands.rs`

**Step 1: Write failing test**

```rust
#[test]
fn test_detect_admin_query() {
    assert!(AdminConsole::is_admin_command("SHOW POOLS"));
    assert!(AdminConsole::is_admin_command("SHOW STATS"));
    assert!(AdminConsole::is_admin_command("PAUSE mydb"));
    assert!(AdminConsole::is_admin_command("RESUME"));
    assert!(AdminConsole::is_admin_command("RELOAD"));

    assert!(!AdminConsole::is_admin_command("SELECT * FROM users"));
}
```

**Step 2: Implement AdminConsole**

```rust
// scry-proxy/src/admin/mod.rs

pub struct AdminConsole {
    pool_manager: Arc<PoolManager>,
    metrics: Arc<ProxyMetrics>,
}

impl AdminConsole {
    pub fn is_admin_command(sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();
        upper.starts_with("SHOW ")
            || upper.starts_with("PAUSE")
            || upper.starts_with("RESUME")
            || upper.starts_with("RELOAD")
            || upper.starts_with("SHUTDOWN")
            || upper.starts_with("KILL")
    }

    pub async fn execute(&self, sql: &str) -> Result<AdminResponse> {
        let upper = sql.trim().to_uppercase();

        if upper.starts_with("SHOW POOLS") {
            self.show_pools()
        } else if upper.starts_with("SHOW STATS") {
            self.show_stats()
        } else if upper.starts_with("SHOW DATABASES") {
            self.show_databases()
        } else if upper.starts_with("PAUSE") {
            self.pause_database(sql)
        } else if upper.starts_with("RESUME") {
            self.resume_database(sql)
        } else if upper.starts_with("RELOAD") {
            self.reload_config().await
        } else {
            Err(anyhow::anyhow!("Unknown admin command"))
        }
    }

    fn show_pools(&self) -> Result<AdminResponse> {
        // Return pool statistics as DataRow messages
        let status = self.pool_manager.pool().status();
        // Build PostgreSQL wire protocol response
        Ok(AdminResponse::RowSet(vec![
            vec![
                "default".to_string(),  // database
                status.size.to_string(),  // cl_active
                "0".to_string(),  // cl_waiting
                status.available.to_string(),  // sv_active
                // ... more columns
            ]
        ]))
    }
}
```

**Step 3: Commit**

```bash
git add scry-proxy/src/admin/
git commit -m "feat(admin): implement admin console commands"
```

---

### Task 3.2: Route admin database queries

**Files:**
- Modify: `scry-proxy/src/proxy/server.rs`

**Step 1: Detect pgbouncer virtual database**

```rust
// In startup message parsing, check for database = "pgbouncer"

if startup_params.get("database") == Some(&"pgbouncer".to_string()) {
    // Route to admin console handler
    return self.handle_admin_connection(client_transport, startup_params).await;
}
```

**Step 2: Implement admin connection handler**

```rust
async fn handle_admin_connection(
    &self,
    mut client: ClientTransport,
    params: HashMap<String, String>,
) -> Result<()> {
    // Send AuthenticationOk
    client.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;

    // Send ReadyForQuery
    client.write_all(&[b'Z', 0, 0, 0, 5, b'I']).await?;

    let admin = AdminConsole::new(
        Arc::clone(self.pool_manager.as_ref().unwrap()),
        Arc::clone(&self.metrics),
    );

    let mut buffer = vec![0u8; 8192];

    loop {
        let n = client.read(&mut buffer).await?;
        if n == 0 { break; }

        // Extract Query message
        if buffer[0] == b'Q' {
            let query = extract_query(&buffer[..n]);
            let response = admin.execute(&query).await?;
            client.write_all(&response.to_wire()).await?;
        }
    }

    Ok(())
}
```

**Step 3: Commit**

```bash
git add scry-proxy/src/proxy/server.rs scry-proxy/src/admin/
git commit -m "feat(admin): route pgbouncer database to admin console"
```

---

## Phase 4: Multi-Database Routing

### Task 4.1: Add database routing table

**Files:**
- Modify: `scry-proxy/src/config/mod.rs`
- Create: `scry-proxy/src/routing/mod.rs`

**Step 1: Extend config for multiple databases**

```rust
// In config/mod.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub pool_size: Option<usize>,
}

// Add to Config
pub databases: Vec<DatabaseConfig>,
```

**Step 2: Create router**

```rust
// scry-proxy/src/routing/mod.rs

pub struct DatabaseRouter {
    routes: HashMap<String, DatabaseConfig>,
    default: Option<DatabaseConfig>,
}

impl DatabaseRouter {
    pub fn from_config(databases: &[DatabaseConfig], default_backend: &BackendConfig) -> Self {
        let mut routes = HashMap::new();
        for db in databases {
            routes.insert(db.name.clone(), db.clone());
        }

        let default = Some(DatabaseConfig {
            name: "*".to_string(),
            host: default_backend.host.clone(),
            port: default_backend.port,
            database: default_backend.database.clone(),
            user: default_backend.user.clone(),
            password: default_backend.password.clone(),
            pool_size: Some(default_backend.pool_size),
        });

        Self { routes, default }
    }

    pub fn route(&self, database_name: &str) -> Option<&DatabaseConfig> {
        self.routes.get(database_name).or(self.default.as_ref())
    }
}
```

**Step 3: Commit**

```bash
git add scry-proxy/src/routing/ scry-proxy/src/config/mod.rs
git commit -m "feat(routing): add multi-database routing table"
```

---

### Task 4.2: Create per-database connection pools

**Files:**
- Modify: `scry-proxy/src/proxy/server.rs`

**Step 1: Change pool from single to map**

```rust
// In ProxyServer

pools: HashMap<String, Arc<TcpConnectionPool>>,
pool_managers: HashMap<String, Arc<PoolManager>>,
router: DatabaseRouter,

// In new(), create pool per database
let mut pools = HashMap::new();
let mut pool_managers = HashMap::new();

for db in &config.databases {
    let pool = TcpConnectionPool::new(/* config from db */)?;
    let pm = PoolManager::new(Arc::clone(&pool), /* ... */);
    pools.insert(db.name.clone(), Arc::new(pool));
    pool_managers.insert(db.name.clone(), pm);
}
```

**Step 2: Route connections to correct pool**

```rust
// In handle_connection()

let database_name = startup_params.get("database").cloned().unwrap_or_default();
let db_config = self.router.route(&database_name)
    .ok_or_else(|| anyhow::anyhow!("Unknown database: {}", database_name))?;

let pool_manager = self.pool_managers.get(&db_config.name)
    .or_else(|| self.pool_managers.get("*"))  // fallback to default
    .cloned();
```

**Step 3: Commit**

```bash
git add scry-proxy/src/proxy/server.rs
git commit -m "feat(routing): route connections to per-database pools"
```

---

## Phase 5: UNIX Socket Support

### Task 5.1: Add UNIX socket listener

**Files:**
- Modify: `scry-proxy/src/config/mod.rs`
- Modify: `scry-proxy/src/proxy/server.rs`

**Step 1: Add socket config**

```rust
// In ProxyConfig

/// UNIX socket path (e.g., /var/run/scry/.s.PGSQL.6432)
pub unix_socket: Option<String>,
```

**Step 2: Create dual listener**

```rust
// In ProxyServer

#[cfg(unix)]
use tokio::net::UnixListener;

enum Listener {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix(UnixListener),
}

// In run(), accept from both
tokio::select! {
    result = tcp_listener.accept() => { /* handle TCP */ }
    #[cfg(unix)]
    result = unix_listener.accept() => { /* handle UNIX */ }
}
```

**Step 3: Commit**

```bash
git add scry-proxy/src/proxy/server.rs scry-proxy/src/config/mod.rs
git commit -m "feat(server): add UNIX socket listener support"
```

---

## Phase 6: Online Reload (SIGHUP)

### Task 6.1: Add signal handler for config reload

**Files:**
- Modify: `scry-proxy/src/proxy/server.rs`
- Modify: `scry-proxy/src/main.rs`

**Step 1: Write test for SIGHUP handling**

```rust
#[tokio::test]
#[cfg(unix)]
async fn test_sighup_reloads_config() {
    // Start server
    // Modify config file
    // Send SIGHUP
    // Verify config was reloaded
}
```

**Step 2: Add reload channel**

```rust
// In ProxyServer

reload_trigger: watch::Receiver<()>,
reload_sender: watch::Sender<()>,

// In main.rs
#[cfg(unix)]
{
    let reload_sender = server.reload_sender();
    tokio::spawn(async move {
        let mut sig = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::hangup()
        ).unwrap();

        loop {
            sig.recv().await;
            info!("Received SIGHUP, reloading config");
            let _ = reload_sender.send(());
        }
    });
}
```

**Step 3: Implement hot reload**

```rust
// In ProxyServer::run()

tokio::select! {
    // ... existing accept logic ...

    _ = self.reload_trigger.changed() => {
        if let Ok(new_config) = Config::load() {
            self.apply_config_reload(new_config).await;
        }
    }
}

async fn apply_config_reload(&mut self, new_config: Config) {
    // Reload auth file
    if let Some(ref auth_file) = new_config.auth.auth_file {
        if let Ok(auth) = FileAuthenticator::from_file(auth_file) {
            self.auth = Some(Arc::new(auth));
        }
    }

    // Update pool sizes (requires draining)
    // ...

    info!("Configuration reloaded");
}
```

**Step 4: Commit**

```bash
git add scry-proxy/src/proxy/server.rs scry-proxy/src/main.rs
git commit -m "feat(server): add SIGHUP config reload support"
```

---

## Phase 7: Integration Tests

### Task 7.1: Add comprehensive pooling integration tests

**Files:**
- Create: `scry-proxy/tests/pgbouncer_compat_test.rs`

```rust
//! PgBouncer compatibility integration tests

#[tokio::test]
async fn test_transaction_mode_releases_on_commit() { /* ... */ }

#[tokio::test]
async fn test_transaction_mode_releases_on_rollback() { /* ... */ }

#[tokio::test]
async fn test_transaction_mode_rejects_set() { /* ... */ }

#[tokio::test]
async fn test_hybrid_mode_pins_on_temp_table() { /* ... */ }

#[tokio::test]
async fn test_hybrid_mode_unpins_on_drop_temp() { /* ... */ }

#[tokio::test]
async fn test_state_replay_prepared_statements() { /* ... */ }

#[tokio::test]
async fn test_state_replay_session_variables() { /* ... */ }

#[tokio::test]
async fn test_admin_show_pools() { /* ... */ }

#[tokio::test]
async fn test_admin_pause_resume() { /* ... */ }

#[tokio::test]
async fn test_auth_file_authentication() { /* ... */ }

#[tokio::test]
async fn test_multi_database_routing() { /* ... */ }
```

**Commit:**

```bash
git add scry-proxy/tests/pgbouncer_compat_test.rs
git commit -m "test(pgbouncer): add PgBouncer compatibility tests"
```

---

### Task 7.2: Add pgbouncer.ini configuration test

**Files:**
- Add: `scry-proxy/tests/pgbouncer_config_test.rs`

```rust
#[test]
fn test_pgbouncer_ini_full_config() {
    // Load real pgbouncer.ini format
    // Verify all settings mapped correctly
}

#[test]
fn test_scry_overrides_pgbouncer_settings() {
    // Set SCRY_* and PGBOUNCER_* env vars
    // Verify SCRY takes precedence
}
```

**Commit:**

```bash
git add scry-proxy/tests/pgbouncer_config_test.rs
git commit -m "test(config): add pgbouncer.ini compatibility tests"
```

---

## Phase 8: Documentation

### Task 8.1: Update README with PgBouncer migration guide

**Files:**
- Modify: `README.md`

Add section:

```markdown
## Migrating from PgBouncer

Scry is a drop-in replacement for PgBouncer with enhanced observability.

### Using your existing pgbouncer.ini

```bash
# Point Scry at your existing config
SCRY_PGBOUNCER_CONFIG=/etc/pgbouncer/pgbouncer.ini ./scry
```

### Mapping PgBouncer Settings

| PgBouncer | Scry | Notes |
|-----------|------|-------|
| pool_mode = session | connection_pooling = session | |
| pool_mode = transaction | connection_pooling = transaction | |
| pool_mode = statement | connection_pooling = transaction | Closest equivalent |
| default_pool_size | pool_size | |
| max_client_conn | proxy.max_connections | |
| auth_file | auth.auth_file | Same format |

### New Scry Features

- **Hybrid pooling mode**: Smart state tracking, automatic pin/unpin
- **Query observability**: Per-query metrics, hot data detection
- **Circuit breaker**: Automatic failure isolation
- **Value fingerprinting**: Anonymized query analysis
```

**Commit:**

```bash
git add README.md
git commit -m "docs: add PgBouncer migration guide"
```

---

## Execution Summary

**Total Tasks:** 25 tasks across 8 phases
**Critical Path:** Phase 1 (PoolManager Integration) - must complete first
**Parallelizable:** Phases 2-6 can proceed in parallel after Phase 1

**Recommended Batch Size:** 3-5 tasks per session
**Review Checkpoints:** After each phase completion

Each task follows TDD:
1. Write failing test
2. Run test to confirm failure
3. Write minimal implementation
4. Run test to confirm pass
5. Commit

---

## Notes

**Unsupported PgBouncer Features:**
- `auth_type = hba` (pg_hba.conf support) - low priority
- `client_tls_sslmode = verify-full` with CRL - edge case
- Statement-level pooling - rarely used, complexity not justified

**Scry Advantages Over PgBouncer:**
- Hybrid pooling mode (smart state tracking)
- Per-query observability and metrics
- Hot data detection
- Circuit breaker and retry logic
- Written in Rust (memory safety, performance)
