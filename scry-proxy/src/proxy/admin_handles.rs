//! Shared operational state for the admin console (WP-10, P4 §4.1).
//!
//! [`AdminHandles`] is the single `Arc`-shared bundle of everything the admin
//! console needs to *report* real proxy state and *control* the process. It is
//! constructed once in [`ProxyServer::new`](super::ProxyServer) and threaded
//! into each admin connection. This module is FOUNDATION/plumbing: later WP-10
//! tasks (SHOW CLIENTS/SERVERS/CONFIG, RELOAD, SHUTDOWN) consume these handles;
//! nothing here changes command behavior yet.
//!
//! Design constraints (see task brief):
//! - Registration is O(1) bookkeeping at connect/disconnect. The proxied byte
//!   stream and per-query hot path are untouched — no per-query locking.
//! - No false state: a [`ClientEntry`] exists iff the connection is live
//!   (register once identity is known, deregister on every exit path via the
//!   connection's RAII guard).

use crate::config::Config;
use crate::proxy::PoolManager;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::watch;
use tokio::task::AbortHandle;

/// Coarse lifecycle state of a client connection.
///
/// Deliberately coarse for 1.0: it is set at connect and can be cheaply nudged
/// at natural boundaries, but is never updated per-query (that would add
/// hot-path locking against the <1ms latency budget).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientState {
    /// Handshake/startup completed; connection is serving (or idle between)
    /// application queries. The common resting state for a regular client.
    Active,
    /// A client attached to the virtual admin (`pgbouncer`) console.
    Admin,
}

/// A single live client connection, as tracked by the [`ClientRegistry`].
#[derive(Debug, Clone)]
pub struct ClientEntry {
    /// Monotonic per-process connection id (matches server logs/metrics).
    pub conn_id: u64,
    /// Client socket address (`ip:port`), or `"unix"` for UNIX-socket clients.
    pub addr: String,
    /// Startup-message user, or empty if the startup was not parsed (e.g. a
    /// UNIX client whose startup is read later by the connection handler).
    pub user: String,
    /// Startup-message database, or empty (same caveat as `user`).
    pub database: String,
    /// Coarse lifecycle state (see [`ClientState`]).
    pub state: ClientState,
    /// When the connection was registered.
    pub connect_time: Instant,
    /// Last time the state was touched. For 1.0 this equals `connect_time`
    /// unless a caller cheaply updates it at a natural boundary.
    pub last_request_time: Instant,
    /// Whether the client transport was upgraded to TLS.
    pub tls: bool,
}

/// `Arc`-shared registry of live client connections.
///
/// Keyed by `conn_id`. Insert on connect (once identity is known), remove on
/// disconnect. All operations take a short write/read lock and touch a single
/// `HashMap` slot — O(1), off the per-query hot path.
#[derive(Debug, Default)]
pub struct ClientRegistry {
    inner: RwLock<HashMap<u64, ClientEntry>>,
    /// Per-connection task cancellation handles, keyed by `conn_id` (WP-10 Task
    /// 5 KILL). Populated at connection spawn (before identity is known, so it
    /// is a *separate* map from `inner`, whose entry is only inserted once the
    /// startup message is parsed). Removed on the same drop path as `inner`, so
    /// it can never outlive its task. `KILL [db]` reads `inner` to find which
    /// connections match a database, then aborts their handles from here.
    abort_handles: RwLock<HashMap<u64, AbortHandle>>,
}

impl ClientRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a live connection. Idempotent per `conn_id` (last write wins).
    pub fn register(&self, entry: ClientEntry) {
        self.inner.write().insert(entry.conn_id, entry);
    }

    /// Store the task [`AbortHandle`] for a connection so `KILL` can cancel it
    /// (WP-10 Task 5). Called at spawn time, before the connection's identity
    /// (and thus its [`ClientEntry`]) is known. O(1), off the hot path.
    pub fn register_abort_handle(&self, conn_id: u64, handle: AbortHandle) {
        self.abort_handles.write().insert(conn_id, handle);
    }

    /// Remove a connection. No-op if it was never registered, so it is always
    /// safe to call from a drop guard regardless of how far startup got. Also
    /// drops the connection's abort handle (Task 5), keeping the two maps in
    /// lockstep on every exit path (clean close, error, terminate, KILL abort).
    pub fn deregister(&self, conn_id: u64) {
        self.inner.write().remove(&conn_id);
        self.abort_handles.write().remove(&conn_id);
    }

    /// `KILL [db]`: forcibly disconnect every live client whose startup
    /// `database` matches `db` (case-insensitive). Aborts each matching
    /// connection's task (closing its client socket) AND removes its registry
    /// entry + abort handle synchronously, so the registry never lists a
    /// killed connection even for the brief window before the aborted task's
    /// drop guard runs. Returns the `conn_id`s that were killed.
    ///
    /// The two maps are locked sequentially (never nested) here and everywhere
    /// else, so this can't deadlock against a concurrent `deregister`.
    pub fn kill_by_database(&self, db: &str) -> Vec<u64> {
        // 1. Find matching connections (read lock on entries only).
        let matched: Vec<u64> = {
            let entries = self.inner.read();
            entries
                .values()
                .filter(|e| e.database.eq_ignore_ascii_case(db))
                .map(|e| e.conn_id)
                .collect()
        };
        if matched.is_empty() {
            return matched;
        }
        // 2. Abort each task and drop its handle (write lock on handles only).
        {
            let mut handles = self.abort_handles.write();
            for id in &matched {
                if let Some(handle) = handles.remove(id) {
                    handle.abort();
                }
            }
        }
        // 3. Remove the entries (write lock on inner only), so a follow-up
        //    SHOW CLIENTS / snapshot can't still see them.
        {
            let mut entries = self.inner.write();
            for id in &matched {
                entries.remove(id);
            }
        }
        matched
    }

    /// Best-effort update of a live entry's coarse state and `last_request_time`.
    ///
    /// Intended for natural boundaries only (not per-query). No-op if the entry
    /// is gone.
    pub fn touch(&self, conn_id: u64, state: ClientState) {
        if let Some(entry) = self.inner.write().get_mut(&conn_id) {
            entry.state = state;
            entry.last_request_time = Instant::now();
        }
    }

    /// Number of live connections.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Whether there are no live connections.
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Snapshot of all live entries (cloned), for a future `SHOW CLIENTS`.
    pub fn snapshot(&self) -> Vec<ClientEntry> {
        self.inner.read().values().cloned().collect()
    }

    /// Look up a single live entry by id.
    pub fn get(&self, conn_id: u64) -> Option<ClientEntry> {
        self.inner.read().get(&conn_id).cloned()
    }
}

/// Pool-level snapshot of a configured backend, for a future `SHOW SERVERS`.
///
/// Granularity note: this is **pool-level**, not per-socket. deadpool does not
/// expose stable per-connection handles, so for 1.0 we surface each pool's
/// aggregate `status()` plus backend identity rather than a row per backend
/// socket. A full per-socket backend registry is a documented nice-to-have.
#[derive(Debug, Clone)]
pub struct ServerPoolSnapshot {
    /// Database key for this pool (`"*"` is the default pool).
    pub database: String,
    /// Backend address (`host:port`) this pool dials.
    pub backend_addr: String,
    /// Wire protocol name (e.g. `"postgres"`).
    pub protocol: &'static str,
    /// Current number of connections held by the pool.
    pub size: usize,
    /// Currently idle/available connections.
    pub available: usize,
    /// Configured max pool size.
    pub max_size: usize,
}

/// Registry exposing per-backend/pool state for a future `SHOW SERVERS`.
///
/// Reads live through the existing [`PoolManager::status`] surface — it does
/// not mirror or cache deadpool internals, so it can never drift from reality.
pub struct ServerRegistry {
    pool_managers: HashMap<String, Arc<PoolManager>>,
}

impl ServerRegistry {
    /// Build a registry over the server's per-database pool managers.
    pub fn new(pool_managers: HashMap<String, Arc<PoolManager>>) -> Self {
        Self { pool_managers }
    }

    /// Live snapshot of every configured pool's status.
    pub fn snapshot(&self) -> Vec<ServerPoolSnapshot> {
        self.pool_managers
            .iter()
            .map(|(database, pm)| {
                let status = pm.pool().status();
                ServerPoolSnapshot {
                    database: database.clone(),
                    backend_addr: status.backend_addr,
                    protocol: status.protocol,
                    size: status.size,
                    available: status.available,
                    max_size: status.max_size,
                }
            })
            .collect()
    }

    /// Whether any pools are configured (pooling may be disabled).
    pub fn is_empty(&self) -> bool {
        self.pool_managers.is_empty()
    }
}

/// The shared operational state threaded into every admin connection.
///
/// Constructed once in [`ProxyServer::new`](super::ProxyServer); cloned as an
/// `Arc` into each connection spawn and handed to
/// [`AdminConsole::new`](crate::admin::AdminConsole). The concrete field set is
/// the interface later WP-10 tasks depend on.
pub struct AdminHandles {
    /// Per-database pool managers (`"*"` is the default). Clone of the server's
    /// map; used for `SHOW POOLS`/`SHOW SERVERS` and PAUSE/RESUME routing.
    pub pool_managers: HashMap<String, Arc<PoolManager>>,
    /// Sender for the config-reload channel (Task 4 RELOAD). Carried for now.
    pub reload_sender: watch::Sender<()>,
    /// Programmatic shutdown trigger. `run()` selects on a subscriber of this
    /// alongside the OS signals, so a future admin SHUTDOWN (Task 5) can
    /// initiate the same graceful drain. `send(true)` starts the drain.
    pub shutdown_trigger: watch::Sender<bool>,
    /// The single config-reload function (WP-10 Task 4 RELOAD). Both the SIGHUP
    /// path (`ProxyServer::apply_config_reload`) and admin `RELOAD` call this
    /// exact closure, so the two can never drift. Returns the real error on a
    /// failed reload (auth_file read/parse failure) so `RELOAD` can report an
    /// honest `ErrorResponse` instead of a false `CommandComplete`. Scope is
    /// auth_file-only (documented limitation).
    pub reload_fn: Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync>,
    /// The effective configuration (Task 3 SHOW CONFIG).
    pub config: Arc<Config>,
    /// Live client-connection registry (Task 2 SHOW CLIENTS).
    pub client_registry: Arc<ClientRegistry>,
    /// Backend/pool registry (Task 2 SHOW SERVERS).
    pub server_registry: Arc<ServerRegistry>,
    /// Drain-completion signal (Task 5 `SHUTDOWN WAIT`). `run()` sets this to
    /// `true` via [`Self::signal_drain_complete`] once `drain_connections`
    /// returns (all connections drained or force-aborted). `SHUTDOWN WAIT`
    /// subscribes via [`Self::subscribe_drain_complete`] and blocks until it
    /// flips, so the admin command can report completion rather than a false
    /// immediate success. Created internally (never wired from an OS signal).
    drain_complete: watch::Sender<bool>,
}

impl AdminHandles {
    /// Assemble the shared handles. Builds the client/server registries from
    /// the supplied pool managers.
    pub fn new(
        config: Arc<Config>,
        pool_managers: HashMap<String, Arc<PoolManager>>,
        reload_sender: watch::Sender<()>,
        shutdown_trigger: watch::Sender<bool>,
        reload_fn: Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync>,
    ) -> Arc<Self> {
        let client_registry = Arc::new(ClientRegistry::new());
        let server_registry = Arc::new(ServerRegistry::new(pool_managers.clone()));
        // Seeded `false`; `run()` flips it to `true` when the drain finishes.
        let (drain_complete, _drain_rx) = watch::channel(false);
        Arc::new(Self {
            pool_managers,
            reload_sender,
            shutdown_trigger,
            reload_fn,
            config,
            client_registry,
            server_registry,
            drain_complete,
        })
    }

    /// The default (`"*"`) pool manager, if pooling is enabled. Preserves the
    /// pre-WP-10 `AdminConsole` behavior where the console operated on the
    /// default pool.
    pub fn default_pool_manager(&self) -> Option<Arc<PoolManager>> {
        self.pool_managers.get("*").cloned()
    }

    /// Signal a programmatic graceful shutdown (Task 5 SHUTDOWN semantics).
    ///
    /// Returns `false` if `run()` is no longer listening (already shutting
    /// down / receiver dropped).
    pub fn trigger_shutdown(&self) -> bool {
        self.shutdown_trigger.send(true).is_ok()
    }

    /// Announce that the graceful drain has finished (Task 5 `SHUTDOWN WAIT`).
    /// Called by `run()` immediately after `drain_connections` returns. Idempotent
    /// and infallible: the sender lives for the whole process, so there is always
    /// at least itself keeping the channel open.
    pub fn signal_drain_complete(&self) {
        let _ = self.drain_complete.send(true);
    }

    /// Subscribe to the drain-completion signal (Task 5 `SHUTDOWN WAIT`). The
    /// returned receiver observes `true` once (and stays `true`) after the drain
    /// completes. Subscribe BEFORE calling [`Self::trigger_shutdown`] so the
    /// completion transition can never be missed.
    pub fn subscribe_drain_complete(&self) -> watch::Receiver<bool> {
        self.drain_complete.subscribe()
    }

    /// Minimal handles for unit/integration tests: default config, no pools,
    /// throwaway reload/shutdown channels.
    #[doc(hidden)]
    pub fn for_test() -> Arc<Self> {
        Self::for_test_with_config(Config::default())
    }

    /// Like [`Self::for_test`] but with a caller-supplied config (e.g. to set
    /// `config.admin` for admin-handshake tests).
    #[doc(hidden)]
    pub fn for_test_with_config(config: Config) -> Arc<Self> {
        let (reload_sender, _reload_rx) = watch::channel(());
        let (shutdown_trigger, _shutdown_rx) = watch::channel(false);
        // Test handles have no real reload seam; a no-op that always succeeds.
        let reload_fn: Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync> = Arc::new(|| Ok(()));
        Self::new(Arc::new(config), HashMap::new(), reload_sender, shutdown_trigger, reload_fn)
    }
}
