//! Integration test for the WP-10 (P4 §4.1) client registry plumbing.
//!
//! Proves the invariant the admin console will rely on: an entry exists in the
//! `ClientRegistry` iff the client connection is live. Open N connections
//! through the proxy -> expect N entries with correct user/database/addr/tls;
//! disconnect them -> expect the registry to drain back to 0.
mod common;

use common::{connect_client, create_test_config, start_test_proxy_with_handles, TestPublisher};
use scry::config::PoolingStrategy;
use scry::publisher::EventPublisher;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::{clients::Cli, RunnableImage};
use testcontainers_modules::postgres::Postgres;
use tokio::time::sleep;

#[tokio::test]
async fn client_registry_tracks_live_connections() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);

    let config =
        create_test_config("127.0.0.1".to_string(), postgres_port, PoolingStrategy::Session);
    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let (proxy_port, handles) =
        start_test_proxy_with_handles(config.clone(), publisher).await.unwrap();

    // Let the listener come up.
    sleep(Duration::from_millis(300)).await;
    assert_eq!(handles.client_registry.len(), 0, "registry should start empty");

    // Open N live connections through the proxy.
    let n = 3usize;
    let mut clients = Vec::new();
    for _ in 0..n {
        let c = connect_client(
            "127.0.0.1",
            proxy_port,
            &config.backend.user,
            &config.backend.password,
            &config.backend.database,
        )
        .await
        .expect("client should connect through proxy");
        clients.push(c);
    }

    // Registration is best-effort bookkeeping at connect; give it a moment.
    let mut waited = 0;
    while handles.client_registry.len() < n && waited < 50 {
        sleep(Duration::from_millis(100)).await;
        waited += 1;
    }

    let entries = handles.client_registry.snapshot();
    assert_eq!(entries.len(), n, "expected {n} live client entries, got {}", entries.len());
    for e in &entries {
        assert_eq!(e.user, config.backend.user, "entry user should match startup user");
        assert_eq!(e.database, config.backend.database, "entry database should match startup db");
        assert!(!e.tls, "plain TCP connections must record tls=false");
        assert!(
            e.addr.starts_with("127.0.0.1"),
            "addr should be the client socket, got {}",
            e.addr
        );
    }

    // Disconnect every client and prove the registry drains to zero (dereg on
    // every exit path).
    drop(clients);
    let mut waited = 0;
    while !handles.client_registry.is_empty() && waited < 50 {
        sleep(Duration::from_millis(100)).await;
        waited += 1;
    }
    assert_eq!(
        handles.client_registry.len(),
        0,
        "every entry must be removed once its connection closes"
    );
}
