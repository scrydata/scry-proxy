//! Secret-absence guardrail for `SHOW CONFIG` (WP-10, P4 §4.3/§5.5).
//!
//! `SHOW CONFIG` used to return 3 hardcoded canned rows. Task 3 makes it
//! reflect the REAL running `Config`, which means it now touches fields that
//! include backend/admin/publisher secrets. Leaking `backend.password`,
//! `admin.admin_password`, `publisher.http_api_key`, or
//! `publisher.anonymize_salt` here would be a Critical defect, so this suite
//! is the guardrail: it drives `SHOW CONFIG` with DISTINCTIVE secret values
//! and asserts none of them ever appear in the output, while real non-secret
//! configured values DO appear (proving the output is live, not canned) and
//! the secret keys are visibly present with a `<redacted>` marker (redaction,
//! not silent omission).
//!
//! Test harness: `AdminHandles::for_test_with_config` builds handles directly
//! from a `Config` with no live pools/backend needed — `SHOW CONFIG` only
//! reads `handles.config`, so this doesn't need a real Postgres container
//! (unlike `admin_truthfulness_test.rs`, which drives live client/server
//! registries that DO need one).

use scry::admin::{AdminConsole, AdminResponse};
use scry::config::Config;
use scry::observability::{HealthConfig, ProxyMetrics};
use scry::proxy::AdminHandles;
use std::sync::Arc;

/// Distinctive secret values: nothing resembling a real config default, so a
/// naive full-config dump (which would print these verbatim) is unambiguously
/// caught, and a correct redaction is unambiguously verified.
const SECRET_BACKEND_PW: &str = "SUPERSECRET_pw_42";
const SECRET_ADMIN_PW: &str = "SUPERSECRET_adminpw_99";
const SECRET_API_KEY: &str = "SUPERSECRET_apikey_77";
const SECRET_SALT: &str = "SUPERSECRET_salt_13";

/// Distinctive non-secret values, so "output is live" can't accidentally be
/// satisfied by a coincidental default (e.g. default host is "localhost").
const TRUTH_HOST: &str = "truth-config-host.example";
const TRUTH_POOL_SIZE: usize = 37;

fn secret_config() -> Config {
    let mut config = Config::default();
    config.backend.host = TRUTH_HOST.to_string();
    config.performance.pool_size = TRUTH_POOL_SIZE;
    config.backend.password = SECRET_BACKEND_PW.to_string();
    config.admin.enabled = true;
    config.admin.admin_password = Some(SECRET_ADMIN_PW.to_string());
    config.publisher.http_api_key = Some(SECRET_API_KEY.to_string());
    config.publisher.anonymize_salt = Some(SECRET_SALT.to_string());
    config
}

async fn show_config_rows(config: Config) -> (Vec<String>, Vec<Vec<String>>) {
    let handles = AdminHandles::for_test_with_config(config);
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let console = AdminConsole::new(handles, metrics);

    match console.execute("SHOW CONFIG").await.expect("SHOW CONFIG failed") {
        AdminResponse::RowSet { columns, rows } => (columns, rows),
        other => panic!("expected RowSet, got {other:?}"),
    }
}

fn flatten(rows: &[Vec<String>]) -> String {
    rows.iter().flat_map(|r| r.iter()).cloned().collect::<Vec<_>>().join("\u{1}")
}

fn value_for_key(columns: &[String], rows: &[Vec<String>], key: &str) -> String {
    let key_col =
        columns.iter().position(|c| c == "key").expect("SHOW CONFIG missing 'key' column");
    let value_col =
        columns.iter().position(|c| c == "value").expect("SHOW CONFIG missing 'value' column");
    rows.iter()
        .find(|r| r[key_col] == key)
        .unwrap_or_else(|| panic!("SHOW CONFIG missing row for key {key}"))[value_col]
        .clone()
}

/// The core guardrail: real values are live, secrets are never present in any
/// cell, and every secret key is visibly redacted (not just omitted).
#[tokio::test]
async fn show_config_never_leaks_secrets_but_reflects_real_config() {
    let (columns, rows) = show_config_rows(secret_config()).await;
    let blob = flatten(&rows);

    // (a) Real, non-secret configured values appear — proving the output is
    // live, not the old canned 3-row response.
    assert!(blob.contains(TRUTH_HOST), "SHOW CONFIG did not reflect real backend.host: {blob}");
    assert!(
        blob.contains(&TRUTH_POOL_SIZE.to_string()),
        "SHOW CONFIG did not reflect real performance.pool_size: {blob}"
    );

    // (b) No distinctive secret string appears anywhere in the output.
    for secret in [SECRET_BACKEND_PW, SECRET_ADMIN_PW, SECRET_API_KEY, SECRET_SALT] {
        assert!(!blob.contains(secret), "SHOW CONFIG leaked secret '{secret}': {blob}");
    }

    // (c) The secret keys ARE present, with value `<redacted>` (redaction is
    // visible, not an omission that could be confused with "not configured").
    for key in [
        "backend.password",
        "admin.admin_password",
        "publisher.http_api_key",
        "publisher.anonymize_salt",
    ] {
        assert_eq!(
            value_for_key(&columns, &rows, key),
            "<redacted>",
            "expected <redacted> for {key} in: {blob}"
        );
    }
}

/// Absent optional secrets must render as an honest "<unset>" (presence
/// signal), never the redaction placeholder (which would falsely imply a
/// secret is configured) and never `None`/empty pretending to be a value.
#[tokio::test]
async fn show_config_shows_unset_for_absent_optional_secrets() {
    // Config::default() has no admin_password/http_api_key/anonymize_salt and
    // an empty backend.password.
    let (columns, rows) = show_config_rows(Config::default()).await;

    for key in ["admin.admin_password", "publisher.http_api_key", "publisher.anonymize_salt"] {
        assert_eq!(value_for_key(&columns, &rows, key), "<unset>", "expected <unset> for {key}");
    }
    assert_eq!(
        value_for_key(&columns, &rows, "backend.password"),
        "<unset>",
        "expected <unset> for an empty backend.password"
    );
}
