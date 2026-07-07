//! Docs/code parity guardrail (P4 §5.1).
//!
//! Every `SCRY_*` environment variable documented under `docs/` must map to a
//! real configuration field. This catches the class of bug where docs advertise
//! a knob (e.g. `SCRY_BACKEND__PASSWORD_FILE`) that the code never reads, so it
//! would silently no-op — the exact failure P4 exists to kill.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

/// Extract every `SCRY_<NAME>` token from a blob of text.
fn extract_scry_vars(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while let Some(pos) = text[i..].find("SCRY_") {
        let start = i + pos;
        let mut end = start;
        while end < bytes.len() {
            let c = bytes[end];
            if c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_' {
                end += 1;
            } else {
                break;
            }
        }
        out.push(text[start..end].to_string());
        i = end;
    }
    out
}

/// Map a `SCRY_SECTION__FIELD` variable to its dotted config path
/// (`section.field`).
fn var_to_path(var: &str) -> String {
    var.strip_prefix("SCRY_").unwrap_or(var).to_lowercase().replace("__", ".")
}

fn docs_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR is the scry package dir; docs live at the repo root.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..").join("docs")
}

fn collect_documented_vars() -> BTreeSet<String> {
    let mut vars = BTreeSet::new();
    let dir = docs_dir();
    let entries = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("failed to read docs dir {}: {e}", dir.display()));
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("md") {
            continue;
        }
        let text = fs::read_to_string(&path).unwrap();
        for v in extract_scry_vars(&text) {
            vars.insert(v);
        }
    }
    vars
}

#[test]
fn every_documented_scry_var_maps_to_a_real_config_field() {
    // Meta variables that configure loading itself, not the Config schema.
    const META: &[&str] = &["SCRY_CONFIG_FILE", "SCRY_ALLOW_UNKNOWN_KEYS"];

    let valid = scry::config::valid_config_paths();
    let documented = collect_documented_vars();

    let mut orphans = Vec::new();
    for var in &documented {
        if META.contains(&var.as_str()) {
            continue;
        }
        let path = var_to_path(var);
        // Bare "SCRY_" mentioned in prose (the env-var prefix itself), not a var.
        if path.is_empty() {
            continue;
        }
        // `databases` is a dynamically-sized Vec; its element paths aren't in
        // the default schema.
        if path == "databases" || path.starts_with("databases.") {
            continue;
        }
        if !valid.contains(&path) {
            orphans.push(var.clone());
        }
    }

    assert!(
        orphans.is_empty(),
        "These SCRY_* variables are documented but do not map to any config field \
         (docs/code parity violation, P4 §5.1):\n  {}\n\nEither implement the field or \
         correct the docs so parity holds.",
        orphans.join("\n  ")
    );
}
