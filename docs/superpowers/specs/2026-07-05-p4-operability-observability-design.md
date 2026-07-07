# P4 — Operability & Observability (Pillar Spec)

**Status:** Draft for review
**Date:** 2026-07-05
**Pillar:** P4 of the [Production-Readiness Vision](./2026-07-05-scry-production-readiness-vision.md)
**Phase:** Complete (primary); admin-auth stub + config validation land in Harden

---

## 1. Purpose & scope

Make scry something an operator can run with confidence: the admin console tells the truth and can actually control the proxy, configuration is validated loudly and matches its documentation, the metrics/tracing surface is complete and access-controlled, and every operational claim in the docs is backed by code. The recurring failure mode this pillar closes is **surfaces that report success while doing nothing** — placeholder `SHOW` output, no-op control commands, and documented config that silently has no effect.

In scope:
- Admin console: real `SHOW` data and functional control commands (PAUSE/RESUME/RELOAD/SHUTDOWN/KILL).
- Config: schema validation, unsafe/unknown-setting rejection, and docs/code parity.
- Metrics/health HTTP surface: access control, safe defaults, dead-feature cleanup, completeness.
- Observability: metrics/tracing/OTel coverage of the signals operators need (including P3's new signals).

Out of scope (cross-referenced):
- Admin console **authentication mechanism** → **P1** (P4 tests that commands are gated and implements what runs behind the gate).
- The shutdown/reload/drain/pause **mechanisms** the admin commands invoke → **P3**.
- Image/deploy provenance → **P7**.

---

## 2. Current state (evidence)

From the 2026-07-05 admin/metrics/config audit and targeted reads.

**Solid foundation — observability primitives:** `ProxyMetrics` uses HDR histograms with lock-free counters and exposes query latency (total/queue/pool-acquire/backend), pool metrics, active/rejected connections, circuit-breaker metrics, health monitor, and uptime (`scry-proxy/src/observability/metrics.rs:28-234`). The `/debug/hotdata` endpoint deliberately exposes only blake3 fingerprints, never raw query text (`observability/hot_data.rs:1-9`, `metrics_server.rs:152-162`). Safe bind defaults: proxy `127.0.0.1:5433`, metrics `127.0.0.1:9090` (`config/mod.rs:387,407`). No secrets logged at startup (`main.rs:28-33`).

**The gaps — surfaces that don't do what they appear to:**

| Sev | Gap | Location |
|-----|-----|----------|
| HIGH | **All admin control commands are no-op stubs** returning success — PAUSE/RESUME/RELOAD/SHUTDOWN/KILL each `// TODO` and return a `CommandComplete` tag while doing nothing | `admin/commands.rs:371-394` |
| HIGH | **`SHOW` commands return hardcoded placeholder data**, not live state: `SHOW DATABASES` always `default/localhost/5432/postgres` (`commands.rs:243-258`), `SHOW CLIENTS`/`SHOW SERVERS` always empty (`:279-329`), `SHOW CONFIG` three fixed rows (`:339-369`). An operator cannot trust the console to reflect reality | `admin/commands.rs:243-369` |
| MED | **Docs reference `SCRY_BACKEND__PASSWORD_FILE`** which no config field implements; the env var is silently ignored and the backend password falls back to the default. Emblematic of unguarded docs/code drift | `docs/deployment.md:120` vs `config/mod.rs:75` |
| MED | **Metrics/health/debug HTTP endpoints have no access control** — only a `TraceLayer`. Safe by default bind, but docs recommend `0.0.0.0`, which exposes all `/debug/*` endpoints unauthenticated | `metrics_server.rs:59-66`; `docs/deployment.md` |
| MED | **`SHOW CONFIG` will leak secrets once made real** — expanding it to the full config must redact `backend.password`/`http_api_key` (shares P1's redaction work) | `admin/commands.rs:339-369` |
| LOW | **`tower-http` `cors` feature enabled but no `CorsLayer` applied** — dead surface (no misconfig today, but should be removed or intentionally configured) | `Cargo.toml:68`, `metrics_server.rs:65` |
| LOW | **Config `validate()` only warns** on bad pool/queue ratios; unknown/unsafe settings are not rejected | `config/mod.rs:525-567` |

Related: `RELOAD`/`SHUTDOWN`/`KILL` need the real mechanisms P3 owns (SIGHUP reload exists at `proxy/mod.rs:54-79`; drain at `server.rs:838-895`); PAUSE/RESUME/KILL need connection/pool registries that also back real `SHOW` output.

---

## 3. Target / 1.0 exit criteria

1. **The admin console tells the truth.** `SHOW POOLS/STATS/DATABASES/CLIENTS/SERVERS/CONFIG` return live state from real registries; secrets are redacted in `SHOW CONFIG`.
2. **The admin console can control the proxy.** PAUSE/RESUME/RELOAD/SHUTDOWN/KILL perform their action (via P3 mechanisms) or return an honest error — never a false success. All gated behind P1 admin authentication.
3. **Config is validated and honest.** Unsafe or unknown settings are rejected at load (fail-closed, shared with P1's `Config::validate()`); every documented `SCRY_*` variable maps to a real field, enforced by an automated drift check.
4. **The metrics surface is safe and complete.** Access is controlled or documented trusted-network-only with safe defaults enforced; `/debug/*` cannot be exposed unauthenticated by following the docs; dead CORS surface removed; metrics cover P3's breaker/timeout/shed signals and the P5 latency budget.
5. **Observability is operator-grade.** OTel tracing spans cover the connection/query lifecycle; structured logs never carry raw SQL/secrets (shared with P1's log hygiene).

---

## 4. Design

### 4.1 Live operational registries (`proxy/`, `admin/`)
Introduce (or expose) the shared registries the console reads: a **client-connection registry** (addr, user, database, state, connect/request time, TLS) and a **server/backend registry** (pool membership, state, backend identity), plus the existing pool and per-backend metrics. `SHOW CLIENTS/SERVERS/DATABASES/POOLS/STATS` read from these instead of placeholders (`commands.rs:243-329`). This registry is the single source of truth shared by the console and the metrics endpoints, so they never disagree.

### 4.2 Functional control commands (`admin/commands.rs` → P3 mechanisms)
Replace the no-op stubs (`commands.rs:371-394`) with real actions:
- **PAUSE/RESUME `[db]`** — flip a per-database/pool accepting-new-work flag in the pool manager; PAUSE lets in-flight transactions finish, RESUME re-enables acquisition.
- **RELOAD** — invoke the existing config-reload path (`proxy/mod.rs:54-79`), returning a real error if reload fails validation.
- **SHUTDOWN `[WAIT]`** — invoke P3's drain mechanism (`server.rs:838-895`); honor the `WAIT` argument (block until drained vs. return immediately).
- **KILL `[db]`** — terminate tracked connections for a database via the registry.
Every command returns an **honest result**: success only on success, an `ErrorResponse` otherwise. No command may report success while doing nothing.

### 4.3 `SHOW CONFIG` redaction (`admin/commands.rs`)
When `SHOW CONFIG` is expanded to reflect the real config, route secret-bearing fields (`backend.password`, `publisher.http_api_key`) through the same redaction used by P1's redacting `Debug`, emitting `<redacted>`.

### 4.4 Config validation & docs/code parity (`config/mod.rs`)
- **Reject, don't warn.** Escalate unsafe configurations (invalid pool/queue ratios, and the P1 security-relevant ones) from warnings (`config/mod.rs:525-567`) to hard `validate()` errors where they are genuinely unsafe.
- **Unknown-key handling.** Detect unknown `SCRY_*` variables / unknown config keys and fail (or at minimum warn loudly with the offending key) so a typo'd or unimplemented setting like `SCRY_BACKEND__PASSWORD_FILE` cannot silently no-op.
- **Implement or remove `password_file`.** Either add real `*_file` secret-loading for the backend password (consistent with `SHADOW_ID_FILE` at `publisher/mod.rs:30-31`) or remove the doc reference. Recommendation: implement `password_file` — file-based secrets are an enterprise expectation.

### 4.5 Metrics/health surface (`observability/metrics_server.rs`, `main.rs`)
- **Access control.** Add optional bearer-token/basic auth for the metrics/health/debug endpoints, and/or split `/debug/*` (richer, sensitive-ish) from `/metrics` + `/health` (scrape/liveness) so the debug surface can be disabled or separately gated. Keep the safe `127.0.0.1` default; make binding to a non-loopback address require an explicit acknowledgement (mirrors P1's TLS/trust opt-in stance) and fix the `0.0.0.0` doc guidance.
- **Remove dead CORS.** Drop the unused `cors` feature (`Cargo.toml:68`) or apply an intentional restrictive `CorsLayer` (`metrics_server.rs:65`).
- **Completeness.** Ensure metrics expose the P3 signals (per-backend breaker state, timeout/shed counts) and the P5 latency-budget percentiles, so the roadmap's other pillars are observable.

### 4.6 Tracing & structured logging (`observability/`)
Ensure OTel spans cover the connection accept → auth → query → response lifecycle with useful attributes (pooling mode, backend id, durations) and that structured logs are safe by construction (raw SQL/secrets gated behind P1's `unsafe_debug_logging` flag). This makes production incidents diagnosable without leaking data.

---

## 5. Automated Auditor (mandatory)

All join the unified CI drift-guard.

1. **Docs/code config-parity check (the signature P4 guardrail).** A test that parses the documented `SCRY_*` variables (and config keys) out of `docs/` and asserts each maps to a real field in the config schema, and vice-versa (every field is documented). This is exactly the check that would have caught `SCRY_BACKEND__PASSWORD_FILE`. Fails CI on any drift.
2. **Config-validation tests.** Assert that unsafe and unknown settings are rejected (not merely warned), shared with P1's `Config::validate()` matrix.
3. **Admin truthfulness tests.** For each `SHOW` command, drive the proxy into a known state (e.g. open N client connections, pause a pool) and assert the console output reflects that real state — a regression guard against reintroducing placeholder data.
4. **Admin command-effect tests.** Assert PAUSE actually stops new acquisition, RESUME restores it, RELOAD applies new config, SHUTDOWN drains, KILL terminates — and that each returns an honest error on failure. Gated-behind-auth is asserted with P1's admin-auth test.
5. **Metrics-surface test.** Assert `/debug/*` is not reachable unauthenticated when bound to a non-loopback address, and that `SHOW CONFIG`/config dumps never contain secret values.

---

## 6. Test strategy

- **Integration (testcontainers + real PG):** admin truthfulness and command-effect tests (§5.3–§5.4) against a running proxy; metrics-endpoint access-control tests.
- **Unit:** config validation matrix and the docs/code parity parser (§5.1–§5.2); `SHOW CONFIG` redaction.
- **Regression:** keep placeholder-data from returning (the truthfulness tests fail if any `SHOW` handler goes static again).

---

## 7. Phasing

- **Harden (Phase 1):** admin-authentication gate (with P1) so no unauthenticated admin session exists even before commands are real; config validation escalated to reject unsafe settings; the docs/code parity guardrail (§5.1) — cheap, high-leverage, and it immediately fixes the `PASSWORD_FILE` class.
- **Complete (Phase 2):** live registries (§4.1), functional control commands (§4.2), `SHOW CONFIG` redaction (§4.3), `password_file` (§4.4), metrics access control + CORS cleanup + completeness (§4.5), tracing coverage (§4.6), and the remaining auditors.

---

## 8. Cross-pillar dependencies

- **P1 (Security):** owns admin-console authentication and the redacting-`Debug` used by `SHOW CONFIG` and `validate()`'s security checks; P4 builds the functional console behind that gate and shares the `Config::validate()` surface.
- **P3 (Reliability):** provides the drain/reload/pause/kill mechanisms PAUSE/RESUME/RELOAD/SHUTDOWN/KILL invoke; P4 must expose breaker/timeout/shed metrics for P3 to be observable, and the parity check enforces P3's doc/code reconciliation.
- **P5 (Performance):** the latency-budget percentiles are surfaced through P4's metrics; P4's registries add per-connection bookkeeping whose overhead P5 measures.
- **P6 (Compatibility):** `SHOW`/admin output should stay PgBouncer-console-compatible where practical (column sets already mirror PgBouncer).

---

## 9. Open questions

1. **`password_file`: implement or remove?** Recommendation: implement generic `*_file` secret loading (enterprise expectation), which also removes the doc/code drift.
2. **Metrics auth model.** Bearer token vs. basic auth vs. split-and-disable `/debug/*`. Recommendation: split `/metrics`+`/health` (unauthed, loopback/scrape) from `/debug/*` (opt-in, gated), plus a non-loopback-bind acknowledgement flag.
3. **Unknown-key strictness.** Hard-reject unknown `SCRY_*` keys vs. warn-loudly. Hard-reject is safest but risks friction with forward/back-compat config; recommendation: reject unknown keys under a strict-config default, with an escape hatch.
4. **Console realism vs. PgBouncer compatibility.** How closely must `SHOW` column sets/semantics match PgBouncer for drop-in tooling? Coordinate the compatibility surface with P6.
