# scry-proxy Production-Readiness (1.0 GA) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take `scry-proxy` from pre-1.0 to enterprise-signable 1.0 GA by implementing all seven pillar specs in the order the [production-readiness vision](../specs/2026-07-05-scry-production-readiness-vision.md) sequences them, with each guarantee fenced by an automated CI guardrail.

**Architecture:** Work proceeds through the vision's **three release gates** — Phase 1 **Harden** (fail-closed), Phase 2 **Complete** (fill the gaps), Phase 3 **Prove** (enterprise bar). Within each gate, pillar-slices are ordered by dependency. Every pillar-slice lands its fix *and* its guardrail together, and all guardrails accrete into a single CI **drift-guard** workflow that must stay green.

**Tech Stack:** Rust (edition 2021), Tokio, `rustls`/`tokio-rustls`, `sqlparser`, `blake3`, `subtle`, `deadpool-postgres`, `axum`, `proptest`, `criterion`, `testcontainers` + real Postgres, `cargo audit`/`cargo deny`/`cargo-semver-checks`, GitHub Actions, cosign, CycloneDX SBOM. Sister crate `scry-protocol` (`../scry-protocol`, published on crates.io) owns the event wire contract.

## Global Constraints

- **Fail closed.** A misconfiguration degrades to *refusal* (startup error), never to *no protection*. "Falling back to trust" is a defect. (Vision §4.1)
- **Transparent proxy.** Forwarded client/backend result bytes are never rewritten; all protocol parsing is observational. This invariant (`proxy/connection.rs` passthrough sites) must be preserved by every change. (Vision §4.3, P2 §2)
- **Observability is best-effort.** Capturing/anonymizing/publishing an observation must never block, slow, or fail a client query. Under pressure, drop observations, never queries. (Vision §4.4)
- **Self-auditing.** A pillar-slice is "done" only when fixed *and* fenced by a CI guardrail in the drift-guard workflow. (Vision §4.2)
- **Latency budget:** target <1ms added latency per query; measured, not asserted (enforced in Phase 3). (CLAUDE.md, P5)
- **MSRV / toolchain:** builder image is `rust:1.85`; dependency upgrades must not regress MSRV or behavior (verified by the guardrail suite). (P7 §4.1, §9.4)
- **Dual license:** MIT OR Apache-2.0; `cargo deny` license allowlist must stay compatible. (P7 §4.2)
- **Sister repo:** `scry-protocol` lives at `../scry-protocol` (git `github.com/scrydata/scry-protocol`). Schema changes are developed via a path dependency and released as a coordinated version bump. (P6)

---

## Execution order & work packages

The 16 work packages below execute top to bottom. Each is an independently testable deliverable that ends green (build + its new tests + the accreting drift-guard). Detailed TDD task breakdowns are given in full for **Phase 1**; Phase 2 and Phase 3 packages list their tasks and are expanded into bite-sized TDD steps just-in-time at execution (each references its spec section, which already carries the design).

| WP | Pillar-slice | Gate | Why here |
|----|-------------|------|----------|
| WP-0 | CI drift-guard skeleton | Harden | Nothing is "fenced" without a CI test workflow — none exists today (only image publish). Foundation for every guardrail. |
| WP-1 | P7 advisories + dependency gate + pinning + hygiene | Harden | Clears the 9 advisories P1's auth/TLS stack sits on; establishes `cargo deny`/audit gate. |
| WP-2 | P6 §4.1 minimal schema-version envelope | Harden | Must land before P1's anonymization schema change (vision §6 P1↔P6 hand-off). |
| WP-3 | P1 Harden (the bulk) | Harden | No fail-open auth/TLS; anonymization redacts; publisher scheme; admin-auth gate. |
| WP-4 | P4 Harden (admin-auth wiring, config validation, docs/code parity) | Harden | Shares `Config::validate()` with P1; parity guardrail fixes the `PASSWORD_FILE` class. |
| WP-5 | P5 Harden (wire timeline, budget config, non-blocking gate) | Harden | Makes the proxy measure itself; non-blocking latency signal. |
| WP-6 | P2 §4.1 fail-closed pinning safety net | Harden | Cheap, high-leverage: turns every P2 detection gap into a perf cost, not a correctness bug. |
| WP-7 | P3 §4.4 SIGTERM graceful shutdown | Complete (early) | Production-blocking for any containerized deploy; cheap. |
| WP-8 | P3 Complete (breaker scope/per-backend, healthcheck, timeouts, retry classifier, fault-injection) | Complete | Every advertised reliability feature gates traffic. |
| WP-9 | P2 Complete (contract, extended-protocol tracking, LISTEN/NOTIFY, reassembly, prepared-stmt honesty, differential suite + fuzzer) | Complete | Proven transparency. |
| WP-10 | P4 Complete (live registries, functional commands, redaction, password_file, metrics access control, tracing) | Complete | The console tells the truth and controls the proxy. |
| WP-11 | P6 Complete (full schema versioning/compat, fail-loud pgbouncer, documented surface, semver-checks + corpus) | Complete | Stable contract; honest compatibility. |
| WP-12 | P1 Complete residual (strict backend-TLS knob, constant-time everywhere, username-enum fix, MD5 hashed userlist) | Complete | Residual hardening. |
| WP-13 | P5 Prove (blocking budget gate, soak/endurance, resource ceilings, metrics-cost bench) | Prove | Measured under load after hardening lands. |
| WP-14 | P6 Prove (Postgres version matrix, protocol-version validation, published matrix) | Prove | Stated, tested PG range. |
| WP-15 | P7 Prove (signing, provenance, SBOM diff, reproducible builds, release process) | Prove | Enterprise-signable artifacts. |
| WP-16 | P2 Prove (broaden differential matrix under load) | Prove | Tail; coordinates with P5/P6. |

**Gate exits (must all be green to advance):**
- **Harden:** no fail-open auth/TLS path; `cargo audit`/`cargo deny` clean in CI; anonymization redacts (fuzzer green); admin session cannot be unauthenticated; proxy measures its own latency.
- **Complete:** every advertised feature works and is guarded (differential suite, fault-injection, admin-truthfulness, docs/code parity, schema-compat all green).
- **Prove:** latency budget enforced under load; PG version matrix green; images signed with SBOM; GA-signable.

---

# PHASE 1 — HARDEN

## WP-0: CI drift-guard skeleton

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `justfile` already has `just ci`; verify/align target.

**Interfaces:**
- Produces: a CI workflow `ci.yml` with jobs `fmt`, `lint`, `test`, `audit` that later WPs append guardrail steps/jobs to. This is the single gate the vision §7 calls the "drift guard."

- [ ] **Step 1:** Add `.github/workflows/ci.yml` running on `pull_request` and `push` to non-tag branches, with jobs: `fmt` (`cargo fmt --check`), `lint` (`cargo clippy --all-targets -- -D warnings`), `test` (`cargo test --workspace` with a Postgres service or `--lib` split for Docker-dependent tests), `audit` (`cargo audit`). Pin `actions/checkout` to a full SHA (P7 discipline from the start).
- [ ] **Step 2:** Confirm the CI commands succeed on the current tree. `cargo fmt --all -- --check` is already clean. `cargo clippy -- -D warnings -A dead_code` fails on the local dev toolchain (1.93) due to newer lints (`manual_is_multiple_of`) whose suggested fix is not stable on the builder's `rust:1.85`; the resolution is to **pin CI's toolchain to 1.85** (matching the Dockerfile builder), where those lints do not fire — not to edit source. No source changes; the toolchain pin also advances P7 reproducibility.
- [ ] **Step 3:** Commit: `ci: add drift-guard workflow skeleton (fmt/lint/test/audit)`.

**Exit:** CI workflow exists and the non-Docker jobs pass on the baseline tree.

## WP-1: P7 advisories, dependency gate, pinning, repo hygiene

Implements P7 §4.1, §4.2, §4.3, §4.7, §5.2 (Harden slice).

**Files:**
- Modify: `Cargo.toml` (workspace deps), `Cargo.lock`
- Create: `deny.toml`
- Modify: `Dockerfile` (digest-pin bases), `.github/workflows/publish-image.yml` (SHA-pin actions), `.github/workflows/ci.yml` (add `cargo deny`, pinning lint, secret scan)
- Modify: `.gitignore` (add `scry.toml`, `*.local.toml`)

**Tasks:**
- [ ] **Task 1.1 — Clear advisories.** `cargo update -p postgres-protocol --precise 0.6.12` (and `>=` bumps for `tokio-postgres`≥0.7.18, `bytes`≥1.11.1, `rustls-webpki`≥0.103.13). Migrate off `rustls-pemfile` (fold into `rustls` PEM support or a maintained equivalent). Resolve `anyhow`/`rand` unsound advisories by upgrade, or record a justified expiring `deny.toml` exception. **After each bump, run `cargo build --workspace` and `cargo test --workspace` (non-Docker at minimum) to confirm no regression** — the `postgres-protocol`/`rustls-webpki` crates sit under the auth/TLS stack. Verify `cargo audit` → 0 vulnerabilities. Commit.
- [ ] **Task 1.2 — `cargo deny` policy.** Write `deny.toml` with `advisories` (deny vulns/unmaintained; only justified, expiring exceptions), `licenses` (allowlist MIT/Apache-2.0/BSD/ISC/Unicode as needed by the tree — enumerate from `cargo deny check licenses` output), `bans` (deny duplicate/yanked), `sources` (crates.io only). Run `cargo deny check` until clean. Add `cargo deny check` step to `ci.yml`. Commit.
- [ ] **Task 1.3 — Digest-pin base images.** In `Dockerfile`, resolve current digests for `rust:1.85-bookworm` and `debian:bookworm-slim` (`docker manifest inspect`), pin as `@sha256:...`. Build the image locally to confirm. Commit.
- [ ] **Task 1.4 — SHA-pin CI actions.** In both workflows, replace every `uses: org/action@vN` with `@<full-sha> # vN`. Add a **pinning lint** step to `ci.yml` (a shell/grep check that fails if any `Dockerfile` `FROM` lacks `@sha256:` or any workflow `uses:` lacks a 40-hex SHA). Commit.
- [ ] **Task 1.5 — Repo hygiene.** Add `scry.toml` and `*.local.toml` to `.gitignore`. Add a secret-scanning step (gitleaks action, SHA-pinned) to `ci.yml`. Commit.

**Guardrail (P7 §5.1, §5.2, §5.5):** `cargo deny`/`audit`, pinning lint, secret scan — all in `ci.yml`.

**Exit:** `cargo audit` clean; `cargo deny check` clean; pinning lint green; secret scan green.

## WP-2: P6 §4.1 minimal schema-version envelope (sister crate)

Implements the minimal slice of P6 §4.1 pulled into Phase 1 per vision §6 (P1↔P6 hand-off). Full P6 is WP-11.

**Files (in `../scry-protocol`):**
- Modify: `src/lib.rs` (add `pub const SCHEMA_VERSION: u32`), `src/serializer.rs` (add `schema_version` to `QueryEventBatch`), `src/deserializer.rs` (read + expose it)
- Test: `../scry-protocol/tests/` round-trip incl. version field
- Modify (in this repo): `scry-proxy/Cargo.toml` — point `scry-protocol` at `{ path = "../scry-protocol" }` for development; note the coordinated release in the plan's release step (WP-11/WP-15).

**Tasks:**
- [ ] **Task 2.1 — Failing test:** in `scry-protocol`, add a test asserting a serialized batch carries `schema_version == SCHEMA_VERSION` and a deserializer exposes it. Run → fails.
- [ ] **Task 2.2 — Implement:** add `pub const SCHEMA_VERSION: u32 = 1;` to `lib.rs`; add `schema_version: u32` field to `QueryEventBatch` (serializer) set to `SCHEMA_VERSION`; expose it on the deserialized batch. Keep it additive (`#[serde(default)]` on read for forward-compat). Run test → passes.
- [ ] **Task 2.3 — Wire path dep:** set `scry-protocol = { path = "../scry-protocol" }` in `scry-proxy/Cargo.toml`; `cargo build --workspace` in the proxy → passes.
- [ ] **Task 2.4 — Commit** in both repos: `feat(protocol): add schema_version envelope field (schema v1)` and, in proxy, `build: use path dep on scry-protocol for coordinated schema work`.

**Guardrail:** the round-trip version test (joins P6's cross-version test in WP-11).

**Exit:** batches carry `schema_version`; proxy builds against the path dep.

## WP-3: P1 Harden — security & privacy fail-closed (the bulk)

Implements P1 §4.1, §4.2, §4.4, §4.5, §4.6 and auditors §5.1–§5.5. (P1 §4.3 strict-backend-TLS knob and §4.7 residual hardening → WP-12.)

**Files:**
- Modify: `scry-proxy/src/config/mod.rs` (`validate()` fail-closed matrix; remove default backend password; salt config; publisher scheme; admin-console default; `unsafe_debug_logging` flag)
- Modify: `scry-proxy/src/auth/authenticator.rs` (SCRAM/cert arms defensive), `scry-proxy/src/proxy/server.rs` (no trust fallback on missing backing; admin-auth gate; client TLS downgrade rejection)
- Modify: `scry-proxy/src/tls/startup.rs` (reject plaintext under require/verify-*)
- Modify: `scry-proxy/src/proxy/connection.rs` (`build_query_event` normalized-only query; redact params; parse-failure fail-closed; error-field scrub; gate raw-query logs)
- Modify: `scry-proxy/src/protocol/anonymize.rs` (wire `with_salt`; remove raw-query WARN), `scry-proxy/src/protocol/extractor.rs` (error scrub), `scry-proxy/src/publisher/debug_logger.rs` + `http_publisher.rs` (scheme validation; gate raw logging)
- Test: `scry-proxy/tests/security_fail_closed_test.rs` (new), `scry-proxy/tests/anonymization_fuzz.rs` (new, proptest), `scry-proxy/tests/tls_downgrade_test.rs` (new, testcontainers), `scry-proxy/tests/admin_auth_test.rs` (new)
- Create: `scry-proxy/tests/fixtures/secure_defaults.snapshot`

**Interfaces:**
- Produces: `Config::validate(&self) -> Result<(), ConfigError>` enforcing the fail-closed matrix; `AnonymizeConfig { salt: Option<String>, parse_failure: ParseFailureMode, unsafe_debug_logging: bool }`; `AdminConfig { admin_users: Option<...>, enabled: bool }`; a `QueryAnonymizer::new_with_salt(salt)` constructor. Consumed by WP-4 (shares `validate()`), WP-5 (measures the added cost).

**Tasks (TDD, each = failing test → run-fail → implement → run-pass → commit):**
- [x] **Task 3.1 — Fail-closed config matrix (P1 §4.1, §5.1).** Unit tests in `config/mod.rs` asserting `validate()` returns `Err` for: `auth_type = scram-sha-256`; `auth_type = cert` without `sslmode ∈ {verify-ca,verify-full}`; any non-`trust` `auth_type` without an `auth_file`; `trust` without explicit `allow_trust = true`; unset/empty backend password (remove default `"password"` at `config/mod.rs:398`); `anonymize = true` without a configured salt; non-`https` publisher endpoint without `allow_insecure = true`. Implement `validate()` arms + config fields. Wire `validate()` into startup so the listener never binds in a fail-open state (`server.rs`). Make the SCRAM per-connection arm (`authenticator.rs:107-117`) a defensive error/`unreachable!`. **Also (P1 §4.1 defense-in-depth):** the runtime `Cert` arm (`authenticator.rs:119-129`) must additionally assert a verified client certificate was actually presented before returning success, rather than assuming TLS validated one.
- [x] **Task 3.2 — Client TLS downgrade rejection (P1 §4.2, §5.4).** testcontainers integration test: connect under `sslmode = require` without SSLRequest → assert connection rejected with a Postgres `ErrorResponse` (SQLSTATE `28000`) and closed, not served plaintext. Implement in `tls/startup.rs:76-79` NoSslRequest / SSL-declined paths using the existing `is_encrypted()` primitive; `disable`/`allow` retain behavior.
- [x] **Task 3.3 — Anonymization redaction core (P1 §4.4).** Unit + property tests: with `anonymize = true`, `build_query_event` sets `query` to the normalized form (never raw); on parse failure the event drops or ships a hard-redacted statement-kind placeholder (default redact, configurable); `params` are replaced with type/null shapes or fingerprints, never raw values; the fingerprint salt comes from config (`QueryAnonymizer` wired with `with_salt`); the error field is scrubbed (attach `S`/SQLSTATE by default). Implement across `connection.rs:100-115`, `anonymize.rs`, `extractor.rs`. Remove/lower the raw-query WARN at `anonymize.rs:46`.
- [x] **Task 3.4 — Anonymization fuzzer (P1 §5.3, the core data-handling guarantee).** `proptest` + adversarial corpus (DDL like `CREATE ROLE x PASSWORD 'secret'`, unparseable vendor syntax, parameterized statements). Assert: **no produced event and no captured log line contains any input literal or parameter value** when anonymization is enabled. This is the crown-jewel guardrail.
- [x] **Task 3.5 — Log hygiene (P1 §4.4 log lines).** Gate every raw-query/full-event log site (`connection.rs:523,569,578,628,1025-1030`; `extractor.rs:195,220`; `debug_logger.rs:62-76`) behind an off-by-default `unsafe_debug_logging` flag. Test: with the flag off, a captured tracing subscriber sees no raw SQL/params. (Folded into 3.4's log-capture assertion.)
- [x] **Task 3.6 — Publisher transport scheme (P1 §4.5).** Unit test: `HttpPublisher`/config rejects non-`https` endpoint unless `allow_insecure = true`. Implement scheme validation at config load (`http_publisher.rs:140`, `config/mod.rs`). (Covered by 3.1's matrix; add the publisher-construction unit test here.)
- [x] **Task 3.7 — Admin console authentication (P1 §4.6, §5.5).** Integration test: an admin connection (database `pgbouncer`) without credentials is refused. Implement: `handle_admin_connection` (`server.rs:1107-1122`) consults an `admin_users`/`admin_password` credential instead of writing `AuthenticationOk` unconditionally; **default admin console disabled** unless an admin credential is configured. (Commands stay stubs — P4/WP-10.)
- [x] **Task 3.8a — Redacting `Debug` (P1 §4.7, pulled forward from Complete).** Implement manual `Debug` for `UserCredentials`, `PasswordEntry`, and config structs holding `password`/`http_api_key` (`auth/types.rs:36,45`; `config/mod.rs:75,114`) printing `<redacted>`. Unit test asserts `{:?}` output never contains the secret value. *Pulled into Phase 1 because WP-10's `SHOW CONFIG` redaction and WP-4's `validate()` security checks depend on it (review-identified WP-10→WP-12 inversion); WP-12 no longer carries this task.*
- [x] **Task 3.8 — Secure-defaults snapshot (P1 §5.2).** Serialize every security-relevant default (auth type, client+backend `sslmode`, publisher scheme requirement, admin-console default, `unsafe_debug_logging`, `allow_trust`) to `tests/fixtures/secure_defaults.snapshot`. Test fails if a default changes toward less-safe without updating the snapshot in the same PR.
- [x] **Task 3.9 — Wire all new tests into `ci.yml`** (Docker-dependent ones in the `test` job with a Docker daemon). Commit.

**Guardrail:** fail-closed config tests, secure-defaults snapshot, anonymization fuzzer, TLS-downgrade test, admin-auth test — all in the drift-guard.

**Exit:** no fail-open auth/TLS path; anonymization fuzzer green; admin session cannot be unauthenticated; secure-defaults snapshot locked.

## WP-4: P4 Harden — admin-auth wiring, config validation, docs/code parity

Implements P4 Harden slice (§4.4 validation escalation, §5.1 parity guardrail; admin-auth gate shared with WP-3).

**Files:**
- Modify: `scry-proxy/src/config/mod.rs` (`validate()`: escalate unsafe pool/queue ratios and unknown-key handling from warn→error)
- Create: `scry-proxy/tests/docs_code_parity_test.rs`
- Modify: `docs/deployment.md` (fix `SCRY_BACKEND__PASSWORD_FILE` reference — either remove or mark pending WP-10's `password_file`)

**Tasks:**
- [ ] **Task 4.1 — Config validation escalation (P4 §4.4, §5.2, §9.3).** Tests asserting `validate()` **rejects** (not warns) genuinely-unsafe pool/queue ratios (`config/mod.rs:525-567`) and **rejects unknown `SCRY_*`/config keys by default** (P4 §9.3 recommendation: hard-reject under a strict-config default, with an explicit `SCRY_ALLOW_UNKNOWN_KEYS`-style escape hatch — a warn-only path would let a typo'd key silently no-op, the exact `SCRY_BACKEND__PASSWORD_FILE` failure this pillar exists to kill). Implement, coordinating with WP-3's `validate()`.
- [ ] **Task 4.2 — Docs/code parity guardrail (P4 §5.1, the signature P4 check).** `docs_code_parity_test.rs`: parse every `SCRY_*` variable out of `docs/`, assert each maps to a real config field, and (reverse) that every field is documented. This is the check that catches the `SCRY_BACKEND__PASSWORD_FILE` gap. Make it pass by either implementing the field (deferred to WP-10) or correcting the doc now — for Phase 1, correct the doc so parity holds; the actual `password_file` implementation is tracked as WP-10 Task 10.4. The test asserts current parity.
- [ ] **Task 4.3 — Wire into `ci.yml`; commit.**

**Guardrail:** docs/code parity check + config-validation tests in the drift-guard.

**Exit:** unsafe/unknown settings rejected; docs/code parity green.

## WP-5: P5 Harden — self-measurement, budget config, non-blocking gate

Implements P5 §4.1, §4.2, §4.3 (non-blocking), §5.2.

**Files:**
- Modify: `scry-proxy/src/proxy/connection.rs` (populate `QueryTimeline`, pass it to `record_query` at `:643,667,1050,1084`), `scry-proxy/src/observability/timeline.rs` / `metrics.rs` (derived `proxy_overhead`)
- Modify: `scry-proxy/src/config/mod.rs` (replace dead `target_latency_ms` at `:270,426` with per-percentile budget)
- Modify: `benchmarks/benches/proxy_throughput.rs` (compute direct-vs-proxy delta; representative workload; larger sample size), `benchmarks/src/...`
- Create: `scry-proxy/tests/metrics_populated_test.rs`
- Modify: `.github/workflows/ci.yml` (non-blocking perf signal job)

**Tasks:**
- [ ] **Task 5.1 — Wire the internal timeline (P5 §4.1, foundational).** Mark pool-acquire start/end and backend start/end at the real points in `connection.rs`; pass the populated `QueryTimeline` (not `QueryTimeline::new()`) into `record_query`. Add derived `proxy_overhead = total − backend − pool_acquire − queue`.
- [ ] **Task 5.2 — Metrics-populated guardrail (P5 §5.2).** Integration test (testcontainers): after real queries, assert the latency histograms are **non-zero** and the phase decomposition is present. Direct regression guard against the unwired state.
- [ ] **Task 5.3 — Budget config (P5 §4.2).** Replace dead `target_latency_ms` with per-percentile overhead thresholds (p50/p95/p99) tied to a named reference workload; document the definition of "added latency."
- [ ] **Task 5.4 — Non-blocking delta gate (P5 §4.3).** Strengthen the criterion/harness bench to compute the direct-vs-proxy delta in code with a representative workload (reuse `benchmarks/` OLTP set) and a tail-adequate sample size; add a **non-blocking** CI job that records the delta vs. budget (baseline; flips to blocking in WP-13). Commit.

**Guardrail:** metrics-populated test (blocking) + non-blocking latency signal.

**Exit:** proxy measures its own latency (histograms non-zero); budget defined; non-blocking perf signal running.

## WP-6: P2 §4.1 fail-closed pinning safety net

Implements P2 §4.1 (Phase-1-eligible), §5.4.

**Files:**
- Modify: `scry-proxy/src/proxy/connection.rs` (`should_release_connection` / Hybrid path `:80-84`)
- Test: `scry-proxy/tests/pooling_safety_test.rs`

**Tasks:**
- [ ] **Task 6.1 — Failing pooling-safety property test (P2 §5.4).** Assert: for any operation the detector does not positively classify as clean, the connection is **pinned** (kept with the client), not released.
- [ ] **Task 6.2 — Implement fail-closed pinning.** Release only when positively known clean; any unrecognized command, parse/reassembly uncertainty, or unclassified extended-protocol message → pinned. Preserves the passthrough invariant. Run tests → pass. Commit.

**Guardrail:** pooling-safety property test.

**Exit:** every unclassified operation pins; blast radius of remaining P2 gaps is performance-only.

**★ PHASE 1 GATE:** re-run full drift-guard. Confirm: `cargo audit`/`deny` clean, all fail-closed/secure-defaults/anon-fuzzer/TLS-downgrade/admin-auth/parity/metrics-populated/pooling-safety tests green. Re-measure `cargo audit` = 0. Tag/PR checkpoint (with user confirmation per Global Constraints — never merge to shared branch without approval).

---

# PHASE 2 — COMPLETE

Each package below is expanded into bite-sized TDD steps at execution time from the cited spec sections.

## WP-7: P3 §4.4 SIGTERM graceful shutdown (early)
- **Files:** `scry-proxy/src/proxy/server.rs` (`:577-588`, `:838-895`), `scry-proxy/src/proxy/mod.rs`; test `scry-proxy/tests/shutdown_test.rs`.
- **Tasks:** (7.1) SIGTERM-drain integration test — long in-flight query, send SIGTERM, assert clean completion within `shutdown_timeout_secs` and over-deadline abort per contract (P3 §5.2). (7.2) Add SIGTERM handler invoking the existing `drain_connections` path identical to SIGINT; expose the shutdown mechanism as a callable for P4's admin `SHUTDOWN`. (7.3) Document the drain contract in `docs/`. 
- **Guardrail:** SIGTERM drain test.

## WP-8: P3 Complete — reliability enforcement
Implements P3 §4.1, §4.2, §4.3, §4.5, §5.1, §5.3–§5.5.
- **Files:** `resilience/circuit_breaker.rs`, `resilience/healthcheck.rs`, `resilience/retry.rs`, `proxy/tcp_pool.rs`, `proxy/pool_manager.rs`, `proxy/connection.rs`, `proxy/server.rs`, `config/mod.rs`, `config/pgbouncer.rs`; new `scry-proxy/tests/resilience_fault_injection_test.rs`; `docs/circuit-breaker.md`, `docs/health-checks.md`.
- **Tasks:** (8.1) Per-backend breakers keyed by backend identity + broaden failure signals (connect/query timeout, health failures) with an explicit classifier that excludes client-caused SQL errors; breaker-scope test (P3 §5.4). (8.2) Integrate `ActiveHealthcheck` into the per-backend breaker/rotation gate; healthcheck-gating test (P3 §5.3). (8.3) Enforce `query_timeout` (timer at dispatch; on expiry close/reclaim connection — connection-close-on-timeout per P3 §9.2, coordinated with P2 dirty-connection); enforce `connection_timeout_ms` (wrap `TcpStream::connect`+TLS); configure deadpool `Timeouts` so `pool_timeout_secs`/`pool_queue_depth` bound wait. (8.4) Retry error classifier — only transient connect/transport errors retried; retry-safety test asserting no query replay (P3 §5.5). (8.5) Reconcile breaker window/consecutive-failure doc/code (P3 §4.1). (8.6) Fault-injection suite (P3 §5.1): kill backend→breaker opens+shed+clean errors; restore→half-open+close within contract; stall TCP→`connection_timeout_ms` fires; hung query→cancelled+connection reclaimed cleanly.
- **Guardrail:** fault-injection suite + breaker-scope + retry-safety + healthcheck-gating tests.

## WP-9: P2 Complete — protocol correctness & transparency
Implements P2 §4.0, §4.2–§4.7, §5.1–§5.3.
- **Files:** `docs/transparency-contract.md` (new), `protocol/command_detector.rs`, `proxy/connection.rs`, `proxy/connection_state.rs`, `proxy/state_replayer.rs`, `protocol/extractor.rs`, `proxy/pool_manager.rs`, `proxy/server.rs`; extend `tests/integration_test.rs`, `tests/stateful_test.rs`, `tests/transaction_pooling_test.rs`, `tests/connection_multiplexing.rs`; new `tests/differential_transparency_test.rs`, `tests/framing_fuzz.rs`.
- **Tasks:** (9.1) Write & commit the transparency contract (P2 §4.0). (9.2) Differential transparency suite (P2 §5.1, the core guardrail): matrix of simple+extended ops × pooling modes, run direct vs. through-proxy, assert equal message sequence + result bytes + session-observable state. (9.3) Extended-protocol state tracking from the `Parse` arm (P2 §4.2). (9.4) LISTEN/NOTIFY modeling + `PinReason::Listen` + delivery test (P2 §4.3). (9.5) Message reassembly — buffer incomplete trailing bytes across reads (P2 §4.4); framing fuzzer (P2 §5.2). (9.6) Prepared-statement honesty — restrict-by-pinning (recommended, P2 §9.1); eliminate silent `clear_all()` state destruction (P2 §4.5). (9.7) Hot-path robustness — typed errors for `expect("connection taken")`, poison-safe auth lock, LRU-bound prepared-statement map (P2 §4.7). (9.8) Dirty-connection invariant test (P2 §5.3). (9.9 tail) Event-logging accuracy — multi-statement/pipelining/COPY attribution (P2 §4.6), must not regress passthrough.
- **Guardrail:** differential suite + framing fuzzer + dirty-connection + pooling-safety.

## WP-10: P4 Complete — operable admin console & metrics
Implements P4 §4.1–§4.6, §5.3–§5.5.
- **Files:** `admin/commands.rs`, new registry module under `proxy/` (client + server registries), `observability/metrics_server.rs`, `main.rs`, `config/mod.rs`, `publisher/mod.rs`, `Cargo.toml` (drop `cors`); new `tests/admin_truthfulness_test.rs`, `tests/admin_command_effect_test.rs`, `tests/metrics_surface_test.rs`; `docs/deployment.md`.
- **Tasks:** (10.1) Live client + server registries as the single source of truth (P4 §4.1); `SHOW DATABASES/CLIENTS/SERVERS/POOLS/STATS` read from them; admin-truthfulness tests (P4 §5.3). (10.2) Functional PAUSE/RESUME/RELOAD/SHUTDOWN/KILL via P3 mechanisms, honest errors, no false success; command-effect tests (P4 §5.4). (10.3) `SHOW CONFIG` real + secret redaction reusing P1 redacting-`Debug` (P4 §4.3). (10.4) Implement `password_file` generic `*_file` secret loading (P4 §4.4, implements the `password_file` field deferred by WP-4 Task 4.2). (10.5) Metrics access control: split `/metrics`+`/health` (unauthed, loopback) from `/debug/*` (opt-in/gated), non-loopback-bind acknowledgement flag; remove dead CORS; metrics-surface test asserting `/debug/*` unreachable unauthenticated on non-loopback and no secrets in dumps (P4 §4.5, §5.5). (10.6) OTel spans over accept→auth→query→response; logs safe by construction (P4 §4.6). (10.7) Ensure metrics expose P3 breaker/timeout/shed signals and P5 percentiles.
- **Guardrail:** admin-truthfulness + command-effect + metrics-surface tests; docs/code parity still green.

## WP-11: P6 Complete — stable contract & honest compatibility
Implements P6 §4.1 (full), §4.2, §4.3, §5.1–§5.3.
- **Files (sister crate + proxy):** `../scry-protocol/src/{lib.rs,serializer.rs,deserializer.rs,event.rs,param.rs}`, remove/quarantine `database_event` + `.fbs`; `scry-proxy/src/config/pgbouncer.rs`; `docs/architecture.md`, `docs/compatibility.md` (new); new `scry-proxy/tests/pgbouncer_corpus_test.rs`, `../scry-protocol/tests/cross_version_test.rs`.
- **Tasks:** (11.1) Full schema versioning + compatibility policy doc (add-optional=compatible, rename/remove/retype-required=breaking); `skip_serializing_if`/`#[serde(default)]`/`#[non_exhaustive]` discipline (P6 §4.1). (11.2) Fail loud on version skew / unparseable param — typed error + `params_incomplete`; eliminate `filter_map(...ok())` silent drop at `deserializer.rs:97` (P6 §4.1). (11.3) Rename misnomer `FlatBuffersSerializer`→FlexBuffers; remove/quarantine dead `database_event` FlatBuffers surface (P6 §4.1, §9.5). (11.4) Fail-loud pgbouncer config — replace `_ => {}` catch-alls with honored/unsupported-loud/unknown-loud; reject invalid values; multi-`[databases]` error loudly (P6 §9.2); pool-mode honesty (P6 §4.2). (11.5) Right-size the compat claim in docs; replace "drop-in replacement" (P6 §4.3). (11.6) Guardrails: `cargo-semver-checks` on `scry-protocol` (§5.1), wire-schema cross-version test (§5.2), pgbouncer-config parse corpus (§5.3). Install `cargo-semver-checks`; add to CI.
- **Guardrail:** semver-checks + cross-version wire test + pgbouncer corpus.

## WP-12: P1 Complete — residual hardening
Implements P1 §4.3, §4.7 (Complete slice), §4.1 MD5 hashed-userlist.
- **Files:** `auth/scram.rs`, `protocol/auth_messages.rs`, `auth/file_auth.rs`, `auth/authenticator.rs`, `auth/types.rs`, `config/mod.rs`, `tls/config.rs`; add `subtle` dep.
- **Tasks:** (12.1) Constant-time comparisons via `subtle::ConstantTimeEq` at `scram.rs:145`, `auth_messages.rs:363`, `file_auth.rs:123,127` (P1 §4.7). (12.2) Eliminate username enumeration — always issue MD5 challenge + dummy verify for unknown users (P1 §4.7). (12.3) Userlist file-permission check rejecting world-readable/writable (P1 §4.7). (12.4) *Redacting `Debug` — moved to WP-3 Task 3.8a (Phase 1) to resolve the WP-10 dependency; no longer here.* (12.5) Route client MD5 through hash-aware `check_password` (P1 §4.1). (12.6) Strict-backend-TLS knob making `require` verify (P1 §4.3) + startup warning when `require` without `verify-*`.
- **Guardrail:** constant-time call-site unit tests; redacting-`Debug` test; userlist-permission test — join drift-guard.

**★ PHASE 2 GATE:** every advertised feature works and is guarded. Full drift-guard green including differential suite, fault-injection, admin-truthfulness, schema-compat, parity.

---

# PHASE 3 — PROVE

## WP-13: P5 Prove — enforced budget under load
Implements P5 §4.3 (blocking), §4.4, §4.5, §4.6, §4.7, §5.1, §5.3, §5.4.
- **Tasks:** (13.1) Flip the delta gate to **blocking** after hardening lands, on a dedicated/stable runner (P5 §9.3). (13.2) Duration/steady-state + ramp mode in the load generator (`runner.rs:238-244`); scheduled soak asserting no latency drift + no memory growth (P5 §4.4, §5.3). (13.3) Resource-ceiling tests driving `max_connections`/`pool_queue_depth`, asserting graceful rejection + bounded memory + no deadlock; first verify the rejection gauges are wired (P5 §4.5, §5.4). (13.4) Metrics self-overhead micro-bench validating the <300ns claim (P5 §4.6). (13.5) Re-measure budget after P1/P2/P3/P7 (P5 §4.7); publish results.
- **Guardrail:** blocking latency gate; scheduled soak/scale (release-tag gate); resource-ceiling test.

## WP-14: P6 Prove — Postgres version matrix
Implements P6 §4.4, §5.4.
- **Tasks:** (14.1) Validate protocol version at startup (accept 3.0; clean `ErrorResponse` for unsupported) (P6 §4.4). (14.2) Handle/cleanly refuse `CancelRequest`/GSSENC (P6 §4.4). (14.3) Run the differential/integration suite across PG 14–17 (state 13+ supported), one per-PR + full matrix pre-release (P6 §9.3); publish the support matrix; fix stale PG15 docs. 
- **Guardrail:** PG version matrix (release-tag gate + per-PR subset).

## WP-15: P7 Prove — signing, SBOM, reproducible builds, release process
Implements P7 §4.4, §4.5, §4.6, §5.3, §5.4.
- **Tasks:** (15.1) Cosign keyless image signing + SLSA provenance via `build-push-action`; sign the multi-arch manifest (P7 §4.4). (15.2) CycloneDX SBOM per build (`cargo-cyclonedx`) attached as attestation + diff vs. prior release (P7 §4.4, §5.3). (15.3) Signature/provenance verification post-publish step (P7 §5.4). (15.4) `rust-toolchain.toml` pin aligned with the digest-pinned builder; document reproducibility (P7 §4.5). (15.5) Documented semver release process + changelog; coordinate `scry-protocol` version bump with P6 semver-checks; publish coordinated `scry-protocol` release (closes WP-2's path-dep with a real version) (P7 §4.6). (15.6) Switch Dockerfile healthcheck to `/health` if `/metrics` is gated (P7 §4.6).
- **Guardrail:** SBOM diff + signature/provenance verification (release-tag gate).

## WP-16: P2 Prove — broaden differential matrix under load
Implements P2 Phase-3 tail. (P2 §7 lists no explicit Phase-3 slice; this package is grounded in vision §6's P2 row "Broaden matrix under load" and P2 §9.2/§9.3, which supersede the P2 spec's own phasing table here.)
- **Tasks:** (16.1) Broaden the differential transparency matrix under sustained load and across the PG matrix (coordinate with WP-13/WP-14); confirm Hybrid mode meets the bar or gate it behind an "advanced" flag (P2 §9.2).
- **Guardrail:** the differential suite run under load (release-tag gate).

**★ PHASE 3 GATE / GA:** latency budget enforced under load; PG matrix green; images signed with published SBOM & provenance; `scry-protocol` released with versioned schema. All Automated Auditors composed into the drift-guard (merge-time subset + release-tag full set). GA-signable.

---

## Self-review notes

- **Spec coverage:** every pillar spec's §4 design items and §5 auditors map to a WP task above (traced by section number). The P1↔P6 Phase-1 hand-off (vision §6) is WP-2. The P4/P1 shared `Config::validate()` surface is WP-3/WP-4. The P5/P4 timeline seam is WP-5. The P3/P4 mechanism/command split is WP-7/WP-8 → WP-10.
- **Ordering:** dependency-correct — P7 advisories (WP-1) precede P1 (WP-3) which sits on the upgraded auth/TLS stack; schema envelope (WP-2) precedes P1's schema change (WP-3); fail-closed pinning (WP-6) precedes deep P2 work (WP-9); self-measurement (WP-5) precedes the enforced budget (WP-13) measured after hardening.
- **Sister-crate handling:** `scry-protocol` developed via path dep from WP-2, released as a coordinated version bump in WP-15.
- **No silent scope drops:** Phase 2/3 packages are task-listed, not omitted; each expands to bite-sized TDD steps at execution.
