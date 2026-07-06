# P1 — Security & Privacy (Pillar Spec)

**Status:** Draft for review
**Date:** 2026-07-05
**Pillar:** P1 of the [Production-Readiness Vision](./2026-07-05-scry-production-readiness-vision.md)
**Phase:** Harden (primary), with residual hardening carried into Complete

---

## 1. Purpose & scope

Make scry secure-by-default and incapable of silently leaking data or credentials. Every authentication path (client→proxy and proxy→backend), the transport layer, the query-anonymization pipeline, the admin console's authentication, and secrets/log handling must **fail closed**: when a guarantee cannot be met, scry refuses rather than degrades.

In scope:
- Client and backend authentication correctness and defaults.
- Client-facing TLS enforcement (no plaintext downgrade); backend TLS verification posture.
- Anonymization that actually removes raw SQL, bind parameters, and embedded secrets from published events and logs.
- Admin console **authentication** (the mechanism).
- Secrets hygiene: constant-time comparisons, no credentials in logs, redacting `Debug`, userlist file-permission checks.

Out of scope (owned elsewhere, cross-referenced below):
- Implementing the admin *control commands* themselves → **P4**.
- Clearing the underlying dependency CVEs (e.g. `postgres-protocol` SCRAM DoS) → **P7**.
- The analytics platform's own auth/storage/retention → external.

---

## 2. Current state (evidence)

All paths below were confirmed in the 2026-07-05 security audit; the two headline items were re-verified directly against source.

### 2a. Client authentication fails open
- `AuthType::ScramSha256` logs a warning and returns a **successful, unauthenticated** handshake — `scry-proxy/src/auth/authenticator.rs:107-117`.
- `AuthType::Cert` performs no check in the authenticator; it assumes TLS validated a client cert, which only happens under `verify-ca`/`verify-full` — `scry-proxy/src/auth/authenticator.rs:119-129`.
- `AuthType::default()` is `Trust`; `AuthConfig::default` has no auth file — `scry-proxy/src/config/mod.rs:158-159`, `:204-208`.
- An auth type set **without** an `auth_file` silently falls back to trust at startup — `scry-proxy/src/proxy/server.rs:304-309`.
- Default backend password is the literal `"password"` — `scry-proxy/src/config/mod.rs:398`.

### 2b. Client TLS can be bypassed
- A client that omits the SSLRequest is served plaintext on the `NoSslRequest` path, which never consults `sslmode` — `scry-proxy/src/tls/startup.rs:76-79`, caller `scry-proxy/src/proxy/server.rs:936-942`. This also bypasses `WebPkiClientVerifier` mandatory client-cert auth.
- Client TLS default is `Disable` — `scry-proxy/src/config/mod.rs:213-216`, `:256`.

### 2c. Backend TLS `require`/`allow` skip verification
- `NoCertificateVerification` accepts any cert with no chain/hostname check for `Allow | Require` — `scry-proxy/src/tls/config.rs:130-138`, `:168-223`. (`verify-ca`/`verify-full` do real WebPKI validation — `:140-164`.) This mirrors libpq semantics and is opt-in (default `Disable`), but exposes operators who set `require` expecting safety.

### 2d. Anonymization does not anonymize
- The event `query` field is set to the **raw** SQL in every branch; anonymization is only additive — `scry-proxy/src/proxy/connection.rs:100-115` (verified).
- A `sqlparser` parse failure ships raw SQL with no normalization or fingerprints, and logs the full raw query at WARN — `scry-proxy/src/protocol/anonymize.rs:43-49`, `:46`; `scry-proxy/src/proxy/connection.rs:107-109`.
- Bind parameters are captured and shipped raw regardless of the anonymize flag — `scry-proxy/src/proxy/connection.rs:544,632,657,1039,1074`; `scry-protocol` `serializer.rs:79-83`.
- Fingerprints use a hardcoded public salt `b"scry-default-salt"`; `with_salt` is never wired — `scry-proxy/src/protocol/anonymize.rs:28`, `scry-proxy/src/proxy/connection.rs:102`.
- The `error` field can echo literal values unanonymized — `scry-proxy/src/protocol/extractor.rs:130-172`; `scry-proxy/src/proxy/connection.rs:636,1043`.
- `DebugLoggerPublisher` serializes full raw events (SQL + params) to logs — `scry-proxy/src/publisher/debug_logger.rs:62-76`; additional raw-query log lines at `scry-proxy/src/proxy/connection.rs:523,569,578,628,1025-1030` and `extractor.rs:195,220`.
- The HTTP publisher accepts a plaintext `http://` endpoint with no scheme validation — `scry-proxy/src/publisher/http_publisher.rs:140`.

### 2e. Admin console is unauthenticated
- `handle_admin_connection` writes `AuthenticationOk` immediately, never consulting the authenticator; reached over the main listen port by naming the database `pgbouncer` — `scry-proxy/src/proxy/server.rs:1107-1122`, `:1028`; `scry-proxy/src/admin/mod.rs:25`.

### 2f. Weak crypto & secrets hygiene
- Non-constant-time credential comparisons — `scry-proxy/src/auth/scram.rs:145`, `scry-proxy/src/protocol/auth_messages.rs:363`, `scry-proxy/src/auth/file_auth.rs:123,127`.
- Username enumeration via timing/flow: unknown users get no MD5 challenge and return early — `scry-proxy/src/auth/authenticator.rs:167-202`.
- Client MD5 requires cleartext userlist entries (the hash-aware `check_password` is dead code) — `authenticator.rs:193`, `file_auth.rs:121-150`.
- No userlist file-permission check — `scry-proxy/src/auth/file_auth.rs:29-38`.
- Credential-bearing structs derive `Debug`, risking future `{:?}` leakage — `scry-proxy/src/auth/types.rs:36,45`, config structs at `config/mod.rs:75,114`.

---

## 3. Target / 1.0 exit criteria

1. **No fail-open auth.** `scram-sha-256` and `cert` either enforce or **refuse to start** (see §4.1). An auth type set without its required backing (auth file / client verifier) is a **startup error**, not a trust fallback.
2. **No default trust in production.** `Trust` requires explicit, acknowledged opt-in; the default posture requires configured authentication. No default backend password.
3. **No TLS downgrade.** Under `require`/`verify-ca`/`verify-full`, a client that does not negotiate TLS is rejected with a Postgres `ErrorResponse` and the connection is closed.
4. **Anonymization is verifiable.** With anonymization enabled, no raw SQL literal, bind-parameter value, or embedded secret appears in any published event or in logs. Parse failures **drop or hard-redact**, never ship raw. Fingerprint salt is operator-configured, not a compiled constant.
5. **Publisher transport is enforced.** A non-`https` publisher endpoint is rejected unless an explicit `allow_insecure` opt-in is set.
6. **Admin console is authenticated.** Access requires an admin credential (or the console is disabled by default). Unauthenticated admin sessions are impossible.
7. **Crypto hygiene.** All credential/verifier/signature comparisons are constant-time; user-not-found and wrong-password are indistinguishable in timing and message flow; userlist files with unsafe permissions are rejected; credential-bearing structs do not leak via `Debug`.

---

## 4. Design

Organized by sub-area. Each change states the fail-closed behavior and the interface/module it touches.

### 4.1 Client authentication (`src/auth/`, `src/proxy/server.rs`)
- **SCRAM → refuse startup.** Replace the trust fallback at `authenticator.rs:107-117` with a hard error surfaced at boot. The decision is made once, at configuration-load/validation time, not per-connection: `Config::validate()` (in `config/mod.rs`) returns an error when `auth_type = scram-sha-256`, so the process never binds a listener in a fail-open state. The per-connection arm becomes `unreachable!()`/defensive error. Rationale: refusal is the minimal safe fix; full client-SCRAM is deferred to a later phase and tracked as an open item.
- **Cert auth requires a verifying TLS mode.** `Config::validate()` errors if `auth_type = cert` while `client_tls_sslmode` is not `verify-ca`/`verify-full`. At runtime the `Cert` arm additionally asserts a verified client certificate was presented before returning success (defense in depth), rather than assuming it.
- **Fail closed on missing backing.** The `server.rs:304-309` "auth type set but no auth_file → trust" branch becomes a startup error via `Config::validate()`.
- **Defaults.** `Trust` stays available but is no longer the silent default: either (a) there is no default `auth_type` and one must be set, or (b) `Trust` must be paired with an explicit `allow_trust = true` acknowledgement. Remove the default backend password `"password"` (`config/mod.rs:398`) — an unset backend password is a config error.
- **MD5 hashed userlists.** Route client MD5 through the hash-aware `check_password` (`file_auth.rs:121-150`) instead of `get_password`, so cleartext storage is not required.

### 4.2 Client TLS enforcement (`src/tls/startup.rs`, `src/proxy/server.rs`)
- On the `NoSslRequest` path (`tls/startup.rs:76-79`) and the SSL-declined paths, when `sslmode ∈ {require, verify-ca, verify-full}`, send a Postgres `ErrorResponse` (`28000`/protocol violation) and close instead of proceeding in plaintext. The `is_encrypted()` primitive already exists (`tls/transport.rs`); this wires it into client admission. `disable`/`allow` retain current behavior.

### 4.3 Backend TLS posture (`src/tls/config.rs`)
- Keep `require`/`allow` libpq-compatible (encryption without verification) but: (a) emit a startup warning when `require` is used without `verify-*`, (b) document `verify-full` as the production recommendation, and (c) add a config knob to make `require` strict (verify) for operators who want fail-closed backend TLS. Full re-semantics of `require` is an open question (§9), not a default change here.

### 4.4 Anonymization redaction (`src/proxy/connection.rs`, `src/protocol/anonymize.rs`, `src/publisher/`)
- **Never ship raw when anonymizing.** In `build_query_event` (`connection.rs:100-115`), when `anonymize = true`, set the event `query` to the **normalized** form (or empty), never the raw text.
- **Parse failure fails closed.** On `anonymize()` returning `None` (`anonymize.rs:43-49`), **drop the event or ship a hard-redacted placeholder** (statement kind only, no text) — configurable, defaulting to redact. Remove/lower the raw-query WARN log at `anonymize.rs:46`; log a fingerprint or statement-kind instead.
- **Redact parameters.** When anonymizing, replace `params` with fingerprints or shapes (type + null/non-null), never raw values (`connection.rs:544` and attach sites). The event schema change is coordinated with **P6** (`scry-protocol` contract).
- **Configurable salt.** Require an operator-provided fingerprint salt; wire `QueryAnonymizer::with_salt` (`anonymize.rs:28`, `connection.rs:102`). Startup errors if anonymization is on and no salt is set.
- **Error-field scrubbing.** Pass the `M`/`S` error text through a redaction step before attaching (`extractor.rs:130-172`; `connection.rs:636,1043`), or attach only `S`/SQLSTATE by default.
- **Log hygiene.** Gate every raw-query/full-event log line (`connection.rs:523,569,578,628,1025-1030`; `extractor.rs:195,220`; `debug_logger.rs:62-76`) behind an explicit, off-by-default `unsafe_debug_logging` flag. Production builds/log levels never emit raw SQL or params.

### 4.5 Publisher transport (`src/publisher/http_publisher.rs`, `src/config/mod.rs`)
- Validate the endpoint scheme at config load: reject non-`https` unless `publisher.allow_insecure = true` is explicitly set (dev-only). Over `https`, the `X-API-Key` header is protected in transit.

### 4.6 Admin console authentication (`src/proxy/server.rs`, `src/admin/`)
- Require authentication before an admin session is established: consult an `admin_users`/`admin_password` credential (PgBouncer-style) in `handle_admin_connection` (`server.rs:1107-1122`) instead of writing `AuthenticationOk` unconditionally. Default: **admin console disabled** unless an admin credential is configured. The *commands* behind it remain P4; this spec only guarantees no unauthenticated session can exist.

### 4.7 Crypto & secrets hygiene (`src/auth/`, `src/protocol/auth_messages.rs`)
- **Constant-time comparisons** via `subtle::ConstantTimeEq` at `scram.rs:145`, `auth_messages.rs:363`, `file_auth.rs:123,127`.
- **Eliminate username enumeration:** always issue the MD5 challenge and perform a dummy verification for unknown users so timing and message flow are identical (`authenticator.rs:167-202`).
- **Userlist permission check:** `FileAuthenticator::from_file` (`file_auth.rs:29-38`) rejects world-readable/writable files (mirrors PgBouncer).
- **Redacting `Debug`:** implement manual `Debug` for `UserCredentials`, `PasswordEntry`, and config structs holding `password`/`http_api_key`, printing `<redacted>` (`types.rs:36,45`; `config/mod.rs:75,114`).

---

## 5. Automated Auditor (mandatory)

The CI guardrails that fail the build when P1 invariants regress. These join the unified drift-guard gate.

1. **Fail-closed config tests.** `Config::validate()` unit tests asserting that `scram-sha-256`, `cert`-without-verify, auth-type-without-backing, anonymize-without-salt, and non-`https`-publisher-without-opt-in each return a startup error. A test that these configs *fail to build a running proxy* — not merely warn.
2. **Secure-defaults snapshot test.** A serialized snapshot of every security-relevant default (auth type, both `sslmode`s, publisher scheme requirement, admin-console default, logging flags). CI fails if a default changes toward less-safe without an intentional snapshot update in the same PR.
3. **Anonymization fuzzer.** A property/fuzz test (proptest + a corpus of adversarial SQL including DDL like `CREATE ROLE ... PASSWORD '...'`, unparseable vendor syntax, and parameterized statements) asserting that **no produced event and no captured log line contains any input literal or parameter value** when anonymization is enabled. This is the core "verifiable data handling" guarantee.
4. **TLS-downgrade test.** An integration test that connects under `sslmode = require` without negotiating TLS and asserts the connection is rejected, not served plaintext.
5. **Admin-auth test.** An integration test asserting an admin connection without credentials is refused.
6. **Dependency policy** (`cargo audit`/`cargo deny`) runs here too, though remediation of specific CVEs is P7.

---

## 6. Test strategy

- **Unit:** `Config::validate()` matrix (§5.1); constant-time comparison call-sites; redacting `Debug` output; userlist permission rejection.
- **Property/fuzz:** the anonymization fuzzer (§5.3) and wire-adjacent redaction of the error field.
- **Integration (testcontainers + real Postgres):** TLS-downgrade rejection; SCRAM/cert refuse-startup (process exits non-zero with a clear message); admin-auth refusal; MD5 with a hashed userlist; publisher scheme rejection.
- **Regression fixtures:** the secure-defaults snapshot.
- All new tests are wired into the CI drift-guard so they gate merges.

---

## 7. Phasing

- **Harden (Phase 1):** §4.1 (SCRAM/cert refuse, no-trust-default, no default password, fail-closed backing), §4.2 (client TLS downgrade fix), §4.4 (anonymization redaction + parse-failure + params + salt + log hygiene), §4.5 (publisher scheme), §4.6 (admin auth / disabled-by-default), plus auditors §5.1–§5.5. This is the bulk of P1 and gates Phase-1 exit ("no fail-open paths").
- **Complete (Phase 2):** §4.3 strict-backend-TLS knob, §4.7 residual hardening (constant-time everywhere, username-enumeration fix, SASLprep/channel-binding groundwork), MD5 hashed-userlist routing.

---

## 8. Cross-pillar dependencies

- **P4 (Operability):** implements the admin control commands behind the authentication this spec adds; owns config-validation UX and the docs/code drift check. Seam: P1 guarantees *no unauthenticated admin session*; P4 makes the authenticated session *do* things.
- **P6 (Compatibility & Contract):** the event-schema changes for redacted `query`/`params`/error must go through `scry-protocol` versioning; coordinate the schema bump and `cargo-semver-checks`.
- **P7 (Supply-chain):** clears the `postgres-protocol` SCRAM/hstore and `rustls-webpki` advisories that underlie the auth/TLS stack; P1 depends on those upgrades not regressing behavior.
- **P5 (Performance):** constant-time comparisons, TLS enforcement, and anonymization redaction add hot-path work; P5 measures the added-latency budget *after* P1 lands.

---

## 9. Open questions

1. **Trust opt-in shape.** No default `auth_type` (must be set) vs. `Trust` allowed only with an explicit `allow_trust = true`. Recommendation: explicit acknowledgement flag, so an unconfigured deploy fails rather than trusts.
2. **Parse-failure default: drop vs. redact-placeholder.** Dropping loses observability signal; a statement-kind placeholder keeps shape without text. Recommendation: redact-placeholder default, drop as an option.
3. **Backend `require` semantics.** Keep libpq-compatible (encrypt-only) with a strict opt-in, or make `require` verify by default (safer, breaks compat)? Recommendation: keep compatible default + strict knob in Phase 2; revisit for GA.
4. **What the platform still receives.** Confirm normalized SQL + fingerprints + parameter shapes remain useful for the analytics product once raw values are removed (coordinate with platform owners).
5. **Admin credential source.** Reuse the userlist/`admin_users` model or a dedicated secret? Recommendation: PgBouncer-style `admin_users` for familiarity, resolved with P4.
