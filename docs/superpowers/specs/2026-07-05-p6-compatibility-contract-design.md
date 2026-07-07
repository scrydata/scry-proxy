# P6 — Compatibility & Contract (Pillar Spec)

**Status:** Draft for review
**Date:** 2026-07-05
**Pillar:** P6 of the [Production-Readiness Vision](./2026-07-05-scry-production-readiness-vision.md)
**Phase:** Complete (schema versioning, semver gate, pgbouncer corpus) → Prove (Postgres version matrix)

---

## 1. Purpose & scope

Make scry a **predictable drop-in and a stable data source**: the `scry-protocol` event schema has an explicit versioning and backward-compatibility commitment, the PgBouncer-compatibility surface is documented and honest (no silent drops), and the supported Postgres version range is stated and tested. The recurring failure mode this pillar closes is **silent divergence** — config directives and schema changes that vanish or corrupt without an error.

In scope:
- `scry-protocol` wire-schema versioning, compatibility policy, and the elimination of silent data-loss/opaque-failure paths.
- PgBouncer config compatibility: an accurate, documented surface with fail-loud handling of the unsupported.
- Postgres version support: validation, a tested matrix, and a stated range.
- Cleanup of the misleading dual serialization surface (FlatBuffers vs FlexBuffers).

Out of scope (cross-referenced):
- The *content* of anonymized events (redacted query/params) → **P1** (P6 owns the schema *envelope* those changes ship in).
- `query_timeout` *enforcement* → **P3** (P6 owns the pgbouncer mapping once enforced).
- The release/semver *process* → **P7** (P6 owns the schema-compat *gate* that drives it).

---

## 2. Current state (evidence)

From the 2026-07-05 compatibility-surface mapping.

**Genuinely solid:** the userlist/auth-file parser (`scry-proxy/src/auth/file_auth.rs`) supports PgBouncer's plaintext/`md5`/`SCRAM-SHA-256` formats correctly; the FlexBuffers round-trip has same-version tests (`scry-protocol` `deserializer.rs:118-302`). `Cargo.lock` pins the protocol crate.

**The gaps — silent divergence in three areas:**

### 2a. `scry-protocol` contract — no versioning (critical)
- **No schema version field anywhere** — none on `QueryEventBatch` (`serializer.rs:14-19`), none on the event, no version constant in `lib.rs`, no `#[non_exhaustive]`. The crate's semver and the HTTP `Content-Type` are the *only* contract.
- The wire event is a transformed `SerializableEvent` (`serializer.rs:21-42`): `timestamp`→`timestamp_us`, `duration`→`duration_us`, and **`params` flattened from typed `ParamValue` to `Vec<String>` of JSON** (`serializer.rs:79-83`) — stringly-typed on the wire.
- **Opaque batch failure:** changing/renaming/removing any *required* field makes the consumer hard-fail the **entire batch** with a `FlexBuffersError` indistinguishable from corruption (`deserializer.rs:66-69`); with no version field the consumer can't even diagnose version skew.
- **Silent per-param data loss:** on `ParamValue` skew, `deserializer.rs:97` `filter_map(...ok())` drops unparseable params **without setting `params_incomplete` or erroring**.
- Compatibility is incidental to FlexBuffers being name-keyed + serde defaults; there is no migration story, negotiation, or compat test across versions. Lock/cache drift already visible (lock pins 0.1.0; cache has 0.1.1).
- **Misleading dual surface:** `FlatBuffersSerializer` is a misnomer (it uses FlexBuffers — `serializer.rs:2,62`); two `.fbs` schemas and the `database_event` FlatBuffers module ship but are **unused dead code** in this repo.

### 2b. PgBouncer config — silent drops & overclaim
- Only **9** `[pgbouncer]` settings honored; every other key silently dropped via `_ => {}` (`config/pgbouncer.rs:141`). Reference configs set `auth_type`, `auth_file`, `reserve_pool_size`, `server_lifetime`, `log_*`, `admin_users`, `stats_users` — **all silently vanish**.
- `query_timeout` parsed but never mapped (`pgbouncer.rs:98-101,139`) — silent no-op.
- **Only the first `[databases]` entry is used** (`pgbouncer.rs:279`, test `:1006`) — multi-database configs silently collapse to one backend.
- `pool_mode = statement` silently downgraded to Transaction (`:246-250`); unknown modes silently become Hybrid (`:251`).
- Invalid numeric values silently become `None` via `.parse().ok()` — typo'd ports ignored, not rejected.
- **"Drop-in replacement for PgBouncer"** claimed (`pgbouncer.rs:3`, `docs/architecture.md:42,49`) but not delivered.

### 2c. Postgres version support — unstated & unvalidated
- **Protocol version parsed but never validated** (`protocol/auth_messages.rs:25-73,35`); any version accepted, no rejection contract.
- **No `CancelRequest` (80877102) or GSSENC (80877104) handling** — query cancellation and GSSAPI encryption unsupported with no explicit error.
- No version detection, min-version gate, or version-specific code. **Only PG16 tested** (`postgres:16-alpine` across all integration tests); docs say PG15 (stale) / 12.0+ (`docs/getting-started.md:20,67`). No support matrix.

---

## 3. Target / 1.0 exit criteria

1. **A versioned, stable event contract.** `scry-protocol` carries an explicit schema version; a documented compatibility policy defines what may change within a version; version skew is **detectable and diagnosable**, never an opaque batch failure; no silent per-param data loss.
2. **An honest, documented PgBouncer surface.** The exact supported settings are documented; unsupported/ignored directives produce a **loud warning or error**, never a silent drop; the "drop-in" claim is corrected to match reality.
3. **A stated, tested Postgres range.** A support matrix (tested across multiple PG versions); the wire protocol version is validated with a clean rejection contract; `CancelRequest`/GSSENC are handled or explicitly, cleanly refused.
4. **No misleading surface.** The serialization format is named and documented accurately; dead FlatBuffers surface is removed or clearly separated.
5. **Guardrails:** `cargo-semver-checks` on `scry-protocol`, a pgbouncer-config parse corpus, and a Postgres version matrix in CI (§5).

---

## 4. Design

### 4.1 `scry-protocol` schema versioning (the crown jewel)
- **Add an explicit schema version** to the wire envelope (`QueryEventBatch` at `serializer.rs:14-19`) — a `schema_version` integer/semver, so a consumer can detect and diagnose skew before attempting to decode.
- **Compatibility policy:** document what constitutes a compatible vs. breaking change (add-optional-field = compatible; rename/remove/retype-required = breaking → new version). Enforce producer discipline (`skip_serializing_if` for optional fields) and consumer discipline (`#[serde(default)]`), and consider `#[non_exhaustive]` on `QueryEvent`/`ParamValue`.
- **Fail loud, not silent:** on version skew or an unparseable param, surface a typed error and/or set `params_incomplete` — eliminate the `filter_map(...ok())` silent drop at `deserializer.rs:97`. A version mismatch must be reported as such, not as generic corruption.
- **Naming/surface cleanup:** rename the misnomer `FlatBuffersSerializer` (or document it clearly as FlexBuffers), and remove or clearly quarantine the unused `database_event` FlatBuffers module and `.fbs` schemas so consumers aren't misled about the wire format.
- This is coordinated with **P7** (which drives crate releases off the compat gate) and, critically, with **P1** (§8 sequencing).

### 4.2 Fail-loud PgBouncer config (`config/pgbouncer.rs`)
- Replace the silent `_ => {}` catch-alls (`pgbouncer.rs:141,58`) with **explicit handling**: recognized-and-honored, recognized-but-unsupported (loud warning naming the directive), or unknown (warning). No supported-looking directive may vanish silently.
- **Reject invalid values** instead of `.parse().ok()` → `None`: a typo'd port/number is a config error, shared with P1/P4's fail-closed `Config::validate()`.
- **Multi-database:** either route multiple `[databases]` entries through scry's native multi-DB routing (`config/mod.rs:44`) or, if unsupported for 1.0, **error loudly** when more than one entry is present rather than silently using the first (`pgbouncer.rs:279`).
- **Pool-mode honesty:** warn on `statement`→Transaction downgrade and reject unknown modes rather than defaulting to Hybrid (`:246-251`).
- **`query_timeout`:** once P3 enforces it, wire the pgbouncer mapping through; until then, the loud-unsupported warning applies.

### 4.3 Right-size the compatibility claim (docs)
Document the **exact** supported PgBouncer subset (the honored settings, the single-database limitation, auth handling), and replace "drop-in replacement" (`pgbouncer.rs:3`, `docs/architecture.md:42,49`) with an accurate "PgBouncer-compatible for the following settings" framing. This documented surface becomes the spec the parse corpus (§5.2) tests against.

### 4.4 Postgres version support (`protocol/`, tests, docs)
- **Validate the protocol version** at startup (`auth_messages.rs:25-73`): accept 3.0, and define an explicit contract for anything else (clean `ErrorResponse` for unsupported, or documented pass-through). No silently-accepted garbage version.
- **Handle or cleanly refuse `CancelRequest` and GSSENC** (currently unhandled) — at minimum a defined, non-confusing response.
- **Version matrix:** run the integration/differential suite (shared with P2) across a tested range (e.g. PG13–17), and **publish a support matrix** in the docs. Correct the stale PG15 reference (`docs/getting-started.md:67`).

---

## 5. Automated Auditor (mandatory)

1. **`cargo-semver-checks` on `scry-protocol`** — fails CI when a change to the crate's public API (and, by extension, the schema types) is breaking without a version bump. The primary guard on the contract's stability.
2. **Wire-schema compatibility test** — encode with the current producer, decode with a pinned prior-version consumer fixture (and vice-versa); assert compatible changes survive and breaking changes are **reported as version errors, not silent loss or opaque corruption**. Directly guards the §4.1 silent-drop/opaque-failure paths.
3. **PgBouncer-config parse corpus** — a corpus of real `pgbouncer.ini` files (including the unsupported directives) asserting each is either honored or **produces the documented loud warning/error** — never silently dropped. Guards §4.2/§4.3.
4. **Postgres version matrix** — the integration/differential suite (with P2) run against each supported PG version; fails if a version regresses. Guards §4.4.

Matrix and corpus tests are heavier; they gate the release tag and run on schedule, with a fast subset per-PR.

---

## 6. Test strategy

- **Contract:** wire-schema cross-version fixtures (§5.2) and `cargo-semver-checks` (§5.1); extend `scry-protocol`'s round-trip tests to cross-version cases.
- **Config:** the pgbouncer parse corpus (§5.3) plus unit tests asserting no silent drop and value rejection.
- **Postgres:** the version matrix (§5.4), shared with P2's differential suite; protocol-version validation unit tests.
- **Docs:** the documented compatibility surface is the source of truth the corpus is checked against.

---

## 7. Phasing

- **Complete (Phase 2):** §4.1 schema versioning + fail-loud contract + surface cleanup, §4.2 fail-loud pgbouncer config, §4.3 documented surface, §5.1–§5.3 guardrails.
- **Prove (Phase 3):** §4.4 Postgres version matrix + protocol validation, §5.4 matrix guardrail; publish the support matrix as GA-signable evidence.

---

## 8. Cross-pillar dependencies

- **P1 (Security) — sequencing-critical.** P1's anonymization work (Harden/Phase 1) changes the event schema (normalized-only `query`, redacted `params`). That schema change must ship inside P6's versioned envelope, so the **minimal schema-version field (§4.1) must land with or before P1's schema change** — pulling that slice of P6 earlier than the rest of the pillar. Flag for the roadmap: this is the one place the phase ordering (P1 Harden vs P6 Complete) needs an explicit early hand-off.
- **P3 (Reliability):** `query_timeout` pgbouncer mapping (§4.2) activates once P3 enforces the timeout.
- **P2 (Protocol):** the Postgres version matrix (§4.4/§5.4) is the differential suite P2 builds; P2's event-attribution changes also flow through P6's schema versioning.
- **P4 (Operability):** fail-loud config (§4.2) shares P4/P1's `Config::validate()` surface; PgBouncer `SHOW`/console compatibility coordinates with P4's admin console.
- **P7 (Supply-chain):** P6's `cargo-semver-checks` gate drives P7's release/version-bump process; the 0.1.0/0.1.1 lock drift is also a P7 hygiene item.

---

## 9. Open questions

1. **Schema version representation.** Integer epoch vs. semver string vs. embedding the crate version. Recommendation: an explicit integer `schema_version` independent of crate semver, so wire compat is decoupled from Rust API compat.
2. **Multi-database via pgbouncer path: support or refuse for 1.0?** Recommendation: refuse loudly for 1.0 (steer users to native multi-DB routing), support later — silent-collapse is the unacceptable state.
3. **Postgres version range.** How wide (13–17? 12+?) and how many are gated per-PR vs. scheduled? Recommendation: test 14–17, state 13+ as supported, gate one version per-PR and the full matrix pre-release. Coordinate cost with P5.
4. **Protocol 3.1 / PG18 forward-compat.** Reject unknown protocol versions or attempt pass-through? Recommendation: explicit reject with a clear error for 1.0; revisit 3.1 support post-GA.
5. **Dead FlatBuffers surface: remove or keep for the platform's CDC path?** The `database_event` module may be intended for the (out-of-scope) platform. Recommendation: move it out of the proxy's dependency surface or clearly mark it non-wire, to avoid consumer confusion.
