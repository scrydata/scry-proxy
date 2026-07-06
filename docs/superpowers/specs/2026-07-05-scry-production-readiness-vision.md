# scry-proxy — Production-Readiness Vision (Road to 1.0 GA)

**Status:** Draft for review
**Date:** 2026-07-05
**Scope:** `scry-proxy` (this repository) and the `scry-protocol` crate contract. The hosted analytics platform, backfill, and dashboards are out of scope and treated as external dependencies.
**Audience for the product:** Enterprises deploying scry as a supported commercial offering.

---

## 1. Thesis

An enterprise puts scry directly in the path of its most sensitive system — the production database. For that to be acceptable, five things must be true, and an operator must be able to *verify* each without taking our word for it:

1. **My data is handled safely.** Credentials are never accepted without authentication, traffic is encrypted when I require it, and the query observations that leave my network never contain raw data or secrets.
2. **It is a transparent drop-in.** scry never corrupts a session, mangles a result, or changes the semantics my application depends on.
3. **It stays up.** Backend failures are contained and recovered, not amplified into an outage.
4. **I can operate it.** It exposes what I need to run it, refuses unsafe configurations loudly, and its documentation matches its behavior.
5. **It is fast enough to forget about.** The proxy adds negligible latency, proven under load, not asserted.

This document defines what "scry 1.0, enterprise-ready" means against those five promises, organizes the work into seven pillars, and sequences it through three release gates. Its central engineering principle is that **every guarantee we make is fenced by an automated guardrail** — we do not merely reach production quality, we make it impossible to silently regress away from it.

---

## 2. Where scry is today

scry is a capable, well-structured Postgres proxy with real strengths: safe network bind defaults, bounded startup-message allocation, a non-root container, best-effort event publishing that never blocks the query path, a dedicated resilience module (circuit breaker, retries, healthchecks), connection pooling, and a criterion benchmark harness. A security review on 2026-07-05, however, found that the product does not yet keep its two most load-bearing promises:

- **Authentication fails open.** Configuring the strongest available client auth mode, `scram-sha-256`, silently degrades to *no authentication* (`scry-proxy/src/auth/authenticator.rs:107-117`). Setting an auth type without a backing file also falls back to trust (`scry-proxy/src/proxy/server.rs:304-309`). The `pgbouncer` admin console accepts any connection with no authentication at all (`scry-proxy/src/proxy/server.rs:1107-1122`).
- **TLS can be bypassed.** A client that omits the SSLRequest is served plaintext even under `sslmode = require/verify-full` (`scry-proxy/src/tls/startup.rs:76-79`), which also bypasses client-certificate verification.
- **Anonymization does not anonymize.** The published event always carries the raw SQL regardless of the anonymize setting (`scry-proxy/src/proxy/connection.rs:100-115`), bind parameters are shipped raw and unscrubbed, and a parse failure ships raw SQL with zero redaction while logging it at WARN.
- **Dependency posture has drifted.** `cargo audit` reports eight advisories, including a high-severity SCRAM CPU-exhaustion DoS in `postgres-protocol` 0.6.10.

None of these are subtle. They are the expected state of a fast-moving pre-1.0 project, and they define the starting line for this roadmap.

---

## 3. What "1.0 GA" means

scry is 1.0-GA when an enterprise buyer's security, platform, and SRE reviewers can each sign off:

- **No fail-open paths.** Every authentication and transport mode either enforces its guarantee or refuses to start. Unsafe configuration is a startup error, not a warning.
- **Verifiable data handling.** With anonymization enabled, no raw SQL literal, bind parameter, or embedded secret leaves the process — and there is an automated test proving it.
- **Proven transparency.** A differential test suite demonstrates that results through the proxy are identical to results direct to Postgres across the supported surface.
- **Contained failure.** Fault-injection tests demonstrate the circuit breaker, drain, and recovery behavior meets a documented contract.
- **Measured performance.** The added-latency budget is enforced by a CI regression gate and validated under sustained load.
- **Clean supply chain.** `cargo audit`/`cargo deny` pass, images are digest-pinned and signed, and an SBOM is published per release.
- **A stable contract.** `scry-protocol`'s event schema has a versioning and compatibility commitment, guarded by `cargo-semver-checks`.

---

## 4. Defining principles

These hold across every pillar and every spec.

1. **Fail closed.** When the safe behavior and the convenient behavior conflict, choose safe. A misconfiguration degrades to *refusal*, never to *no protection*. "Falling back to trust" is a defect.
2. **Self-auditing — no drift by construction.** Every guarantee this vision makes is enforced by an automated guardrail that runs in CI and fails the build on regression. A pillar is not "done" when it is fixed; it is done when it is fixed *and fenced*. Guarding is a first-class deliverable, specced and sequenced alongside the fix.
3. **Transparent proxy.** scry sits in the query path and must be indistinguishable from a direct connection with respect to correctness. Observability is additive; it never changes results or session semantics.
4. **Observability is best-effort.** Capturing, anonymizing, and publishing an observation must never block, slow, or fail a client query. Under pressure, we drop observations, never queries.

---

## 5. The seven pillars

Each pillar is one "part" and receives its own spec. Each pillar spec must define an **Automated Auditor**: the CI guardrail that fails the build when the pillar's invariants regress.

### P1 — Security & Privacy
**Intent:** scry is secure-by-default and never leaks data or credentials. Auth (client and backend), transport, anonymization, admin access, and secrets handling all fail closed.
**1.0 exit criteria:** `scram-sha-256`/`cert` either enforce or refuse to start; no TLS downgrade path; admin console authenticated; anonymization strips raw SQL, parameters, and embedded secrets; constant-time credential comparisons; no secrets in logs; userlist permission checks.
**Automated Auditor:** `cargo audit`/`cargo deny` in CI; fail-closed unit tests (e.g. "SCRAM configured without an implementation must refuse startup"); a **secure-defaults snapshot** test that fails if any default becomes less safe; an **anonymization fuzzer** asserting no raw literal or parameter value ever appears in a produced event.
**Phase:** Harden (primary), with residual hardening through Complete.

### P2 — Protocol Correctness & Transparency
**Intent:** scry faithfully implements the Postgres wire protocol and never corrupts a session. Extended/prepared-statement flows, connection-state tracking, and session/transaction pooling are correct under real workloads.
**1.0 exit criteria:** a documented correctness bar; extended-protocol and prepared-statement edge cases covered; connection-state and pooling-mode correctness proven against real Postgres.
**Automated Auditor:** a **differential "transparency" suite** — the same operations run direct-to-Postgres and through the proxy must produce identical results and session behavior — plus wire-parser fuzzing, both against real Postgres via testcontainers.
**Phase:** Complete.

### P3 — Reliability & Resilience
**Intent:** backend failures are contained and recovered, never amplified. Circuit breaking, retries, healthchecks, graceful drain/shutdown, and backpressure meet documented contracts.
**1.0 exit criteria:** breaker open/half-open/close semantics specified and enforced; graceful drain on shutdown; backpressure behavior bounded and documented; failover semantics defined.
**Automated Auditor:** **fault-injection tests** in CI — kill or stall the backend and assert the breaker opens, the pool drains, and recovery occurs within the documented contract.
**Phase:** Complete.

### P4 — Operability & Observability
**Intent:** operators can run scry confidently. The admin console is functional and authenticated, metrics/tracing/OTel are complete, configuration validates loudly, and documentation matches behavior.
**1.0 exit criteria:** admin control commands (PAUSE/RESUME/RELOAD/SHUTDOWN/KILL) implemented and gated behind admin auth; metrics endpoint access-controlled or documented as trusted-network-only with safe defaults; config validation rejects unsafe/unknown settings; docs/code parity.
**Automated Auditor:** admin-auth tests; config-schema validation tests; a **docs/code drift check** asserting every documented `SCRY_*` variable maps to a real config field (this class of check would have caught the `SCRY_BACKEND__PASSWORD_FILE` gap between `docs/deployment.md:120` and `scry-proxy/src/config/mod.rs:75`).
**Phase:** Complete (admin auth stub lands in Harden).

### P5 — Performance & Scale
**Intent:** the proxy's added latency is negligible and proven — the project's stated <1ms-per-query target holds under sustained, concurrent load.
**1.0 exit criteria:** a defined and enforced added-latency budget (p50/p95/p99); pooling efficiency validated; soak/load results published; resource ceilings (connections, queue depth, timeouts) enforced.
**Automated Auditor:** a criterion benchmark with a **regression gate** that fails CI when added latency exceeds the budget; scheduled soak/load runs against a representative workload.
**Phase:** Prove (budget gate can land earlier as a non-blocking signal).

### P6 — Compatibility & Contract
**Intent:** scry is a predictable drop-in and a stable data source. PgBouncer configuration compatibility, the `scry-protocol` event-schema contract, and Postgres version support are explicit and guaranteed.
**1.0 exit criteria:** documented PgBouncer-config compatibility surface; a published versioning and backward-compatibility policy for `scry-protocol`; a supported Postgres version matrix.
**Automated Auditor:** `cargo-semver-checks` on `scry-protocol` to block accidental breaking changes; a **PgBouncer-config parse corpus**; a Postgres-version test matrix in CI.
**Phase:** Complete → Prove.

### P7 — Supply-chain & Release Engineering
**Intent:** scry ships with the provenance and hygiene an enterprise security team requires.
**1.0 exit criteria:** `cargo audit`/`cargo deny` clean; base images and CI actions digest/SHA-pinned; reproducible builds; signed images with published provenance; SBOM generated and diffed per release; a semver-based release process.
**Automated Auditor:** `cargo deny` policy enforcement; **SBOM generation and diff** per build; digest-pinning and action-pinning lints that fail CI on an unpinned reference.
**Phase:** Harden (advisories + pinning) → Prove (signing, SBOM, reproducibility).

---

## 6. The three-gate roadmap

Each pillar contributes exit criteria to each gate it appears in. A pillar's guardrail lands with or immediately after its fix so drift cannot reopen.

| Pillar | Phase 1 — Harden (fail-closed) | Phase 2 — Complete (fill the gaps) | Phase 3 — Prove (enterprise bar) |
|--------|-------------------------------|-----------------------------------|----------------------------------|
| P1 Security | Secure defaults, no fail-open auth/TLS, anonymization redacts, admin auth, secure-defaults + anon-fuzzer guardrails | Residual hardening (constant-time, SASLprep, channel binding) | — |
| P2 Protocol | — | Correctness bar + differential/fuzz suite | Broaden matrix under load |
| P3 Reliability | — | Contracts + fault-injection guardrail | Validate under load |
| P4 Operability | Admin-auth stub, config validation | Functional admin commands, docs/code drift guardrail | — |
| P5 Performance | Budget gate as non-blocking signal | — | Enforced budget + soak/load |
| P6 Compatibility | — | `scry-protocol` policy + semver-checks, pgbouncer corpus | Postgres version matrix |
| P7 Supply-chain | Clear advisories, digest/action pinning | — | Signing, SBOM, reproducible builds |

**Gate exits:** Phase 1 — no fail-open paths, `cargo audit` clean. Phase 2 — every advertised feature works and is guarded. Phase 3 — measured against the latency budget under load; GA-signable.

---

## 7. Automated-auditing strategy

The per-pillar auditors compose into a single CI gate — the **drift guard** — that must be green to merge. It runs the union of: dependency policy (`cargo audit`/`cargo deny`), fail-closed and secure-defaults tests, the anonymization fuzzer, the transparency differential suite, fault-injection tests, config/docs-parity checks, the latency regression gate, `cargo-semver-checks`, and the pinning/SBOM lints. New guardrails are added to this gate as their pillars land, so the set of things that "can't regress" grows monotonically toward GA. Where a check is too slow for every push (soak, full version matrix), it runs on a schedule and blocks the release tag rather than the merge.

---

## 8. Out of scope & dependencies

Out of scope for this vision: the hosted analytics platform, backfill tooling, and dashboards. scry depends on them only through the `scry-protocol` event contract (P6), which is the seam this repository owns and guarantees. The analytics endpoint's own authentication, storage, and retention are the platform's responsibility; scry's obligation ends at shipping well-formed, properly anonymized, best-effort events over an enforced-TLS channel.

---

## 9. Open questions & risks

- **Client-side SCRAM: implement or refuse?** Phase 1 requires `scram-sha-256` to stop failing open. The minimum safe fix is to refuse startup; full client-SCRAM support is more work. Decide per the P1 spec.
- **Anonymization vs. utility.** Redacting bind parameters and raw SQL protects data but reduces observation richness. The P1 spec must define exactly what the platform still receives (normalized SQL, fingerprints, shapes) and confirm that remains useful.
- **Performance cost of hardening.** Constant-time comparisons, TLS enforcement, and anonymization redaction all add work on the hot path; P5's budget must be measured *after* P1 lands, not before.
- **Dependency upgrades and MSRV.** Clearing advisories (P7) may pull in newer crate versions; confirm no MSRV or API regressions.
- **PgBouncer-compatibility scope.** How much of PgBouncer's config surface do we commit to (P6)? A narrower, documented surface is safer than implicit full compatibility.

---

## 10. Next steps

Per the agreed workflow, each pillar now gets its own spec (`docs/superpowers/specs/YYYY-MM-DD-<pillar>-design.md`) drafted from the audit evidence and the shared template — **Purpose & scope · Current state (file:line evidence) · Target / 1.0 exit criteria · Design · Automated Auditor · Test strategy · Phasing · Cross-pillar dependencies · Open questions** — and reviewed before the next is drafted. Drafting proceeds in dependency order beginning with **P1 — Security & Privacy**.
