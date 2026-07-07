# P3 — Reliability & Resilience (Pillar Spec)

**Status:** Draft for review
**Date:** 2026-07-05
**Pillar:** P3 of the [Production-Readiness Vision](./2026-07-05-scry-production-readiness-vision.md)
**Phase:** Complete (primary); SIGTERM graceful shutdown (§4.4) is high-priority-early

---

## 1. Purpose & scope

Backend failures must be **contained and recovered, never amplified**, and every reliability guarantee scry advertises must actually gate traffic — no aspirational features that only log. The theme of this pillar is **integration and enforcement**: the primitives largely exist and are unit-tested; the work is wiring them into the request path, enforcing the timeouts that are currently parsed-and-ignored, and proving the behavior with fault injection.

In scope:
- Circuit-breaker scope (what trips it) and per-backend isolation.
- Active healthcheck actually gating traffic (or its removal).
- Timeout enforcement: `query_timeout`, backend `connection_timeout_ms`, pool acquisition.
- Graceful shutdown for containerized production (**SIGTERM**) and the drain contract.
- Retry safety preservation (no non-idempotent replay).
- Documented reliability contracts and fault-injection guardrails.

Out of scope (cross-referenced):
- Implementing admin `SHUTDOWN`/`RELOAD`/`KILL` *commands* → **P4** (P3 provides the underlying shutdown/reload mechanism).
- Connection-state/pooling correctness → **P2**. Latency budget → **P5**.

---

## 2. Current state (evidence)

From the 2026-07-05 reliability-surface mapping. The primitives are solid units; the exposure is in integration, enforcement, and containerized shutdown.

**Genuinely implemented & tested:** circuit-breaker state machine (`scry-proxy/src/resilience/circuit_breaker.rs:24-329`, tests `:343-517`), retry backoff/jitter (`resilience/retry.rs:27-91`), passive recycle-time healthchecks (`proxy/tcp_pool.rs:304-387`), `max_connections` enforcement (`proxy/server.rs:612-641`), backpressure modes (`proxy/connection.rs:265-327`), SIGINT drain-with-timeout (`proxy/server.rs:838-895`).

**Correctness — good, preserve it:** retry wraps **only** `pool.get()` connection acquisition (`tcp_pool.rs:150-164`); query execution is never retried, so **non-idempotent statements are never replayed**. This is safe and must stay so.

| Sev | Gap | Location |
|-----|-----|----------|
| HIGH | **No SIGTERM handler.** Only SIGINT triggers graceful drain (`server.rs:577-588`). Under Kubernetes/Docker (SIGTERM) the process is hard-killed, dropping every in-flight query | `proxy/server.rs:577-588` |
| HIGH | **Active healthcheck gates nothing.** `ActiveHealthcheck::check` runs on an interval but its result is only logged; `is_healthy()` has zero non-test consumers — it does not feed the breaker, HealthMonitor, or rotation | `resilience/healthcheck.rs:45-100`, `server.rs:110-135` |
| HIGH | **`query_timeout` parsed but never enforced.** Read from pgbouncer-compat config, zero consumers in the query path. A hung query is never timed out | `config/pgbouncer.rs:101,139,201` (no query-path consumer) |
| MED | **Circuit breaker only observes connection-acquisition failures**, not query/backend errors or timeouts (`record_failure`/`record_success` called only on `pool.get()`), so an unhealthy-but-connectable backend does not trip it | `tcp_pool.rs:139-144` |
| MED | **Global (not per-backend) circuit breaker** shared across all DB pools — one flaky backend trips the breaker for all | `proxy/server.rs:148-170` |
| MED | **`connection_timeout_ms` parsed but never applied**; `TcpStream::connect` has no timeout and deadpool sets no `Timeouts`, so a backend that accepts TCP but hangs blocks connection creation indefinitely | `config/mod.rs:77`, `tcp_pool.rs:263-296,88-100` |
| MED | **deadpool `get()` has no timeout**, so it blocks on exhaustion instead of erroring into the bounded `WaitQueue` — `pool_timeout_secs`/`pool_queue_depth` may not bound wait time as designed | `pool_manager.rs:183` |
| MED | **Drain force-aborts on timeout** — a query still running past `shutdown_timeout_secs` is abruptly cut (expected trade-off, but real) | `server.rs:891` |
| LOW-MED | **Doc/code drift:** breaker opens on `consecutive_failures`, not the documented failure *window* (window is metrics-only) | `circuit_breaker.rs:231,331-340` vs `docs/circuit-breaker.md` |
| LOW | Retry does not classify errors — safe only incidentally because `create()` does connect+TLS only; fragile if it ever grows auth logic | `retry.rs`, `tcp_pool.rs:263-296` |

Admin `SHUTDOWN`/`RELOAD`/`KILL` are no-op success stubs (`admin/commands.rs:381-394`) — the *commands* are P4, but they need a real P3 mechanism to call.

---

## 3. Target / 1.0 exit criteria

1. **Containerized graceful shutdown.** SIGTERM triggers the same bounded drain as SIGINT; the drain contract (window, what happens to over-deadline queries) is documented and tested.
2. **Every advertised reliability feature gates traffic.** The active healthcheck either removes an unhealthy backend from rotation / trips the breaker, **or** it is removed and the docs corrected. No feature that only logs.
3. **Hung work is bounded.** `query_timeout` and backend `connection_timeout_ms` are enforced; pool acquisition is bounded by `pool_timeout_secs` (deadpool timeout configured) so backpressure behaves as designed.
4. **Failure containment is meaningful.** The breaker responds to genuine backend faults (connection failures, connect/query timeouts, health failures), not only connection acquisition, and is **per-backend** so one bad backend does not trip others. Client SQL errors do **not** trip it (they are not backend faults).
5. **Retry stays safe.** Connection-acquisition-only retry is preserved and made safe *by explicit error classification*, not by accident.
6. **Documented contracts + fault-injection guardrail** (§5) prove the above and prevent regression.

---

## 4. Design

### 4.1 Circuit-breaker scope & isolation (`resilience/circuit_breaker.rs`, `proxy/tcp_pool.rs`, `server.rs`)
- **Broaden failure signals** feeding `record_failure`/`record_success` to include connect timeouts, query timeouts (§4.3), and health-check failures (§4.2) — with an explicit classifier so that **client-caused errors** (SQL syntax, constraint violations, auth) never count as backend faults. This is the key correctness boundary: the breaker protects against *backend* unhealth, not *query* failure.
- **Per-backend breakers.** Replace the single globally-shared breaker (`server.rs:148-170`) with one per backend/pool, keyed by backend identity, so isolation matches the multi-DB routing model.
- **Reconcile window semantics** (`circuit_breaker.rs:231,331-340`): either make the documented failure-*window* actually drive tripping, or update `docs/circuit-breaker.md` to describe the consecutive-failure model. Pick one and make code and docs agree (P4's docs/code drift check will enforce it thereafter).

### 4.2 Active healthcheck — integrate or remove (`resilience/healthcheck.rs`, `server.rs`)
- **Integrate (recommended):** wire `ActiveHealthcheck` results into the per-backend breaker and/or a rotation gate so a backend that fails N consecutive active probes is treated as Open (traffic shed) until it recovers, independent of whether a client happens to hit it. `is_healthy()` gains real consumers.
- If integration is deferred past 1.0, **remove the feature and its config** and correct `docs/health-checks.md` — fail-closed on honesty: no config knob may imply protection it does not provide.

### 4.3 Timeout enforcement (`config`, `proxy/connection.rs`, `tcp_pool.rs`, `pool_manager.rs`)
- **`query_timeout`:** enforce in the managed query path — start a timer at query dispatch, and on expiry send a Postgres-appropriate cancel/error and reclaim or close the connection (never leave a hung query holding a pooled connection). Coordinate the cancel semantics with P2 (must not corrupt session/pool state).
- **`connection_timeout_ms`:** wrap `TcpStream::connect` (+TLS upgrade) in a timeout (`tcp_pool.rs:263-296`).
- **Pool acquisition:** configure deadpool `Timeouts` (`tcp_pool.rs:88-100`) so `get()` errors on exhaustion and control flows into the bounded `WaitQueue`, making `pool_timeout_secs`/`pool_queue_depth` authoritative (`pool_manager.rs:183`).
- Principle: a timeout that is *parsed but not enforced* is a reliability lie; each must either enforce or be rejected at config load (coordinate with P1/P4 config validation).

### 4.4 Graceful shutdown for containers (`proxy/server.rs`, `proxy/mod.rs`)
- Add a **SIGTERM** handler that triggers the existing `drain_connections` path (`server.rs:838-895`), identical to SIGINT (`:577-588`). Document the drain contract: in-flight connections finish within `shutdown_timeout_secs`; over-deadline tasks are aborted (`:891`) — consider a "hard vs. soft" deadline where a longer grace is honored for genuinely-active queries if the platform's termination grace allows. Expose the shutdown mechanism as a callable so P4's admin `SHUTDOWN` can invoke it.

### 4.5 Retry safety hardening (`resilience/retry.rs`, `tcp_pool.rs`)
- Keep retry scoped to connection acquisition. Add an explicit **error classifier** so only transient connect/transport errors are retried; anything else surfaces immediately. This preserves "no non-idempotent replay" *by design* rather than incidentally, protecting against future changes to `create()`.

---

## 5. Automated Auditor (mandatory)

The fault-injection guardrail — the core P3 auditor — plus enforcement checks. All join the unified CI drift-guard.

1. **Fault-injection suite (core).** Against real Postgres (testcontainers) with a controllable fault layer: (a) kill the backend mid-workload → assert the per-backend breaker opens, traffic is shed, and clients get clean errors; (b) restore the backend → assert half-open probe and close within the documented timing; (c) stall the backend TCP → assert `connection_timeout_ms` fires; (d) run a query past `query_timeout` → assert it is cancelled and the connection is reclaimed cleanly (no pool corruption, coordinated with P2's dirty-connection check).
2. **SIGTERM drain test.** Start a long in-flight query, send SIGTERM, assert the query is allowed to finish within the window and the client sees a clean completion (and that an over-deadline query is aborted per contract).
3. **Healthcheck-gating test.** Make the active probe fail while the backend still accepts TCP; assert traffic is shed (proves the feature gates something) — or, if removed, a test asserting the config knob is gone.
4. **Breaker-scope test.** Assert client SQL errors do **not** trip the breaker and that a fault on backend A does **not** open backend B's breaker.
5. **Retry-safety test.** Assert non-connection errors are not retried and no query is ever replayed.

---

## 6. Test strategy

- **Fault-injection integration** (§5.1–§5.4): the primary new investment; extend `tests/shutdown_test.rs` and add a resilience integration suite with a fault-injection harness.
- **Unit:** breaker failure classification, per-backend keying, error classifier for retry, timeout timers.
- **Regression:** preserve existing breaker unit tests (`circuit_breaker.rs:343-517`) and connection-limit/backpressure tests.
- Docker-dependent tests run on CI runners with a daemon; classify-and-timeout unit tests run everywhere.

---

## 7. Phasing

- **Early (high-priority within Complete):** §4.4 SIGTERM graceful shutdown — production-blocking for any containerized deployment; cheap.
- **Complete (Phase 2):** §4.1 breaker scope + per-backend, §4.2 active-healthcheck integration, §4.3 timeout enforcement, §4.5 retry classifier, §5 fault-injection guardrail, doc/code reconciliation.

---

## 8. Cross-pillar dependencies

- **P4 (Operability):** admin `SHUTDOWN`/`RELOAD`/`KILL` commands call the P3 shutdown/reload mechanism; breaker state and pool/health metrics must be exposed for operators; P4's docs/code drift check enforces the §4.1/§4.2 doc reconciliation.
- **P2 (Protocol):** `query_timeout` cancellation and connection reclamation must not corrupt session/pool state — the cancel path is co-designed with P2's transparency and dirty-connection guarantees.
- **P5 (Performance):** timeout timers and per-backend breaker bookkeeping add hot-path work; the fault-injection and shed behavior are validated under load in P5.
- **P1 (Security):** config validation for timeout/shutdown settings shares the fail-closed `Config::validate()` surface.

---

## 9. Open questions

1. **Active healthcheck: integrate or cut for 1.0?** Recommendation: integrate into the per-backend breaker — it's most of the value of the feature and the code already exists.
2. **`query_timeout` cancel mechanism.** Postgres query cancellation requires the cancel key / a separate connection; simplest safe behavior may be to close the backend connection and return an error rather than issue a true `CancelRequest`. Decide the mechanism with P2. Recommendation: start with connection-close-on-timeout, add true CancelRequest later.
3. **Drain deadline model.** Single hard deadline vs. soft/hard two-tier for long-running queries during shutdown. Recommendation: single documented deadline for 1.0, tied to the platform termination grace period.
4. **Breaker failure definition.** Exact set of backend-fault signals that trip it; ensure it excludes all client-attributable errors. Needs a precise classifier spec.
