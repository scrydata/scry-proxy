# P2 — Protocol Correctness & Transparency (Pillar Spec)

**Status:** Draft for review
**Date:** 2026-07-05
**Pillar:** P2 of the [Production-Readiness Vision](./2026-07-05-scry-production-readiness-vision.md)
**Phase:** Complete (primary); the conservative-pinning safety default (§4.1) is Phase-1-eligible

---

## 1. Purpose & scope

Guarantee that scry is **indistinguishable from a direct Postgres connection** with respect to correctness. A client must never be able to tell — from results, session state, or protocol behavior — that a proxy is in the path, regardless of pooling mode. Where scry cannot guarantee that, it must **fail closed**: keep the connection pinned/1:1 rather than risk a wrong pooling decision.

In scope:
- A precise **transparency contract** (what "transparent" means, testably).
- **Connection-state tracking completeness** across the *extended* query protocol (Parse/Bind/Execute), not just simple query.
- **LISTEN/NOTIFY** and other non-replayable session state modeling.
- **Message reassembly** correctness (TCP fragmentation / large messages).
- **Prepared-statement handling** and transaction-mode replay robustness (or honest restriction).
- The **differential transparency test suite** and **wire-parser fuzzing** that guard all of the above.

Out of scope (cross-referenced):
- Client-side SCRAM auth correctness → **P1**.
- Admin console → **P4**. Performance of the parsing path → **P5**. Event-schema changes → **P6**.

---

## 2. Current state (evidence)

From the 2026-07-05 protocol-surface mapping. The foundational good news first:

- **Byte passthrough is preserved on every data path** — raw client/backend bytes are forwarded verbatim (`scry-proxy/src/proxy/connection.rs:604,701,778,992,1092`). All protocol parsing is *observational* (event logging, pooling/pinning), never rewriting the forwarded stream. **Result bytes cannot be silently altered.** This is the anchor of scry's transparency story and must be protected.
- **Parsers are defensively written** — every short read returns `None`/`Err`; startup length is bounds-checked (`scry-proxy/src/protocol/extractor.rs`, `bind.rs`, `startup.rs:57-71`). No `panic!`/`todo!`/`unimplemented!` in the non-test protocol path.

The correctness exposure is concentrated in **state-tracking completeness** and **testing**:

| Sev | Gap | Location |
|-----|-----|----------|
| HIGH | Extended-protocol state changes never tracked. `update_connection_state` runs only in the simple-`Query` arm; the `Parse` arm records the statement but never inspects it. `SET`/`CREATE TEMP TABLE`/`DECLARE CURSOR`/`pg_advisory_lock` issued via Parse/Bind (i.e. virtually every modern driver) set **no pin state** → in Hybrid mode the connection is deemed unpinned and released, losing/leaking session state | `connection.rs:510-540` vs `:586` |
| HIGH | LISTEN/NOTIFY/UNLISTEN not modeled at all; no `PinReason::Listen`. A listening session can be recycled (DISCARD ALL drops the registration) and async `NotificationResponse` messages lost or misrouted | `command_detector.rs` (absent); `connection_state.rs:7-13` |
| HIGH | Transaction-mode prepared-statement replay via SQL-level `PREPARE` is semantically fragile (param OIDs not reissued; unnamed statements unreplayable); silent `clear_all()` on replay I/O failure leaves the client's cached statement name dangling | `state_replayer.rs:196-263`, `connection.rs:724-760` |
| MED | TCP-fragmentation / large-message desync: `extract_messages` breaks on an incomplete trailing message **without buffering the remainder** (the `buffer` field is used only by legacy `extract_query`). A Parse/Bind spanning two reads desyncs state, cache, and events | `extractor.rs:295-334`, `:8` |
| MED | Multi-statement simple query: only the leading command is detected | `command_detector.rs:37-96` |
| MED | Pipelining / multiple CommandCompletes in one response: event logging keyed on the empty portal, first-completion-wins (transaction-status tracking is sound; event attribution is not) | `connection.rs:626-672`, `extractor.rs:47-82` |
| MED | COPY protocol unmodeled (works by passthrough; only event logging is inaccurate) | proxy paths generally |
| LOW | `ConnectionState` prepared-statement map unbounded (`// TODO: LRU eviction`) | `connection_state.rs:104` |
| LOW | `expect("connection taken")` invariant; `RwLock::unwrap()` poison-amplifier on the auth lock | `pool_manager.rs:104-109`; `server.rs:385,455,911,977` |
| — | **No differential/byte-level or fuzz/conformance testing** vs real Postgres. Behavioral integration tests exist (testcontainers + real PG16) but assert app-level results, not protocol equivalence; several document their own gaps | `tests/` |

Pooling-mode maturity: **Session/Disabled** — mature, effectively 1:1 (`connection.rs:80,790`). **Transaction** — partial; release/replay logic exists but with the gaps above. **Hybrid** — least mature and riskiest; its safety depends entirely on pin-detection completeness, which is incomplete (`connection.rs:84`).

---

## 3. Target / 1.0 exit criteria

1. **A written transparency contract** (§4.0) that defines observable equivalence, and a test suite that enforces it.
2. **Complete state tracking across both protocols.** Every session-state change that affects poolability — `SET`, temp tables, cursors, advisory locks, LISTEN, prepared statements — is detected whether issued via simple *or* extended query.
3. **Fail-closed pinning.** When the proxy cannot classify whether a connection carries non-poolable state, it **pins** (keeps it with the client) rather than releasing. Unclassifiable ≠ safe.
4. **Correct message reassembly.** Framing is correct across arbitrary TCP fragmentation and messages larger than the read buffer; no state/event desync.
5. **Honest prepared-statement semantics.** Transaction-mode either replays prepared statements correctly or does not claim to preserve them (pinning instead). No silent state destruction on replay failure.
6. **LISTEN/NOTIFY correctness.** A listening session is pinned; notifications are never lost or misrouted by pooling.
7. **Guardrails in place:** a differential transparency suite and framing fuzzer run in CI (§5).

---

## 4. Design

### 4.0 The transparency contract
Write and commit a short normative contract defining what scry guarantees. Draft form:

> For any client interaction, the sequence of backend→client protocol messages, the final result set bytes, and all session-observable state (transaction status, session GUCs, prepared statements, temp objects, cursors, advisory locks, LISTEN registrations) are identical to those the client would obtain over a direct connection — in every pooling mode. Pooling may change *which* physical backend serves a client between transactions, but never in a way the client can observe.

This contract is the specification the differential suite (§5.1) encodes.

### 4.1 Fail-closed pinning (safety net — land early)
Make the pooling decision conservative by default. In `should_release_connection` / the Hybrid path (`connection.rs:80-84`), release only when the connection is **positively known clean**. Any of: an unrecognized command that could change session state, a parse/reassembly uncertainty, or an extended-protocol message the detector doesn't understand → treat as pinned. This bounds the blast radius of every detection gap below to a *performance* cost (less pooling), never a *correctness* cost. This stance should land ahead of the deeper fixes.

### 4.2 Extended-protocol state tracking (`connection.rs`, `command_detector.rs`)
Run state detection from the `Parse` arm (`connection.rs:510-540`), not only the simple-`Query` arm (`:586`). Factor `CommandDetector` so it can classify the SQL text carried in a Parse message and update `ConnectionState` accordingly. Because Bind/Execute carry no SQL text, the classification happens at Parse time keyed by statement name. Verified by the differential suite running its full matrix over the extended protocol.

### 4.3 LISTEN/NOTIFY modeling (`connection_state.rs`, `command_detector.rs`)
Add `PinReason::Listen` (non-replayable, so `is_unsafe`-class) and detect `LISTEN`/`UNLISTEN`/`NOTIFY`. A session with an active LISTEN pins its connection for the session's duration. Confirm async `NotificationResponse` ('A') frames are forwarded on the pinned path (they are, via passthrough) and add an integration test that actually asserts delivery (the current `stateful_test.rs` LISTEN test only asserts execution).

### 4.4 Message reassembly (`extractor.rs`)
Fix `extract_messages` (`extractor.rs:295-334`) to buffer the incomplete trailing bytes across reads (use or replace the unused `buffer` field at `:8`) so a message spanning multiple `read()`s — or exceeding the read buffer — is reassembled before parsing. This is the single fix that closes the framing-desync class; the fuzzer (§5.2) guards it.

### 4.5 Prepared-statement / transaction-replay honesty (`state_replayer.rs`, `connection.rs`)
Two acceptable resolutions, decide per §9:
- **(Recommended) Restrict:** in transaction mode, pin any connection that holds protocol-level (Parse) prepared statements or an unnamed statement, rather than attempting SQL-`PREPARE` replay. Correct-by-construction; costs some pooling for prepared-heavy workloads.
- **Robust replay:** reissue statements at the protocol level with original parameter OIDs, and on replay failure surface a clean error to the client instead of `clear_all()`-and-dangle (`connection.rs:759`).
Either way, **eliminate silent state destruction**.

### 4.6 Event-logging accuracy (lower priority; observational only)
Multi-statement simple queries (`command_detector.rs:37-96`), pipelining attribution (`connection.rs:626-672`), and COPY event logging are *observability* correctness, not transparency correctness (forwarded bytes are unaffected). Improve detection to walk all `;`-separated statements and attribute pipelined CommandCompletes correctly; these are Phase-2-tail items and must not regress the passthrough guarantee.

### 4.7 Hot-path robustness (`pool_manager.rs`, `server.rs`)
Replace the `expect("connection taken")` invariants (`pool_manager.rs:104-109`) with typed errors, and address the `RwLock::unwrap()` poison-amplifier on the auth lock (`server.rs:385,455,911,977`) so a single poisoned lock cannot cascade into a per-connection panic. Bound the `ConnectionState` prepared-statement map with LRU (`connection_state.rs:104`).

---

## 5. Automated Auditor (mandatory)

1. **Differential transparency suite (the core guardrail).** For a matrix of operations — simple and extended protocol; `SET`/temp tables/cursors/advisory locks/LISTEN-NOTIFY/prepared statements; multi-statement and pipelined batches — run each **direct to Postgres** and **through the proxy in every pooling mode**, and assert equivalence of: the backend→client message sequence, final result bytes, and post-interaction session-observable state. Any divergence fails CI. This encodes §4.0 and is the mechanism that keeps transparency from regressing.
2. **Framing fuzzer.** Property/fuzz testing (proptest + a corpus) that feeds the framing/reassembly parsers arbitrarily fragmented and malformed byte streams and asserts: no panic, and reassembled messages match a reference framing. Directly targets the §4.4 desync class.
3. **Dirty-connection invariant test.** After a connection is released to the pool, assert (a) no session state from the prior client is observable to the next, and (b) no state the prior client will need was silently dropped without pinning.
4. **Pooling-safety property.** A test asserting the fail-closed invariant (§4.1): for any operation the detector does not positively classify as clean, the connection is pinned.

These join the unified CI drift-guard. The Docker-dependent differential/integration tests run on CI runners with a Docker daemon; the fuzzer runs everywhere.

---

## 6. Test strategy

- **Differential/integration (testcontainers + real PG):** the §5.1 matrix — the primary investment. Extend the existing suites (`integration_test.rs`, `stateful_test.rs`, `transaction_pooling_test.rs`, `connection_multiplexing.rs`) from app-level assertions to protocol/state equivalence.
- **Property/fuzz:** framing/reassembly (§5.2); no new panics.
- **Unit:** extended-protocol detection from Parse; LISTEN pinning; fail-closed classification default; LRU eviction.
- **Regression:** keep the CRIT-1 split-buffer DISCARD-ALL and 'Z'-in-parameter cases (`connection_multiplexing.rs:327,432`) and add the reassembly cases.

---

## 7. Phasing

- **Phase 1 (early / Harden-adjacent):** §4.1 fail-closed pinning — the safety net that makes every remaining gap a performance issue, not a correctness one. Cheap and high-leverage.
- **Phase 2 (Complete):** §4.0 contract, §4.2 extended-protocol tracking, §4.3 LISTEN/NOTIFY, §4.4 reassembly, §4.5 prepared-statement honesty, §5 differential suite + fuzzer, §4.7 robustness.
- **Phase 2 tail:** §4.6 event-logging accuracy.

---

## 8. Cross-pillar dependencies

- **P1 (Security):** client-side SCRAM (backend-required SCRAM) is a P1 item; P2 assumes the auth handshake is correct before the query path begins.
- **P5 (Performance):** complete state tracking and reassembly add hot-path work; the fail-closed-pinning default reduces pooling and thus affects the latency/throughput budget — P5 measures after P2 lands.
- **P6 (Compatibility):** any event-schema change from improved attribution goes through `scry-protocol` versioning; the differential suite also underpins the Postgres-version support matrix P6 owns.
- **P4 (Operability):** pooling-mode behavior and pinning must be observable via metrics for operators to reason about pool efficiency.

---

## 9. Open questions

1. **Prepared-statement resolution:** restrict-by-pinning (recommended, correct-by-construction) vs. robust protocol-level replay (more pooling, more complexity). Recommendation: restrict for 1.0, revisit replay post-GA.
2. **Hybrid mode for GA?** Given its dependence on complete detection, do we ship Hybrid at 1.0, or gate it behind an "advanced" flag until the differential suite has broad coverage? Recommendation: keep it, but only once §4.1–§4.4 and §5.1 are green for the Hybrid matrix.
3. **Differential-suite scope vs. CI time:** the full matrix × pooling modes × PG versions is large. Which subset gates every merge vs. runs on schedule / before a release tag? Coordinate with P5/P6.
4. **COPY and cursor `WITH HOLD`:** confirm the passthrough-only stance is acceptable for 1.0, or whether COPY needs explicit modeling for correct pooling boundaries.
