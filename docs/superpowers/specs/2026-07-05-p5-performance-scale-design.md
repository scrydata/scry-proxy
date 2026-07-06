# P5 — Performance & Scale (Pillar Spec)

**Status:** Draft for review
**Date:** 2026-07-05
**Pillar:** P5 of the [Production-Readiness Vision](./2026-07-05-scry-production-readiness-vision.md)
**Phase:** Prove (primary); timeline-wiring (§4.1) is foundational and lands early; non-blocking budget signal in Harden

---

## 1. Purpose & scope

Make scry's stated **<1ms added-latency-per-query** target real: measured correctly, enforced by a CI regression gate, and proven under sustained load — not asserted in prose. The precondition, and the surprising centerpiece of this pillar, is that **the proxy does not currently measure its own latency**; before any budget can be enforced, the internal measurement must be made truthful.

In scope:
- Wiring the internal latency instrumentation so the proxy measures its own per-query overhead.
- Defining the added-latency budget (p50/p95/p99) precisely and wiring it to a gate.
- A CI performance regression gate (merge-time) and release-time soak/scale/ceiling validation.
- Resource-ceiling behavior under load (connections, queue, memory).

Out of scope (cross-referenced):
- Surfacing metrics via HTTP/admin → **P4** (P5 makes the latency numbers *correct*; P4 exposes them).
- The overhead added by hardening/correctness work → measured here but owned by **P1/P2/P3/P7**.

---

## 2. Current state (evidence)

From the 2026-07-05 performance-surface mapping. The building blocks are good; the connective tissue and — critically — the internal measurement are missing.

**Solid foundation:**
- A genuinely mature **4-way comparison harness** (`benchmarks/` crate): closed-loop load generator with a weighted OLTP workload (`benchmarks/src/queries.rs:151-159`), HDR-histogram p50/p75/p90/p95/p99/p99.9 output (`benchmarks/src/results.rs:23-36,82-83`), `docker stats` CPU/mem accounting (`runner.rs:96-159`), docker-compose comparison against direct/pgbouncer/pgcat (`benchmarks/docker-compose.yml`), and **published results** (scry p99 within single-digit % of direct — `benchmarks/results/comparison-20260203-215448/SUMMARY.md`).
- A well-designed HDR-histogram metrics layer with the *correct* phase decomposition (queue / pool-acquire / backend), documented as distinct summaries (`observability/metrics.rs:196-317`, `timeline.rs:1-96`).
- Profiling tooling (flamegraph via `perf`+inferno; `justfile:340-395`).

**The gaps:**

| Sev | Gap | Location |
|-----|-----|----------|
| HIGH | **The proxy does not measure its own latency.** `QueryTimeline` is never populated on the hot path — every `record_query` gets a fresh empty `QueryTimeline::new()`; `mark_*` phase methods are called only in tests. So `scry_query_latency_seconds` records ~0µs, the queue/pool/backend phase histograms stay empty, `/debug/timeline` and health-p99 are meaningless | prod call sites `proxy/connection.rs:643,667,1050,1084`; markers `timeline.rs:43-60`; real (unused-by-metrics) duration `connection.rs:627,652` |
| HIGH | **No enforcement anywhere.** No CI performance job exists (only the Docker image workflow); no threshold/assertion in the criterion bench or harness; `just ci` = fmt+lint+test only | `.github/workflows/`, `justfile:71` |
| MED | **`target_latency_ms` is dead config** — defined and defaulted to 1, read nowhere | `config/mod.rs:270,426` |
| MED | **No proxy-overhead metric**, even by design — `total − backend − pool_acquire − queue` is derivable but never computed or exposed | `observability/metrics.rs` |
| MED | **Criterion bench is a weak gate substrate:** trivial `SELECT 1` workload, `sample_size(50)` (under-samples the tail that a p99 budget needs), reports two independent numbers with no computed direct-vs-proxy delta | `benches/proxy_throughput.rs:162,185,188,210` |
| MED | **No soak/endurance testing.** Scale runs (`bench-scale` 50/100/200, `bench-full` 1–100) are short bounded bursts, not sustained; the load generator has no duration/steady-state/ramp mode | `justfile:295-326`; `benchmarks/src/runner.rs:238-244` |
| MED | **No resource-ceiling test.** Connection/queue/memory gauges exist (`metrics.rs:321-493`) but nothing drives the proxy to its limits; the "instrumentation exists but isn't wired" pattern (per the timeline) should be re-verified for the queue/rejection gauges | `metrics.rs:321-493` |
| LOW | **Metrics self-overhead (<300ns) is a prose claim**, never benchmarked | `docs/metrics.md:640-669`, `metrics.rs:10-12` |

The maintainers' own vision already frames enforced budget + CI gate + soak as unmet 1.0 exit criteria — this spec matches that, and the code confirms it.

---

## 3. Target / 1.0 exit criteria

1. **The proxy measures itself.** The internal latency histograms reflect real per-query latency and phase decomposition; a **proxy-overhead** signal (`total − backend − pool_acquire − queue`) is computed and exposed. A test asserts the histograms are non-zero in a running proxy.
2. **The budget is defined and wired.** A concrete added-latency budget (p50/p95/p99 overhead) under a defined workload and concurrency, expressed in config (replacing the dead `target_latency_ms`) and used by the gate.
3. **The budget is enforced.** A CI regression gate computes proxy-minus-direct added latency and fails when it exceeds budget.
4. **Proven under load.** Scheduled soak/endurance and scale runs demonstrate the budget holds under sustained concurrency with no latency drift or memory growth; results published.
5. **Ceilings are validated.** Connection-limit, queue-saturation, and memory-under-load behavior is tested and degrades gracefully (per P3 backpressure).

---

## 4. Design

### 4.1 Wire the internal measurement (foundational — land first)
Populate `QueryTimeline` on the managed and owned query paths: mark pool-acquire start/end and backend start/end (`timeline.rs:43-60`) at the real points in `connection.rs`, and pass the *populated* timeline (not `QueryTimeline::new()`) into `record_query` (`connection.rs:643,667,1050,1084`). The real duration already computed at `connection.rs:627,652` feeds the total histogram. Add a derived **`proxy_overhead`** metric = `total − backend − pool_acquire − queue`, exposed alongside the existing summaries. This is the substrate everything else depends on; it is also the fix that makes P4's latency metrics and `/debug/timeline` truthful (shared seam with P4). Guarded by §5.1.

### 4.2 Define and wire the budget (`config/mod.rs`)
Replace the dead `target_latency_ms` (`config/mod.rs:270,426`) with a real budget — ideally per-percentile overhead thresholds (e.g. p50/p95/p99) tied to a named reference workload and concurrency level, since a single scalar hides the tail that matters. The gate (§4.3) reads this. Document the budget's definition precisely (what "added latency" means: overhead over a direct connection for the reference workload).

### 4.3 CI regression gate (`benches/`, CI)
Strengthen the benchmark into a gate substrate and wire it into CI:
- Compute the **direct-vs-proxy delta** in code (the criterion bench currently leaves subtraction to a human — `proxy_throughput.rs:188,210`), and assert it against the §4.2 budget.
- Use a **representative workload** (reuse the `benchmarks/` OLTP query set, not `SELECT 1`) and a **tail-adequate sample size** (raise from 50 — a p99 gate needs enough samples to be stable), addressing the port-exhaustion reason that forced the low count.
- Add a CI job that runs this gate. Merge-time uses a fast, bounded variant as the blocking signal; the full sweep runs at release time. Start as a **non-blocking signal** (Harden phase) to establish a baseline and shake out flakiness, then flip to **blocking** (Prove phase).

### 4.4 Soak / endurance & scale (`benchmarks/`, CI schedule)
Add a **duration/steady-state mode** to the load generator (`runner.rs` currently terminates on query-count exhaustion at `:238-244`) — hold a target concurrency for a fixed wall-clock duration with optional ramp. Schedule long soak runs (e.g. nightly/pre-release) that assert: latency percentiles stay within budget over time (no drift) and memory does not grow (leak detection). Keep the existing scale sweep (`bench-scale`/`bench-full`) and gate the release tag on it.

### 4.5 Resource ceilings (`benchmarks/`, tests)
Add tests that drive the proxy to its configured `max_connections` and `pool_queue_depth`, asserting: correct rejection behavior (ties to P3 backpressure and the `connections_rejected`/`queue_rejected` gauges), bounded memory, and no panic/deadlock at saturation. First **verify the queue-depth/rejection gauges are actually updated on the hot path** — given the §2 timeline finding, the "instrumented but unwired" pattern must be ruled out here.

### 4.6 Validate the metrics layer's own cost
Add a micro-benchmark of `record_query` + timeline marking to confirm (or correct) the documented <300ns/query self-overhead claim (`docs/metrics.md:640-669`), so the observability path is itself within the latency story rather than an unmeasured assumption.

### 4.7 Measure hardening cost
The budget must be measured **after** P1/P2/P3/P7 land, since TLS enforcement, anonymization redaction, fail-closed pinning, timeout timers, and dependency bumps all add hot-path work. P5 owns the measurement; the other pillars own the cost. Sequence the enforced (blocking) gate after those pillars' Phase-1/2 work so the budget reflects the shipped proxy, not an unhardened one.

---

## 5. Automated Auditor (mandatory)

1. **Latency regression gate (core).** The §4.3 benchmark: computes added latency (proxy − direct) on a representative workload and **fails CI when p50/p95/p99 overhead exceeds the §4.2 budget.** Non-blocking baseline first, then blocking. This is the guardrail that keeps the <1ms promise from silently eroding.
2. **"Metrics are populated" test.** An integration test asserting the latency histograms are **non-zero** and the phase decomposition is present in a running proxy after real queries — a direct regression guard against the timeline reverting to the unwired state found in §2. (High value given the bug it targets.)
3. **Scheduled soak/scale run.** Gates the release tag: budget holds over sustained load; no memory growth.
4. **Resource-ceiling test.** Asserts graceful rejection at connection/queue limits and bounded memory.

Merge-gating checks (1, 2) run in CI on PRs; the heavier checks (3, 4) run on schedule / before a release tag.

---

## 6. Test strategy

- **Bench/gate:** the strengthened criterion (or harness-driven) delta bench (§4.3) as the merge gate; the metrics-populated integration test (§5.2).
- **Load/soak:** duration-mode harness runs (§4.4) scheduled; scale sweep before release.
- **Ceilings:** saturation integration tests (§4.5).
- **Micro:** metrics-layer self-overhead (§4.6).
- Docker-dependent load/soak runs execute on CI runners with a daemon or dedicated perf runners; the fast delta gate must be stable enough for per-PR use.

---

## 7. Phasing

- **Early / Harden:** §4.1 wire the internal timeline (foundational — nothing else is meaningful without it), §4.2 define the budget, §4.3 as a **non-blocking** signal, §5.2 metrics-populated guardrail.
- **Prove (Phase 3):** flip §4.3 to **blocking**, §4.4 soak/endurance, §4.5 resource ceilings, §4.6 metrics-cost bench, §4.7 measure-after-hardening. Publish results as part of the GA-signable evidence.

---

## 8. Cross-pillar dependencies

- **P4 (Operability):** §4.1 makes P4's latency metrics and `/debug/timeline` truthful — this wiring is the shared seam; P4 exposes what P5 makes correct, and P4's metrics-completeness criterion overlaps §5.2.
- **P1/P2/P3/P7:** each adds hot-path cost (TLS/anonymization; fail-closed pinning/state tracking; timeout timers/breaker bookkeeping; dependency bumps). P5's budget is measured after they land (§4.7); the blocking gate is sequenced accordingly.
- **P3 (Reliability):** resource-ceiling behavior (§4.5) validates P3's backpressure/rejection contracts under load.

---

## 9. Open questions

1. **Budget shape.** Single scalar (`<1ms`) vs. per-percentile thresholds vs. per-workload budgets. Recommendation: per-percentile (at least p50/p99) against one named reference workload for 1.0, expand later.
2. **Gate substrate: criterion vs. the load harness.** Criterion is per-PR-fast but micro; the `benchmarks/` harness is realistic but heavier. Recommendation: a bounded harness-driven delta check for the merge gate (realistic workload), criterion for component micro-benches.
3. **Where does the gate run?** Shared CI runners have noisy neighbors that destabilize latency gates; a dedicated/self-hosted perf runner is more stable but more ops. Recommendation: dedicated runner for the blocking gate; non-blocking signal can tolerate shared runners.
4. **Reference workload & concurrency for the budget.** Which query mix and connection count define "the budget"? Recommendation: reuse the published OLTP mix at a fixed mid-range concurrency; coordinate with P6's supported-version matrix so the budget is stated per environment.
