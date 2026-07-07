# Transparency Contract

Scry is a *transparent* proxy: a client speaking the Postgres wire protocol MUST NOT be able to
tell, from protocol-observable behavior alone, that Scry is in the path rather than a direct
connection to the backend. This document is the normative definition of that guarantee. It exists
so the guarantee is testable: the differential transparency test suite (see
[How this is enforced](#how-this-is-enforced)) encodes exactly the statements below.

## Table of Contents

- [Scope](#scope)
- [1. Observable equivalence](#1-observable-equivalence)
- [2. The passthrough invariant](#2-the-passthrough-invariant)
- [3. The fail-closed rule](#3-the-fail-closed-rule)
- [4. Pooling-mode scope](#4-pooling-mode-scope)
- [How this is enforced](#how-this-is-enforced)

## Scope

This contract covers Postgres wire-protocol behavior only — what a client can observe by sending
and receiving protocol messages. It does not cover side channels outside the protocol (TCP-level
timing, TLS certificate details, network hops) or the deliberate, out-of-band observability Scry
performs (event publishing to the analytics service). Those are separate concerns; the point of
this contract is that they must never leak into the protocol stream the client sees.

## 1. Observable equivalence

For any client interaction, three surfaces MUST be identical to what the client would observe over
a direct, unpooled connection to the backend:

1. **Message sequence** — the sequence of backend→client protocol messages (types, order, and
   framing), for every request the client sends.
2. **Result set bytes** — the final result set bytes (row data, field descriptions, command tags,
   error payloads) delivered to the client.
3. **Session-observable state** — all state a client can query or infer via the protocol,
   including: transaction status (the `Z` message's status byte), session GUCs (`SET`/`SHOW`
   values), prepared statements, temporary objects, cursors, advisory locks, and `LISTEN`
   registrations.

This is the definition the differential transparency test suite asserts: for a matched pair of
sessions — one direct, one through Scry — driven with the same input, these three surfaces MUST
match byte-for-byte (surfaces 1–2) or state-for-state (surface 3).

Pooling MAY change *which* physical backend connection serves a client between transactions or
statements. It MUST NOT change any of the three surfaces above in a way the client can observe.
If a backend switch would change observable state, Scry MUST NOT perform that switch (see
[§3](#3-the-fail-closed-rule)).

## 2. The passthrough invariant

Client request bytes and backend response bytes that make up surfaces 1 and 2 above MUST NOT be
rewritten by Scry. All protocol parsing Scry performs — message extraction for event logging,
transaction-boundary detection for pooling/pinning decisions, query anonymization for the
analytics pipeline — is **observational**: it reads the forwarded stream but never mutates it.

Concretely:

- Scry MUST forward client requests to the backend and backend responses to the client unchanged
  on the wire, byte-for-byte, for the messages defined in [§1](#1-observable-equivalence).
- Anonymization, metadata extraction, and any other analysis happen on a copy of the data taken
  for the out-of-band event pipeline, never on the bytes actually forwarded.
- Injected protocol traffic that is itself part of pool lifecycle management (e.g. `DISCARD ALL`
  issued during connection recycling, passive health-check queries) MUST happen only between
  client sessions, never interleaved into a live client's message stream.

## 3. The fail-closed rule

Some session-observable state is not exhaustively detectable by inspecting the protocol stream
(e.g. a stored procedure that sets a GUC as a side effect Scry's parser doesn't recognize). When
Scry cannot positively classify whether a connection carries non-poolable session state, it MUST
pin the connection — keep it 1:1 with the client — rather than release it back to the pool for
reuse by another client.

**Unclassifiable is not the same as safe.** The default action on ambiguity MUST be the
transparency-preserving one. Concretely:

- Every detection gap MUST degrade to a performance cost (less pooling, more pinned connections),
  **never** a correctness cost (state leaking between clients, or a client observing another
  client's session artifacts).
- Detection logic MAY be extended over time to recognize more cases and pool more aggressively.
  It MUST NOT be relaxed to assume "probably safe" for a case it cannot positively classify.

## 4. Pooling-mode scope

Scry supports four pooling modes (`Disabled`, `Session`, `Transaction`, `Hybrid`). The
observable-equivalence guarantee in [§1](#1-observable-equivalence) holds in **every** mode — a
client MUST NOT be able to distinguish which mode is configured by observing protocol behavior.

- **Disabled** — no pooling; each client owns a dedicated backend connection for the life of its
  session. Trivially transparent.
- **Session** — a backend connection is dedicated to a client for the life of its session, reset
  and returned to the pool on disconnect. Transparent by construction: no backend switch occurs
  mid-session.
- **Transaction** — a backend connection may be returned to the pool between transactions.
  Transparency depends on `DISCARD ALL` (or equivalent) fully resetting session state on recycle,
  and on the mode enforcer rejecting session-state-establishing statements outside a pooling-safe
  window (see `src/proxy/mode_enforcer.rs`).
- **Hybrid** — connections pin to a client dynamically when session state is detected, and pool
  otherwise. This mode carries the most detection surface and is honestly **the least mature**
  mode for this guarantee: its transparency is only as good as the completeness of state
  detection described in [§3](#3-the-fail-closed-rule). Where detection is incomplete, Hybrid
  MUST fall back to pinning rather than risk leaking state.

## How this is enforced

This contract is a specification, not a code comment — it must be backed by automated guardrails:

- **Differential transparency test suite** — drives matched direct-vs-proxied sessions with the
  same inputs and asserts the three observable surfaces in [§1](#1-observable-equivalence) match,
  across all four pooling modes.
- **Framing fuzzer** — generates adversarial and edge-case message sequences (fragmented frames,
  interleaved extended-protocol messages, malformed lengths) to catch passthrough violations
  ([§2](#2-the-passthrough-invariant)) that hand-written test cases would miss.

These guardrails may not both exist yet at the time this document is written; they are the
intended enforcement mechanism for this contract, and this document is what they are written
against.

## See Also

- [Architecture](architecture.md) — where transparency fits in the overall request flow
- [Connection Pooling](connection-pooling.md) — pool lifecycle, recycling, and `DISCARD ALL`
