# P7 — Supply-chain & Release Engineering (Pillar Spec)

**Status:** Draft for review
**Date:** 2026-07-05
**Pillar:** P7 of the [Production-Readiness Vision](./2026-07-05-scry-production-readiness-vision.md)
**Phase:** Harden (advisories + pinning + policy) → Prove (signing, SBOM, reproducibility)

---

## 1. Purpose & scope

Ship scry with the provenance and hygiene an enterprise security team requires before they will run a binary in front of their database. Concretely: a clean dependency posture that *stays* clean, tamper-evident and reproducible artifacts, a machine-readable bill of materials, and a disciplined release process. This pillar is largely about **CI policy and build plumbing**, and it is the one whose guardrails most directly prevent silent regression over time.

In scope:
- Clearing and continuously policing dependency advisories (`cargo audit`/`cargo deny`).
- Digest/SHA pinning of base images and CI actions.
- Image signing, build provenance (SLSA-style attestation), and SBOM generation + diff.
- Reproducible builds and toolchain pinning.
- Semver-based release process and changelog discipline.
- Repo secret hygiene (`.gitignore`, secret scanning).

Out of scope (cross-referenced):
- The `scry-protocol` *schema* semver check → **P6** (P7 owns release mechanics; P6 owns the schema-compat gate).
- Fixing the *behavior* behind an advisory (e.g. auth logic) → **P1/P2** (P7 owns the version bump and that it doesn't regress).

---

## 2. Current state (evidence)

From the 2026-07-05 `cargo audit` run and Dockerfile/CI review.

**Good foundation:** `Cargo.lock` is committed for reproducible builds (recent commit `9c1ee7f`). The Dockerfile is a clean multi-stage build running as **non-root** uid 1000 (`Dockerfile:41,46`), installing only `ca-certificates`/`libssl3`/`curl` at runtime (`:34-38`). CI uses least-privilege `permissions: contents: read, packages: write` (`.github/workflows/publish-image.yml:13-15`) and only the ephemeral `GITHUB_TOKEN` (`:40,68`); it triggers on tag push / manual dispatch, not untrusted PRs.

**The gaps:**

| Sev | Gap | Location / detail |
|-----|-----|-------------------|
| HIGH | **8 open dependency advisories**, incl. `postgres-protocol` 0.6.10 unbounded-SCRAM-iteration CPU-exhaustion DoS (RUSTSEC-2026-0179, 8.7) and malformed-`hstore` panic; `tokio-postgres` 0.7.16 `DataRow` panic DoS; `bytes` 1.11.0 integer overflow; `rustls-webpki` 0.103.9 (four cert/CRL issues); `rustls-pemfile` 2.2.0 unmaintained; `anyhow`/`rand` unsound advisories | `Cargo.lock`; fixes mostly a `cargo update` away (`postgres-protocol`→0.6.12, `tokio-postgres`→0.7.18, `bytes`→1.11.1, `rustls-webpki`→0.103.13) |
| HIGH | **No CI dependency gate.** `cargo audit`/`cargo deny` is not run anywhere, so advisories accrue silently (this audit found them out-of-band) | no workflow step; no `deny.toml` present |
| MED | **Base images tag-pinned, not digest-pinned** — `rust:1.85-bookworm` and `debian:bookworm-slim` float | `Dockerfile:8,31` |
| MED | **CI actions tag-pinned, not SHA-pinned** — `checkout@v4`, `login-action@v3`, `metadata-action@v5`, `build-push-action@v6`; a moved tag is a supply-chain foothold | `publish-image.yml:33,36,44,52,64` |
| MED | **No image signing, provenance, or SBOM.** Images are built and pushed unsigned (`build-push-action@v6` with no `provenance`/`sbom`/attestation; manifest assembled via raw `docker manifest`) — nothing an enterprise can verify | `publish-image.yml:51-57,70-85` |
| LOW | **No toolchain pin** (`rust-toolchain.toml`) — build uses whatever `rust:1.85` resolves to; reproducibility depends on the floating base image | repo root |
| LOW | **`scry.toml` not in `.gitignore`** — a real config with a backend password could be committed | `.gitignore` (covers `.env`, `config.local.toml`, not `scry.toml`) |
| LOW | **No secret scanning** in CI | no workflow |
| — | No documented semver/release process or changelog discipline beyond ad-hoc tags | repo |

---

## 3. Target / 1.0 exit criteria

1. **Clean, policed dependencies.** `cargo audit` and `cargo deny` pass, and both run in CI as a merge gate so a new advisory or disallowed license/source fails the build.
2. **Pinned inputs.** Base images are digest-pinned (`@sha256:`) and CI actions are SHA-pinned; a lint fails CI on any unpinned reference.
3. **Verifiable artifacts.** Released images are **signed** (cosign/keyless) with **build provenance** (SLSA-style attestation), and a **CycloneDX/SPDX SBOM** is generated per release and diffed against the prior release.
4. **Reproducible builds.** Toolchain is pinned; a build from a given commit is byte-reproducible (or documented deviations only).
5. **Disciplined releases.** A semver-based release process with a changelog; version bumps coordinated with P6's `scry-protocol` schema gate.
6. **Repo hygiene.** `scry.toml` (and other secret-bearing config) is git-ignored, and secret scanning runs in CI.

---

## 4. Design

### 4.1 Clear the advisories (`Cargo.toml`, `Cargo.lock`)
`cargo update` the affected transitive/direct crates to the fixed versions (`postgres-protocol`≥0.6.12, `tokio-postgres`≥0.7.18, `bytes`≥1.11.1, `rustls-webpki`≥0.103.13); migrate off unmaintained `rustls-pemfile` (fold into `rustls`'s PEM support or a maintained equivalent); resolve the `anyhow`/`rand` unsound advisories (upgrade or, if not applicable to our usage, record a reviewed `deny.toml` exception with justification). Re-run the full test suite (P1/P2/P3 guardrails) to confirm no behavioral regression from the bumps — the `postgres-protocol` and `rustls-webpki` upgrades sit under the auth/TLS stack.

### 4.2 `cargo deny` policy + CI gate (`deny.toml`, CI)
Add a `deny.toml` with four sections — `advisories` (deny known vulns/unmaintained, with justified, expiring exceptions only), `licenses` (allowlist compatible with MIT/Apache-2.0 dual license), `bans` (no duplicate/yanked crates, banned crates list), `sources` (only crates.io + explicitly-trusted registries). Run `cargo deny check` (and `cargo audit`) in CI on every PR. This is shared with P1, which also relies on the dependency gate.

### 4.3 Pin build inputs (`Dockerfile`, `publish-image.yml`)
- **Digest-pin base images** (`Dockerfile:8,31`): `FROM rust:1.85-bookworm@sha256:...` and `debian:bookworm-slim@sha256:...`, with a documented update cadence (e.g. Dependabot/renovate PRs that bump the digest).
- **SHA-pin CI actions** (`publish-image.yml:33,36,44,52,64`): replace `@vN` with `@<full-sha> # vN`, updated via automation.

### 4.4 Sign, attest, and SBOM (`publish-image.yml`)
- **Sign** released images with cosign (keyless/OIDC via the GitHub identity) and publish the signature to GHCR.
- **Provenance:** enable `build-push-action`'s provenance/attestation (SLSA) so each image carries a verifiable build record; sign the multi-arch manifest as well (the current raw `docker manifest` path at `:70-85` must be signed too, or replaced with a tool that signs the index).
- **SBOM:** generate a CycloneDX (or SPDX) SBOM per build (from `Cargo.lock` via `cargo-cyclonedx`/`syft`), attach it as an image attestation, and **diff** it against the previous release's SBOM so dependency changes are visible in the release notes.

### 4.5 Reproducible builds & toolchain pin (`rust-toolchain.toml`)
Add a `rust-toolchain.toml` pinning the compiler version (aligned with the digest-pinned builder image). Document/verify reproducibility (deterministic `Cargo.lock` build; note any non-determinism). This makes the signed artifact independently rebuildable — the strongest form of the provenance claim.

### 4.6 Release process (docs, CI)
Document a semver release process: version bump → changelog entry → tag `vX.Y.Z` → signed image + SBOM published. Coordinate crate/version bumps with **P6**'s `cargo-semver-checks` on `scry-protocol` so a schema-breaking change forces a major bump. Note the Dockerfile healthcheck currently curls `/metrics` (`Dockerfile:53`); if **P4** gates `/metrics`, switch the healthcheck to the unauthenticated `/health` liveness endpoint.

### 4.7 Repo hygiene (`.gitignore`, CI)
Add `scry.toml` (and `*.local.toml` generally) to `.gitignore`, and add a secret-scanning step (gitleaks or GitHub secret scanning) to CI so a committed credential fails the build.

---

## 5. Automated Auditor (mandatory)

All join the unified CI drift-guard; the release-time checks gate the release tag rather than every merge.

1. **Dependency policy gate (core).** `cargo deny check` + `cargo audit` on every PR — fails on any new advisory, disallowed license, banned/duplicate crate, or untrusted source. This is the guardrail that keeps §2's advisory list from ever re-accumulating.
2. **Pinning lint.** A CI check that fails if any Dockerfile `FROM` lacks an `@sha256:` digest or any workflow `uses:` lacks a full commit SHA.
3. **SBOM generation + diff.** Generate the SBOM in CI and diff against the last release's; surface added/removed/changed dependencies. Fails or flags on unexpected changes.
4. **Signature/provenance verification.** A post-publish step that verifies the pushed image's cosign signature and provenance attestation resolve — proving the signing path actually worked.
5. **Secret scan.** Fails CI on a detected committed secret.

---

## 6. Test strategy

Mostly CI-level rather than unit tests:
- **CI gates:** dependency policy, pinning lint, secret scan run on every PR; SBOM diff and signature verification run on release.
- **Verification:** after the first signed release, independently `cosign verify` the image and rebuild from the pinned toolchain to confirm reproducibility.
- **Regression:** the dependency gate itself is the regression test — the specific advisories in §2 must show green after §4.1.

---

## 7. Phasing

- **Harden (Phase 1):** §4.1 clear advisories, §4.2 `cargo deny` + CI gate, §4.3 pinning + §5.2 pinning lint, §4.7 repo hygiene + secret scan. These close the immediate exposure and stop drift cheaply.
- **Prove (Phase 3):** §4.4 signing + provenance + SBOM, §4.5 reproducible builds + toolchain pin, §4.6 release process. These are the enterprise-signable artifacts and can land later without blocking earlier phases.

---

## 8. Cross-pillar dependencies

- **P1 (Security):** shares the `cargo audit`/`cargo deny` gate; the `postgres-protocol`/`rustls-webpki` upgrades underlie P1's auth/TLS fixes — P7's bumps must land without regressing P1/P2 behavior (verified by their guardrails).
- **P6 (Compatibility):** P7's release/versioning process is driven by P6's `scry-protocol` schema-compat gate; a P6-detected breaking change forces P7 to cut a major version.
- **P4 (Operability):** if P4 gates the `/metrics` endpoint, P7's Dockerfile healthcheck moves to `/health`.
- **P5 (Performance):** dependency upgrades (§4.1) may shift performance; P5's budget gate re-measures after the bumps.

---

## 9. Open questions

1. **`anyhow`/`rand` unsound advisories:** upgrade vs. reviewed `deny.toml` exception. Recommendation: upgrade if a fixed version exists; otherwise a justified, expiring exception with a note on why our usage is unaffected.
2. **Signing model:** cosign keyless (OIDC, no key management, ties identity to the GitHub workflow) vs. a managed key. Recommendation: keyless for 1.0.
3. **SBOM format:** CycloneDX vs. SPDX. Recommendation: CycloneDX (better Rust tooling via `cargo-cyclonedx`), revisit if a customer requires SPDX.
4. **MSRV policy.** Do we commit to a Minimum Supported Rust Version, and does clearing advisories push it up? Coordinate with the toolchain pin (§4.5).
5. **Base-image update cadence.** How often do we bump the digest-pinned bases, and via what automation (Dependabot/renovate)?
