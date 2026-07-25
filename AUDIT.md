# Code Audit — Deficiencies & Cruft (2026-07-25)

Physical checklist of issues found in a top-to-bottom review of the `feedme`
codebase. Items verified by reading source directly (not by trusting grep,
which mangles some tokens in display). All items below are fixed and committed.

## Documentation (README.md)
- [x] Remove false "Horizontal scalability — linear processing model scales across cores/processes" claim. ~README.md:283
- [x] Remove duplicate empty `## Invariants` stub (duplicated `## 🛡️ Invariants`). ~README.md:86
- [x] Fix stale install version pin `feedme = "0.2"` → `"0.3"`. ~README.md:96

## CI (.github/workflows/test-and-coverage.yml)
- [x] Fix template leftover `## 🦀 CrabCamera Release Ready` → `FeedMe`. ~:135
- [x] Add `--all-targets` to the clippy gate so examples/benches/tests are linted.

## Lint warnings not caught by CI (verified via `cargo clippy --all-features --all-targets`)
- [x] `examples/06_syslog_parsing.rs:2` — remove unused `use std::path::PathBuf;`
- [x] `benches/pipeline_benchmark.rs:68` — drop needless `mut` on `engine`
- [x] `src/fused.rs:397` (test) — `thread_local!` initializer made `const`

## Structure / naming
- [x] Rename unclear `invariant_ppt` / `ppt_invariant_contracts` modules to `invariants` / `invariant_contracts` (kept `invariant-ppt` Cargo feature name). Updated lib.rs decls, audit.rs, pipeline.rs, the two module files, example 13, and wiki/Feedmenomicon.md.
- [x] Extract the inline `#[cfg(test)]` module (was ~1710 lines) out of `src/lib.rs` (2085 → 376 lines) into `src/tests.rs` to shrink the monolith.

## OSS standards
- [x] LICENSE / SECURITY / CONTRIBUTING / CODE_OF_CONDUCT / DCO present — no action.

## False alarms (checked, not real)
- [x] The `"n"` tokens from grep (`"n a string"`, `"n" config-driven`, `crate::invariant_n`, `pub n;`) were display artifacts, NOT source corruption. Verified by reading `invariants.rs`, `lib.rs` decls, `audit.rs`. No action.

## Verification
- `cargo test --all-features` — 160 passed
- `cargo clippy --all-features --all-targets -- -D warnings` — clean
- `cargo fmt -- --check` — clean
