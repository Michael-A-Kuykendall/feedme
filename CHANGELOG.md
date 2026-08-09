# Changelog

All notable changes to FeedMe will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-08-09

### Metrics & Observability
- `Metrics::stage_latencies` and `Metrics::drop_reasons` now use `BTreeMap` instead of `HashMap`, so `export_json_logs()`/`export_prometheus()` emit entries in a stable, key-sorted order on every run.
- Added `Metrics::deadletter_write_failures`, `Pipeline::deadletter_write_failures()`, and a corresponding `deadletter_write_failures` entry in both `export_json_logs()` and `export_prometheus()` output, counting deadletter-sink write failures (e.g. disk full).
- `AuditManager`'s `pipeline_hash` now hashes each stage's full configuration (name, stage ID, version, and serialized config via `Stage::to_replay_spec`), not just the stage type sequence. `AttestationBundle` gained a `fully_attested: bool` field (with `#[serde(default)]` for older attestation JSON) indicating whether every stage in the pipeline could be config-bound into the hash.
- `Stage::to_replay_spec` is now implemented for `PIIRedaction`, `DerivedFields`, `Filter`, `TypeChecking`, `ValueConstraints`, `FileOutput`, `Deadletter`, `FieldRemap`, and `FusedRuleEngine`, so `PipelineReplaySpec::from_pipeline` supports pipelines using any of them.
- README.md's Determinism Verification section now documents metrics-export ordering as a separately-verified property, alongside the existing processed-event determinism check.

### Fixed
- Fixed a text-encoding artifact in four deadletter-failure log messages in `src/input.rs`.
- CI's publish-readiness job now targets `refs/heads/main` (was `refs/heads/master`); removed the unused `master`/`develop` triggers.
- Added the missing `[[bench]]` entry (`harness = false`) for `benches/pipeline_benchmark.rs` in `Cargo.toml`, so `cargo bench` executes the suite.
- Fixed the language-switcher links in `docs/zh-CN/README.md`/`docs/zh-TW/README.md`, and synced both translations' version pin and "Scope and Limits" section with the English README.
- README.md's Contributing section now matches CONTRIBUTING.md's open-source/not-open-contribution policy.
- README.md and CONTRIBUTING.md now state the dual MIT/Apache-2.0 license (matching `Cargo.toml`) and link to `LICENSE-MIT`/`LICENSE-APACHE`.
- Removed stale `/src/camera.rs`, `/src/permissions.rs`, and `/src/platform/` references from `CODEOWNERS` and `CONTRIBUTING.md`.

### Added
- Property-based test suite (`src/proptests.rs`, using `proptest`): checks determinism, order preservation, `require_validation` fail-closed behavior, and metrics monotonicity across generated pipeline/event combinations. Includes a regression guard enumerating every built-in stage and asserting `Stage::to_replay_spec` is wired.
- `fused_vs_naive_rule_count` benchmark (`benches/pipeline_benchmark.rs`): measures the Fused Rule Engine's runtime as rule count M grows, against a naive per-rule-`Filter`-stage baseline. Results in README.md "Performance".
- `InputSourceAdapter` trait (`src/input.rs`): extension point for async edge adapters (Kafka/S3/HTTP). `InputSource` implements it directly; no change to existing callers. Design contract documented in `docs/decisions/0001-input-output-async-edges.md`.
- `.github/FUNDING.yml` and `SPONSORS.md`.

### Changed
- **Breaking**: renamed `stages::Output` → `stages::OutputSink`, matching the name used in README.md/design_rationale.md for the async-edge counterpart to `InputSourceAdapter`.

### Removed
- **Breaking**: removed `replay::PipelineReplaySpec` / `ReplayStageSpec` / `ReplayManager` / `SpecDiff` / `SpecComparison` / `ReplayReport` / `SpecFingerprintReport` (`src/replay.rs`). Use `feedme::replay_spec::PipelineReplaySpec` instead, which is unaffected and covers the same functionality with full config hashing. `record_execution`/`replay_execution`/`ExecutionTrace` (execution-trace replay) are unaffected and remain in `src/replay.rs`.

## [0.4.0] - 2026-07-25

### ✨ Deficiencies & Audit Overhaul
- Made `fused` and `invariant-ppt` Cargo features functional (they previously gated nothing)
- Modularized `src/lib.rs` into focused modules (`pipeline`, `stages`, `input`, `metrics`, `invariants`, `audit`, `replay`, `fault_injection`, `fused`, `config`, `error`, `event`)
- Hardened `PIIRedaction`: opt-in field targeting, recursive nested-value redaction, and idempotent re-redaction
- Added `Pipeline::require_validation(true)` to enforce the validation guarantee fail-closed
- Fuzzing documented as opt-in/local (cargo-fuzz on nightly); no nightly CI job runs automatically
- Documented agent-generated provenance and the engine's single-threaded, local scope limits
- Documentation/CI cruft fixes: removed a wrong-project template string, added `--all-targets` to the clippy gate, fixed hidden lint warnings, renamed `invariant_ppt`→`invariants` / `ppt_invariant_contracts`→`invariant_contracts`, and extracted the ~1700-line inline test module out of `lib.rs`
- Added Simplified Chinese (`docs/zh-CN`) and Traditional Chinese (`docs/zh-TW`) documentation with a language switcher

### 🔧 Previously in Unreleased (consolidation)
- Merged all previously separate feedme-pro features into the main `feedme` crate; fully open source (Apache 2.0 / MIT)
- Added `fault_injection` module (`FaultAwareStage`, `FaultInjector`)
- Added `audit` module (`AuditManager`, `AttestationBundle`)
- Added `PptManager` performance baseline / regression detection (now in the `invariants` module)
- Extended `replay` module with `ReplayManager`
- Removed `feedme-pro` workspace member and license-gate dependencies

## [0.3.0] - 2026-06-13

### ✨ **Expanded Examples & Features**
- Added 5 new examples demonstrating core capabilities:
  - **17_fused_rule_engine**: The O(M) selector-first rule evaluation (FSE architecture)
  - **18_derived_and_remap**: DerivedFields + FieldRemap transformations
  - **19_constraints_and_types**: TypeChecking + ValueConstraints validation
  - **20_file_output**: Writing processed events to files
  - **21_common_pipeline**: Ergonomic `common_redact_validate_pipeline()` helper
- Added `common_redact_validate_pipeline()`: One-call production-ready pipeline

### 🐛 **Bug Fixes**
- Fixed example registration in Cargo.toml (examples 13-16 weren't listed)
- Various example improvements for clarity

### 📚 **Documentation**
- Updated README to reflect all available examples
- Enhanced wiki documentation for new features

## [0.2.0] - 2025-12-20

### 🔄 **Version Correction**
- Retracted 1.0.0 release for additional community validation
- Republished as 0.2.0 to allow proper 0.x iteration
- Improved test coverage to 95%+
- Enhanced fuzzing and property-based testing
- Added comprehensive benchmarks

### ✨ **Enhancements**
- Increased test coverage from 85% to 95%+
- Expanded fuzz testing with multiple targets
- Added performance regression benchmarks
- Strengthened PPT invariants and contract testing

### 🐛 **Bug Fixes**
- Fixed minor clippy warnings
- Improved error handling edge cases

## [0.1.0] - 2025-12-20

### 🚀 **Initial Release** — Foundational Data Pipeline Engine

**FeedMe is born!** A high-performance, streaming data pipeline engine for Rust with deterministic processing, bounded resources, and comprehensive observability.

**Key Features:**
- ✅ **Streaming Processing**: One-by-one event processing with bounded memory
- ✅ **Deterministic Execution**: Consistent output for identical inputs
- ✅ **Structured Error Handling**: Categorized errors with stage attribution
- ✅ **Deadletter Queues**: Failed events logged with full context
- ✅ **Built-in Metrics**: Prometheus/JSON exportable observability
- ✅ **Extensible Architecture**: Plugin system for custom stages
- ✅ **12 Comprehensive Examples**: Covering all major use cases

**Release Stats:**
- ✅ **100% test coverage** on core functionality
- ✅ **12 examples** demonstrating real-world usage
- ✅ **All platforms supported** (Linux/macOS/Windows)
- ✅ **Zero unsafe code** for memory safety
- ✅ **Full API documentation** on docs.rs

### Added
- Core `Pipeline` and `Stage` architecture
- `InputSource` for files, directories, and stdin
- Built-in stages: `PIIRedaction`, `Filter`, `FieldSelect`, `RequiredFields`, etc.
- Structured `PipelineError` with categories and codes
- Deadletter error handling with JSON attribution
- Metrics collection with bounded storage
- Plugin registry for custom stages
- Comprehensive example suite
- GitHub Actions CI/CD workflows
- Full documentation and README

### Performance
- **Memory**: Bounded usage regardless of input size
- **Throughput**: Efficient streaming with minimal allocations
- **Observability**: Zero-overhead metrics collection

### Known Limitations
- Directory ingestion order now deterministic (sorted)
- Deadletter attribution fully structured
- No distributed processing (by design)
- No network I/O except stubbed HTTP_Post

---

## Contributing to the Changelog

When making changes, please update this file following the format above. Changes should be categorized as:
- `Added` for new features
- `Changed` for changes in existing functionality
- `Deprecated` for soon-to-be removed features
- `Removed` for now removed features
- `Fixed` for any bug fixes
- `Security` in case of vulnerabilities

