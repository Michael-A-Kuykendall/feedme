# Changelog

All notable changes to FeedMe will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### ✨ **Consolidated — one open-source crate**
- Merged all previously separate feedme-pro features into the main `feedme` crate
- All features are now 100% open source (Apache 2.0 / MIT)
- Added `fault_injection` module: `FaultAwareStage`, `FaultInjector` — wrap any stage for controlled resilience testing
- Added `audit` module: `AuditManager`, `AttestationBundle`, compliance policies — pipeline attestation and audit trail generation
- Extended `invariant_ppt` with `PptManager`: performance baseline capture and latency/error-rate regression detection
- Extended `replay` module with `ReplayManager`: pipeline spec serialization, A/B comparison, config drift detection
- Removed `feedme-pro` workspace member and all `gatewarden` license-gate dependencies

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

