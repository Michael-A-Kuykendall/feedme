# FeedMe Roadmap

## Current release: v0.5.0

The crate is **pre-1.0**. Per SemVer/Cargo convention for `0.x` releases, a
minor version bump **may include breaking changes** — pinning to
`feedme = "0.5"` does not protect you from that the way it would post-1.0.
Check `CHANGELOG.md` before upgrading a minor version. Patch releases within
`0.5.x` will not break the public API.

This release includes one breaking change: the legacy `replay::PipelineReplaySpec`
was removed (use `replay_spec::PipelineReplaySpec`). See "What's new in 0.5.0"
below and `CHANGELOG.md` for the full list.

Shipped in 0.4.0: Fused Rule Engine (FSE), fault injection, `AuditManager`
compliance attestation, `PipelineReplaySpec` for A/B comparison and config
drift detection, `PptManager` performance baseline regression detection, 21
working examples, and the one-call `common_redact_validate_pipeline()` helper.
See `CHANGELOG.md` for the full history.

## In progress

- **Async I/O edge adapters** — network/streaming sources and sinks (Kafka,
  S3, HTTP), keeping the stage loop synchronous. The extension boundary
  (`InputSourceAdapter` in `src/input.rs`, `OutputSink` in `src/stages.rs`,
  documented in `docs/decisions/0001-input-output-async-edges.md`) is in
  place; the Kafka/S3/HTTP adapter implementations themselves have not
  started.

## What's new in 0.5.0

Full details in `CHANGELOG.md` [0.5.0]. Highlights: metrics export ordering
is now deterministic, audit attestation hashes bind to stage configuration,
`Stage::to_replay_spec` is wired for all built-in stages, the benchmark suite
now runs (with real numbers backing the Fused Rule Engine's scaling claim),
and a property-based test suite covers the core pipeline invariants.

## Next up

- **`cargo test --no-default-features`** does not currently succeed —
  `invariant_contracts.rs` and example 13 both assume the `invariant-ppt`
  feature or `test` cfg is present in contexts where it isn't. Not covered by
  CI (CI runs `--all-features` only).
- **Benchmark numbers need a real-hardware baseline.** The 0.5.0 numbers were
  taken in a resource-constrained sandbox (rule counts above 25 were
  unreliable there). Re-run `cargo bench` on production hardware, commit the
  baseline, and wire criterion's baseline-comparison mode into CI as a
  non-blocking regression signal.
- **Closure-based stages have a ceiling on attestation fidelity.** `Filter`,
  `DerivedFields`, and `ValueConstraints` take closures, which can't be
  serialized or hashed — their `to_replay_spec()` config is a placeholder
  string, so `AttestationBundle::fully_attested` reports true without binding
  to the actual predicate logic for these three. A named/serializable
  predicate type as an alternative to raw closures would close this gap; it's
  an API surface change and needs its own design pass before implementation.
- **Extend property-test coverage beyond the core pipeline.**
  `src/proptests.rs` covers `Pipeline`/`Stage`/`Metrics`. `audit.rs`,
  `fault_injection.rs`, and the `fused` engine have no property coverage yet
  — the fused engine's selector-deduplication invariant is a natural fit for
  a generated-rule-set property test.
- **Document an MSRV (Minimum Supported Rust Version) policy** — add a
  `rust-version` field to `Cargo.toml` and pin a CI job to it.

## Under consideration (not committed)

- Richer constraint predicates (regex, range, custom validators)
- Pipeline composition (sub-pipelines as stages)
- Checkpointing for large file processing restart
- Output formatters (CSV, XML, Parquet)
- Enrichment stage (lookup external data sources)

## Long-term, unscheduled

- WASM compilation target for edge computing
- Schema registry integration

## Path to 1.0

Not scheduled. The API will be considered for a 1.0 stability commitment once
the async edge-adapter work lands and has had real usage — locking the API
before that would mean locking in whatever shape the async work forces.
