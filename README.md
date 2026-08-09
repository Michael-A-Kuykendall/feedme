<div align="center">
  <img src="assets/feedme-logo.png" width="400" height="200" alt="FeedMe Logo">

  # FeedMe

  ### Deterministic, Bounded Data Plumbing for Rust

  [![Crates.io](https://img.shields.io/crates/v/feedme.svg)](https://crates.io/crates/feedme)
  [![Docs.rs](https://docs.rs/feedme/badge.svg)](https://docs.rs/feedme)
  [![CI](https://img.shields.io/github/actions/workflow/status/Michael-A-Kuykendall/feedme/test-and-coverage.yml?branch=main&label=CI)](https://github.com/Michael-A-Kuykendall/feedme/actions/workflows/test-and-coverage.yml)
  [![License](https://img.shields.io/github/license/Michael-A-Kuykendall/feedme)](https://github.com/Michael-A-Kuykendall/feedme/blob/main/LICENSE-MIT)

  [![💝 Sponsor this project](https://img.shields.io/badge/💝_Sponsor_this_project-ea4aaa?style=for-the-badge&logo=github&logoColor=white)](https://github.com/sponsors/Michael-A-Kuykendall)

  **Languages:** [简体中文](docs/zh-CN/README.md) · [繁體中文](docs/zh-TW/README.md)
</div>

<p align="center">
  <strong>FeedMe is a deterministic, linear, streaming ingest pipeline with mechanical guarantees around memory, ordering, and failure.</strong>
</p>

### 💝 Support FeedMe's Development

🚀 **If FeedMe is useful to you, consider [sponsoring](https://github.com/sponsors/Michael-A-Kuykendall) — it directly funds bug fixes, the async edge adapters, and continued maintenance.**

- **$5/month**: Coffee tier ☕ — eternal gratitude + sponsor badge
- **$25/month**: Bug prioritizer 🐛 — priority support + name in [SPONSORS.md](SPONSORS.md)
- **$100/month**: Corporate backer 🏢 — logo placement + roadmap input

[**🎯 Become a Sponsor**](https://github.com/sponsors/Michael-A-Kuykendall) | See our [sponsors](SPONSORS.md) 🙏

---

## About

FeedMe is the Rust data pipeline you'd build yourself if you had the time.

Every Rust application that processes a stream of events ends up reinventing the same wheel: read input, transform it, drop the bad records somewhere, count what happened, and prove it worked the same way twice. FeedMe is that wheel, done once, done right — so you can focus on the stages that are actually specific to your problem.

It provides a linear, deterministic processing model with bounded resource usage, explicit error handling, and comprehensive observability. Drop it into any Rust application that reads events, logs, records, or messages and needs to do something predictable with them.

## Key Features

- **Streaming, bounded memory** — processes one event at a time; memory usage stays flat
- **Deterministic processing** — same input + same config → same output
- **Structured errors** — stage, code, and message for every failure
- **Observability** — metrics exportable (Prometheus or JSON) without affecting execution
- **Fused Rule Engine (FSE)** — FeedMe's signature O(M) selector-first single-pass rule evaluation — runtime stays independent of rule count for shared selectors
- **Fault injection** — wrap any stage in `FaultAwareStage` to inject failures, timeouts, or resource exhaustion
- **Baseline regression detection** — `PptManager` captures performance baselines and reports regressions
- **Pipeline attestation** — `AuditManager` generates compliance bundles and audit trails with cryptographic hashes
- **Pipeline evolution tracking** — `PipelineReplaySpec` enables A/B comparison and config drift detection
- **Ergonomic helpers** — `common_redact_validate_pipeline()` for one-call production pipelines
- **Extensible** — add custom stages via a defined plugin contract

## Mechanical Guarantees

FeedMe provides these mechanical guarantees:

- Events are processed strictly in input order
- Memory usage is bounded and input-size independent
- Stages cannot observe shared or mutated state
- Validation failures are not silently ignored: any Validator stage (RequiredFields,
  TypeChecking, ValueConstraints) rejects invalid events. This is opt-in — it holds
  only when such a stage is present. Call `Pipeline::require_validation(true)` to
  enforce it fail-closed: with no Validator stage, `process_event` returns an error
  instead of silently passing invalid events through.
- Metrics collection cannot influence execution

## Why FeedMe is Not X

FeedMe is intentionally **not** these things, and that's by design:

### Not Distributed (like Vector or Fluent Bit)
FeedMe runs in a single process with no networking or cluster management.

### Not Stateful (like traditional ETL tools)
Stages are deterministic: same input + config → same output. No hidden state or concurrency.

### Not Async-First (like many modern Rust libraries)
The processing core is synchronous by design — and that is a feature, not a
limitation. Async is reserved exclusively for I/O edge adapters (sources/sinks);
it never enters the stage loop (see [Scope and Limits](#-scope-and-limits)).

### Not a DSL (like Logstash)
No embedded languages or required config files. Code-first with optional YAML support.

### Not a Daemon (like filebeat)
No long-running services or auto-restart. FeedMe is a library you embed in your application.

**This focus enables FeedMe's core guarantees while keeping the codebase small and maintainable.**

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
feedme = "0.5"
serde_json = "1.0"
regex = "1.0"
```

Or install via cargo:

```bash
cargo add feedme serde_json regex
```

## Quick Start

Here's a 12-line pipeline that:

* ingests logs
* redacts PII
* validates schema
* filters noise
* guarantees determinism
* and fails safely

```rust
use feedme::{
    Pipeline, FieldSelect, RequiredFields, StdoutOutput, Deadletter,
    PIIRedaction, Filter, InputSource, Stage
};
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    // Create pipeline: select fields → redact PII → require fields → filter → output
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(FieldSelect::new(vec![
        "timestamp".into(), "level".into(), "message".into(), "email".into()
    ])));
    pipeline.add_stage(Box::new(PIIRedaction::new(vec![
        regex::Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b").unwrap()
    ])));
    pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".into()])));
    pipeline.add_stage(Box::new(Filter::new(Box::new(|event| {
        event.data.get("level").and_then(|v| v.as_str()) != Some("debug")
    }))));
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // Deadletter for errors
    let mut deadletter = Deadletter::new(PathBuf::from("errors.ndjson"));

    // Process input file
    let mut input = InputSource::File(PathBuf::from("input.ndjson"));
    input.process_input(&mut pipeline, &mut Some(&mut deadletter))?;

    // Export final metrics
    println!("Pipeline complete. Metrics:");
    for metric in pipeline.export_json_logs() {
        println!("{}", serde_json::to_string(&metric)?);
    }

    Ok(())
}
```

**Input** (`input.ndjson`):
```
{"timestamp":"2023-10-01T10:00:00Z","level":"info","message":"User logged in","email":"user@example.com"}
{"level":"debug","message":"Debug info"}
{"message":"Missing level"}
```

**Output** (stdout):
```
{"timestamp":"2023-10-01T10:00:00Z","level":"info","message":"User logged in","email":"[REDACTED]"}
```

**Deadletter** (`errors.ndjson`):
```
{"error":{"stage":"RequiredFields","code":"MISSING_FIELD","message":"Required field 'level' is missing"},"raw":"{\"message\":\"Missing level\"}"}
```

**Metrics** (JSON logs):
```
{"metric":"events_processed","value":2}
{"metric":"events_dropped","value":1}
{"metric":"errors","value":1}
{"metric":"stage_latencies","stage":"FieldSelect","count":3,"sum":0.05,"min":0.01,"max":0.02}
...
```

## Determinism Verification

> **Determinism is a core guarantee** — identical runs produce identical outputs.

FeedMe guarantees deterministic output for identical inputs. Verify this with:

```bash
cargo run --example 09_complex_pipeline > run1.out
cargo run --example 09_complex_pipeline > run2.out
# On Unix: sha256sum run1.out run2.out
# On Windows: certutil -hashfile run1.out SHA256 && certutil -hashfile run2.out SHA256
```

> **Note:** this ritual exercises the processed-event stream. Metrics export
> (`export_json_logs`/`export_prometheus`) is verified deterministic
> separately, since it has its own ordering concern (stage/drop-reason entry
> order) independent of event output — see
> `test_metrics_export_ordering_is_deterministic` in `src/tests.rs`.

The hashes should match, proving deterministic behavior.

## 📊 Metrics & Observability

`Pipeline` collects metrics as it processes events. Collection is pure
observation — it never affects execution or output (see Mechanical
Guarantees above) — and every counter is exposed two ways: typed accessor
methods on `Pipeline`, and a full export via `export_json_logs()` /
`export_prometheus()`.

### Counters

| Counter | Accessor | Meaning |
|---|---|---|
| Events processed | `Pipeline::events_processed()` | Total events that entered the pipeline |
| Events dropped | `Pipeline::events_dropped()` | Events a non-output stage returned `None` for (filtered) |
| Errors | `Pipeline::error_count()` | Events that stopped the pipeline with `Err` |
| Deadletter write failures | `Pipeline::deadletter_write_failures()` | Error records lost because the deadletter sink itself failed to write them (disk full, permissions) |
| Stage latency | `Pipeline::avg_latency_ms()` | Weighted mean latency across all stage executions |
| Error rate | `Pipeline::error_rate()` | `errors / events_processed`, `0.0` if nothing processed |

`export_json_logs()`/`export_prometheus()` additionally break latency and
drop counts down per-stage and per-drop-reason (`stage_latencies`,
`drop_reasons`).

### Export formats

```rust
// JSON logs — one line per metric, machine-parseable
for line in pipeline.export_json_logs() {
    println!("{}", line);
}
```
```
{"metric":"events_processed","value":42}
{"metric":"events_dropped","value":3}
{"metric":"errors","value":0}
{"metric":"deadletter_write_failures","value":0}
{"metric":"stage_latencies","stage":"FieldSelect","count":42,"sum":1.2,"min":0.01,"max":0.08}
{"metric":"drop_reasons","reason":"filtered","count":3}
```

```rust
// Prometheus text exposition format
println!("{}", pipeline.export_prometheus());
```
```
# HELP feedme_events_processed_total Total events processed
feedme_events_processed_total 42
feedme_stage_latency_ms_sum{stage="FieldSelect"} 1.2
feedme_drop_reasons_total{reason="filtered"} 3
```

### Ordering is deterministic

Both export methods iterate their underlying `BTreeMap`s, so entries for
`stage_latencies` and `drop_reasons` are always emitted in the same
(alphabetical-by-key) order for a given pipeline shape, run after run.
Latency *values* (`sum`/`min`/`max`) are real wall-clock measurements and
will naturally differ between runs; the set of entries, their keys, and
their order will not. This is checked directly by
`test_metrics_export_ordering_is_deterministic` (`src/tests.rs`) and by the
`prop_determinism_across_runs` property test (`src/proptests.rs`).

## Examples

### Messy Input → Clean Output

Given `messy.ndjson`:
```
{"timestamp":"2023-10-01T10:00:00Z","level":"info","message":"User logged in","email":"user@example.com"}
{"level":"info","message":"Missing timestamp"}
{invalid json}
```

Run:
```bash
cargo run --example 01_redact_validate_deadletter
```

**Processed output** (`samples/processed.ndjson`):
```
{"timestamp":"2023-10-01T10:00:00Z","level":"info","message":"User logged in","email":"[REDACTED]"}
```

**Deadletter** (errors logged with context):
```
{"error":{"stage":"Input_File","code":"PARSE_ERROR","message":"expected value at line 1 column 1"},"raw":"{invalid json}"}
```

**Metrics:**
```
{"metric":"events_processed","value":1}
{"metric":"events_dropped","value":1}
{"metric":"errors","value":1}
{"metric":"stage_latencies","stage":"PIIRedaction","count":1,"sum":0.1,"min":0.1,"max":0.1}
```

### Available Examples

FeedMe includes 16 examples. These are the highlights:

| What you need | Example |
|---|---|
| Redact PII, validate schema, catch bad records | [01_redact_validate_deadletter](examples/01_redact_validate_deadletter.rs) |
| Filter events by field value | [02_filter_warn_error](examples/02_filter_warn_error.rs) |
| Select / rename fields | [03_field_projection](examples/03_field_projection.rs) |
| Process a whole directory of NDJSON files | [04_directory_ingest](examples/04_directory_ingest.rs) |
| Write your own transform stage | [05_custom_stage](examples/05_custom_stage.rs) |
| Parse syslog / structured log formats | [06_syslog_parsing](examples/06_syslog_parsing.rs) |
| Export Prometheus or JSON metrics | [07_metrics_export_demo](examples/07_metrics_export_demo.rs) |
| Stream from stdin without buffering | [08_stdin_streaming](examples/08_stdin_streaming.rs) |
| Multi-stage ETL with deadletter | [09_complex_pipeline](examples/09_complex_pipeline.rs) |
| Plugin system for custom stages | [10_plugin_usage](examples/10_plugin_usage.rs) |
| Config-driven pipeline from YAML | [11_config_driven_pipeline](examples/11_config_driven_pipeline.rs) |
| Error handling patterns | [12_error_handling_variations](examples/12_error_handling_variations.rs) |
| Prove your pipeline doesn't slow down (CI guard) | [13_performance_baseline_guard](examples/13_performance_baseline_guard.rs) |
| Test what happens when a stage breaks | [14_fault_injection_testing](examples/14_fault_injection_testing.rs) |
| Generate an audit trail with compliance checks | [15_audit_trail](examples/15_audit_trail.rs) |
| Track and diff pipeline config between versions | [16_pipeline_evolution](examples/16_pipeline_evolution.rs) |
| **Fused Rule Engine (O(M) selector-first)** | [17_fused_rule_engine](examples/17_fused_rule_engine.rs) |
| Derived fields and field remapping | [18_derived_and_remap](examples/18_derived_and_remap.rs) |
| Type checking and value constraints | [19_constraints_and_types](examples/19_constraints_and_types.rs) |
| Write processed events to files | [20_file_output](examples/20_file_output.rs) |
| One-call production pipeline | [21_common_pipeline](examples/21_common_pipeline.rs) |

Run any example:
```bash
cargo run --example 01_redact_validate_deadletter
```

See [examples/](examples/) for the complete list.

## 🏗️ API Reference

- [Pipeline](https://docs.rs/feedme/latest/feedme/struct.Pipeline.html) - Core processing pipeline
- [Stage](https://docs.rs/feedme/latest/feedme/trait.Stage.html) - Processing stage trait
- [InputSource](https://docs.rs/feedme/latest/feedme/enum.InputSource.html) - Data input sources
- [Event](https://docs.rs/feedme/latest/feedme/struct.Event.html) - Data event structure
- [Metrics](https://docs.rs/feedme/latest/feedme/struct.Metrics.html) - Observability metrics

Full documentation: [docs.rs/feedme](https://docs.rs/feedme)

## Performance

FeedMe ships a [criterion](https://github.com/bheisler/criterion.rs) benchmark
suite (`benches/pipeline_benchmark.rs`, run with `cargo bench`). The numbers
below are from one local run (WSL2, debug-free `bench` profile) — not a
rigorous multi-machine statistical baseline. Reproduce with `cargo bench`
before relying on them for a capacity decision.

**Pipeline throughput** (`FieldSelect` → `RequiredFields`, cloned events):

| Events | No output stage | With `StdoutOutput` |
|---|---|---|
| 100 | 62 µs | 286 µs |
| 1,000 | 594 µs | 2.95 ms |
| 10,000 | 6.81 ms | 30.1 ms |

Both scale linearly with event count, as the sync, one-event-at-a-time design
predicts. The `StdoutOutput` column is dominated by the `println!` I/O itself,
not pipeline overhead.

**Fused Rule Engine vs. naive per-rule stages** — this is the benchmark that
actually tests the FSE's specific claim ("runtime stays independent of rule
count for shared selectors"), run as `fused_vs_naive_rule_count` in the same
suite: M rules all reading one shared field, 1000 events, `FusedRuleEngine`
(one selector extraction regardless of M) vs. M separate `Filter` stages
(each independently re-reading the field):

| Rules (M) | Fused | Naive (M stages) |
|---|---|---|
| 1 | 342 µs | 343 µs |
| 5 | 380 µs | 811 µs |
| 10 | 354 µs | 1.28 ms |
| 25 | 393 µs | 2.79 ms |

The fused engine stays roughly flat (~340–400 µs for 1000 events regardless
of M) while the naive stage-per-rule approach grows close to linearly with M
— empirical support for the selector-deduplication claim, not just the
algorithmic argument. Numbers above 25 rules were unreliable on the sandboxed
environment this was measured in (resource-constrained container, not a
capacity limit of the engine itself); rerun `cargo bench` on real hardware for
higher M or for numbers you intend to depend on.

Structural properties that hold regardless of throughput:

- **Bounded memory usage** — no unbounded buffering regardless of input size
- **Efficient streaming** — minimal allocations with ownership transfer
- **Zero-overhead observability** — metrics collection doesn't affect execution

## 🛡️ Invariants

FeedMe enforces **mechanical behavioral guarantees** that are tested via runtime assertions and contract tests. These invariants ensure reliability and prevent regressions. Key guarantees include:

- **Metrics purity**: Exporting metrics doesn't affect pipeline state
- **Drop counting rules**: Events are only counted as dropped under specific conditions  
- **Latency recording**: Successful stage execution always records timing
- **Directory determinism**: Same directory input produces identical output

See [src/invariant_contracts.rs](src/invariant_contracts.rs) for the complete contract test suite.

## 🚫 Non-Goals

- Distributed processing (no clustering, remote offsets, or coordination in the core)
- Persistent storage
- Query languages
- Compression or encryption
- Relational aggregation / joins (multi-event analytics — out of scope)

Async I/O at the edges (network/streaming sources & sinks) is **in scope** and
planned. Built-in sources today are local.

## 🔭 Scope and Limits

FeedMe has a deliberately narrow lane: it is a **deterministic, bounded,
sync-core data-plumbing library** — linear and embeddable. It turns messy input
streams into clean, ordered events for any sink or downstream system. It is
*not* a relational/aggregation engine, a distributed system, or a daemon.

Its guarantees hold for the model it actually implements — a single-process,
local, ordered stream processor:

- **Sync processing core**: the stage pipeline runs single-threaded and in input
  order. This is a *feature*, not a limitation — determinism and bounded memory
  come from the sync stage loop. There is no concurrency inside the stage loop.
- **Async only at the edges**: a type implementing `InputSourceAdapter`, or a
  `Stage` implementing `OutputSink`, may perform async I/O (network,
  streaming) internally. The pipeline *coordination* stays synchronous; async
  never enters the stage loop. This is the extension point planned Kafka/S3/HTTP
  ingestion will use; see `docs/decisions/0001-input-output-async-edges.md`
  for the trait contract.
- **Local input today**: built-in sources are `stdin`, a single file, or a
  non-recursive directory of files read into a sorted `Vec` of paths. No remote
  offsets or distributed coordination yet.
- **No backpressure**: input is read and processed line-by-line; the engine does
  not buffer unbounded input or apply flow control. "Bounded memory" means
  per-event working state is flat, not that arbitrarily large inputs are spooled.
- **Out of scope**: distributed processing, persistent storage, query languages,
  compression/encryption, and relational aggregation/joins. The "done once, done
  right" framing refers to correctness of this local model, not to distributed or
  high-throughput deployment.
- **Fuzzing needs nightly**: the `fuzz/` targets require a nightly toolchain and
  `cargo-fuzz` (see [Fuzzing](#fuzzing)).

## 📋 API Overview

### Stage Contract
```rust
pub trait Stage {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError>;
    fn name(&self) -> &str;
    fn is_output(&self) -> bool { false }  // true if consumes event
}
```

### Execution Semantics
- `Some(event)`: Pass to next stage
- `None`: Drop (filtered); if `is_output()`, consumed
- `Err`: Stop pipeline, error with attribution

### Core Types
- `Pipeline`: Add stages, process events, export metrics
- `Event`: JSON data + optional metadata
- `InputSource`: Stream from stdin/file/directory
- `PipelineError`: Categorized errors (Parse/Transform/Validation/Output/System)

## 🤝 Contributing

FeedMe is **open source, not open contribution** — the code is freely available under MIT/Apache-2.0, but pull requests aren't accepted by default; architectural and roadmap decisions stay with the maintainer. Bug reports and security issues via GitHub Issues are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the full policy and how to reach the maintainer.

### Provenance

Significant portions of this codebase (including several `[autonomous phaseN]`
merge commits) were generated by AI agents rather than written by a human author
end-to-end. These sections have **not** all received individual human review
markers. Treat the code as a working prototype: the test suite, clippy, and
formatting gates are enforced in CI, but absence of a human sign-off should not be
read as a correctness guarantee. When modifying agent-generated code, verify
behavior with the existing tests and add coverage for any change.

### Development Setup

```bash
git clone https://github.com/Michael-A-Kuykendall/feedme.git
cd feedme
cargo build
cargo test
```

### Property-Based Tests

Alongside hand-written unit tests, `src/proptests.rs` uses
[proptest](https://github.com/proptest-rs/proptest) to check core invariants
— determinism (same pipeline + events, run twice, byte-identical output and
metrics structure), order preservation, `require_validation` fail-closed
behavior, and metrics monotonicity — against many generated pipeline/event
combinations rather than a fixed set of examples. These run as part of
`cargo test --all-features` (no separate command needed); on failure,
proptest writes a minimal reproducing case to `proptest-regressions/`.

We use property tests here instead of a separate formal spec (e.g. TLA+)
deliberately: FeedMe's stage loop is single-threaded by design, so there's no
concurrent-interleaving space for a model checker to explore, and a
hand-maintained formal spec would verify a model of the design rather than
the actual Rust code. Property tests run the real crate directly against
generated inputs, so there's no design/implementation gap to keep in sync.

### Fuzzing

The `fuzz/` directory is a separate [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz)
workspace. It is **excluded** from the main workspace and requires a **nightly**
toolchain plus the `cargo-fuzz` subcommand:

```bash
# install the fuzzer harness (one time)
cargo +nightly install cargo-fuzz

# build all fuzz targets
cargo +nightly fuzz build

# run a target for a bounded number of executions
cargo +nightly fuzz run fuzz_pipeline -- -runs=1000
```

Fuzzing is **not** run automatically in CI. It is an opt-in, local-only activity
for contributors who want to stress the parsing/redaction boundary. Stable
`cargo test`, clippy, and fmt remain the only CI gates for correctness.

### Code of Conduct

This project follows a code of conduct to ensure a welcoming environment for all contributors. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for details.

## License

Dual-licensed under MIT or Apache-2.0, at your option. See [LICENSE-MIT](LICENSE-MIT) / [LICENSE-APACHE](LICENSE-APACHE) for details.

---

<p align="center">
  Made with ❤️ by <a href="https://github.com/Michael-A-Kuykendall">Michael Kuykendall</a>
</p>