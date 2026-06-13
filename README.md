<p align="center">
  <img src="assets/feedme-logo.png" width="400" height="200" alt="FeedMe Logo">
</p>

<h1 align="center">FeedMe</h1>

<p align="center">
  <strong>FeedMe is a deterministic, linear, streaming ingest pipeline with mechanical guarantees around memory, ordering, and failure.</strong>
</p>

<p align="center">
  <a href="https://crates.io/crates/feedme">
    <img src="https://img.shields.io/crates/v/feedme.svg" alt="Crates.io">
  </a>
  <a href="https://docs.rs/feedme">
    <img src="https://docs.rs/feedme/badge.svg" alt="Docs.rs">
  </a>
  <a href="https://github.com/Michael-A-Kuykendall/feedme/actions/workflows/test-and-coverage.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/Michael-A-Kuykendall/feedme/test-and-coverage.yml?branch=main&label=CI" alt="CI">
  </a>
  <a href="https://github.com/Michael-A-Kuykendall/feedme/blob/main/LICENSE-MIT">
    <img src="https://img.shields.io/github/license/Michael-A-Kuykendall/feedme" alt="License">
  </a>
</p>


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
- Validation failures cannot be silently ignored
- Metrics collection cannot influence execution

## Why FeedMe is Not X

FeedMe is intentionally **not** these things, and that's by design:

### Not Distributed (like Vector or Fluent Bit)
FeedMe runs in a single process with no networking or cluster management.

### Not Stateful (like traditional ETL tools)
Stages are deterministic: same input + config → same output. No hidden state or concurrency.

### Not Async-First (like many modern Rust libraries)
Processing is synchronous by default. Async is an implementation detail for I/O stages.

### Not a DSL (like Logstash)
No embedded languages or required config files. Code-first with optional YAML support.

### Not a Daemon (like filebeat)
No long-running services or auto-restart. FeedMe is a library you embed in your application.

**This focus enables FeedMe's core guarantees while keeping the codebase small and maintainable.**

## Invariants

FeedMe enforces **mechanical behavioral guarantees** that are tested via runtime assertions and contract tests. These invariants ensure reliability and prevent regressions.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
feedme = "0.2"
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

The hashes should match, proving deterministic behavior.
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

FeedMe is designed for high-throughput, low-latency data processing:

- **Bounded memory usage** — no unbounded buffering regardless of input size
- **Efficient streaming** — minimal allocations with ownership transfer
- **Zero-overhead observability** — metrics collection doesn't affect execution
- **Horizontal scalability** — linear processing model scales across cores/processes

## 🛡️ Invariants

FeedMe enforces **mechanical behavioral guarantees** that are tested via runtime assertions and contract tests. These invariants ensure reliability and prevent regressions. Key guarantees include:

- **Metrics purity**: Exporting metrics doesn't affect pipeline state
- **Drop counting rules**: Events are only counted as dropped under specific conditions  
- **Latency recording**: Successful stage execution always records timing
- **Directory determinism**: Same directory input produces identical output

See [src/ppt_invariant_contracts.rs](src/ppt_invariant_contracts.rs) for the complete contract test suite.

## 🚫 Non-Goals

- Distributed processing
- Network I/O (except stubbed HTTP_Post)
- Persistent storage
- Query languages
- Compression or encryption

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

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup

```bash
git clone https://github.com/Michael-A-Kuykendall/feedme.git
cd feedme
cargo build
cargo test
```

### Code of Conduct

This project follows a code of conduct to ensure a welcoming environment for all contributors. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for details.

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

<p align="center">
  Made with ❤️ by <a href="https://github.com/Michael-A-Kuykendall">Michael Kuykendall</a>
</p>