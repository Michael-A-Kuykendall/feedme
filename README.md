<p align="center">
  <img src="assets/feedme-logo.png" width="400" height="200" alt="FeedMe Logo">
</p>

<h1 align="center">FeedMe</h1>

<p align="center">
  <strong>An embeddable, deterministic ingest engine for Rust</strong>
</p>

<p align="center">
  <a href="https://crates.io/crates/feedme">
    <img src="https://img.shields.io/crates/v/feedme.svg" alt="Crates.io">
  </a>
  <a href="https://docs.rs/feedme">
    <img src="https://img.shields.io/docsrs/feedme" alt="Docs.rs">
  </a>
  <a href="https://github.com/micha/feedme/actions">
    <img src="https://github.com/micha/feedme/workflows/%F0%9F%A6%80%20FeedMe%20CI/badge.svg" alt="CI">
  </a>
  <a href="https://codecov.io/gh/micha/feedme">
    <img src="https://codecov.io/gh/micha/feedme/branch/main/graph/badge.svg" alt="Coverage">
  </a>
  <a href="https://github.com/micha/feedme/blob/main/LICENSE">
    <img src="https://img.shields.io/github/license/micha/feedme" alt="License">
  </a>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#installation">Installation</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#examples">Examples</a> •
  <a href="#invariants">Invariants</a> •
  <a href="#api-reference">API Reference</a> •
  <a href="#contributing">Contributing</a> •
  <a href="#license">License</a>
</p>

---

## About

FeedMe is a high-performance, streaming data pipeline engine for Rust applications. It provides a linear, deterministic processing model with bounded resource usage, explicit error handling, and comprehensive observability. Perfect for ETL, log processing, data cleaning, and real-time ingestion pipelines.

## Key Guarantees

- **Streaming, bounded memory**: Processes events one-by-one; memory usage doesn't grow with input size.
- **Deterministic and testable**: Ownership transfer prevents shared state; stages are deterministic given the same inputs and configuration.
- **Fail-fast with attribution**: Errors include stage, code, and message; no silent failures.
- **Observable without overhead**: Metrics collected automatically, exportable to Prometheus/JSON.
- **Extensible with contracts**: Plugin system for custom stages without runtime discovery.

## Why FeedMe is Not X

FeedMe is intentionally **not** these things, and that's by design. Here's why:

### Not Distributed (like Vector or Fluent Bit)
FeedMe runs in a single process with no networking, coordination, or cluster management. This eliminates complexity from consensus, partitioning, and network failures. If you need distributed processing, FeedMe can be a building block within a larger system, but it doesn't handle distribution itself.

### Not Stateful (like traditional ETL tools)
Stages should be deterministic: given the same input and configuration, they produce the same output. The framework does not introduce concurrency or hidden state—stages are responsible for their own determinism. This makes pipelines predictable, testable, and easy to reason about—perfect for "data plumbing" where state management belongs at the edges.

### Not Async-First (like many modern Rust libraries)
Processing is synchronous by default. Async is an implementation detail for I/O-bound stages, not the core API. This keeps the mental model simple: a pipeline is a sequence of transformations, not a graph of futures. If you need high-concurrency async processing, FeedMe integrates cleanly but doesn't force it.

### Not a DSL (like Logstash)
No embedded languages, no required configuration files. Code-first design with optional YAML support for complex configurations. This means you get compile-time safety, IDE support, and the full power of Rust's type system—while still supporting declarative configuration when needed. DSLs are great for non-programmers, but they hide complexity and limit expressiveness—FeedMe assumes you're writing code, but provides YAML as an optional convenience.

### Not a Daemon (like filebeat)
No long-running services, no background processes, no auto-restart. FeedMe is designed for batch processing and streaming pipelines that you control. It's a library you embed in your application, not a tool you deploy separately. This keeps deployment simple and resource usage predictable.

This focus enables FeedMe's core guarantees while keeping the codebase small and maintainable.

## Invariants

FeedMe enforces **12 non-negotiable behavioral guarantees** that are tested mechanically. These invariants ensure reliability and prevent regressions. See [docs/invariants.md](docs/invariants.md) for the complete list.

## Features

- 🚀 **High Performance**: Streaming processing with bounded memory usage
- 🔒 **Memory Safe**: Bounded resource usage prevents memory leaks
- 🎯 **Deterministic**: Consistent output for identical inputs
- 📊 **Observable**: Built-in metrics and monitoring
- 🛡️ **Error Resilient**: Structured error handling with deadletter queues
- 🔧 **Extensible**: Plugin architecture for custom processing stages
- 📝 **Well Documented**: Comprehensive examples and API docs

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
feedme = "0.1"
serde_json = "1.0"
regex = "1.0"
```

Or install via cargo:

```bash
cargo add feedme serde_json regex
```

## Quick Start

```rust
use feedme::{Pipeline, FieldSelect, RequiredFields, StdoutOutput, InputSource, Stage};
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".to_string(), "message".to_string()])));
    pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".to_string()])));
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    let mut input = InputSource::File(PathBuf::from("input.ndjson"));
    let mut deadletter: Option<&mut dyn Stage> = None;
    input.process_input(&mut pipeline, &mut deadletter)?;

    println!("Metrics: {:?}", pipeline.export_json_logs());
    Ok(())
}
```

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

### Available Examples

FeedMe includes 12 comprehensive examples covering:

- **Data Cleaning**: PII redaction, field validation
- **Compliance**: GDPR/CCPA handling
- **Monitoring**: Bounded metrics, observability
- **ETL/Ingestion**: Multi-stage pipelines
- **Error Resilience**: Deadletter queues
- **Extensibility**: Custom stages and plugins

Run any example:
```bash
cargo run --example <number>_<description>
```

See [examples/](examples/) for the full list.

## API Reference

- [Pipeline](https://docs.rs/feedme/latest/feedme/struct.Pipeline.html) - Core processing pipeline
- [Stage](https://docs.rs/feedme/latest/feedme/trait.Stage.html) - Processing stage trait
- [InputSource](https://docs.rs/feedme/latest/feedme/enum.InputSource.html) - Data input sources
- [Event](https://docs.rs/feedme/latest/feedme/struct.Event.html) - Data event structure
- [Metrics](https://docs.rs/feedme/latest/feedme/struct.Metrics.html) - Observability metrics

Full documentation: [docs.rs/feedme](https://docs.rs/feedme)

## Performance

FeedMe is designed for high-throughput, low-latency data processing:

- **Memory**: Bounded usage regardless of input size
- **CPU**: Efficient streaming with minimal allocations
- **Observability**: Metrics collection with zero runtime overhead
- **Scalability**: Linear processing model scales horizontally

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup

```bash
git clone https://github.com/micha/feedme.git
cd feedme
cargo build
cargo test
```

### Determinism Verification

FeedMe guarantees deterministic output for identical inputs. Verify this with:

```bash
cargo run --example 09_complex_pipeline > run1.out
cargo run --example 09_complex_pipeline > run2.out
# On Unix: sha256sum run1.out run2.out
# On Windows: certutil -hashfile run1.out SHA256 && certutil -hashfile run2.out SHA256
```

The hashes should match, proving deterministic behavior.

### Code of Conduct

This project follows a code of conduct to ensure a welcoming environment for all contributors. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for details.

## Sponsors

FeedMe is supported by our amazing sponsors. See [SPONSORS.md](SPONSORS.md) for details.

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

<p align="center">
  Made with ❤️ by <a href="https://github.com/Michael-A-Kuykendall">Michael Kuykendall</a>
</p>

**Processed output** (`samples/processed.ndjson`):
```
{"timestamp":"2023-10-01T10:00:00Z","level":"info","message":"User logged in","email":"[REDACTED]"}
```

**Deadletter** (errors logged with context):
```
{"error":{"stage":"Input_File","code":"PARSE_ERROR","message":"expected value at line 1 column 1"},"raw":"{invalid json}"}
```

**Metrics**:
```
{"metric":"events_processed","value":1}
{"metric":"events_dropped","value":1}
{"metric":"errors","value":1}
{"metric":"stage_latencies","stage":"PIIRedaction","count":1,"sum":0.1,"min":0.1,"max":0.1}
```

## More Examples

- `cargo run --example 01_redact_validate_deadletter`: PII redaction + validation + deadletter
- `cargo run --example 02_filter_warn_error`: Filter logs to warn/error only
- `cargo run --example 03_field_projection`: Shrink events to essential fields
- `cargo run --example 04_directory_ingest`: Process directory of log files
- `cargo run --example 05_custom_stage`: Write and use a custom stage
- `cargo run --example 06_syslog_parsing`: Parse syslog into structured events
- `cargo run --example 07_metrics_export_demo`: Focus on metrics collection and export
- `cargo run --example 08_stdin_streaming`: Stream processing from stdin
- `cargo run --example 09_complex_pipeline`: Multi-stage pipeline with transforms
- `cargo run --example 10_plugin_usage`: Register and use custom stages via plugins
- `cargo run --example 11_config_driven_pipeline`: Load and use YAML configuration
- `cargo run --example 12_error_handling_variations`: Fail-fast vs continue with deadletter

See `/examples` for code.

## Non-Goals

- Distributed processing
- Network I/O (except stubbed HTTP_Post)
- Persistent storage
- Query languages
- Compression or encryption

## API Overview

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