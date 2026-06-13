# Feedmenomicon — FeedMe Architecture Reference

## Overview

FeedMe is a **deterministic streaming pipeline engine** for Rust. It processes ordered event streams through composable stages with strong guarantees: deterministic ordering, fail-closed error handling, and single-pass rule evaluation that keeps runtime independent of rule count for shared selectors.

**License**: Apache 2.0 / MIT  
**Crate**: `feedme` (single crate, all features included)

---

## Core Concepts

### Events

An `Event` is the unit of data moving through the pipeline. It carries:
- `data: serde_json::Value` — the JSON payload
- `metadata: Option<HashMap<String, Value>>` — optional routing/trace metadata

Events are **owned** values; stages transform or drop them, never share them.

### Pipeline

A `Pipeline` is an ordered sequence of stages. Events flow through stages left-to-right:
- Each stage receives an `Event` and returns `Ok(Some(event))`, `Ok(None)` (drop), or `Err(...)` (error)
- If any stage returns `Ok(None)`, the event is dropped and does not proceed
- Errors stop the event at the failing stage and increment `error_count()`

```rust
let mut pipeline = Pipeline::new();
pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".into(), "message".into()])));
pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".into(), "message".into()])));
pipeline.add_stage(Box::new(StdoutOutput::new()));
```

### Stages

The `Stage` trait is the core extension point:

```rust
pub trait Stage: Send {
    fn name(&self) -> &str;
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError>;
    fn is_output(&self) -> bool { false }
}
```

Built-in stages:

| Stage | Purpose |
|-------|---------|
| `FieldSelect` | Keep only the listed fields |
| `FieldRemap` | Rename fields |
| `RequiredFields` | Drop events missing required fields |
| `Filter` | Keep/drop events by predicate |
| `PIIRedaction` | Redact regex-matched values |
| `DerivedFields` | Compute new fields from existing ones |
| `StdoutOutput` | Write events to stdout as NDJSON |
| `FileOutput` | Write events to a file as NDJSON |
| `Deadletter` | Route rejected events to a deadletter sink |
| `HttpPost` | (planned / user Stage) Post events to an HTTP endpoint — implement via custom Stage for now |

---

## Fused Rule Evaluation

FeedMe performs semantic analysis in a single fused pass that collapses multiple traversals into one while preserving all correctness guarantees and delivering significantly better performance. Where conventional pipelines traverse an event once per rule stage, FeedMe's engine evaluates all rules in a single pass.

### Architecture

A **Selector Prefix Trie** is built at construction time. Rules sharing a selector path prefix share trie nodes — the path `user.id` and `user.name` traverse the `user` node exactly once.

```
Root
 └── user (shared prefix)
      ├── id   → [Rule 0, Rule 2]
      └── name → [Rule 1]
```

At runtime:
1. The trie traverses the event's JSON structure in lock-step
2. Each terminal node broadcasts the extracted value to all interested rules
3. A **Rule State Bitmap** (`00=Unresolved, 01=True, 10=False`) tracks resolution
4. **Early exit** triggers when `pending_count == 0` — no further field access occurs
5. **Fail-closed**: unresolved rules resolve to false (never silently true)
6. `BTreeMap` for trie children ensures **deterministic traversal order**

### Usage

```rust
use feedme::fused::{FusedRuleEngine, Rule};

let engine = FusedRuleEngine::new(vec![
    Rule::exists("user.id"),
    Rule::exists("user.name"),
    Rule::type_is("amount", FieldType::Number),
]);

match engine.execute(event)? {
    Some(event) => { /* all rules passed */ }
    None        => { /* at least one rule failed */ }
}
```

---

## Pipeline Performance Tracking (PPT)

PPT provides baseline regression detection for pipelines in CI environments.

### Invariant System

`assert_invariant(condition, message, context)` records checks and panics on violation in test/invariant-ppt feature builds. In release builds it uses `debug_assert!` for zero overhead.

Enable runtime invariant checking:
```toml
feedme = { version = "0.2.0", features = ["invariant-ppt"] }
```

### PptManager

```rust
use feedme::invariant_ppt::PptManager;

let mut manager = PptManager::new();
manager.establish_baseline(&pipeline)?;   // snapshot current metrics
let report = manager.check_regression(&pipeline)?;  // compare to baseline
let health  = manager.health_check(&pipeline)?;     // overall health score
```

---

## Fault Injection

`FaultAwareStage` and `FaultInjector` enable resilience testing:

```rust
use feedme::fault_injection::{FaultInjector, FaultType};

let mut injector = FaultInjector::new();
let fault_id = injector.inject_stage_timeout(&mut pipeline, 0, 5000)?;
// run pipeline...
injector.clear_all_faults(&mut pipeline)?;
let report = injector.get_fault_report()?;
```

---

## Audit and Attestation

`AuditManager` generates cryptographically-hashed execution attestations for compliance:

```rust
use feedme::audit::AuditManager;

let mut manager = AuditManager::new();
let bundle = manager.generate_attestation_bundle(&pipeline, "exec_001")?;
println!("Pipeline hash: {}", bundle.pipeline_hash);

let report = manager.generate_compliance_report()?;
println!("Compliance score: {:.1}", report.compliance_score);
```

---

## Replay and Comparison

`ReplayManager` records and compares pipeline specifications:

```rust
use feedme::replay::ReplayManager;

let mut manager = ReplayManager::new();
manager.serialize_pipeline(&pipeline, "v1.0")?;
let report = manager.generate_replay_report("v1.0", "v2.0")?;
println!("Modified stages: {}", report.comparison.modified_stages);
```

---

## CLI

The `feedme` binary processes NDJSON event streams:

```bash
# Validate NDJSON (reports parse errors)
feedme validate --input events.ndjson

# Process through a pipeline (stdin/stdout compatible)
cat events.ndjson | feedme run
feedme run --input events.ndjson --fields level,message
feedme run --input events.ndjson --require level,message
```

---

## Code Structure

```
feedme/
├── src/
│   ├── lib.rs               # Pipeline, stages, metrics, InputSource
│   ├── fused.rs             # Rule evaluation engine: TrieNode, FusedRuleEngine, Rule, Predicate
│   ├── invariant_ppt.rs     # PPT invariant system, PptManager, regression detection
│   ├── replay.rs            # ReplayManager, SpecComparison
│   ├── fault_injection.rs   # FaultInjector, FaultAwareStage
│   ├── audit.rs             # AuditManager, AttestationBundle, compliance
│   └── bin/
│       └── feedme.rs        # CLI (validate, run)
├── examples/                # 15 annotated examples
├── benches/                 # Criterion benchmarks
├── docs/                    # Design rationale, invariants guide
├── fuzz/                    # Fuzz targets (cargo-fuzz)
└── wiki/
    └── Feedmenomicon.md     # This file
```

---

## Invariants

Mechanical guarantees enforced at every pipeline execution:

| Invariant | Guarantee |
|-----------|-----------|
| `events_processed` is monotonically increasing | No counter rollback |
| `errors` increments exactly once per error | No silent swallowing |
| `events_dropped` increments only for non-output `Ok(None)` | Output stages never drop |
| Trie visits each shared prefix exactly once | O(M) per shared selector |
| Engine resolves fail-closed | Missing fields → false, never true |
| BTreeMap trie traversal | Deterministic rule evaluation order |

See [docs/invariants.md](../docs/invariants.md) for the full invariant specification.
