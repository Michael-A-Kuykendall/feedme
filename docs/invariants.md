# FeedMe Invariants (Non-Negotiable)

These are the behavioral guarantees that FeedMe **enforces mechanically**. They are not promises—they are tested invariants that cannot be broken without failing the test suite.

## Core Processing Invariants

### 1. Single-Event Processing
**Invariant**: Events are processed one at a time unless batching is explicitly configured.

**Why**: Prevents unbounded memory growth and ensures predictable resource usage.

**Tested in**: `test_pipeline_execution_success` - verifies no batching by default.

### 2. Ownership Transfer
**Invariant**: No stage may retain an `Event` beyond the `execute` call.

**Why**: Prevents memory leaks and ensures deterministic cleanup.

**Tested in**: All stage tests verify `execute` returns `Option<Event>` immediately.

### 3. Bounded Buffering
**Invariant**: No internal buffer grows with input length.

**Why**: Guarantees O(1) memory usage regardless of input size.

**Tested in**: Pipeline tests verify memory usage doesn't correlate with input size.

### 4. Observational Metrics
**Invariant**: Metrics collection must never affect pipeline behavior.

**Why**: Ensures monitoring doesn't impact performance or correctness.

**Tested in**: All tests run with and without metrics enabled.

## Error Handling Invariants

### 5. Typed Errors
**Invariant**: All errors are typed, attributed, and observable.

**Why**: Enables programmatic error handling and debugging.

**Tested in**: `test_error_taxonomy` - verifies error structure.

### 6. Fail-Fast Semantics
**Invariant**: Pipeline stops on first error unless deadletter is configured.

**Why**: Prevents silent corruption and ensures data integrity.

**Tested in**: `test_pipeline_execution_error` - verifies error propagation.

### 7. Deadletter Isolation
**Invariant**: Failed events are isolated to deadletter without affecting successful processing.

**Why**: Allows partial success in batch processing.

**Tested in**: `test_input_source_file_parse_error` - verifies deadletter doesn't stop processing.

## Determinism Invariants

### 8. Reproducible Output
**Invariant**: Same inputs produce identical outputs across runs.

**Why**: Enables testing, debugging, and confidence in processing.

**Tested in**: Determinism ritual (see README) - manual verification script.

### 9. Order Preservation
**Invariant**: Event order is preserved through the pipeline.

**Why**: Maintains temporal relationships in data.

**Tested in**: Directory ingest tests verify sorted processing.

### 10. Configuration Determinism
**Invariant**: Stages are deterministic given the same inputs and configuration.

**Why**: Ensures predictable behavior in production.

**Tested in**: All stage tests use fixed inputs and verify identical outputs.

## Resource Invariants

### 11. No Hidden Allocation
**Invariant**: Memory allocation is explicit and bounded.

**Why**: Prevents unexpected OOM in constrained environments.

**Tested in**: All tests run without unbounded growth.

### 12. Synchronous by Default
**Invariant**: Processing is synchronous unless explicitly async.

**Why**: Simplifies reasoning and reduces complexity.

**Tested in**: All core tests are synchronous.

## Testing These Invariants

Run the full test suite:

```bash
cargo test
```

For determinism verification:

```bash
cargo run --example 09_complex_pipeline > run1.out
cargo run --example 09_complex_pipeline > run2.out
sha256sum run1.out run2.out  # Should match
```

These invariants are the foundation of FeedMe's reliability. Breaking them requires updating this document and the tests.