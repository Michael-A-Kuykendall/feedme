# FeedMe: Design Rationale

This document explains the fundamental design decisions behind FeedMe. It answers "why" rather than "what" or "how". Understanding these choices is key to using FeedMe effectively and contributing to its evolution.

## Core Philosophy

FeedMe is **data plumbing for Rust applications**. It moves data from sources to sinks through transformations, with strong guarantees about reliability, performance, and predictability. It's not a general-purpose data processing framework—it's specialized for the common case of "ingest, clean, transform, output."

## Why Linear Pipelines?

### The Problem with DAGs
Most data processing tools use directed acyclic graphs (DAGs) for pipelines. This allows complex dependencies but introduces:

- **Complexity**: Reasoning about execution order becomes hard
- **Deadlocks**: Circular dependencies are possible
- **Resource contention**: Parallel execution needs coordination
- **Testing difficulty**: All combinations of stage interactions must be tested

### Linear by Design
FeedMe uses a strict linear pipeline: stages execute in sequence, one event at a time. Benefits:

- **Predictable execution**: Stage N always runs after stage N-1
- **Simple reasoning**: "What happens to this event?" is straightforward
- **No coordination**: No locks, no async coordination primitives
- **Easy testing**: Each stage can be tested in isolation

### When Linearity Works
Linear pipelines excel at:
- ETL (Extract, Transform, Load) workflows
- Log processing and cleaning
- Data validation and normalization
- Streaming data transformation

They struggle with:
- Complex dependencies between data items
- Real-time analytics requiring aggregation
- Machine learning pipelines with branching logic

FeedMe embraces this limitation—it keeps the tool focused and reliable.

## Why Ownership-Based Stages?

### The Problem with Shared State
Traditional pipeline stages often share state through:
- Global variables
- Shared mutable references
- External databases
- Configuration files

This leads to:
- **Race conditions** in concurrent execution
- **Hidden dependencies** between stages
- **Testing complexity** (state must be reset)
- **Resource leaks** (state not properly cleaned up)

### Ownership Transfer
FeedMe stages receive `Event` by value and return `Option<Event>`. This enforces:

- **No shared mutable state**: Each stage owns its data
- **Explicit data flow**: Ownership transfer is visible in the type system
- **Automatic cleanup**: Rust's ownership prevents leaks
- **Thread safety**: No mutable sharing means no concurrency issues

### Implications
- Stages are **pure functions** of their input (plus internal configuration)
- **Deterministic behavior**: Same input → same output
- **Memory safety**: No reference cycles or dangling pointers
- **Performance**: Zero-copy when possible, explicit copying when needed

## Why Fail-Fast by Default?

### The Problem with Silent Failures
Many data processing tools continue processing after errors:
- Invalid data is skipped with warnings
- Failed transformations produce null/empty outputs
- Errors are logged but don't stop the pipeline

This leads to:
- **Data corruption**: Bad data silently becomes good data
- **Hard debugging**: Errors are buried in logs
- **False confidence**: "Processing completed" doesn't mean "data is correct"

### Fail-Fast Philosophy
FeedMe stops on the first error by default. Rationale:

- **Data integrity**: Bad input should not produce bad output
- **Early detection**: Failures are caught immediately, not at the end
- **Clear responsibility**: Each stage declares what it accepts
- **Simple recovery**: Deadletter queues isolate failures from successes

### Configurable Resilience
When deadletter queues are configured, FeedMe becomes resilient:
- Failed events are isolated
- Successful events continue processing
- Metrics track both success and failure rates
- Operators can monitor and act on failures

This gives you **both** strict correctness and operational resilience.

## Why No Async Core?

### The Problem with Async-First
Many modern Rust libraries are async-first:
- Everything returns `Future`
- Blocking operations are forbidden
- The entire application must be async

This creates:
- **Complexity cascade**: All callers must be async
- **Performance overhead**: Async runtime for simple operations
- **Mental overhead**: Reasoning about futures and await points
- **Integration pain**: Sync code must be wrapped in `spawn_blocking`

### Sync by Default, Async When Needed
FeedMe's core is synchronous:
- **Simple mental model**: Functions call functions
- **Zero runtime overhead**: No async executor needed
- **Easy integration**: Works in any Rust application
- **Performance**: No async indirection for simple operations

Async support is added at the edges:
- I/O stages (file, network) can be async
- Streaming sources can use async iterators
- But the pipeline coordination remains sync

### When Async Matters
Use async FeedMe stages for:
- High-throughput network I/O
- Streaming from async sources (Kafka, S3, etc.)
- Integration with async ecosystems

Keep sync for:
- CPU-bound transformations
- Simple file processing
- Testing and development

## Why Typed Errors?

### The Problem with String Errors
Traditional error handling uses strings:
```rust
return Err("Something went wrong".to_string())
```

This leads to:
- **No programmatic handling**: Callers can't catch specific errors
- **Inconsistent messages**: Different stages format errors differently
- **Lost context**: No structured information for debugging
- **Hard maintenance**: Refactoring breaks error matching

### Structured Error Taxonomy
FeedMe uses typed errors with taxonomy:

```rust
pub enum PipelineError {
    Parse(ParseError),
    Transform(TransformError),
    Validation(ValidationError),
    Output(OutputError),
    System(SystemError),
}
```

Each error includes:
- **Category**: What type of failure (Parse, Transform, etc.)
- **Stage**: Which stage failed
- **Code**: Machine-readable error code
- **Message**: Human-readable description

### Benefits
- **Programmatic handling**: Catch specific error types
- **Consistent attribution**: Every error knows its source
- **Structured logging**: Errors can be indexed and searched
- **API stability**: Error types are part of the public API

## Why Observational Metrics Only?

### The Problem with Active Monitoring
Some systems use metrics to control behavior:
- Circuit breakers based on error rates
- Backpressure based on queue depth
- Dynamic scaling based on throughput

This creates:
- **Complex interactions**: Metrics affect behavior affects metrics
- **Testing difficulty**: Metrics state must be controlled
- **Unpredictable behavior**: System behavior changes with load
- **Debugging complexity**: Is the problem in logic or monitoring?

### Observational Metrics
FeedMe metrics are **pure observation**:
- Counters for events processed/dropped/errors
- Latency histograms per stage
- Drop reason frequencies
- Exported to Prometheus/JSON for external monitoring

They **never** affect pipeline behavior:
- No circuit breakers
- No backpressure
- No dynamic routing
- No alerting logic

### Why This Works
- **Separation of concerns**: Monitoring is external to processing
- **Predictable behavior**: Pipeline logic is independent of metrics
- **Simple testing**: Metrics don't change test outcomes
- **Flexible monitoring**: Use any monitoring system you want

## Why No Hidden Buffering?

### The Problem with Buffers
Many streaming systems buffer data internally:
- For efficiency (batch I/O)
- For parallelism (work queues)
- For resilience (retry queues)

This leads to:
- **Unbounded memory**: Buffers can grow without limit
- **Complex state**: What's in the buffer? What's being processed?
- **Ordering issues**: Buffers can reorder events
- **Resource leaks**: Forgotten buffers consume memory

### Explicit Streaming
FeedMe processes one event at a time with no internal buffering:
- **Bounded memory**: Memory usage is O(1) regardless of input size
- **Predictable ordering**: Events maintain their sequence
- **Simple reasoning**: At any point, you know exactly what's happening
- **Resource safety**: No background cleanup needed

### When Buffering is Needed
Add buffering at the edges:
- Input sources can buffer reads
- Output sinks can buffer writes
- External queues can provide resilience
- Application logic can batch operations

FeedMe's job is the transformation pipeline—the plumbing, not the plumbing infrastructure.

## Why Bounded Everything?

### The Problem with "Unlimited"
Systems that claim "unlimited" scale often have hidden bounds:
- Memory grows with input size
- CPU usage increases unpredictably
- Network connections accumulate
- Disk space fills up

This leads to:
- **Resource exhaustion**: Systems fail under load
- **Unpredictable performance**: Behavior changes with data size
- **Hard capacity planning**: No way to predict resource needs
- **Operational surprises**: "It worked in dev, why not in prod?"

### Explicit Bounds
FeedMe makes all bounds explicit:
- **Memory**: O(1) per event, no internal buffers
- **CPU**: Linear with input size, predictable operations
- **I/O**: Streaming, no bulk operations
- **State**: No persistent state, no accumulation

### Benefits
- **Predictable scaling**: Resource usage is proportional to input
- **Capacity planning**: You can calculate required resources
- **Reliable operation**: No surprises under load
- **Simple deployment**: No complex resource management

## Why Plugin Architecture Without Discovery?

### The Problem with Auto-Discovery
Plugin systems often auto-discover stages:
- Scan directories for shared libraries
- Load plugins at runtime
- Dynamic linking and symbol resolution

This creates:
- **Deployment complexity**: Plugin management and versioning
- **Security risks**: Arbitrary code execution
- **Debugging difficulty**: Which plugin caused the crash?
- **Compatibility issues**: ABI mismatches between versions

### Contract-Based Plugins
FeedMe uses Rust's trait system for plugins:
- **Compile-time safety**: Plugins are compiled with the application
- **Type safety**: `Stage` trait enforces the contract
- **No runtime discovery**: Plugins are explicit code
- **Zero overhead**: No dynamic dispatch, no indirection

### Benefits
- **Security**: No arbitrary code execution
- **Reliability**: Compile-time verification
- **Performance**: Static dispatch when possible
- **Maintainability**: Plugins are just Rust code

## Why This Design Matters

These choices aren't arbitrary—they're the result of building data processing systems and seeing what breaks in production. FeedMe is designed to be:

- **Reliable**: Failures are caught and attributed
- **Predictable**: Behavior is deterministic and bounded
- **Maintainable**: Simple code with clear contracts
- **Performant**: No hidden overhead or complexity

The constraints enable the guarantees. The guarantees enable trust. Trust enables adoption.

## Contributing to FeedMe

When proposing changes, ask:
- Does this maintain the core constraints?
- Does it strengthen the guarantees?
- Does it keep the mental model simple?
- Does it help users build reliable systems?

If the answer to any is "no," the change probably doesn't belong here.

FeedMe is not trying to be everything to everyone. It's trying to be excellent at one thing: reliable, bounded data plumbing.