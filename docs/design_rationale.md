# FeedMe: Design Rationale

This document explains the fundamental design decisions behind FeedMe. It answers
"why" rather than "what" or "how". Understanding these choices is key to using
FeedMe effectively and to keeping the project coherent as it evolves.

## The Lane (read this first)

FeedMe is **reliable, bounded data plumbing for Rust applications** — the
deterministic, sync-core, embeddable member of the same architectural family as
engines like `dzero-cas` and `mikedb`. Those engines prove the central bet: a
*synchronous, single-threaded, bounded-memory core* can do serious work without
async, because performance and scale come from the **invariant**, not from a
runtime. FeedMe is the **linear, embeddable front door** of that family: it turns
messy input streams into clean, ordered facts for a downstream engine or any sink.

It is deliberately **not**:

- a relational / aggregation engine (joins, `GROUP BY`, hypothetical queries are
  the job of a saturation-fabric engine such as `dzero-cas` / `mikedb`),
- a distributed system (no networking, clustering, or remote offsets in the core),
- a daemon or server (it is a library you embed).

The one sentence: **ingest, clean, transform, hand off — deterministically and
with bounded memory.** That is the whole lane. Everything below serves it.

## Why Linear Pipelines?

### The Problem with DAGs
Most data processing tools use directed acyclic graphs (DAGs) for pipelines. This
allows complex dependencies but introduces:

- **Complexity**: Reasoning about execution order becomes hard
- **Deadlocks**: Circular dependencies are possible
- **Resource contention**: Parallel execution needs coordination
- **Testing difficulty**: All combinations of stage interactions must be tested

### Linear by Design
FeedMe uses a strict linear pipeline: stages execute in sequence, one event at a
time. Benefits:

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

They struggle with (and we hand off to a fabric engine):
- Relational joins and aggregation
- Real-time analytics requiring cross-event state
- Machine learning pipelines with branching logic

FeedMe embraces this limitation — it keeps the tool focused and reliable.

## Why Ownership-Based Stages?

### The Problem with Shared State
Traditional pipeline stages often share state through global variables, shared
mutable references, external databases, or config files. This leads to race
conditions, hidden dependencies, testing complexity, and resource leaks.

### Ownership Transfer
FeedMe stages receive `Event` by value and return `Option<Event>`. This enforces:

- **No shared mutable state**: each stage owns its data
- **Explicit data flow**: ownership transfer is visible in the type system
- **Automatic cleanup**: Rust's ownership prevents leaks
- **Thread safety**: no mutable sharing means no concurrency issues

### Implications
- Stages are **pure functions** of their input (plus internal configuration)
- **Deterministic behavior**: same input → same output
- **Memory safety**: no reference cycles or dangling pointers
- **Performance**: zero-copy when possible, explicit copying when needed

## Why Fail-Fast by Default?

Silent failures corrupt data and bury errors. FeedMe stops on the first error by
default, then becomes resilient when a deadletter sink is configured: failed
events are isolated, successful events continue, and metrics track both. This
gives you **both** strict correctness and operational resilience.

## Why a Synchronous Core (and Async Only at the Edges)?

This is the single most important lane decision, so it is stated explicitly.

### The Bet
Sync-first is not a limitation — it is the same architectural bet made by
`dzero-cas` and `mikedb`. Those engines achieve datalog-scale performance from a
fixed-size-fact invariant and a saturation loop, **not** from an async runtime.
A synchronous core gives FeedMe:

- **Determinism**: identical runs produce identical outputs
- **Bounded memory**: no background buffers, queues, or runtime bookkeeping
- **A simple mental model**: functions call functions
- **Zero runtime overhead**: no executor needed for the common case
- **Universal embedding**: it drops into any Rust application, sync or async

### Async at the Edges — and Nowhere Else
The processing **core** (the stage loop) is strictly synchronous and
single-threaded. Async is permitted **only** at the I/O boundary: an
`InputSource` / `OutputSink` may perform async I/O (network, streaming) to pull
from or push to Kafka, S3, HTTP, etc. This matches how the fabric engines keep
I/O external (an mpsc channel, never a thread spawn inside the handler).

Async never enters the stage loop, so the core's guarantees (ordering, bounded
memory, determinism) are preserved regardless of how data arrives.

> **Status:** built-in sources today are local (`stdin`, file, directory). Async
> edge adapters are planned work — see bead `feedme-m5c`.

## Why Typed Errors?

String errors cannot be caught programmatically, are formatted inconsistently,
and lose context. FeedMe uses a typed taxonomy (`PipelineError` with
`Parse`/`Transform`/`Validation`/`Output`/`System` variants), each carrying a
category, the failing stage, a machine-readable code, and a human-readable
message. Errors are part of the public API and are attributable to their source.

## Why Observational Metrics Only?

Some systems let metrics drive behavior (circuit breakers, backpressure, dynamic
routing). That couples monitoring to logic and makes behavior load-dependent.
FeedMe metrics are **pure observation** — counters, latency histograms, drop
reasons — exported to Prometheus or JSON. They **never** affect pipeline behavior.
Monitoring stays external; the pipeline stays predictable.

## Why No Hidden Buffering?

Streaming systems that buffer internally trade predictability for putative
efficiency: unbounded memory, reordering, and leaks. FeedMe processes one event
at a time with no internal buffering, so memory is O(1) regardless of input size
and ordering is exact. Buffering, when needed, is added at the **edges** — an
input source may read ahead; an output sink may batch. The transformation
pipeline is the plumbing, not the plumbing infrastructure.

## Why Plugins Without Discovery?

Runtime plugin discovery (scanning for shared libraries, dynamic linking) adds
deployment complexity, security risk, and debugging pain. FeedMe uses Rust's
trait system: plugins are compile-time, type-checked `Stage` implementations with
zero dynamic dispatch overhead and no arbitrary code execution.

## Why This Design Matters

These choices aren't arbitrary — they're the result of building data-processing
systems and seeing what breaks in production. FeedMe is designed to be:

- **Reliable**: failures are caught and attributed
- **Predictable**: behavior is deterministic and bounded
- **Maintainable**: simple code with clear contracts
- **Performant**: no hidden overhead or complexity

The constraints enable the guarantees. The guarantees enable trust. Trust
enables adoption.

## Contributing to FeedMe

When proposing changes, ask:

- Does this maintain the core constraints (sync core, linear, bounded)?
- Does it strengthen the guarantees?
- Does it keep the mental model simple?
- Does it help users build reliable systems?

If the answer to any is "no," the change probably doesn't belong here. FeedMe is
not trying to be everything to everyone. It is trying to be excellent at one
thing: **reliable, bounded data plumbing.**
