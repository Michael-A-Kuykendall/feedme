# ADR 0001: InputSource/OutputSink extension boundary for async edge adapters

**Status:** Accepted (2026-08-09) — resolves `feedme-1cq`, unblocks `feedme-m5c`.

## Context

`feedme-m5c` commits FeedMe to "sync core, async I/O at the edges": the stage
loop stays synchronous, but `InputSource`/`OutputSink` may perform async I/O
(Kafka, S3, HTTP) at the boundary. Before that work starts, two gaps needed
resolving:

1. `InputSource` was a closed `enum` (`Stdin`, `File`, `Directory`), not
   `#[non_exhaustive]`. Adding `Kafka`/`S3`/`Http` variants would break any
   downstream code that matches on it exhaustively.
2. `OutputSink` was described in `README.md` and `design_rationale.md` as a
   real, symmetric extension point alongside `InputSource`, but no such type
   existed in the code — outputs were just `Stage` impls flagged by
   `is_output()`.

## Decision

**Trait-based extension, not enum growth.** Two small traits, both additive
(no breaking change to existing callers):

```rust
// src/input.rs
pub trait InputSourceAdapter {
    fn process_input(
        &mut self,
        pipeline: &mut Pipeline,
        deadletter: &mut Option<&mut dyn Stage>,
    ) -> Result<(), PipelineError>;
}

impl InputSourceAdapter for InputSource { /* delegates to the existing inherent impl */ }
```

```rust
// src/stages.rs — was already there, just misnamed
pub trait OutputSink: Stage {}
impl OutputSink for StdoutOutput {}
impl OutputSink for FileOutput {}
impl OutputSink for Deadletter {}
```

- `InputSource` stays exactly as it is today (`Stdin`/`File`/`Directory`) —
  it is **not** deprecated, `#[non_exhaustive]`'d, or converted into a trait
  object. It simply also implements `InputSourceAdapter`.
- A future async adapter (`KafkaSource`, `S3Source`, `HttpSource`) implements
  `InputSourceAdapter` directly, as an independent type. It is used via
  `Box<dyn InputSourceAdapter>` (or generic `<T: InputSourceAdapter>`) rather
  than by growing the enum. No existing exhaustive `match` on `InputSource`
  is ever affected by adding a new adapter.
- `OutputSink` was **not** a missing abstraction — `stages.rs` already had
  the identical pattern under the name `Output` (a marker trait over `Stage`,
  implemented by `StdoutOutput`/`FileOutput`/`Deadletter`). It was renamed to
  `OutputSink` to match what the docs already promised, rather than inventing
  a second, redundant trait. It had zero usages outside `stages.rs`, so the
  rename is a no-op for every current caller.

## The async-bridging contract (for `feedme-m5c` to implement against)

Both traits' methods are **synchronous in signature**. An async adapter must
bridge internally — it owns the async side entirely:

- Do the async I/O (network read/write) on a task/thread the adapter itself
  owns and manages.
- Move data across the sync/async boundary through a **bounded** channel
  (e.g. `std::sync::mpsc` with a fixed capacity, or equivalent) — bounded so
  an adapter can't silently reintroduce the unbounded buffering
  `design_rationale.md` ("Why No Hidden Buffering?") explicitly rejects.
- `InputSourceAdapter::process_input` / the `OutputSink`'s `Stage::execute`
  drain/feed that channel synchronously. The stage loop and `Pipeline` never
  see or await a `Future`; from `Pipeline`'s perspective this is just another
  synchronous, blocking call.
- No thread is spawned *inside* the per-event `execute`/`process_input` path
  — thread/task ownership lives at adapter-construction time, not per-event.
  Bridge async I/O through a bounded channel (e.g. `std::sync::mpsc` with a
  fixed capacity); never spawn a thread inside the handler.

## Dependency isolation

Any async runtime (tokio or otherwise) an adapter needs is that adapter's
concern, not the core crate's. `feedme-m5c` adapters (Kafka/S3/HTTP) MUST be
gated behind their own Cargo feature(s), off by default, so a consumer who
only uses the sync built-ins never compiles or links an async runtime. This
follows the existing `fused`/`invariant-ppt` gating pattern already used in
`Cargo.toml`.

## Consequences

- `feedme-m5c` can proceed directly: implement `InputSourceAdapter` (and
  `OutputSink`, for sinks) for each new adapter type, feature-gated, without
  touching `InputSource` or breaking any existing consumer.
- `README.md` / `docs/design_rationale.md`'s references to `OutputSink` are
  now literally true rather than aspirational.
- This does not itself add any async I/O or new dependency — it only opens
  the extension point `feedme-m5c` builds on.
