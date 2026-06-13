# FeedMe Roadmap

## v1.x Series (Current)

### v1.0.1 (This Release)
- Fused Rule Engine (FSE) — O(M) selector-first single-pass rule evaluation
- Fault Injection for resilience testing
- Audit Manager with compliance attestation
- Pipeline ReplaySpec for A/B comparison and config drift detection
- PptManager for performance baseline regression detection
- 21 working examples demonstrating all features
- One-call `common_redact_validate_pipeline()` helper

### Potential v1.1.0

Consider:
- [ ] Async I/O stages (non-blocking file/directory reading)
- [ ] Streaming HTTP input (webhook receiver)
- [ ] GraphQL input support
- [ ] Richer constraint predicates (regex, range, custom validators)
- [ ] Pipeline composition (sub-pipelines as stages)
- [ ] Checkpointing for large file processing restart
- [ ] Output formatters (CSV, XML, Parquet)
- [ ] Enrichment stage (lookup external data sources)

### Long-term Considerations

- [ ] WASM compilation target for edge computing
- [ ] Kubernetes operator for pipeline deployment
- [ ] WebUI for visual pipeline building
- [ ] Schema registry integration

---

*FeedMe follows semantic versioning. The API is stable — no breaking changes in 1.x series.*