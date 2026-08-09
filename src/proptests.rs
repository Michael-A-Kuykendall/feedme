//! Property-based tests for core invariants (feedme-3c6).
//!
//! Hand-written unit tests only cover the specific cases someone thought to
//! write. The two most severe bugs found in the 2026-08-09 audit -- metrics
//! export ordering depending on `HashMap`'s randomized hasher (feedme-sq8),
//! and several stages silently failing to wire `Stage::to_replay_spec`
//! (feedme-6il) -- both would have been caught automatically by generating
//! many pipeline/event combinations and checking the invariant directly
//! against the real implementation, instead of a fixed set of examples.
//!
//! TLA+ was considered and rejected for this module's job: FeedMe's stage
//! loop is deliberately single-threaded (no interleaving space for TLA+ to
//! explore), and a TLA+ spec verifies a model of the design, not the actual
//! Rust code. `proptest` generates inputs and runs the real crate directly,
//! so there is no design/implementation gap to maintain.

use crate::{
    Deadletter, DerivedFields, Event, FieldRemap, FieldSelect, FileOutput, Filter, PIIRedaction,
    Pipeline, RequiredFields, Stage, StdoutOutput, TypeChecking, ValueConstraints,
};
use proptest::prelude::*;
use std::collections::HashMap;

// ── Arbitrary event generation ──────────────────────────────────────────────

/// A small, fixed field-name pool keeps generated events meaningful (stages
/// key off specific field names) while staying shrinkable.
const FIELD_POOL: &[&str] = &["level", "message", "user_id", "email", "count"];

fn arb_field_name() -> impl Strategy<Value = String> {
    proptest::sample::select(FIELD_POOL).prop_map(|s| s.to_string())
}

fn arb_json_value() -> impl Strategy<Value = serde_json::Value> {
    prop_oneof![
        "[a-z]{0,8}".prop_map(serde_json::Value::from),
        any::<i32>().prop_map(|n| serde_json::Value::from(n as i64)),
        any::<bool>().prop_map(serde_json::Value::from),
    ]
}

/// A flat JSON object using 0-4 fields from `FIELD_POOL`.
fn arb_event_json() -> impl Strategy<Value = serde_json::Value> {
    prop::collection::hash_map(arb_field_name(), arb_json_value(), 0..4)
        .prop_map(|map| serde_json::Value::Object(map.into_iter().collect()))
}

// ── Arbitrary stage generation ──────────────────────────────────────────────
//
// Stages that take closures (Filter, DerivedFields, ValueConstraints) can't
// be generated directly by proptest's Arbitrary/shrinking machinery, so we
// generate a small description enum and interpret it into a real Stage when
// building the pipeline -- the standard pattern for property-testing systems
// with closures.

#[derive(Debug, Clone)]
enum ArbStage {
    FieldSelect(Vec<String>),
    RequiredFields(Vec<String>),
    FilterHasField(String),
    PiiRedactNoop,
}

fn arb_stage_spec() -> impl Strategy<Value = ArbStage> {
    prop_oneof![
        prop::collection::vec(arb_field_name(), 0..3).prop_map(ArbStage::FieldSelect),
        prop::collection::vec(arb_field_name(), 0..3).prop_map(ArbStage::RequiredFields),
        arb_field_name().prop_map(ArbStage::FilterHasField),
        Just(ArbStage::PiiRedactNoop),
    ]
}

/// Like [`arb_stage_spec`] but excludes Validator stages (`RequiredFields`),
/// for properties that need to control whether a Validator is present.
fn arb_non_validator_stage_spec() -> impl Strategy<Value = ArbStage> {
    prop_oneof![
        prop::collection::vec(arb_field_name(), 0..3).prop_map(ArbStage::FieldSelect),
        arb_field_name().prop_map(ArbStage::FilterHasField),
        Just(ArbStage::PiiRedactNoop),
    ]
}

fn build_stage(spec: &ArbStage) -> Box<dyn Stage> {
    match spec {
        ArbStage::FieldSelect(fields) => Box::new(FieldSelect::new(fields.clone())),
        ArbStage::RequiredFields(fields) => Box::new(RequiredFields::new(fields.clone())),
        ArbStage::FilterHasField(field) => {
            let field = field.clone();
            Box::new(Filter::new(Box::new(move |e: &Event| {
                e.data.get(&field).is_some()
            })))
        }
        ArbStage::PiiRedactNoop => Box::new(PIIRedaction::new(vec![])),
    }
}

/// Zeroes out wall-clock latency fields (`sum`/`min`/`max`, present only on
/// `stage_latencies` log lines) before comparing exported metric logs across
/// runs -- those are real elapsed-time measurements and are *expected* to
/// vary, unlike everything else in the log (which stage, which order,
/// deterministic counts), which must not.
fn normalize_timing_fields(json_log_line: &str) -> String {
    let mut value: serde_json::Value =
        serde_json::from_str(json_log_line).expect("metrics export must emit valid JSON");
    if let serde_json::Value::Object(map) = &mut value {
        for key in ["sum", "min", "max"] {
            if map.contains_key(key) {
                map.insert(key.to_string(), serde_json::Value::from(0.0));
            }
        }
    }
    value.to_string()
}

fn build_pipeline(specs: &[ArbStage]) -> Pipeline {
    let mut pipeline = Pipeline::new();
    for spec in specs {
        pipeline.add_stage(build_stage(spec));
    }
    pipeline
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// feedme-sq8 regression, generalized: for ANY pipeline configuration and
    /// ANY event sequence (not just the one hand-written test case), running
    /// twice from fresh instances must produce byte-identical processed
    /// output AND byte-identical exported metrics. This is the exact
    /// property that a HashMap-ordering regression would violate.
    #[test]
    fn prop_determinism_across_runs(
        stage_specs in prop::collection::vec(arb_stage_spec(), 0..5),
        events in prop::collection::vec(arb_event_json(), 0..20),
    ) {
        let run = |specs: &[ArbStage]| {
            let mut pipeline = build_pipeline(specs);
            let mut outputs = Vec::new();
            for data in &events {
                let event = Event { data: data.clone(), metadata: None };
                if let Ok(Some(out)) = pipeline.process_event(event) {
                    outputs.push(out.data.to_string());
                }
            }
            let logs: Vec<String> = pipeline
                .export_json_logs()
                .iter()
                .map(|l| normalize_timing_fields(l))
                .collect();
            (outputs, logs)
        };

        let (out1, logs1) = run(&stage_specs);
        let (out2, logs2) = run(&stage_specs);

        prop_assert_eq!(out1, out2, "processed output differs across identical runs");
        // Compares metric/stage ordering and deterministic counts (events
        // processed/dropped/errors, per-stage execution count) while
        // ignoring wall-clock latency values (sum/min/max), which correctly
        // vary run to run -- that's real elapsed time, not a bug. What must
        // NOT vary is which entries appear and in what order (the exact
        // property a HashMap-ordering regression would violate).
        prop_assert_eq!(logs1, logs2, "export_json_logs structure/ordering differs across identical runs");
    }

    /// Events that survive the pipeline (aren't dropped or erroring) appear
    /// in the output in the same relative order they were fed in. Order is
    /// tracked via `Event::metadata`, since no built-in stage touches it.
    #[test]
    fn prop_order_preserved_for_surviving_events(
        stage_specs in prop::collection::vec(arb_stage_spec(), 0..5),
        events in prop::collection::vec(arb_event_json(), 0..20),
    ) {
        let mut pipeline = build_pipeline(&stage_specs);
        let mut seen_indices = Vec::new();

        for (idx, data) in events.iter().enumerate() {
            let event = Event {
                data: data.clone(),
                metadata: Some(std::collections::BTreeMap::from([(
                    "__order_idx".to_string(),
                    serde_json::Value::from(idx as u64),
                )])),
            };
            if let Ok(Some(out)) = pipeline.process_event(event) {
                if let Some(meta) = &out.metadata {
                    if let Some(v) = meta.get("__order_idx").and_then(|v| v.as_u64()) {
                        seen_indices.push(v);
                    }
                }
            }
        }

        let mut sorted = seen_indices.clone();
        sorted.sort_unstable();
        prop_assert_eq!(seen_indices, sorted, "surviving events were not emitted in input order");
    }

    /// For a pipeline with NO Validator stage, `require_validation(true)`
    /// must reject every event -- fail-closed, not opt-in.
    #[test]
    fn prop_require_validation_fails_closed_without_validator(
        stage_specs in prop::collection::vec(arb_non_validator_stage_spec(), 0..3),
        event_data in arb_event_json(),
    ) {
        let mut pipeline = Pipeline::new().require_validation(true);
        for spec in &stage_specs {
            pipeline.add_stage(build_stage(spec));
        }
        let event = Event { data: event_data, metadata: None };
        prop_assert!(pipeline.process_event(event).is_err());
    }

    /// For a pipeline that DOES contain a Validator stage, require_validation
    /// must not itself cause a rejection (the Validator stage's own logic
    /// may still reject a specific event, but not the require_validation gate).
    #[test]
    fn prop_require_validation_does_not_block_when_validator_present(
        event_data in arb_event_json(),
    ) {
        let mut pipeline = Pipeline::new().require_validation(true);
        pipeline.add_stage(Box::new(RequiredFields::new(vec![])));
        let event = Event { data: event_data, metadata: None };
        let result = pipeline.process_event(event);
        // An empty required-fields list can never itself reject -- any Err
        // here would have to come from the require_validation gate, not the
        // Validator's own logic.
        prop_assert!(result.is_ok());
    }

    /// Metrics counters never decrease across calls, and dropped can never
    /// exceed processed, for any pipeline/event combination.
    #[test]
    fn prop_metrics_are_monotonic_and_consistent(
        stage_specs in prop::collection::vec(arb_stage_spec(), 0..5),
        events in prop::collection::vec(arb_event_json(), 0..20),
    ) {
        let mut pipeline = build_pipeline(&stage_specs);
        let (mut prev_processed, mut prev_dropped, mut prev_errors) = (0u64, 0u64, 0u64);

        for data in &events {
            let event = Event { data: data.clone(), metadata: None };
            let _ = pipeline.process_event(event);

            prop_assert!(pipeline.events_processed() >= prev_processed);
            prop_assert!(pipeline.events_dropped() >= prev_dropped);
            prop_assert!(pipeline.error_count() >= prev_errors);
            prop_assert!(pipeline.events_dropped() <= pipeline.events_processed());

            prev_processed = pipeline.events_processed();
            prev_dropped = pipeline.events_dropped();
            prev_errors = pipeline.error_count();
        }
    }
}

// ── Regression guard (not a property test): every built-in stage that
// implements ReplayableStage must actually wire Stage::to_replay_spec.
// This is exactly the class of bug feedme-6il found in 7 stages at once. ──

#[test]
fn test_all_builtin_stages_wire_to_replay_spec() {
    let stages: Vec<Box<dyn Stage>> = vec![
        Box::new(FieldSelect::new(vec!["a".to_string()])),
        Box::new(FieldRemap::new(HashMap::new())),
        Box::new(PIIRedaction::new(vec![])),
        Box::new(DerivedFields::new(HashMap::new())),
        Box::new(Filter::new(Box::new(|_e: &Event| true))),
        Box::new(RequiredFields::new(vec!["a".to_string()])),
        Box::new(TypeChecking::new(HashMap::new())),
        Box::new(ValueConstraints::new(HashMap::new())),
        Box::new(StdoutOutput::new()),
        Box::new(FileOutput::new(
            "test_proptest_builtin_wiring.ndjson".into(),
        )),
        Box::new(Deadletter::new(
            "test_proptest_builtin_wiring_dl.ndjson".into(),
        )),
    ];

    for stage in &stages {
        assert!(
            stage.to_replay_spec().is_some(),
            "{} implements ReplayableStage but Stage::to_replay_spec() returned None -- \
             this silently breaks PipelineReplaySpec::from_pipeline() for any pipeline \
             containing it (this is exactly the feedme-6il bug class)",
            stage.name()
        );
    }

    #[cfg(feature = "fused")]
    {
        use crate::fused::{FailAction, FusedRuleEngine};
        let fused: Box<dyn Stage> = Box::new(
            FusedRuleEngine::builder("fused")
                .on_fail(FailAction::DropEvent)
                .build(),
        );
        assert!(
            fused.to_replay_spec().is_some(),
            "FusedRuleEngine implements ReplayableStage but Stage::to_replay_spec() returned None"
        );
    }
}
