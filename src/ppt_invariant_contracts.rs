use crate::invariant_ppt;
use crate::{Event, Pipeline, PipelineError, Stage, StdoutOutput, InputSource, Filter, RequiredFields, FieldSelect};
use crate::replay_spec::ReplayableStage;

struct Passthrough;

impl Stage for Passthrough {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError> {
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "Passthrough"
    }
}

struct Dropper;

impl Stage for Dropper {
    fn execute(&mut self, _event: Event) -> Result<Option<Event>, PipelineError> {
        Ok(None)
    }

    fn name(&self) -> &str {
        "Dropper"
    }
}

struct OutputSink;

impl Stage for OutputSink {
    fn execute(&mut self, _event: Event) -> Result<Option<Event>, PipelineError> {
        Ok(None)
    }

    fn name(&self) -> &str {
        "OutputSink"
    }

    fn is_output(&self) -> bool {
        true
    }
}

#[test]
fn contract_pipeline_metrics_laws_exercised() {
    invariant_ppt::clear_invariant_log();

    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(Passthrough));

    let event = Event::from_raw_input(r#"{"level":"info","message":"hello"}"#).expect("Failed to create test event");
    let _ = pipeline.process_event(event).expect("Pipeline processing failed"); // Result ignored for invariant testing

    assert!(invariant_ppt::contract_test(
        "pipeline metrics laws",
        &[
            crate::INVARIANT_PROCESSED_INCREMENTS_ONCE,
            crate::INVARIANT_LATENCY_RECORDED_ON_SUCCESS,
        ],
    )
    .is_ok());
}

#[test]
fn contract_drop_only_counts_for_non_output_stage() {
    // Non-output stage returning None must count as dropped.
    invariant_ppt::clear_invariant_log();

    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(Dropper));

    let event = Event::from_raw_input(r#"{"level":"info","message":"hello"}"#).expect("Failed to create test event");
    let out = pipeline.process_event(event).expect("Pipeline processing failed");
    assert!(out.is_none());

    assert!(invariant_ppt::contract_test(
        "drop counts",
        &[
            crate::INVARIANT_PROCESSED_INCREMENTS_ONCE,
            crate::INVARIANT_DROPPED_ONLY_FOR_NON_OUTPUT_NONE,
        ],
    )
    .is_ok());

    // Output stage returning None must NOT count as dropped.
    invariant_ppt::clear_invariant_log();

    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(OutputSink));

    let event = Event::from_raw_input(r#"{"level":"info","message":"hello"}"#).expect("Failed to create test event");
    let out = pipeline.process_event(event).expect("Pipeline processing failed");
    assert!(out.is_none());

    // This invariant is checked in the core logic, and this contract ensures the check executes.
    assert!(invariant_ppt::contract_test(
        "output consumption",
        &[
            crate::INVARIANT_PROCESSED_INCREMENTS_ONCE,
            crate::INVARIANT_OUTPUT_NONE_NOT_DROPPED,
        ],
    )
    .is_ok());
}

#[test]
fn metrics_export_is_pure() {
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(Passthrough));

    let event = Event::from_raw_input(r#"{"level":"info","message":"hello"}"#).expect("Failed to create test event");
    let _ = pipeline.process_event(event).expect("Pipeline processing failed"); // Result ignored for purity testing

    let a = pipeline.export_prometheus();
    let b = pipeline.export_prometheus();
    assert_eq!(a, b);

    let ja = pipeline.export_json_logs();
    let jb = pipeline.export_json_logs();
    assert_eq!(ja, jb);
}

#[test]
fn contract_directory_determinism() {
    use std::fs;
    use std::io::Write;
    use tempfile::TempDir;

    // Create a temp directory with files
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let file1_path = temp_dir.path().join("file1.ndjson");
    let file2_path = temp_dir.path().join("file2.ndjson");

    {
        let mut file1 = fs::File::create(&file1_path).expect("Failed to create file1");
        writeln!(file1, r#"{{"level":"info","message":"first"}}"#).expect("Failed to write to file1");
    }
    {
        let mut file2 = fs::File::create(&file2_path).expect("Failed to create file2");
        writeln!(file2, r#"{{"level":"info","message":"second"}}"#).expect("Failed to write to file2");
    }

    // Process directory twice
    let run1 = || {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(Passthrough));
        pipeline.add_stage(Box::new(StdoutOutput::new()));

        let mut input = InputSource::Directory(temp_dir.path().to_path_buf());
        input.process_input(&mut pipeline, &mut None).expect("Failed to process input");
        // Only compare non-timing metrics
        pipeline.export_json_logs().into_iter().filter(|s| !s.contains("stage_latencies")).collect::<Vec<_>>()
    };

    let output1 = run1();
    let output2 = run1();

    assert_eq!(output1, output2);
}

#[test]
fn contract_no_output_after_drop() {
    // Once a stage returns None, no subsequent stages execute for that event
    invariant_ppt::clear_invariant_log();

    let mut pipeline = Pipeline::new();
    // Add a stage that drops, then a stage that would fail if executed
    pipeline.add_stage(Box::new(Filter::new(Box::new(|_| false)))); // Always drops
    pipeline.add_stage(Box::new(RequiredFields::new(vec!["nonexistent".into()]))); // Would fail

    let event = Event::from_raw_input(r#"{"level":"info","message":"hello"}"#).expect("Failed to create test event");
    let result = pipeline.process_event(event).expect("Pipeline processing failed");
    assert!(result.is_none()); // Event was dropped

    // If RequiredFields executed, it would have failed, but it didn't
    // This contract ensures the drop short-circuits execution
    assert!(invariant_ppt::contract_test(
        "no output after drop",
        &[
            crate::INVARIANT_PROCESSED_INCREMENTS_ONCE,
            crate::INVARIANT_DROPPED_ONLY_FOR_NON_OUTPUT_NONE,
        ],
    )
    .is_ok());
}

#[test]
fn replay_spec_serialization_roundtrip() {
    use crate::replay_spec::*;

    // Create a simple pipeline spec manually
    let stages = vec![
        StageSpec {
            stage_id: "field_select".to_string(),
            stage_version: "1.0".to_string(),
            config: serde_json::json!({"fields": ["level", "message"]}),
        },
        StageSpec {
            stage_id: "required_fields".to_string(),
            stage_version: "1.0".to_string(),
            config: serde_json::json!({"fields": ["level"]}),
        },
    ];

    let original_spec = PipelineReplaySpec {
        feedme_version: "0.3.0".to_string(),
        spec_version: "1.0".to_string(),
        stages: stages.clone(),
        settings: PipelineSettings::default(),
        spec_hash: String::new(),
    };

    // Serialize to JSON
    let json = serde_json::to_string(&original_spec).expect("Failed to serialize");
    println!("Serialized spec: {}", json);

    // Deserialize back
    let _deserialized: PipelineReplaySpec = serde_json::from_str(&json).expect("Failed to deserialize");

    // Verify the spec_hash gets computed correctly
    let spec_with_hash = PipelineReplaySpec::from_stages(stages).expect("Failed to create spec with hash");
    assert!(!spec_with_hash.spec_hash.is_empty());
    assert_eq!(spec_with_hash.feedme_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(spec_with_hash.stages.len(), 2);
}

#[test]
fn replay_spec_deterministic_hash() {
    use crate::replay_spec::*;

    let stages1 = vec![StageSpec {
        stage_id: "test".to_string(),
        stage_version: "1.0".to_string(),
        config: serde_json::json!({"value": 42}),
    }];

    let stages2 = vec![StageSpec {
        stage_id: "test".to_string(),
        stage_version: "1.0".to_string(),
        config: serde_json::json!({"value": 42}),
    }];

    let spec1 = PipelineReplaySpec::from_stages(stages1).expect("Failed to create spec1");
    let spec2 = PipelineReplaySpec::from_stages(stages2).expect("Failed to create spec2");

    // Same content should produce same hash
    assert_eq!(spec1.spec_hash, spec2.spec_hash);

    // Different content should produce different hash
    let stages3 = vec![StageSpec {
        stage_id: "test".to_string(),
        stage_version: "1.0".to_string(),
        config: serde_json::json!({"value": 43}),
    }];

    let spec3 = PipelineReplaySpec::from_stages(stages3).expect("Failed to create spec3");
    assert_ne!(spec1.spec_hash, spec3.spec_hash);
}

#[test]
fn replay_spec_diff() {
    use crate::replay_spec::*;

    let stages1 = vec![StageSpec {
        stage_id: "field_select".to_string(),
        stage_version: "1.0".to_string(),
        config: serde_json::json!({"fields": ["a"]}),
    }];

    let stages2 = vec![
        StageSpec {
            stage_id: "field_select".to_string(),
            stage_version: "1.0".to_string(),
            config: serde_json::json!({"fields": ["b"]}), // Modified
        },
        StageSpec {
            stage_id: "required_fields".to_string(),
            stage_version: "1.0".to_string(),
            config: serde_json::json!({"fields": ["x"]}), // Added
        },
    ];

    let spec1 = PipelineReplaySpec::from_stages(stages1).expect("Failed to create spec1");
    let spec2 = PipelineReplaySpec::from_stages(stages2).expect("Failed to create spec2");

    let diff = spec1.diff(&spec2);
    assert_eq!(diff.added_stages.len(), 1);
    assert_eq!(diff.removed_stages.len(), 0);
    assert_eq!(diff.modified_stages.len(), 1);
}

#[test]
fn replayable_stage_trait() {
    // Test that our replayable stages work
    let field_select = FieldSelect::new(vec!["level".to_string(), "message".to_string()]);
    let spec = field_select.to_spec();

    assert_eq!(spec.stage_id, "field_select");
    assert_eq!(spec.stage_version, "1.0");
    assert_eq!(spec.config["fields"], serde_json::json!(["level", "message"]));

    let required_fields = RequiredFields::new(vec!["level".to_string()]);
    let spec2 = required_fields.to_spec();

    assert_eq!(spec2.stage_id, "required_fields");
    assert_eq!(spec2.stage_version, "1.0");
    assert_eq!(spec2.config["fields"], serde_json::json!(["level"]));
}

#[test]
fn stage_registry_functionality() {
    use crate::replay_spec::*;

    let mut registry = StageRegistry::new();

    // Register factories
    registry.register_stage("field_select".to_string(), Box::new(|config| {
        let fields: Vec<String> = serde_json::from_value(config["fields"].clone())?;
        Ok(Box::new(FieldSelect::new(fields)))
    }));

    registry.register_stage("required_fields".to_string(), Box::new(|config| {
        let fields: Vec<String> = serde_json::from_value(config["fields"].clone())?;
        Ok(Box::new(RequiredFields::new(fields)))
    }));

    // Test registration
    assert!(registry.is_replayable(&"field_select".to_string()));
    assert!(registry.is_replayable(&"required_fields".to_string()));
    assert!(!registry.is_replayable(&"unknown_stage".to_string()));

    // Test stage creation
    let field_select_spec = StageSpec {
        stage_id: "field_select".to_string(),
        stage_version: "1.0".to_string(),
        config: serde_json::json!({"fields": ["level", "message"]}),
    };

    let stage = registry.create_stage(&field_select_spec).expect("Failed to create stage");
    assert_eq!(stage.name(), "FieldSelect");
}

#[test]
fn pipeline_composition_analysis() {
    // Test empty pipeline detection
    let empty_pipeline = Pipeline::new();
    let issues = invariant_ppt::analyze_pipeline_composition(&empty_pipeline);
    assert!(issues.iter().any(|i| i.contains("no stages")));

    // Test healthy pipeline
    let mut healthy_pipeline = Pipeline::new();
    healthy_pipeline.add_stage(Box::new(Passthrough));
    healthy_pipeline.add_stage(Box::new(StdoutOutput::new()));
    let issues = invariant_ppt::analyze_pipeline_composition(&healthy_pipeline);
    assert!(issues.is_empty());

    // Test pipeline with no output
    let mut no_output_pipeline = Pipeline::new();
    no_output_pipeline.add_stage(Box::new(Passthrough));
    let issues = invariant_ppt::analyze_pipeline_composition(&no_output_pipeline);
    assert!(issues.iter().any(|i| i.contains("no output stages")));
}

#[test]
fn pipeline_health_check() {
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(Passthrough));
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // Process some events to generate metrics
    let event = Event::from_raw_input(r#"{"level":"info","message":"test"}"#).unwrap();
    let _ = pipeline.process_event(event.clone());
    let _ = pipeline.process_event(event);

    let health = invariant_ppt::pipeline_health_check(&pipeline);
    assert!(health.is_healthy);
    assert_eq!(health.stage_count, 2);
    assert_eq!(health.total_processed, 2);
    assert!(health.error_rate < 0.01); // Should be 0
}

#[test]
fn performance_regression_detection() {
    use invariant_ppt::{PipelineMetrics, check_performance_regression};

    let baseline = PipelineMetrics {
        stage_count: 2,
        total_events_processed: 1000,
        avg_latency_ms: 10.0,
        memory_baseline_kb: 1024,
        error_rate: 0.01,
    };

    // Establish baseline
    invariant_ppt::record_pipeline_metrics("test_pipeline", baseline.clone());

    // Test with improved performance (should pass)
    let improved = PipelineMetrics {
        stage_count: 2,
        total_events_processed: 1000,
        avg_latency_ms: 8.0, // 20% improvement
        memory_baseline_kb: 1024,
        error_rate: 0.005, // Better error rate
    };

    assert!(check_performance_regression("test_pipeline", &improved, 0.15).is_ok());

    // Test with regression (should fail)
    let regressed = PipelineMetrics {
        stage_count: 2,
        total_events_processed: 1000,
        avg_latency_ms: 12.0, // 20% regression
        memory_baseline_kb: 1024,
        error_rate: 0.01,
    };

    assert!(check_performance_regression("test_pipeline", &regressed, 0.15).is_err());
}

#[test]
fn replay_from_pipeline_roundtrip_unified() {
    use crate::replay_spec::*;

    let mut reg = StageRegistry::new();
    reg.register_stage("field_select".to_string(), Box::new(|c| {
        let fields: Vec<String> = serde_json::from_value(c["fields"].clone())?;
        Ok(Box::new(FieldSelect::new(fields)))
    }));
    reg.register_stage("required_fields".to_string(), Box::new(|c| {
        let fields: Vec<String> = serde_json::from_value(c["fields"].clone())?;
        Ok(Box::new(RequiredFields::new(fields)))
    }));
    reg.register_stage("stdout_output".to_string(), Box::new(|_c| Ok(Box::new(StdoutOutput::new()))));

    let mut p = Pipeline::new();
    p.add_stage(Box::new(FieldSelect::new(vec!["level".into(), "message".into()])));
    p.add_stage(Box::new(RequiredFields::new(vec!["level".into()])));
    p.add_stage(Box::new(StdoutOutput::new()));

    // Now works thanks to unified from_pipeline + ReplayableStage impls on core stages
    let spec = PipelineReplaySpec::from_pipeline(&p, &reg).expect("from_pipeline must succeed for replayable stages");
    assert_eq!(spec.stages.len(), 3);

    let p2 = spec.to_pipeline(&reg).expect("to_pipeline roundtrip");
    assert_eq!(p2.stage_count(), 3);
    assert_eq!(p2.stage_names(), vec!["FieldSelect", "RequiredFields", "StdoutOutput"]);
}
