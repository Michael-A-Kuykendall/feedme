use crate::invariant_ppt;
use crate::{Event, Pipeline, PipelineError, Stage, StdoutOutput, InputSource};

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

    let event = Event::from_raw_input(r#"{"level":"info","message":"hello"}"#).unwrap();
    let _ = pipeline.process_event(event).unwrap();

    invariant_ppt::contract_test(
        "pipeline metrics laws",
        &[
            crate::INVARIANT_PROCESSED_INCREMENTS_ONCE,
            crate::INVARIANT_LATENCY_RECORDED_ON_SUCCESS,
        ],
    );
}

#[test]
fn contract_drop_only_counts_for_non_output_stage() {
    // Non-output stage returning None must count as dropped.
    invariant_ppt::clear_invariant_log();

    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(Dropper));

    let event = Event::from_raw_input(r#"{"level":"info","message":"hello"}"#).unwrap();
    let out = pipeline.process_event(event).unwrap();
    assert!(out.is_none());

    invariant_ppt::contract_test(
        "drop counts",
        &[
            crate::INVARIANT_PROCESSED_INCREMENTS_ONCE,
            crate::INVARIANT_DROPPED_ONLY_FOR_NON_OUTPUT_NONE,
        ],
    );

    // Output stage returning None must NOT count as dropped.
    invariant_ppt::clear_invariant_log();

    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(OutputSink));

    let event = Event::from_raw_input(r#"{"level":"info","message":"hello"}"#).unwrap();
    let out = pipeline.process_event(event).unwrap();
    assert!(out.is_none());

    // This invariant is checked in the core logic, and this contract ensures the check executes.
    invariant_ppt::contract_test(
        "output consumption",
        &[
            crate::INVARIANT_PROCESSED_INCREMENTS_ONCE,
            crate::INVARIANT_OUTPUT_NONE_NOT_DROPPED,
        ],
    );
}

#[test]
fn metrics_export_is_pure() {
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(Passthrough));

    let event = Event::from_raw_input(r#"{"level":"info","message":"hello"}"#).unwrap();
    let _ = pipeline.process_event(event).unwrap();

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
    let temp_dir = TempDir::new().unwrap();
    let file1_path = temp_dir.path().join("file1.ndjson");
    let file2_path = temp_dir.path().join("file2.ndjson");

    {
        let mut file1 = fs::File::create(&file1_path).unwrap();
        writeln!(file1, r#"{{"level":"info","message":"first"}}"#).unwrap();
    }
    {
        let mut file2 = fs::File::create(&file2_path).unwrap();
        writeln!(file2, r#"{{"level":"info","message":"second"}}"#).unwrap();
    }

    // Process directory twice
    let run1 = || {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(Passthrough));
        pipeline.add_stage(Box::new(StdoutOutput::new()));

        let mut input = InputSource::Directory(temp_dir.path().to_path_buf());
        input.process_input(&mut pipeline, &mut None).unwrap();
        // Only compare non-timing metrics
        pipeline.export_json_logs().into_iter().filter(|s| !s.contains("stage_latencies")).collect::<Vec<_>>()
    };

    let output1 = run1();
    let output2 = run1();

    assert_eq!(output1, output2);
}
