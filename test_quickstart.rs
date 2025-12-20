use feedme::{
    Pipeline, FieldSelect, RequiredFields, StdoutOutput, Deadletter,
    PIIRedaction, Filter, InputSource
};
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    // Create pipeline: select fields → redact PII → require fields → filter → output
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(FieldSelect::new(vec![
        "timestamp".into(), "level".into(), "message".into(), "email".into()
    ])));
    let email_pattern = regex::Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")?;
    pipeline.add_stage(Box::new(PIIRedaction::new(vec![email_pattern])));
    pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".into()])));
    pipeline.add_stage(Box::new(Filter::new(Box::new(|event| {
        event.data.get("level").and_then(|v| v.as_str()) != Some("debug")
    }))));
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // Deadletter for errors
    let mut deadletter = Deadletter::new(PathBuf::from("errors.ndjson"));

    // Process input file
    let mut input = InputSource::File(PathBuf::from("input.ndjson"));
    input.process_input(&mut pipeline, &mut Some(&mut deadletter))?;

    // Export final metrics
    println!("Pipeline complete. Metrics:");
    for metric in pipeline.export_json_logs() {
        println!("{}", serde_json::to_string(&metric)?);
    }

    Ok(())
}