use feedme::*;
use std::collections::HashMap;
use std::path::PathBuf;

fn processed_at(_: &Event) -> serde_json::Value {
    serde_json::Value::String("2025-12-20T00:00:00Z".to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Complex pipeline with multiple transforms
    // Input: File
    // Pipeline: PIIRedaction -> Filter -> FieldRemap -> DerivedFields -> RequiredFields -> FileOutput
    // Deadletter: For errors

    let mut pipeline = Pipeline::new();

    // PII Redaction
    let patterns = vec![
        regex::Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")?, // email
        regex::Regex::new(r"\b\d{3}-\d{2}-\d{4}\b")?,                               // SSN
    ];
    pipeline.add_stage(Box::new(PIIRedaction::new(patterns)));

    // Filter out debug
    pipeline.add_stage(Box::new(Filter::new(Box::new(|event: &Event| {
        event
            .get_string("level")
            .map_or(true, |level| level != "debug")
    }))));

    // Remap fields
    let mut mappings = HashMap::new();
    mappings.insert("user_id".to_string(), "user".to_string());
    pipeline.add_stage(Box::new(FieldRemap::new(mappings)));

    // Derived fields
    let derivations = HashMap::from([(
        "processed_at".to_string(),
        Box::new(processed_at) as Box<dyn Fn(&Event) -> serde_json::Value>,
    )]);
    pipeline.add_stage(Box::new(DerivedFields::new(derivations)));

    // Require fields
    pipeline.add_stage(Box::new(RequiredFields::new(vec![
        "timestamp".to_string(),
        "level".to_string(),
        "message".to_string(),
    ])));

    // Output
    pipeline.add_stage(Box::new(FileOutput::new(PathBuf::from(
        "samples/complex_output.ndjson",
    ))));

    // Deadletter
    let mut deadletter = Box::new(Deadletter::new(PathBuf::from(
        "samples/complex_deadletter.ndjson",
    )));

    // Process
    let mut input = InputSource::File(PathBuf::from("samples/messy.ndjson"));
    let mut deadletter_opt = Some(&mut *deadletter as &mut dyn Stage);
    input.process_input(&mut pipeline, &mut deadletter_opt)?;

    println!("Complex pipeline completed.");
    Ok(())
}
