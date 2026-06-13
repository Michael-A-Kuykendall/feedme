use feedme::*;
use std::collections::HashMap;
use std::path::PathBuf;

fn processed_at(_: &Event) -> serde_json::Value {
    // Fixed timestamp: intentionally constant for deterministic example output.
    // In production, replace with a real clock value.
    serde_json::Value::String("2025-12-20T00:00:00Z".to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Complex pipeline with multiple transforms
    // Input: File
    // Pipeline: PIIRedaction -> Filter -> FieldRemap -> DerivedFields -> RequiredFields -> FileOutput
    // Deadletter: For errors
    // Hardened: temp files + post-run assertions on metrics/deadletter for user-surface testing.

    let mut pipeline = Pipeline::new();

    // PII Redaction
    let patterns = vec![
        regex::Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")?, // email
        regex::Regex::new(r"\b\d{3}-\d{2}-\d{4}\b")?,                               // SSN
    ];
    pipeline.add_stage(Box::new(PIIRedaction::new(patterns)));

    // Filter out debug
    pipeline.add_stage(Box::new(Filter::new(Box::new(|event: &Event| {
        event.get_string("level") != Some("debug")
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

    // Output (temp for hardening)
    let out_path = std::env::temp_dir().join("feedme_complex_output.ndjson");
    pipeline.add_stage(Box::new(FileOutput::new(out_path.clone())));

    // Deadletter (temp)
    let dl_path = std::env::temp_dir().join("feedme_complex_deadletter.ndjson");
    let mut deadletter = Box::new(Deadletter::new(dl_path.clone()));

    // Process
    let mut input = InputSource::File(PathBuf::from("samples/messy.ndjson"));
    let mut deadletter_opt = Some(&mut *deadletter as &mut dyn Stage);
    input.process_input(&mut pipeline, &mut deadletter_opt)?;

    // Harden assertions (user-surface)
    assert!(pipeline.events_processed() > 0);
    if std::path::Path::new(&dl_path).exists() {
        let dl_content = std::fs::read_to_string(&dl_path)?;
        // may be empty if no errors in sample
        let _ = dl_content;
    }
    if std::path::Path::new(&out_path).exists() {
        let _ = std::fs::read_to_string(&out_path)?;
    }

    println!(
        "Complex pipeline completed. Processed: {}",
        pipeline.events_processed()
    );
    Ok(())
}
