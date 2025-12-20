use feedme::*;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: PII redaction + schema validation + deadletter
    // Input: NDJSON with PII and missing fields
    // Pipeline: Redact PII -> Require fields -> Output to file
    // Errors: Fail fast (for demo; in production, add deadletter handling)

    let mut pipeline = Pipeline::new();

    // PII Redaction: redact emails and SSNs
    let email_pattern = regex::Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")?;
    let ssn_pattern = regex::Regex::new(r"\b\d{3}-\d{2}-\d{4}\b")?;
    pipeline.add_stage(Box::new(PIIRedaction::new(vec![email_pattern, ssn_pattern])));

    // Require fields: timestamp, level, message
    pipeline.add_stage(Box::new(RequiredFields::new(vec![
        "timestamp".to_string(),
        "level".to_string(),
        "message".to_string(),
    ])));

    // Output to file
    pipeline.add_stage(Box::new(FileOutput::new(PathBuf::from("samples/processed.ndjson"))));

    // Deadletter for errors
    let mut deadletter = Box::new(Deadletter::new(PathBuf::from("samples/deadletter.ndjson")));

    // Process input file
    let mut input = InputSource::File(PathBuf::from("samples/messy.ndjson"));
    let mut deadletter_opt = Some(&mut *deadletter as &mut dyn Stage);
    input.process_input(&mut pipeline, &mut deadletter_opt)?;

    // Export metrics
    println!("Metrics:");
    for log in pipeline.export_json_logs() {
        println!("{}", log);
    }

    Ok(())
}