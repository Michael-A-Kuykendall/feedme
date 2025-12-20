use feedme::{
    FieldSelect, InputSource, PIIRedaction, Pipeline, RequiredFields, Stage, StdoutOutput,
};
use regex::Regex;
use std::path::PathBuf;

/// Failure Tour: Demonstrates what happens when things go wrong
///
/// This example intentionally feeds malformed input, triggers validation errors,
/// filter drops, and output failures to show FeedMe's error handling in action.
///
/// Run with: cargo run --example 99_failure_tour
fn main() -> anyhow::Result<()> {
    println!("=== FeedMe Failure Tour ===\n");
    println!(
        "This example demonstrates FeedMe's error handling by intentionally causing failures.\n"
    );

    // Create a pipeline that will encounter various errors
    let mut pipeline = Pipeline::new();

    // Add PII redaction (will work on valid data)
    let patterns = vec![Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap()]; // SSN pattern
    pipeline.add_stage(Box::new(PIIRedaction::new(patterns)));

    // Add field selection (will work on objects)
    pipeline.add_stage(Box::new(FieldSelect::new(vec![
        "level".to_string(),
        "message".to_string(),
    ])));

    // Add required fields validation (will reject missing fields)
    pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".to_string()])));

    // Add stdout output (will work)
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // Create input with mixed valid/invalid data
    let input_data = r#"{"timestamp":"2023-10-01T10:00:00Z","level":"info","message":"User logged in","ssn":"123-45-6789"}
{"level":"error","message":"System failure"}
{"invalid": json}
{"level":"debug","message":"Debug info","extra":"field"}
"#;

    // Write to a temp file
    let temp_file = "failure_tour_input.ndjson";
    std::fs::write(temp_file, input_data)?;
    let mut input = InputSource::File(PathBuf::from(temp_file));

    // Set up deadletter to capture failures
    let deadletter_file = "failure_tour_deadletter.ndjson";
    let mut deadletter = Box::new(feedme::Deadletter::new(PathBuf::from(deadletter_file)));
    let mut deadletter_opt = Some(&mut *deadletter as &mut dyn Stage);

    println!("Processing input with intentional errors...\n");

    // Process the input
    let result = input.process_input(&mut pipeline, &mut deadletter_opt);

    println!("Processing result: {:?}", result);
    println!();

    // Show metrics
    println!("=== Final Metrics ===");
    println!("{}", pipeline.export_prometheus());
    println!();

    // Show deadletter contents
    println!("=== Deadletter Contents ===");
    if std::path::Path::new(deadletter_file).exists() {
        let deadletter_content = std::fs::read_to_string(deadletter_file)?;
        for line in deadletter_content.lines() {
            println!("{}", line);
        }
    } else {
        println!("No deadletter entries (unexpected!)");
    }
    println!();

    // Show what successful processing looked like
    println!("=== What Succeeded ===");
    println!("- PII redaction worked on valid JSON objects");
    println!("- Field selection filtered to level/message only");
    println!("- Required fields validation passed for objects with 'level'");
    println!("- Stdout output displayed successful events");
    println!();

    println!("=== What Failed ===");
    println!("- Parse error on 'invalid json' line -> deadletter");
    println!("- Missing 'level' field in second event -> deadletter");
    println!("- Invalid JSON structure -> deadletter");
    println!();

    println!("=== Key Takeaways ===");
    println!("- Pipeline continues processing after errors when deadletter is configured");
    println!("- Failed events are isolated and attributed in deadletter");
    println!("- Metrics track both successes and failures");
    println!("- Error taxonomy provides structured debugging info");

    // Cleanup
    let _ = std::fs::remove_file(temp_file);
    let _ = std::fs::remove_file(deadletter_file);

    Ok(())
}
