use feedme::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Streaming from stdin
    // Input: Stdin (pipe data in)
    // Pipeline: PIIRedaction -> StdoutOutput
    // Usage: echo '{"message": "User email@example.com logged in"}' | cargo run --example 08_stdin_streaming

    let mut pipeline = Pipeline::new();

    // Redact PII
    let email_pattern = regex::Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")?;
    pipeline.add_stage(Box::new(PIIRedaction::new(vec![email_pattern])));

    // Output to stdout
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // Process stdin
    let mut input = InputSource::Stdin;
    let mut none = None;
    input.process_input(&mut pipeline, &mut none)?;

    Ok(())
}