use feedme::*;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Metrics export demo
    // Input: File
    // Pipeline: Simple pass-through
    // Focus: Demonstrating metrics collection and export

    let mut pipeline = Pipeline::new();

    // No transforms, just output
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // Process
    let mut input = InputSource::File(PathBuf::from("samples/messy.ndjson"));
    let mut none = None;
    input.process_input(&mut pipeline, &mut none)?;

    // Export metrics in different formats
    println!("Prometheus format:");
    println!("{}", pipeline.export_prometheus());

    println!("\nJSON logs format:");
    for log in pipeline.export_json_logs() {
        println!("{}", log);
    }

    Ok(())
}