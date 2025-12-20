use feedme::*;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Directory ingest (non-recursive) with stable ordering
    // Input: Directory of NDJSON files
    // Pipeline: (no transforms) -> Output combined to file

    let mut pipeline = Pipeline::new();

    // No transforms, just output
    pipeline.add_stage(Box::new(FileOutput::new(PathBuf::from("samples/combined.ndjson"))));

    // Create a directory with files for demo
    std::fs::create_dir_all("samples/logs")?;
    std::fs::write("samples/logs/file1.ndjson", r#"{"timestamp":"2023-10-01T10:00:00Z","level":"info","message":"File 1"}
{"timestamp":"2023-10-01T10:01:00Z","level":"warn","message":"File 1 warn"}"#)?;
    std::fs::write("samples/logs/file2.ndjson", r#"{"timestamp":"2023-10-01T10:02:00Z","level":"error","message":"File 2"}"#)?;

    // Process directory
    let mut input = InputSource::Directory(PathBuf::from("samples/logs"));
    let mut none = None;
    input.process_input(&mut pipeline, &mut none)?;

    // Export metrics
    println!("Metrics:");
    for log in pipeline.export_json_logs() {
        println!("{}", log);
    }

    Ok(())
}