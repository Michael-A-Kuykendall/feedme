use feedme::*;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Directory ingest (non-recursive) with stable ordering
    // Input: Directory of NDJSON files
    // Pipeline: (no transforms) -> Output combined to file
    // Hardened for user-surface testing: temp dirs + assertions on processed + output content.

    let mut pipeline = Pipeline::new();

    // Use temp for hardening
    let out_path: PathBuf = std::env::temp_dir().join(format!("feedme_dir_combined_{}.ndjson", std::process::id()));
    let dir_path: PathBuf = std::env::temp_dir().join(format!("feedme_dir_logs_{}", std::process::id()));

    // No transforms, just output
    pipeline.add_stage(Box::new(FileOutput::new(out_path.clone())));

    // Create temp dir with files for demo (cleanup on exit)
    std::fs::create_dir_all(&dir_path)?;
    std::fs::write(
        dir_path.join("file1.ndjson"),
        r#"{"timestamp":"2023-10-01T10:00:00Z","level":"info","message":"File 1"}
{"timestamp":"2023-10-01T10:01:00Z","level":"warn","message":"File 1 warn"}"#,
    )?;
    std::fs::write(
        dir_path.join("file2.ndjson"),
        r#"{"timestamp":"2023-10-01T10:02:00Z","level":"error","message":"File 2"}"#,
    )?;

    // Process directory
    let mut input = InputSource::Directory(dir_path.clone());
    let mut none = None;
    input.process_input(&mut pipeline, &mut none)?;

    // Harden assertions
    assert!(pipeline.events_processed() == 3);
    let out_content = std::fs::read_to_string(&out_path)?;
    assert!(out_content.contains("File 1"));
    assert!(out_content.contains("File 2"));

    // Export metrics
    println!("Metrics:");
    for log in pipeline.export_json_logs() {
        println!("{}", log);
    }

    // Best-effort cleanup
    let _ = std::fs::remove_file(&out_path);
    let _ = std::fs::remove_dir_all(&dir_path);

    Ok(())
}
