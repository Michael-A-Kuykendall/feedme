/// Example 20 — File Output
///
/// Write processed events to files instead of stdout.
/// 
/// - FileOutput: Write to files with buffered I/O for efficiency
use feedme::*;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create temp file for output
    let output_path = std::env::temp_dir().join("feedme_output.ndjson");

    // ── Pipeline that writes to file ────────────────────────────────────────
    let mut pipeline = Pipeline::new();
    
    // Filter out debug level
    pipeline.add_stage(Box::new(Filter::new(Box::new(|ev| {
        ev.data.get("level").and_then(|l| l.as_str()) != Some("debug")
    }))));

    // Add a computed field showing processing time
    pipeline.add_stage(Box::new(DerivedFields::new({
        let mut d = std::collections::HashMap::new();
        d.insert(
            "processed_at".to_string(),
            Box::new(|_ev: &feedme::Event| {
                serde_json::json!("2026-06-13T10:00:00Z")
            }) as feedme::EventDerivationFn,
        );
        d
    })));

    // Write to file
    pipeline.add_stage(Box::new(FileOutput::new(output_path.clone())));

    // ── Process input file ───────────────────────────────────────────────────
    println!("=== File Output Demo ===\n");

    let mut input = InputSource::File(PathBuf::from("samples/input.ndjson"));
    input.process_input(&mut pipeline, &mut None)?;

    println!("Processed {} events", pipeline.events_processed());
    println!("Dropped {} events (debug level filtered)", pipeline.events_dropped());

    // Show what was written to the file
    println!("\n--- Output file contents ---");
    let content = std::fs::read_to_string(&output_path)?;
    for line in content.lines() {
        println!("{}", line);
    }

    // Cleanup
    let _ = std::fs::remove_file(&output_path);

    println!("\n--- Capabilities ---");
    println!("FileOutput: Write processed events to any file path");
    println!("Buffered: Events are buffered for efficient batch I/O");

    Ok(())
}