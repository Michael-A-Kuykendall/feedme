use feedme::*;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Field projection (shrink events)
    // Input: NDJSON
    // Pipeline: FieldSelect -> Output to file

    let mut pipeline = Pipeline::new();

    // Select fields: timestamp, level, message, user_id
    pipeline.add_stage(Box::new(FieldSelect::new(vec![
        "timestamp".to_string(),
        "level".to_string(),
        "message".to_string(),
        "user_id".to_string(),
    ])));

    // Output to file
    pipeline.add_stage(Box::new(FileOutput::new(PathBuf::from(
        "samples/projected.ndjson",
    ))));

    // Process input file
    let mut input = InputSource::File(PathBuf::from("samples/messy.ndjson"));
    let mut none = None;
    input.process_input(&mut pipeline, &mut none)?;

    // Export metrics
    println!("Metrics:");
    for log in pipeline.export_json_logs() {
        println!("{}", log);
    }

    Ok(())
}
