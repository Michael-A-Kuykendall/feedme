use feedme::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Filter noisy logs, keep only warn/error
    // Input: NDJSON
    // Pipeline: Filter(level != "debug") -> Output to stdout

    let mut pipeline = Pipeline::new();

    // Filter: keep only warn and error
    pipeline.add_stage(Box::new(Filter::new(Box::new(|event: &Event| {
        event
            .get_string("level")
            .is_some_and(|level| level != "debug")
    }))));

    // Output to stdout
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // Process input file
    let mut input = InputSource::File(std::path::PathBuf::from("samples/messy.ndjson"));
    let mut none = None;
    input.process_input(&mut pipeline, &mut none)?;

    // Export metrics
    println!("Metrics:");
    for log in pipeline.export_json_logs() {
        println!("{}", log);
    }

    Ok(())
}
