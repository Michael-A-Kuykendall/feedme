use feedme::*;
use std::path::PathBuf;

// Custom stage: Add env field
pub struct AddEnv;

impl Stage for AddEnv {
    fn execute(&mut self, mut event: Event) -> Result<Option<Event>, PipelineError> {
        if let serde_json::Value::Object(ref mut map) = event.data {
            map.insert("env".to_string(), serde_json::Value::String("prod".to_string()));
        }
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "AddEnv"
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Custom stage
    // Input: NDJSON
    // Pipeline: AddEnv -> Output to stdout

    let mut pipeline = Pipeline::new();

    // Custom stage
    pipeline.add_stage(Box::new(AddEnv));

    // Output to stdout
    pipeline.add_stage(Box::new(StdoutOutput::new()));

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