use feedme::*;

// Custom stage for plugin demo
pub struct UppercaseMessage;

impl Stage for UppercaseMessage {
    fn execute(&mut self, mut event: Event) -> Result<Option<Event>, PipelineError> {
        if let serde_json::Value::Object(ref mut map) = event.data {
            if let Some(serde_json::Value::String(msg)) = map.get_mut("message") {
                *msg = msg.to_uppercase();
            }
        }
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "UppercaseMessage"
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Plugin usage
    // Demonstrates registering and using custom stages

    let mut registry = PluginRegistry::new();

    // Register custom stage
    registry.register("uppercase".to_string(), Box::new(|| Box::new(UppercaseMessage)));

    // Build pipeline using plugin
    let mut pipeline = Pipeline::new();
    if let Some(stage) = registry.get_stage("uppercase") {
        pipeline.add_stage(stage);
    }
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // Process
    let mut input = InputSource::File(std::path::PathBuf::from("samples/messy.ndjson"));
    let mut none = None;
    input.process_input(&mut pipeline, &mut none)?;

    Ok(())
}