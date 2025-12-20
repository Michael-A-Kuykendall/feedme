use feedme::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Config-driven pipeline
    // Demonstrates loading YAML config (though minimal here)

    // Sample config
    let yaml = r#"
version: 1
"#;

    let _config = Config::from_yaml(yaml)?;
    println!("Config loaded successfully.");

    // Build pipeline based on config (placeholder)
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".to_string(), "message".to_string()])));
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // Process
    let mut input = InputSource::File(std::path::PathBuf::from("samples/messy.ndjson"));
    let mut none = None;
    input.process_input(&mut pipeline, &mut none)?;

    Ok(())
}