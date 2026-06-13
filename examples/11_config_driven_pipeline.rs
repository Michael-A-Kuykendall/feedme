use feedme::replay_spec::StageRegistry;
use feedme::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Config-driven pipeline (now functional via unified replay_spec + registry)
    let yaml = r#"
version: 1
stages:
  - stage_id: "field_select"
    stage_version: "1.0"
    config:
      fields: ["level", "message"]
  - stage_id: "stdout_output"
    stage_version: "1.0"
    config: {}
"#;

    let config = Config::from_yaml(yaml)?;
    println!("Config loaded successfully.");

    let mut registry = StageRegistry::new();
    registry.register_stage(
        "field_select".to_string(),
        Box::new(|c| {
            let fields: Vec<String> = serde_json::from_value(c["fields"].clone())?;
            Ok(Box::new(FieldSelect::new(fields)))
        }),
    );
    registry.register_stage(
        "stdout_output".to_string(),
        Box::new(|_c| Ok(Box::new(StdoutOutput::new()))),
    );

    let mut pipeline = config.build_pipeline(&registry)?;

    // Process
    let mut input = InputSource::File(std::path::PathBuf::from("samples/messy.ndjson"));
    let mut none = None;
    input.process_input(&mut pipeline, &mut none)?;

    Ok(())
}
