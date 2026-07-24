use crate::error::{PipelineError, SystemError, SystemErrorCode};
use crate::pipeline::{Pipeline, Stage};
use crate::replay_spec;
use crate::stages::{FieldSelect, PIIRedaction, RequiredFields};
use crate::StageFactoryFn;
use anyhow;
use serde_yaml;
use std::collections::HashMap;
/// Configuration: ensure pipeline behavior is fully declared and validated before execution.
/// YAML input, version required, schema validated, unknown field rejection, no runtime mutation.
/// Supports optional "stages" list using replay_spec style for built-in stages (ties to unified replay).
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    version: u32,
    /// Optional list of stage specs for config-driven pipeline construction.
    /// Each item: { "stage_id": "field_select", "config": { "fields": ["level"] } }
    /// Use with a StageRegistry that has factories registered for the ids.
    stages: Option<Vec<replay_spec::StageSpec>>,
}

impl Config {
    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn from_yaml(yaml: &str) -> anyhow::Result<Self> {
        let config: Config = serde_yaml::from_str(yaml)?;
        if config.version != 1 {
            return Err(anyhow::anyhow!("Unsupported version: {}", config.version));
        }
        Ok(config)
    }

    /// Build a Pipeline from the config's stages list using the given registry.
    /// Ties directly into the unified replay_spec machinery for "bells and whistles" config-driven use.
    pub fn build_pipeline(
        &self,
        registry: &replay_spec::StageRegistry,
    ) -> Result<Pipeline, PipelineError> {
        if self.version != 1 {
            return Err(PipelineError::System(SystemError {
                stage: "Config".to_string(),
                code: SystemErrorCode::InvalidConfig,
                message: "version must be 1".to_string(),
            }));
        }
        let mut pipeline = Pipeline::new();
        if let Some(stages) = &self.stages {
            for spec in stages {
                let stage = registry.create_stage(spec)?;
                pipeline.add_stage(stage);
            }
        }
        Ok(pipeline)
    }
}

/// Ergonomic preset for a common "bells and whistles" redact + select + validate pipeline.
/// Returns a fully replayable pipeline (all stages now implement ReplayableStage).
/// Fits the "preferred complete pipe" shape: one call brings PII + projection + required without user wiring.
pub fn common_redact_validate_pipeline(
    select_fields: Vec<String>,
    required_fields: Vec<String>,
    pii_patterns: Vec<regex::Regex>,
) -> Pipeline {
    let mut p = Pipeline::new();
    p.add_stage(Box::new(PIIRedaction::new(pii_patterns)));
    p.add_stage(Box::new(FieldSelect::new(select_fields)));
    p.add_stage(Box::new(RequiredFields::new(required_fields)));
    p
}

/// Plugins: enable user-defined stages with explicit registration and isolation.
/// No implicit discovery.
pub struct PluginRegistry {
    plugins: HashMap<String, StageFactoryFn>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        PluginRegistry {
            plugins: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: String, factory: StageFactoryFn) {
        self.plugins.insert(name, factory);
    }

    pub fn get_stage(&self, name: &str) -> Option<Box<dyn Stage>> {
        self.plugins.get(name).map(|f| f())
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}
