//! # FeedMe
//!
//! **FeedMe is a deterministic, linear, streaming ingest pipeline with mechanical guarantees around memory, ordering, and failure.**
//!
//! FeedMe provides a linear, deterministic processing model for Rust applications that need
//! reliable data ingestion. It emphasizes bounded resource usage, explicit error handling,
//! and comprehensive observability without affecting execution.
//!
//! ## Key Features
//!
//! - **Streaming, bounded memory**: Processes one event at a time; memory usage stays flat
//! - **Deterministic processing**: Same input + same config → same output
//! - **Structured errors**: Stage, code, and message for every failure
//! - **Observability**: Metrics exportable (Prometheus or JSON) without affecting execution
//! - **Extensible**: Add custom stages via a defined plugin contract
//!
//! ## Guarantees
//!
//! FeedMe provides these mechanical guarantees:
//!
//! - Events are processed strictly in input order
//! - Memory usage is bounded and input-size independent
//! - Stages cannot observe shared or mutated state
//! - Validation failures are not silently ignored: any Validator stage
//!   (RequiredFields, TypeChecking, ValueConstraints) rejects invalid events.
//!   This is an opt-in guarantee — it holds only when such a stage is present.
//!   Call `Pipeline::require_validation(true)` to enforce it fail-closed: if no
//!   Validator stage is present, `process_event` returns an error instead of
//!   silently passing invalid events through.
//! - Metrics collection cannot influence execution
//!
//! ## Example
//!
//! ```rust,no_run
//! use feedme::{
//!     Pipeline, FieldSelect, RequiredFields, StdoutOutput, Deadletter,
//!     PIIRedaction, Filter, InputSource, Stage
//! };
//! use std::path::PathBuf;
//!
//! fn main() -> anyhow::Result<()> {
//!     // Create pipeline: select fields → redact PII → require fields → filter → output
//!     let mut pipeline = Pipeline::new();
//!     pipeline.add_stage(Box::new(FieldSelect::new(vec![
//!         "timestamp".into(), "level".into(), "message".into(), "email".into()
//!     ])));
//!     let email_pattern = regex::Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")?;
//!     pipeline.add_stage(Box::new(PIIRedaction::new(vec![email_pattern])));
//!     pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".into()])));
//!     pipeline.add_stage(Box::new(Filter::new(Box::new(|event| {
//!         event.data.get("level").and_then(|v| v.as_str()) != Some("debug")
//!     }))));
//!     pipeline.add_stage(Box::new(StdoutOutput::new()));
//!
//!     // Deadletter for errors
//!     let mut deadletter = Deadletter::new(PathBuf::from("samples/errors.ndjson"));
//!
//!     // Process input file
//!     let mut input = InputSource::File(PathBuf::from("samples/input.ndjson"));
//!     input.process_input(&mut pipeline, &mut Some(&mut deadletter))?;
//!
//!     // Export final metrics
//!     println!("Pipeline complete. Metrics:");
//!     for metric in pipeline.export_json_logs() {
//!         println!("{}", serde_json::to_string(&metric)?);
//!     }
//!
//!     Ok(())
//! }
//! ```

use sha2::{Digest, Sha256};

/// Pipeline serialization for deterministic replay and A/B comparison.
/// Only serializes pipeline definition, not runtime state.
pub mod replay_spec {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    /// Stable identifier for a stage type (not tied to Rust type names)
    pub type StageId = String;

    /// Version of a stage implementation
    pub type StageVersion = String;

    /// Configuration payload for a stage (must be serializable)
    pub type StageConfig = serde_json::Value;

    /// Factory function to create a stage from config
    pub type StageFactory = Box<dyn Fn(StageConfig) -> Result<Box<dyn Stage>, PipelineError>>;

    /// Specification for a single stage in replay
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct StageSpec {
        /// Stable stage identifier
        pub stage_id: StageId,
        /// Stage implementation version
        pub stage_version: StageVersion,
        /// Stage configuration (must not contain secrets)
        pub config: StageConfig,
    }

    /// Global pipeline settings that affect mechanics
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct PipelineSettings {
        /// Deadletter mode enabled
        pub deadletter_enabled: bool,
        /// Metrics collection enabled
        pub metrics_enabled: bool,
        /// Spec version for this schema
        pub spec_version: String,
    }

    impl Default for PipelineSettings {
        fn default() -> Self {
            Self {
                deadletter_enabled: true,
                metrics_enabled: true,
                spec_version: "1.0".to_string(),
            }
        }
    }

    /// Complete pipeline specification for replay and comparison
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct PipelineReplaySpec {
        /// FeedMe crate version
        pub feedme_version: String,
        /// Spec schema version
        pub spec_version: String,
        /// Ordered list of stage specifications
        pub stages: Vec<StageSpec>,
        /// Pipeline settings
        pub settings: PipelineSettings,
        /// Deterministic hash of the canonical spec
        pub spec_hash: String,
    }

    /// Registry for stage factories (opt-in replay support)
    pub struct StageRegistry {
        factories: HashMap<StageId, StageFactory>,
    }

    impl StageRegistry {
        pub fn new() -> Self {
            Self {
                factories: HashMap::new(),
            }
        }

        /// Register a stage factory for replay support
        pub fn register_stage(&mut self, stage_id: StageId, factory: StageFactory) {
            self.factories.insert(stage_id, factory);
        }

        /// Check if a stage is replayable
        pub fn is_replayable(&self, stage_id: &StageId) -> bool {
            self.factories.contains_key(stage_id)
        }

        /// Create a stage from spec
        pub fn create_stage(&self, spec: &StageSpec) -> Result<Box<dyn Stage>, PipelineError> {
            if let Some(factory) = self.factories.get(&spec.stage_id) {
                factory(spec.config.clone())
            } else {
                Err(PipelineError::System(SystemError {
                    stage: "ReplaySpec".to_string(),
                    code: SystemErrorCode::InvalidConfig,
                    message: format!("Stage '{}' is not registered for replay", spec.stage_id),
                }))
            }
        }
    }

    impl Default for StageRegistry {
        fn default() -> Self {
            Self::new()
        }
    }

    impl PipelineReplaySpec {
        /// Create a spec from a pipeline (requires all stages to support to_replay_spec via ReplayableStage).
        pub fn from_pipeline(
            pipeline: &Pipeline,
            _registry: &StageRegistry,
        ) -> Result<Self, PipelineError> {
            let stage_specs = pipeline.extract_replay_specs();
            if stage_specs.len() != pipeline.stages.len() {
                return Err(PipelineError::System(SystemError {
                    stage: "ReplaySpec".to_string(),
                    code: SystemErrorCode::InvalidConfig,
                    message: "Pipeline serialization not possible: not all stages implement ReplayableStage (to_replay_spec)".to_string(),
                }));
            }
            Self::from_stages(stage_specs)
        }

        /// Create a spec from stages directly (for testing)
        pub fn from_stages(stages: Vec<StageSpec>) -> Result<Self, PipelineError> {
            let spec = Self {
                feedme_version: env!("CARGO_PKG_VERSION").to_string(),
                spec_version: "1.0".to_string(),
                stages,
                settings: PipelineSettings::default(),
                spec_hash: String::new(),
            };

            // Compute deterministic hash
            let canonical_json = serde_json::to_string(&spec).map_err(|e| {
                PipelineError::System(SystemError {
                    stage: "ReplaySpec".to_string(),
                    code: SystemErrorCode::SerializationError,
                    message: format!("Failed to serialize spec: {}", e),
                })
            })?;

            let hash = Sha256::digest(canonical_json.as_bytes());
            let hash_hex = format!("{:x}", hash);

            Ok(Self {
                spec_hash: hash_hex,
                ..spec
            })
        }

        /// Deserialize and validate a pipeline from spec
        pub fn to_pipeline(&self, registry: &StageRegistry) -> Result<Pipeline, PipelineError> {
            // Verify hash first
            let mut spec_for_hash = self.clone();
            spec_for_hash.spec_hash = String::new();
            let canonical_json = serde_json::to_string(&spec_for_hash).map_err(|e| {
                PipelineError::System(SystemError {
                    stage: "ReplaySpec".to_string(),
                    code: SystemErrorCode::SerializationError,
                    message: format!("Failed to verify spec: {}", e),
                })
            })?;

            let computed_hash = format!("{:x}", Sha256::digest(canonical_json.as_bytes()));
            if computed_hash != self.spec_hash {
                return Err(PipelineError::System(SystemError {
                    stage: "ReplaySpec".to_string(),
                    code: SystemErrorCode::InvalidConfig,
                    message: "Spec hash verification failed - spec may be corrupted".to_string(),
                }));
            }

            let mut pipeline = Pipeline::new();

            for stage_spec in &self.stages {
                let stage = registry.create_stage(stage_spec)?;
                pipeline.add_stage(stage);
            }

            Ok(pipeline)
        }

        /// Compute diff between two specs for A/B comparison
        pub fn diff(&self, other: &Self) -> SpecDiff {
            let mut added_stages = Vec::new();
            let mut removed_stages = Vec::new();
            let mut modified_stages = Vec::new();

            // Simple diff: stages in self but absent in other → removed
            //             stages in other but absent in self → added
            for (i, stage) in self.stages.iter().enumerate() {
                if i >= other.stages.len() {
                    removed_stages.push(stage.clone());
                } else if stage != &other.stages[i] {
                    modified_stages.push((i, stage.clone(), other.stages[i].clone()));
                }
            }

            for i in self.stages.len()..other.stages.len() {
                added_stages.push(other.stages[i].clone());
            }

            SpecDiff {
                added_stages,
                removed_stages,
                modified_stages,
                settings_changed: self.settings != other.settings,
            }
        }
    }

    /// Difference between two pipeline specs
    #[derive(Debug, Clone)]
    pub struct SpecDiff {
        pub added_stages: Vec<StageSpec>,
        pub removed_stages: Vec<StageSpec>,
        pub modified_stages: Vec<(usize, StageSpec, StageSpec)>,
        pub settings_changed: bool,
    }

    /// Trait for stages that support replay serialization
    pub trait ReplayableStage {
        /// Get stable stage identifier
        fn stage_id() -> StageId
        where
            Self: Sized;

        /// Get stage version
        fn stage_version() -> StageVersion
        where
            Self: Sized;

        /// Convert stage to spec (config only, no runtime state)
        fn to_spec(&self) -> StageSpec;
    }
}

// The invariants module is gated so it is excluded from `--no-default-features`
// release builds; it stays active under `test` (matching its own `any(test, feature)`
// design) so contract/invariant tests still run without the feature enabled.
#[cfg(any(test, feature = "invariant-ppt"))]
pub mod invariants;

#[cfg(test)]
mod invariant_contracts;

pub mod replay;

// The fused rule engine is gated by the `fused` feature; the example that uses it
// declares `required-features = ["fused"]` so default/no-default builds stay clean.
#[cfg(feature = "fused")]
pub mod fused;

pub mod fault_injection;

pub mod audit;

#[cfg(feature = "invariant-ppt")]
pub(crate) const INVARIANT_PROCESSED_INCREMENTS_ONCE: &str =
    "processed increments exactly once per process_event";
#[cfg(feature = "invariant-ppt")]
pub(crate) const INVARIANT_ERRORS_INCREMENT_ON_ERROR: &str =
    "errors increment exactly once per error";
#[cfg(feature = "invariant-ppt")]
pub(crate) const INVARIANT_DROPPED_ONLY_FOR_NON_OUTPUT_NONE: &str =
    "dropped increments only when non-output stage returns None";
#[cfg(feature = "invariant-ppt")]
pub(crate) const INVARIANT_OUTPUT_NONE_NOT_DROPPED: &str =
    "output stage returning None does not count as dropped";
#[cfg(feature = "invariant-ppt")]
pub(crate) const INVARIANT_LATENCY_RECORDED_ON_SUCCESS: &str =
    "latency is recorded for each successful stage execution";

/// Type aliases for complex function types to reduce clippy warnings
pub type EventDerivationFn = Box<dyn Fn(&Event) -> serde_json::Value>;
pub type ValueConstraintFn = Box<dyn Fn(&serde_json::Value) -> bool>;
pub type StageFactoryFn = Box<dyn Fn() -> Box<dyn Stage>>;

// ---- Extracted modules (deficiency-overhaul / B2 modularization) ----
pub mod config;
pub mod error;
pub mod event;
pub mod input;
pub mod metrics;
pub mod pipeline;
pub mod stages;

pub use config::{common_redact_validate_pipeline, Config, PluginRegistry};
pub use error::*;
pub use event::Event;
pub use input::{InputSource, JSONArrayParser, NDJSONParser, Parser, SyslogParser};
pub use metrics::{DropReason, LatencyStats, Metrics};
pub use pipeline::{Pipeline, Stage};
pub use stages::{
    Deadletter, DerivedFields, FieldRemap, FieldSelect, FileOutput, Filter, PIIRedaction,
    RequiredFields, StdoutOutput, TypeChecking, ValueConstraints,
};

#[cfg(test)]
mod tests;
