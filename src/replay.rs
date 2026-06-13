//! Execution traces for determinism verification.
//!
//! Use `record_execution`/`replay_execution` for runtime checks.
//! Structural specs live in `replay_spec`.

use crate::{Event, Pipeline};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Recorded execution trace for replay testing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTrace {
    pub input_events: Vec<String>,             // Raw JSON strings
    pub expected_outputs: Vec<Option<String>>, // None for dropped, Some(json) for output
}

/// Records a pipeline execution for later replay.
pub fn record_execution<P: AsRef<Path>>(
    pipeline: &mut Pipeline,
    input_events: &[Event],
    output_path: P,
) -> anyhow::Result<()> {
    let mut trace = ExecutionTrace {
        input_events: input_events
            .iter()
            .map(|e| serde_json::to_string(&e.data).unwrap())
            .collect(),
        expected_outputs: Vec::new(),
    };

    for event in input_events {
        let result = pipeline.process_event(event.clone())?;
        let output = result.map(|e| serde_json::to_string(&e.data).unwrap());
        trace.expected_outputs.push(output);
    }

    let json = serde_json::to_string_pretty(&trace)?;
    std::fs::write(output_path, json)?;
    Ok(())
}

/// Replays a recorded execution to verify deterministic behavior.
pub fn replay_execution<P: AsRef<Path>>(
    pipeline: &mut Pipeline,
    trace_path: P,
) -> anyhow::Result<()> {
    let trace_json = std::fs::read_to_string(trace_path)?;
    let trace: ExecutionTrace = serde_json::from_str(&trace_json)?;

    for (input_json, expected_output) in trace.input_events.iter().zip(&trace.expected_outputs) {
        let event = Event::from_raw_input(input_json)?;
        let actual_output = pipeline.process_event(event)?;
        let actual_output_json = actual_output.map(|e| serde_json::to_string(&e.data).unwrap());

        if actual_output_json != *expected_output {
            return Err(anyhow!(
                "replay mismatch: input={}, expected={:?}, actual={:?}",
                input_json,
                expected_output,
                actual_output_json
            ));
        }
    }

    Ok(())
}

// Replay Manager (legacy thin structural layer; prefer replay_spec for new code)

/// A minimal specification for a single stage in a replay spec.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayStageSpec {
    pub name: String,
    pub stage_type: String,
    pub config: serde_json::Value,
}

/// Serialisable pipeline specification for A/B comparison and drift detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineReplaySpec {
    pub name: String,
    pub stages: Vec<ReplayStageSpec>,
    pub config: serde_json::Value,
    pub metadata: std::collections::HashMap<String, String>,
}

impl PipelineReplaySpec {
    /// Build a spec from a list of stage specs.
    pub fn from_stages(stages: Vec<ReplayStageSpec>) -> Self {
        Self {
            name: String::new(),
            stages,
            config: serde_json::Value::Null,
            metadata: std::collections::HashMap::new(),
        }
    }
}

/// Detailed diff between two pipeline specs.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SpecDiff {
    pub added_stages: Vec<String>,
    pub removed_stages: Vec<String>,
    pub modified_stages: Vec<String>,
    pub settings_changed: bool,
}

/// Comparison result between two pipeline specifications.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpecComparison {
    pub added_stages: usize,
    pub removed_stages: usize,
    pub modified_stages: usize,
    pub settings_changed: bool,
    pub diff: SpecDiff,
}

/// Comprehensive replay report comparing two recorded specifications.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayReport {
    pub baseline_name: String,
    pub current_name: String,
    pub comparison: SpecComparison,
    pub baseline_spec: PipelineReplaySpec,
    pub current_spec: PipelineReplaySpec,
    pub generated_at: std::time::SystemTime,
}

/// Structural fingerprint of a pipeline spec.
///
/// # Note
///
/// This is a **structural-only** check: it computes a deterministic hash of the
/// spec configuration. It does **not** run the pipeline or compare execution
/// outputs. For runtime determinism verification use [`record_execution`] +
/// [`replay_execution`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpecFingerprintReport {
    /// Always `true` — a spec can always be structurally fingerprinted.
    /// This field indicates hash computation succeeded, not that outputs were
    /// verified.
    pub spec_hash_stable: bool,
    pub total_events: usize,
    pub spec_hash: String,
    pub test_timestamp: std::time::SystemTime,
}

/// Manages pipeline specification recording and A/B comparison.
#[derive(Default)]
pub struct ReplayManager {
    recorded_specs: std::collections::HashMap<String, PipelineReplaySpec>,
}

impl ReplayManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serialise a live pipeline to a [`PipelineReplaySpec`].
    pub fn serialize_pipeline(&self, pipeline: &Pipeline, name: &str) -> PipelineReplaySpec {
        let stages: Vec<ReplayStageSpec> = pipeline
            .stage_names()
            .into_iter()
            .map(|stage_name| ReplayStageSpec {
                name: stage_name.to_string(),
                stage_type: stage_name.to_string(),
                config: serde_json::Value::Null,
            })
            .collect();

        let mut spec = PipelineReplaySpec::from_stages(stages);
        spec.name = name.to_string();
        spec.metadata.insert(
            "stage_count".to_string(),
            pipeline.stage_count().to_string(),
        );
        spec
    }

    /// Record a pipeline specification for historical comparison.
    pub fn record_spec(&mut self, name: impl Into<String>, spec: PipelineReplaySpec) {
        self.recorded_specs.insert(name.into(), spec);
    }

    /// Compare two pipeline specifications and report differences.
    pub fn compare_specs(
        &self,
        spec1: &PipelineReplaySpec,
        spec2: &PipelineReplaySpec,
    ) -> SpecComparison {
        diff_specs(spec1, spec2)
    }

    /// Generate a replay report comparing two recorded specifications.
    pub fn generate_replay_report(
        &self,
        baseline_name: &str,
        current_name: &str,
    ) -> Result<ReplayReport, String> {
        let baseline = self
            .recorded_specs
            .get(baseline_name)
            .ok_or_else(|| format!("Baseline spec '{}' not found", baseline_name))?;

        let current = self
            .recorded_specs
            .get(current_name)
            .ok_or_else(|| format!("Current spec '{}' not found", current_name))?;

        let comparison = diff_specs(baseline, current);

        Ok(ReplayReport {
            baseline_name: baseline_name.to_string(),
            current_name: current_name.to_string(),
            comparison,
            baseline_spec: baseline.clone(),
            current_spec: current.clone(),
            generated_at: std::time::SystemTime::now(),
        })
    }

    /// Compute a structural fingerprint for the given spec.
    ///
    /// Returns a [`SpecFingerprintReport`] containing a deterministic hash of
    /// the spec structure. Identical specs always produce the same hash.
    ///
    /// # Note
    ///
    /// This does **not** run the pipeline or compare execution outputs. For
    /// runtime determinism verification, record a trace with
    /// [`record_execution`] and verify it with [`replay_execution`].
    pub fn compute_spec_fingerprint(
        &self,
        spec: &PipelineReplaySpec,
        test_events: &[crate::Event],
    ) -> SpecFingerprintReport {
        SpecFingerprintReport {
            spec_hash_stable: true,
            total_events: test_events.len(),
            spec_hash: compute_spec_hash(spec),
            test_timestamp: std::time::SystemTime::now(),
        }
    }
}

// Private helpers

fn diff_specs(a: &PipelineReplaySpec, b: &PipelineReplaySpec) -> SpecComparison {
    use std::collections::HashMap;

    let a_stages: HashMap<&str, &ReplayStageSpec> =
        a.stages.iter().map(|s| (s.name.as_str(), s)).collect();
    let b_stages: HashMap<&str, &ReplayStageSpec> =
        b.stages.iter().map(|s| (s.name.as_str(), s)).collect();

    let added: Vec<String> = b_stages
        .keys()
        .filter(|name| !a_stages.contains_key(*name))
        .map(|s| s.to_string())
        .collect();

    let removed: Vec<String> = a_stages
        .keys()
        .filter(|name| !b_stages.contains_key(*name))
        .map(|s| s.to_string())
        .collect();

    let modified: Vec<String> = a_stages
        .iter()
        .filter_map(|(name, a_spec)| {
            b_stages.get(name).and_then(|b_spec| {
                if a_spec.config != b_spec.config || a_spec.stage_type != b_spec.stage_type {
                    Some(name.to_string())
                } else {
                    None
                }
            })
        })
        .collect();

    let settings_changed = a.name != b.name || a.config != b.config;

    let diff = SpecDiff {
        added_stages: added.clone(),
        removed_stages: removed.clone(),
        modified_stages: modified.clone(),
        settings_changed,
    };

    SpecComparison {
        added_stages: added.len(),
        removed_stages: removed.len(),
        modified_stages: modified.len(),
        settings_changed,
        diff,
    }
}

fn compute_spec_hash(spec: &PipelineReplaySpec) -> String {
    let stage_names: Vec<&str> = spec.stages.iter().map(|s| s.name.as_str()).collect();
    format!("spec-{}-{}", spec.stages.len(), stage_names.join("|"))
}

// ── Replay Manager Tests ──────────────────────────────────────────────────────

#[cfg(test)]
mod replay_manager_tests {
    use super::*;

    #[test]
    fn test_replay_manager_creation() {
        let manager = ReplayManager::new();
        assert!(manager.recorded_specs.is_empty());
    }

    #[test]
    fn test_serialize_pipeline() {
        let manager = ReplayManager::new();
        let pipeline = Pipeline::new();
        let spec = manager.serialize_pipeline(&pipeline, "test");
        assert_eq!(spec.name, "test");
        assert!(spec.stages.is_empty());
    }

    #[test]
    fn test_compare_specs_empty() {
        let manager = ReplayManager::new();
        let a = PipelineReplaySpec::from_stages(vec![]);
        let b = PipelineReplaySpec::from_stages(vec![]);
        let cmp = manager.compare_specs(&a, &b);
        assert_eq!(cmp.added_stages, 0);
        assert_eq!(cmp.removed_stages, 0);
        assert_eq!(cmp.modified_stages, 0);
    }

    #[test]
    fn test_diff_detects_added_stage() {
        let a = PipelineReplaySpec::from_stages(vec![]);
        let b = PipelineReplaySpec::from_stages(vec![ReplayStageSpec {
            name: "filter".to_string(),
            stage_type: "Filter".to_string(),
            config: serde_json::Value::Null,
        }]);
        let cmp = diff_specs(&a, &b);
        assert_eq!(cmp.added_stages, 1);
        assert_eq!(cmp.removed_stages, 0);
    }

    #[test]
    fn test_generate_replay_report_missing_spec() {
        let manager = ReplayManager::new();
        assert!(manager
            .generate_replay_report("baseline", "current")
            .is_err());
    }

    #[test]
    fn test_generate_replay_report_ok() {
        let mut manager = ReplayManager::new();
        manager.record_spec("v1", PipelineReplaySpec::from_stages(vec![]));
        manager.record_spec(
            "v2",
            PipelineReplaySpec::from_stages(vec![ReplayStageSpec {
                name: "x".to_string(),
                stage_type: "X".to_string(),
                config: serde_json::Value::Null,
            }]),
        );
        let report = manager.generate_replay_report("v1", "v2").unwrap();
        assert_eq!(report.comparison.added_stages, 1);
    }
}
