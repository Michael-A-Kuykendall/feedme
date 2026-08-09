//! Execution traces for determinism verification.
//!
//! Use `record_execution`/`replay_execution` for runtime checks.
//! Structural pipeline specs (serialization, hashing, A/B diff, config-driven
//! reconstruction) live in [`crate::replay_spec`] — see
//! [`crate::replay_spec::PipelineReplaySpec`].

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
