use crate::error::PipelineError;
use crate::event::Event;
use crate::invariant_ppt;
use crate::metrics::{DropReason, Metrics};
use crate::replay_spec;
use crate::INVARIANT_DROPPED_ONLY_FOR_NON_OUTPUT_NONE;
use crate::INVARIANT_ERRORS_INCREMENT_ON_ERROR;
use crate::INVARIANT_LATENCY_RECORDED_ON_SUCCESS;
use crate::INVARIANT_OUTPUT_NONE_NOT_DROPPED;
use crate::INVARIANT_PROCESSED_INCREMENTS_ONCE;
use std::time::Instant;
/// Stage contract: ownership-based execution.
/// Takes `Event`, returns `Option<Event>`, with explicit drop semantics.
pub trait Stage {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError>;
    fn name(&self) -> &str;
    fn is_output(&self) -> bool {
        false
    }
    /// Optional support for structural replay specs.
    /// Returning Some allows the stage to participate in PipelineReplaySpec roundtrips.
    fn to_replay_spec(&self) -> Option<replay_spec::StageSpec> {
        None
    }
}

/// Pipeline: linear, deterministic execution of stages.
/// No distributed coordination, constant memory streaming.
pub struct Pipeline {
    pub(crate) stages: Vec<Box<dyn Stage>>,
    metrics: Metrics,
}

impl Pipeline {
    pub fn new() -> Self {
        Pipeline {
            stages: Vec::new(),
            metrics: Metrics::new(),
        }
    }

    pub fn add_stage(&mut self, stage: Box<dyn Stage>) {
        self.stages.push(stage);
    }

    pub fn process_event(&mut self, event: Event) -> Result<Option<Event>, PipelineError> {
        let prev_processed = self.metrics.events_processed;
        let prev_errors = self.metrics.errors;
        let prev_dropped = self.metrics.events_dropped;

        self.metrics.increment_processed();
        invariant_ppt::assert_invariant(
            self.metrics.events_processed == prev_processed + 1,
            INVARIANT_PROCESSED_INCREMENTS_ONCE,
            Some("Pipeline::process_event"),
        );

        let mut current = Some(event);
        for stage in &mut self.stages {
            if let Some(evt) = current {
                let start = Instant::now();
                match stage.execute(evt) {
                    Ok(opt) => {
                        let duration = start.elapsed().as_secs_f64() * 1000.0;

                        let prev_stage_count = self
                            .metrics
                            .stage_latencies
                            .get(stage.name())
                            .map(|s| s.count)
                            .unwrap_or(0);
                        self.metrics.record_latency(stage.name(), duration);
                        let new_stage_count = self
                            .metrics
                            .stage_latencies
                            .get(stage.name())
                            .map(|s| s.count)
                            .unwrap_or(0);
                        invariant_ppt::assert_invariant(
                            new_stage_count == prev_stage_count + 1,
                            INVARIANT_LATENCY_RECORDED_ON_SUCCESS,
                            Some("Pipeline::process_event"),
                        );

                        current = opt;
                        if current.is_none() {
                            if !stage.is_output() {
                                let dropped_before = self.metrics.events_dropped;
                                let reason_before = *self
                                    .metrics
                                    .drop_reasons
                                    .get(&DropReason::Filtered)
                                    .unwrap_or(&0);

                                self.metrics.increment_dropped(DropReason::Filtered);

                                let reason_after = *self
                                    .metrics
                                    .drop_reasons
                                    .get(&DropReason::Filtered)
                                    .unwrap_or(&0);

                                invariant_ppt::assert_invariant(
                                    self.metrics.events_dropped == dropped_before + 1
                                        && reason_after == reason_before + 1,
                                    INVARIANT_DROPPED_ONLY_FOR_NON_OUTPUT_NONE,
                                    Some("Pipeline::process_event"),
                                );
                            } else {
                                invariant_ppt::assert_invariant(
                                    self.metrics.events_dropped == prev_dropped,
                                    INVARIANT_OUTPUT_NONE_NOT_DROPPED,
                                    Some("Pipeline::process_event"),
                                );
                            }
                        }
                    }
                    Err(e) => {
                        self.metrics.increment_errors();
                        invariant_ppt::assert_invariant(
                            self.metrics.errors == prev_errors + 1,
                            INVARIANT_ERRORS_INCREMENT_ON_ERROR,
                            Some("Pipeline::process_event"),
                        );
                        return Err(e);
                    }
                }
            } else {
                break;
            }
        }

        // Sanity: these counters should never run backward.
        invariant_ppt::assert_invariant(
            self.metrics.events_processed >= prev_processed
                && self.metrics.errors >= prev_errors
                && self.metrics.events_dropped >= prev_dropped,
            "metrics counters are monotonic",
            Some("Pipeline::process_event"),
        );
        Ok(current)
    }

    pub fn export_prometheus(&self) -> String {
        self.metrics.to_prometheus()
    }

    pub fn export_json_logs(&self) -> Vec<String> {
        self.metrics.to_json_logs()
    }

    /// Returns the number of stages in this pipeline.
    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }

    /// Returns the name of each stage in pipeline order.
    pub fn stage_names(&self) -> Vec<&str> {
        self.stages.iter().map(|s| s.name()).collect()
    }

    /// Internal: extract replay specs from stages that support ReplayableStage.
    /// Used by replay_spec::PipelineReplaySpec::from_pipeline.
    pub(crate) fn extract_replay_specs(&self) -> Vec<replay_spec::StageSpec> {
        self.stages
            .iter()
            .filter_map(|s| s.to_replay_spec())
            .collect()
    }

    /// Returns the total number of events processed.
    pub fn events_processed(&self) -> u64 {
        self.metrics.events_processed
    }

    /// Returns the total number of events dropped (filtered out) across all stages.
    pub fn events_dropped(&self) -> u64 {
        self.metrics.events_dropped
    }

    /// Returns the total number of errors encountered.
    pub fn error_count(&self) -> u64 {
        self.metrics.errors
    }

    /// Returns the average latency across all stages in milliseconds.
    ///
    /// This is a **true weighted mean**: total processing time divided by total
    /// event-stage executions. A stage that runs 10,000 events contributes
    /// proportionally more than one that runs 10.
    pub fn avg_latency_ms(&self) -> f64 {
        let total_count: u64 = self.metrics.stage_latencies.values().map(|s| s.count).sum();
        if total_count == 0 {
            return 0.0;
        }
        let total_sum: f64 = self.metrics.stage_latencies.values().map(|s| s.sum).sum();
        total_sum / total_count as f64
    }

    /// Returns whether the last stage in the pipeline is an output stage.
    pub fn has_output_stage(&self) -> bool {
        self.stages.last().map(|s| s.is_output()).unwrap_or(false)
    }

    /// Returns the error rate (errors / events_processed), or 0.0 if nothing processed.
    pub fn error_rate(&self) -> f64 {
        if self.metrics.events_processed == 0 {
            return 0.0;
        }
        self.metrics.errors as f64 / self.metrics.events_processed as f64
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}
