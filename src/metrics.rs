use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum DropReason {
    Filtered,
}

impl fmt::Display for DropReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DropReason::Filtered => write!(f, "filtered"),
        }
    }
}

/// Metrics for observability: counters, latency summaries, drop reason codes.
/// No execution feedback loops. Bounded storage.
///
/// `stage_latencies` and `drop_reasons` use `BTreeMap` (not `HashMap`) so that
/// `to_json_logs`/`to_prometheus` iterate in a stable, key-sorted order —
/// `HashMap`'s randomized per-process hasher would otherwise make exported
/// metrics ordering vary run-to-run, breaking the crate's determinism guarantee.
#[derive(Debug)]
pub struct Metrics {
    pub(crate) events_processed: u64,
    pub(crate) events_dropped: u64,
    pub(crate) errors: u64,
    /// Records lost because the deadletter sink itself failed to write them
    /// (e.g. disk full, permissions). This is the one path whose entire job
    /// is "never lose an error record without a trace" — so a failure here
    /// is counted rather than only logged to stderr.
    pub(crate) deadletter_write_failures: u64,
    pub(crate) stage_latencies: BTreeMap<String, LatencyStats>, // bounded stats
    pub(crate) drop_reasons: BTreeMap<DropReason, u64>,         // bounded reasons
}

#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub(crate) count: u64,
    pub(crate) sum: f64,
    pub(crate) min: f64,
    pub(crate) max: f64,
}

impl LatencyStats {
    pub fn new() -> Self {
        LatencyStats {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    pub fn record(&mut self, duration: f64) {
        self.count += 1;
        self.sum += duration;
        if duration < self.min {
            self.min = duration;
        }
        if duration > self.max {
            self.max = duration;
        }
    }
}

impl Default for LatencyStats {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            events_processed: 0,
            events_dropped: 0,
            errors: 0,
            deadletter_write_failures: 0,
            stage_latencies: BTreeMap::new(),
            drop_reasons: BTreeMap::new(),
        }
    }

    pub fn increment_processed(&mut self) {
        self.events_processed += 1;
    }

    pub fn increment_dropped(&mut self, reason: DropReason) {
        self.events_dropped += 1;
        *self.drop_reasons.entry(reason).or_insert(0) += 1;
    }

    pub fn increment_errors(&mut self) {
        self.errors += 1;
    }

    /// Records that the deadletter sink failed to write an error record,
    /// which would otherwise be silently lost beyond an stderr line.
    pub fn increment_deadletter_write_failure(&mut self) {
        self.deadletter_write_failures += 1;
    }

    pub fn record_latency(&mut self, stage: &str, duration: f64) {
        self.stage_latencies
            .entry(stage.to_string())
            .or_default()
            .record(duration);
    }

    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();
        output.push_str("# HELP feedme_events_processed_total Total events processed\n");
        output.push_str(&format!(
            "feedme_events_processed_total {}\n",
            self.events_processed
        ));
        output.push_str("# HELP feedme_events_dropped_total Total events dropped\n");
        output.push_str(&format!(
            "feedme_events_dropped_total {}\n",
            self.events_dropped
        ));
        output.push_str("# HELP feedme_errors_total Total errors\n");
        output.push_str(&format!("feedme_errors_total {}\n", self.errors));
        output.push_str(
            "# HELP feedme_deadletter_write_failures_total Error records lost because the deadletter sink itself failed to write\n",
        );
        output.push_str(&format!(
            "feedme_deadletter_write_failures_total {}\n",
            self.deadletter_write_failures
        ));
        output.push_str("# HELP feedme_stage_latency_ms Stage latency in milliseconds\n");
        output.push_str("# TYPE feedme_stage_latency_ms gauge\n");
        for (stage, stats) in &self.stage_latencies {
            if stats.count > 0 {
                output.push_str(&format!(
                    "feedme_stage_latency_ms_sum{{stage=\"{}\"}} {}\n",
                    stage, stats.sum
                ));
                output.push_str(&format!(
                    "feedme_stage_latency_ms_count{{stage=\"{}\"}} {}\n",
                    stage, stats.count
                ));
                output.push_str(&format!(
                    "feedme_stage_latency_ms_min{{stage=\"{}\"}} {}\n",
                    stage, stats.min
                ));
                output.push_str(&format!(
                    "feedme_stage_latency_ms_max{{stage=\"{}\"}} {}\n",
                    stage, stats.max
                ));
            }
        }
        output.push_str("# HELP feedme_drop_reasons_total Drop reasons\n");
        output.push_str("# TYPE feedme_drop_reasons_total counter\n");
        for (reason, count) in &self.drop_reasons {
            output.push_str(&format!(
                "feedme_drop_reasons_total{{reason=\"{}\"}} {}\n",
                reason, count
            ));
        }
        output
    }

    pub fn to_json_logs(&self) -> Vec<String> {
        let mut logs = Vec::new();
        logs.push(
            serde_json::json!({
                "metric": "events_processed",
                "value": self.events_processed
            })
            .to_string(),
        );
        logs.push(
            serde_json::json!({
                "metric": "events_dropped",
                "value": self.events_dropped
            })
            .to_string(),
        );
        logs.push(
            serde_json::json!({
                "metric": "errors",
                "value": self.errors
            })
            .to_string(),
        );
        logs.push(
            serde_json::json!({
                "metric": "deadletter_write_failures",
                "value": self.deadletter_write_failures
            })
            .to_string(),
        );
        for (stage, stats) in &self.stage_latencies {
            logs.push(
                serde_json::json!({
                    "metric": "stage_latencies",
                    "stage": stage,
                    "count": stats.count,
                    "sum": stats.sum,
                    "min": stats.min,
                    "max": stats.max
                })
                .to_string(),
            );
        }
        for (reason, count) in &self.drop_reasons {
            logs.push(
                serde_json::json!({
                    "metric": "drop_reasons",
                    "reason": reason,
                    "count": count
                })
                .to_string(),
            );
        }
        logs
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
