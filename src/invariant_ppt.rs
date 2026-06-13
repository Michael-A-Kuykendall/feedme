#[cfg(any(test, feature = "invariant-ppt"))]
use std::collections::BTreeSet;

#[cfg(any(test, feature = "invariant-ppt"))]
use std::cell::RefCell;

#[cfg(any(test, feature = "invariant-ppt"))]
#[derive(Debug, Clone)]
struct InvariantRecord {
    message: &'static str,
    context: Option<&'static str>,
}

#[cfg(any(test, feature = "invariant-ppt"))]
thread_local! {
    static INVARIANT_LOG: RefCell<Vec<InvariantRecord>> = const { RefCell::new(Vec::new()) };
}

/// Asserts an invariant (a semantic law) and records that it was checked.
///
/// Behavior:
/// - In `test` builds or when the `invariant-ppt` feature is enabled: records checks and panics on violation.
/// - Otherwise: uses `debug_assert!` and does not record (zero overhead in release).
#[inline]
pub fn assert_invariant(condition: bool, message: &'static str, context: Option<&'static str>) {
    #[cfg(any(test, feature = "invariant-ppt"))]
    {
        INVARIANT_LOG.with(|log| {
            log.borrow_mut().push(InvariantRecord { message, context });
        });

        if !condition {
            panic!(
                "Invariant violated: {}{}",
                message,
                context
                    .map(|c| format!(" (context: {})", c))
                    .unwrap_or_default()
            );
        }
    }

    #[cfg(not(any(test, feature = "invariant-ppt")))]
    {
        let _ = context;
        debug_assert!(condition, "Invariant violated: {}", message);
    }
}

/// Clears the invariant log (useful to isolate contract tests).
#[cfg(any(test, feature = "invariant-ppt"))]
pub fn clear_invariant_log() {
    INVARIANT_LOG.with(|log| {
        log.borrow_mut().clear();
    });
}

/// Returns all distinct invariant messages that were exercised.
#[cfg(any(test, feature = "invariant-ppt"))]
pub fn exercised_invariants() -> BTreeSet<&'static str> {
    INVARIANT_LOG.with(|log| {
        log.borrow()
            .iter()
            .map(|r| {
                let _ = r.context;
                r.message
            })
            .collect()
    })
}

/// Contract test: returns Err if any expected invariants were not exercised.
#[cfg(any(test, feature = "invariant-ppt"))]
pub fn contract_test(
    contract_name: &str,
    expected_invariants: &[&'static str],
) -> Result<(), String> {
    let exercised = exercised_invariants();

    let missing: Vec<&'static str> = expected_invariants
        .iter()
        .copied()
        .filter(|inv| !exercised.contains(inv))
        .collect();

    if !missing.is_empty() {
        let mut present: Vec<&'static str> = exercised.into_iter().collect();
        present.sort();
        return Err(format!(
            "Contract '{}' missing invariants: {:?}. Present: {:?}",
            contract_name, missing, present
        ));
    }

    Ok(())
}

// ── Pipeline composition analysis ─────────────────────────────────────────────

/// Analyses the structure of a pipeline and returns a list of diagnostic
/// strings. An empty return means no issues were found.
#[cfg(any(test, feature = "invariant-ppt"))]
pub fn analyze_pipeline_composition(pipeline: &crate::Pipeline) -> Vec<String> {
    let mut issues = Vec::new();

    if pipeline.stage_count() == 0 {
        issues.push("pipeline has no stages".to_string());
        return issues;
    }

    if !pipeline.has_output_stage() {
        issues.push("pipeline has no output stages".to_string());
    }

    issues
}

// ── Pipeline health check ──────────────────────────────────────────────────────

/// Summary of a pipeline's runtime health.
#[cfg(any(test, feature = "invariant-ppt"))]
#[derive(Debug, Clone)]
pub struct PipelineHealthStatus {
    pub is_healthy: bool,
    pub stage_count: usize,
    pub total_processed: u64,
    pub error_rate: f64,
    pub issues: Vec<String>,
}

/// Returns a snapshot of the pipeline's current health.
#[cfg(any(test, feature = "invariant-ppt"))]
pub fn pipeline_health_check(pipeline: &crate::Pipeline) -> PipelineHealthStatus {
    let issues = analyze_pipeline_composition(pipeline);
    let error_rate = pipeline.error_rate();
    let is_healthy = issues.is_empty() && error_rate < 0.05;

    PipelineHealthStatus {
        is_healthy,
        stage_count: pipeline.stage_count(),
        total_processed: pipeline.events_processed(),
        error_rate,
        issues,
    }
}

// ── Performance regression detection ──────────────────────────────────────────

/// Snapshot of pipeline performance metrics for regression tracking.
#[derive(Debug, Clone)]
pub struct PipelineMetrics {
    pub stage_count: usize,
    pub total_events_processed: u64,
    pub avg_latency_ms: f64,
    pub memory_baseline_kb: u64,
    pub error_rate: f64,
}

#[cfg(any(test, feature = "invariant-ppt"))]
thread_local! {
    static METRICS_STORE: RefCell<std::collections::HashMap<String, PipelineMetrics>> =
        RefCell::new(std::collections::HashMap::new());
}

/// Records a named performance baseline for later regression comparison.
#[cfg(any(test, feature = "invariant-ppt"))]
pub fn record_pipeline_metrics(name: &str, metrics: PipelineMetrics) {
    METRICS_STORE.with(|store| {
        store.borrow_mut().insert(name.to_string(), metrics);
    });
}

/// Compares `current` metrics against the stored baseline for `name`.
///
/// Returns `Err` if latency has regressed by more than `threshold` (0.0–1.0)
/// or if no baseline exists.
#[cfg(any(test, feature = "invariant-ppt"))]
pub fn check_performance_regression(
    name: &str,
    current: &PipelineMetrics,
    threshold: f64,
) -> Result<(), String> {
    METRICS_STORE.with(|store| {
        let borrow = store.borrow();
        let baseline = borrow
            .get(name)
            .ok_or_else(|| format!("no baseline recorded for '{}'", name))?;

        if baseline.avg_latency_ms > 0.0 {
            let change =
                (current.avg_latency_ms - baseline.avg_latency_ms) / baseline.avg_latency_ms;
            if change > threshold {
                return Err(format!(
                    "latency regression for '{}': baseline {:.2}ms → current {:.2}ms ({:+.1}%)",
                    name,
                    baseline.avg_latency_ms,
                    current.avg_latency_ms,
                    change * 100.0
                ));
            }
        }

        Ok(())
    })
}

#[macro_export]
macro_rules! assert_invariant {
    ($cond:expr, $msg:expr) => {
        $crate::invariant_ppt::assert_invariant($cond, $msg, None)
    };
    ($cond:expr, $msg:expr, $ctx:expr) => {
        $crate::invariant_ppt::assert_invariant($cond, $msg, Some($ctx))
    };
}

// ── Baseline regression manager ───────────────────────────────────────────────

/// Snapshot of pipeline performance metrics captured from a live pipeline.
#[derive(Debug, Clone)]
pub struct BaselineSnapshot {
    pub stage_count: usize,
    pub total_events_processed: u64,
    pub avg_latency_ms: f64,
    pub error_rate: f64,
}

impl From<&crate::Pipeline> for BaselineSnapshot {
    fn from(p: &crate::Pipeline) -> Self {
        Self {
            stage_count: p.stage_count(),
            total_events_processed: p.events_processed(),
            avg_latency_ms: p.avg_latency_ms(),
            error_rate: p.error_rate(),
        }
    }
}

/// Manages performance baselines and regression detection for pipelines.
pub struct PptManager {
    baseline: Option<BaselineSnapshot>,
    regression_threshold: f64,
}

impl PptManager {
    /// Create a new PPT manager with a 10% regression threshold.
    pub fn new() -> Self {
        Self {
            baseline: None,
            regression_threshold: 0.1,
        }
    }

    /// Set the performance regression threshold (0.0–1.0).
    pub fn with_regression_threshold(mut self, threshold: f64) -> Self {
        self.regression_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Capture a performance baseline from the pipeline's current state.
    pub fn establish_baseline(&mut self, pipeline: &crate::Pipeline) {
        self.baseline = Some(BaselineSnapshot::from(pipeline));
    }

    /// Compare the pipeline's current metrics against the stored baseline.
    ///
    /// Returns `Err` if no baseline has been established.
    pub fn check_regression(&self, pipeline: &crate::Pipeline) -> Result<RegressionReport, String> {
        let Some(baseline) = &self.baseline else {
            return Err("No baseline established. Call establish_baseline() first.".to_string());
        };

        let current = BaselineSnapshot::from(pipeline);
        let regression = detect_regression(baseline, &current, self.regression_threshold);

        Ok(RegressionReport {
            has_regression: regression.is_some(),
            regression_details: regression,
            baseline_stage_count: baseline.stage_count,
            current_stage_count: current.stage_count,
            baseline_latency_ms: baseline.avg_latency_ms,
            current_latency_ms: current.avg_latency_ms,
            baseline_error_rate: baseline.error_rate,
            current_error_rate: current.error_rate,
            threshold: self.regression_threshold,
        })
    }

    /// Perform a comprehensive pipeline health check.
    #[cfg(any(test, feature = "invariant-ppt"))]
    pub fn health_check(&self, pipeline: &crate::Pipeline) -> HealthReport {
        let status = pipeline_health_check(pipeline);
        let recommendations = generate_recommendations(&status);

        HealthReport {
            health_score: calculate_health_score(&status),
            is_healthy: status.is_healthy,
            stage_count: status.stage_count,
            total_processed: status.total_processed,
            error_rate: status.error_rate,
            issues: status.issues,
            recommendations,
            timestamp: std::time::SystemTime::now(),
        }
    }
}

impl Default for PptManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a regression check.
#[derive(Debug, Clone)]
pub struct RegressionReport {
    pub has_regression: bool,
    pub regression_details: Option<RegressionDetails>,
    pub baseline_stage_count: usize,
    pub current_stage_count: usize,
    pub baseline_latency_ms: f64,
    pub current_latency_ms: f64,
    pub baseline_error_rate: f64,
    pub current_error_rate: f64,
    pub threshold: f64,
}

/// Details of a detected performance regression.
#[derive(Debug, Clone)]
pub struct RegressionDetails {
    pub metric: String,
    pub baseline_value: f64,
    pub current_value: f64,
    pub change_percent: f64,
    pub severity: RegressionSeverity,
}

/// Severity classification for a regression.
#[derive(Debug, Clone, PartialEq)]
pub enum RegressionSeverity {
    Minor,
    Moderate,
    Severe,
    Critical,
}

/// Comprehensive pipeline health report.
#[derive(Debug, Clone)]
pub struct HealthReport {
    /// 0.0 (critical) to 1.0 (perfect).
    pub health_score: f64,
    pub is_healthy: bool,
    pub stage_count: usize,
    pub total_processed: u64,
    pub error_rate: f64,
    pub issues: Vec<String>,
    pub recommendations: Vec<String>,
    pub timestamp: std::time::SystemTime,
}

// ── Regression helpers ────────────────────────────────────────────────────────

fn detect_regression(
    baseline: &BaselineSnapshot,
    current: &BaselineSnapshot,
    threshold: f64,
) -> Option<RegressionDetails> {
    if baseline.avg_latency_ms > 0.0 {
        let change = (current.avg_latency_ms - baseline.avg_latency_ms) / baseline.avg_latency_ms;
        if change > threshold {
            return Some(RegressionDetails {
                metric: "avg_latency_ms".to_string(),
                baseline_value: baseline.avg_latency_ms,
                current_value: current.avg_latency_ms,
                change_percent: change * 100.0,
                severity: classify_severity(change),
            });
        }
    }

    if baseline.error_rate > 0.0 {
        let change = (current.error_rate - baseline.error_rate) / baseline.error_rate;
        if change > threshold {
            return Some(RegressionDetails {
                metric: "error_rate".to_string(),
                baseline_value: baseline.error_rate,
                current_value: current.error_rate,
                change_percent: change * 100.0,
                severity: RegressionSeverity::Severe,
            });
        }
    }

    None
}

fn classify_severity(change_ratio: f64) -> RegressionSeverity {
    if change_ratio > 0.5 {
        RegressionSeverity::Critical
    } else if change_ratio > 0.25 {
        RegressionSeverity::Severe
    } else if change_ratio > 0.15 {
        RegressionSeverity::Moderate
    } else {
        RegressionSeverity::Minor
    }
}

#[cfg(any(test, feature = "invariant-ppt"))]
fn calculate_health_score(status: &PipelineHealthStatus) -> f64 {
    let mut score = 1.0_f64;
    score -= status.issues.len() as f64 * 0.1;
    if status.error_rate > 0.05 {
        score -= (status.error_rate - 0.05) * 2.0;
    }
    score.clamp(0.0, 1.0)
}

#[cfg(any(test, feature = "invariant-ppt"))]
fn generate_recommendations(status: &PipelineHealthStatus) -> Vec<String> {
    let mut recs = Vec::new();

    for issue in &status.issues {
        if issue.contains("no stages") {
            recs.push("Add at least one processing stage to the pipeline.".to_string());
        } else if issue.contains("no output") {
            recs.push(
                "Add an output stage (StdoutOutput, FileOutput, or Deadletter) so events are not silently dropped.".to_string(),
            );
        }
    }

    if status.error_rate > 0.05 {
        recs.push(format!(
            "Error rate {:.1}% is above the 5% warning threshold. Investigate upstream data quality.",
            status.error_rate * 100.0
        ));
    }

    recs
}
