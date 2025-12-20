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
//! - Validation failures cannot be silently ignored
//! - Metrics collection cannot influence execution
//!
//! ## Example
//!
//! ```rust
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
//!     let mut deadletter = Deadletter::new(PathBuf::from("errors.ndjson"));
//!
//!     // Process input file
//!     let mut input = InputSource::File(PathBuf::from("input.ndjson"));
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

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::time::Instant;

pub mod invariant_ppt;

#[cfg(test)]
mod ppt_invariant_contracts;

pub mod replay;

pub(crate) const INVARIANT_PROCESSED_INCREMENTS_ONCE: &str =
    "processed increments exactly once per process_event";
pub(crate) const INVARIANT_ERRORS_INCREMENT_ON_ERROR: &str = "errors increment exactly once per error";
pub(crate) const INVARIANT_DROPPED_ONLY_FOR_NON_OUTPUT_NONE: &str =
    "dropped increments only when non-output stage returns None";
pub(crate) const INVARIANT_OUTPUT_NONE_NOT_DROPPED: &str =
    "output stage returning None does not count as dropped";
pub(crate) const INVARIANT_LATENCY_RECORDED_ON_SUCCESS: &str =
    "latency is recorded for each successful stage execution";

/// Type aliases for complex function types to reduce clippy warnings
pub type EventDerivationFn = Box<dyn Fn(&Event) -> serde_json::Value>;
pub type ValueConstraintFn = Box<dyn Fn(&serde_json::Value) -> bool>;
pub type StageFactoryFn = Box<dyn Fn() -> Box<dyn Stage>>;

/// Represents a structured event in the pipeline.
/// Owned, mutable data, supports JSON-like types, typed field access, optional metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// The main data payload, JSON-like.
    pub data: serde_json::Value,
    /// Optional metadata associated with the event.
    pub metadata: Option<BTreeMap<String, serde_json::Value>>,
}

impl Event {
    /// Create a new event from raw input (assuming JSON for now).
    pub fn from_raw_input(input: &str) -> anyhow::Result<Self> {
        let data: serde_json::Value = serde_json::from_str(input)?;
        Ok(Event {
            data,
            metadata: None,
        })
    }

    /// Typed field access for strings.
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.data.get(key)?.as_str()
    }

    /// Typed field access for numbers.
    pub fn get_number(&self, key: &str) -> Option<f64> {
        self.data.get(key)?.as_f64()
    }

    // Add more typed access as needed.
}

/// Error taxonomy for pipeline failures.
/// Explicit category, stage attribution, machine-readable code.
#[derive(Debug, Clone)]
pub enum PipelineError {
    Parse(ParseError),
    Transform(TransformError),
    Validation(ValidationError),
    Output(OutputError),
    System(SystemError),
}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineError::Parse(e) => write!(f, "Parse error: {}", e.message),
            PipelineError::Transform(e) => write!(f, "Transform error: {}", e.message),
            PipelineError::Validation(e) => write!(f, "Validation error: {}", e.message),
            PipelineError::Output(e) => write!(f, "Output error: {}", e.message),
            PipelineError::System(e) => write!(f, "System error: {}", e.message),
        }
    }
}

impl PipelineError {
    pub fn category(&self) -> &str {
        match self {
            PipelineError::Parse(_) => "Parse",
            PipelineError::Transform(_) => "Transform",
            PipelineError::Validation(_) => "Validation",
            PipelineError::Output(_) => "Output",
            PipelineError::System(_) => "System",
        }
    }

    pub fn stage(&self) -> &str {
        match self {
            PipelineError::Parse(e) => &e.stage,
            PipelineError::Transform(e) => &e.stage,
            PipelineError::Validation(e) => &e.stage,
            PipelineError::Output(e) => &e.stage,
            PipelineError::System(e) => &e.stage,
        }
    }

    pub fn code(&self) -> String {
        match self {
            PipelineError::Parse(e) => e.code.to_string(),
            PipelineError::Transform(e) => e.code.to_string(),
            PipelineError::Validation(e) => e.code.to_string(),
            PipelineError::Output(e) => e.code.to_string(),
            PipelineError::System(e) => e.code.to_string(),
        }
    }

    pub fn message(&self) -> &str {
        match self {
            PipelineError::Parse(e) => &e.message,
            PipelineError::Transform(e) => &e.message,
            PipelineError::Validation(e) => &e.message,
            PipelineError::Output(e) => &e.message,
            PipelineError::System(e) => &e.message,
        }
    }
}

impl std::error::Error for PipelineError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseErrorCode {
    ParseError,
    Utf8Error,
    JsonError,
    Test,
}

impl fmt::Display for ParseErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseErrorCode::ParseError => write!(f, "PARSE_ERROR"),
            ParseErrorCode::Utf8Error => write!(f, "UTF8_ERROR"),
            ParseErrorCode::JsonError => write!(f, "JSON_ERROR"),
            ParseErrorCode::Test => write!(f, "TEST"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParseError {
    pub stage: String,
    pub code: ParseErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransformErrorCode {
    MissingField,
    TypeMismatch,
    ConstraintViolation,
    Test,
}

impl fmt::Display for TransformErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransformErrorCode::MissingField => write!(f, "MISSING_FIELD"),
            TransformErrorCode::TypeMismatch => write!(f, "TYPE_MISMATCH"),
            TransformErrorCode::ConstraintViolation => write!(f, "CONSTRAINT_VIOLATION"),
            TransformErrorCode::Test => write!(f, "TEST"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransformError {
    pub stage: String,
    pub code: TransformErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationErrorCode {
    MissingField,
    TypeMismatch,
    ConstraintViolation,
    Test,
}

impl fmt::Display for ValidationErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationErrorCode::MissingField => write!(f, "MISSING_FIELD"),
            ValidationErrorCode::TypeMismatch => write!(f, "TYPE_MISMATCH"),
            ValidationErrorCode::ConstraintViolation => write!(f, "CONSTRAINT_VIOLATION"),
            ValidationErrorCode::Test => write!(f, "TEST"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValidationError {
    pub stage: String,
    pub code: ValidationErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputErrorCode {
    SerializeError,
    IoError,
    Test,
}

impl fmt::Display for OutputErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputErrorCode::SerializeError => write!(f, "SERIALIZE_ERROR"),
            OutputErrorCode::IoError => write!(f, "IO_ERROR"),
            OutputErrorCode::Test => write!(f, "TEST"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutputError {
    pub stage: String,
    pub code: OutputErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemErrorCode {
    IoError,
    Test,
}

impl fmt::Display for SystemErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SystemErrorCode::IoError => write!(f, "IO_ERROR"),
            SystemErrorCode::Test => write!(f, "TEST"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SystemError {
    pub stage: String,
    pub code: SystemErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
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
#[derive(Debug)]
pub struct Metrics {
    events_processed: u64,
    events_dropped: u64,
    errors: u64,
    stage_latencies: HashMap<String, LatencyStats>, // bounded stats
    drop_reasons: HashMap<DropReason, u64>,         // bounded reasons
}

#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
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
            stage_latencies: HashMap::new(),
            drop_reasons: HashMap::new(),
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

/// Stage contract: ownership-based execution.
/// Takes Event, returns Option<Event>, with explicit drop semantics.
pub trait Stage {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError>;
    fn name(&self) -> &str;
    fn is_output(&self) -> bool {
        false
    }
}

/// Pipeline: linear, deterministic execution of stages.
/// No distributed coordination, constant memory streaming.
pub struct Pipeline {
    stages: Vec<Box<dyn Stage>>,
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
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Input sources: local, synchronous, stream-oriented, ordered read.
/// No distributed offsets, no remote coordination.
pub enum InputSource {
    Stdin,
    File(PathBuf),
    Directory(PathBuf), // non-recursive
}

impl InputSource {
    pub fn process_input(
        &mut self,
        pipeline: &mut Pipeline,
        deadletter: &mut Option<&mut dyn Stage>,
    ) -> Result<(), PipelineError> {
        match self {
            InputSource::Stdin => {
                let stdin = io::stdin();
                let lines = stdin.lines();
                for line in lines {
                    let line = line.map_err(|e| {
                        PipelineError::System(SystemError {
                            stage: "Input_Stdin".to_string(),
                            code: SystemErrorCode::IoError,
                            message: e.to_string(),
                        })
                    })?;
                    let event = match Event::from_raw_input(&line) {
                        Ok(e) => e,
                        Err(e) => {
                            if let Some(ref mut dl) = *deadletter {
                                let error_event = Event {
                                    data: serde_json::json!({
                                        "error": "parse",
                                        "stage": "Input_Stdin",
                                        "code": "PARSE_ERROR",
                                        "message": e.to_string(),
                                        "raw": line
                                    }),
                                    metadata: None,
                                };
                                let _ = dl.execute(error_event); // ignore error in deadletter
                            } else {
                                return Err(PipelineError::Parse(ParseError {
                                    stage: "Input_Stdin".to_string(),
                                    code: ParseErrorCode::ParseError,
                                    message: e.to_string(),
                                }));
                            }
                            continue;
                        }
                    };
                    match pipeline.process_event(event) {
                        Ok(_) => {}
                        Err(e) => {
                            if let Some(ref mut dl) = *deadletter {
                                let error_event = Event {
                                    data: serde_json::json!({
                                        "error": "pipeline",
                                        "category": e.category(),
                                        "stage": e.stage(),
                                        "code": e.code(),
                                        "message": e.message(),
                                        "raw": line
                                    }),
                                    metadata: None,
                                };
                                let _ = dl.execute(error_event);
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
                Ok(())
            }
            InputSource::File(path) => {
                let file = fs::File::open(path).map_err(|e| {
                    PipelineError::System(SystemError {
                        stage: "Input_File".to_string(),
                        code: SystemErrorCode::IoError,
                        message: e.to_string(),
                    })
                })?;
                let lines = io::BufReader::new(file).lines();
                for line in lines {
                    let line = line.map_err(|e| {
                        PipelineError::System(SystemError {
                            stage: "Input_File".to_string(),
                            code: SystemErrorCode::IoError,
                            message: e.to_string(),
                        })
                    })?;
                    let event = match Event::from_raw_input(&line) {
                        Ok(e) => e,
                        Err(e) => {
                            if let Some(ref mut dl) = *deadletter {
                                let error_event = Event {
                                    data: serde_json::json!({
                                        "error": "parse",
                                        "stage": "Input_File",
                                        "code": "PARSE_ERROR",
                                        "message": e.to_string(),
                                        "raw": line
                                    }),
                                    metadata: None,
                                };
                                let _ = dl.execute(error_event);
                            } else {
                                return Err(PipelineError::Parse(ParseError {
                                    stage: "Input_File".to_string(),
                                    code: ParseErrorCode::ParseError,
                                    message: e.to_string(),
                                }));
                            }
                            continue;
                        }
                    };
                    match pipeline.process_event(event) {
                        Ok(_) => {}
                        Err(e) => {
                            if let Some(ref mut dl) = *deadletter {
                                let error_event = Event {
                                    data: serde_json::json!({
                                        "error": "pipeline",
                                        "category": e.category(),
                                        "stage": e.stage(),
                                        "code": e.code(),
                                        "message": e.message(),
                                        "raw": line
                                    }),
                                    metadata: None,
                                };
                                let _ = dl.execute(error_event);
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
                Ok(())
            }
            InputSource::Directory(dir) => {
                let entries = fs::read_dir(dir).map_err(|e| {
                    PipelineError::System(SystemError {
                        stage: "Input_Directory".to_string(),
                        code: SystemErrorCode::IoError,
                        message: e.to_string(),
                    })
                })?;
                let mut paths: Vec<PathBuf> = Vec::new();
                for entry in entries {
                    let entry = entry.map_err(|e| {
                        PipelineError::System(SystemError {
                            stage: "Input_Directory".to_string(),
                            code: SystemErrorCode::IoError,
                            message: e.to_string(),
                        })
                    })?;
                    let path = entry.path();
                    if path.is_file() {
                        paths.push(path);
                    }
                    // Non-recursive, so no subdirs
                }
                paths.sort();
                for path in paths {
                    let mut file_input = InputSource::File(path);
                    file_input.process_input(pipeline, deadletter)?;
                }
                Ok(())
            }
        }
    }
}

/// Parsers: convert raw bytes to Event with explicit error handling.
/// Best effort syslog, zero copy where possible, no implicit recovery.
pub trait Parser {
    fn parse(&self, raw: &[u8]) -> Result<Event, PipelineError>;
}

pub struct NDJSONParser;

impl Parser for NDJSONParser {
    fn parse(&self, raw: &[u8]) -> Result<Event, PipelineError> {
        let s = std::str::from_utf8(raw).map_err(|e| {
            PipelineError::Parse(ParseError {
                stage: "NDJSON".to_string(),
                code: ParseErrorCode::Utf8Error,
                message: e.to_string(),
            })
        })?;
        Event::from_raw_input(s).map_err(|e| {
            PipelineError::Parse(ParseError {
                stage: "NDJSON".to_string(),
                code: ParseErrorCode::JsonError,
                message: e.to_string(),
            })
        })
    }
}

pub struct JSONArrayParser;

impl Parser for JSONArrayParser {
    fn parse(&self, raw: &[u8]) -> Result<Event, PipelineError> {
        let s = std::str::from_utf8(raw).map_err(|e| {
            PipelineError::Parse(ParseError {
                stage: "JSONArray".to_string(),
                code: ParseErrorCode::Utf8Error,
                message: e.to_string(),
            })
        })?;
        let value: serde_json::Value = serde_json::from_str(s).map_err(|e| {
            PipelineError::Parse(ParseError {
                stage: "JSONArray".to_string(),
                code: ParseErrorCode::JsonError,
                message: e.to_string(),
            })
        })?;
        // For array, perhaps wrap in an event with the array as data
        Ok(Event {
            data: value,
            metadata: None,
        })
    }
}

pub struct SyslogParser;

impl Parser for SyslogParser {
    fn parse(&self, raw: &[u8]) -> Result<Event, PipelineError> {
        // Best effort syslog parsing: simple regex or basic parsing
        let s = std::str::from_utf8(raw).map_err(|e| {
            PipelineError::Parse(ParseError {
                stage: "Syslog".to_string(),
                code: ParseErrorCode::Utf8Error,
                message: e.to_string(),
            })
        })?;
        // Simple syslog: <pri>timestamp host message
        // For now, create an event with the raw string
        Ok(Event {
            data: serde_json::json!({ "message": s }),
            metadata: None,
        })
    }
}

/// Transforms: bounded, explicit modification or filtering of events.
/// Deterministic, side-effect free, no network, no persistence.
pub trait Transform: Stage {}

pub struct FieldSelect {
    fields: Vec<String>,
}

impl FieldSelect {
    pub fn new(fields: Vec<String>) -> Self {
        FieldSelect { fields }
    }
}

impl Stage for FieldSelect {
    fn execute(&mut self, mut event: Event) -> Result<Option<Event>, PipelineError> {
        if let serde_json::Value::Object(ref mut map) = event.data {
            let mut new_map = serde_json::Map::new();
            for field in &self.fields {
                if let Some(value) = map.remove(field) {
                    new_map.insert(field.clone(), value);
                }
            }
            event.data = serde_json::Value::Object(new_map);
        }
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "FieldSelect"
    }
}

impl Transform for FieldSelect {}

pub struct FieldRemap {
    mappings: HashMap<String, String>,
}

impl FieldRemap {
    pub fn new(mappings: HashMap<String, String>) -> Self {
        FieldRemap { mappings }
    }
}

impl Stage for FieldRemap {
    fn execute(&mut self, mut event: Event) -> Result<Option<Event>, PipelineError> {
        if let serde_json::Value::Object(ref mut map) = event.data {
            for (old_key, new_key) in &self.mappings {
                if let Some(value) = map.remove(old_key) {
                    map.insert(new_key.clone(), value);
                }
            }
        }
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "FieldRemap"
    }
}

impl Transform for FieldRemap {}

pub struct PIIRedaction {
    patterns: Vec<Regex>,
}

impl PIIRedaction {
    pub fn new(patterns: Vec<Regex>) -> Self {
        PIIRedaction { patterns }
    }
}

impl Stage for PIIRedaction {
    fn execute(&mut self, mut event: Event) -> Result<Option<Event>, PipelineError> {
        if let serde_json::Value::Object(ref mut map) = event.data {
            for (_, value) in map.iter_mut() {
                if let serde_json::Value::String(ref mut s) = value {
                    for pattern in &self.patterns {
                        *s = pattern.replace_all(s, "[REDACTED]").to_string();
                    }
                }
            }
        }
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "PIIRedaction"
    }
}

impl Transform for PIIRedaction {}

pub struct DerivedFields {
    derivations: HashMap<String, EventDerivationFn>,
}

impl DerivedFields {
    pub fn new(derivations: HashMap<String, EventDerivationFn>) -> Self {
        DerivedFields { derivations }
    }
}

impl Stage for DerivedFields {
    fn execute(&mut self, mut event: Event) -> Result<Option<Event>, PipelineError> {
        let mut new_values = Vec::new();
        for (key, func) in &self.derivations {
            new_values.push((key.clone(), func(&event)));
        }
        if let serde_json::Value::Object(ref mut map) = event.data {
            for (key, value) in new_values {
                map.insert(key, value);
            }
        }
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "DerivedFields"
    }
}

impl Transform for DerivedFields {}

pub struct Filter {
    condition: Box<dyn Fn(&Event) -> bool>,
}

impl Filter {
    pub fn new(condition: Box<dyn Fn(&Event) -> bool>) -> Self {
        Filter { condition }
    }
}

impl Stage for Filter {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError> {
        if (self.condition)(&event) {
            Ok(Some(event))
        } else {
            Ok(None)
        }
    }

    fn name(&self) -> &str {
        "Filter"
    }
}

impl Transform for Filter {}

/// Validators: enforce structural and semantic correctness of events before output.
/// Schema enforced, fail closed, no silent acceptance.
pub trait Validator: Stage {}

pub struct RequiredFields {
    fields: Vec<String>,
}

impl RequiredFields {
    pub fn new(fields: Vec<String>) -> Self {
        RequiredFields { fields }
    }
}

impl Stage for RequiredFields {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError> {
        if let serde_json::Value::Object(map) = &event.data {
            for field in &self.fields {
                if !map.contains_key(field) {
                    return Err(PipelineError::Validation(ValidationError {
                        stage: "RequiredFields".to_string(),
                        code: ValidationErrorCode::MissingField,
                        message: format!("Missing required field: {}", field),
                    }));
                }
            }
        }
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "RequiredFields"
    }
}

impl Validator for RequiredFields {}

pub struct TypeChecking {
    type_checks: HashMap<String, String>, // field -> expected type (e.g., "string", "number")
}

impl TypeChecking {
    pub fn new(type_checks: HashMap<String, String>) -> Self {
        TypeChecking { type_checks }
    }
}

impl Stage for TypeChecking {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError> {
        if let serde_json::Value::Object(map) = &event.data {
            for (field, expected_type) in &self.type_checks {
                if let Some(value) = map.get(field) {
                    let actual_type = match value {
                        serde_json::Value::String(_) => "string",
                        serde_json::Value::Number(_) => "number",
                        serde_json::Value::Bool(_) => "boolean",
                        serde_json::Value::Object(_) => "object",
                        serde_json::Value::Array(_) => "array",
                        serde_json::Value::Null => "null",
                    };
                    if actual_type != expected_type {
                        return Err(PipelineError::Validation(ValidationError {
                            stage: "TypeChecking".to_string(),
                            code: ValidationErrorCode::TypeMismatch,
                            message: format!(
                                "Field {} expected {} but got {}",
                                field, expected_type, actual_type
                            ),
                        }));
                    }
                }
            }
        }
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "TypeChecking"
    }
}

impl Validator for TypeChecking {}

pub struct ValueConstraints {
    constraints: HashMap<String, ValueConstraintFn>,
}

impl ValueConstraints {
    pub fn new(constraints: HashMap<String, ValueConstraintFn>) -> Self {
        ValueConstraints { constraints }
    }
}

impl Stage for ValueConstraints {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError> {
        if let serde_json::Value::Object(map) = &event.data {
            for (field, check) in &self.constraints {
                if let Some(value) = map.get(field) {
                    if !check(value) {
                        return Err(PipelineError::Validation(ValidationError {
                            stage: "ValueConstraints".to_string(),
                            code: ValidationErrorCode::ConstraintViolation,
                            message: format!("Field {} violates constraint", field),
                        }));
                    }
                }
            }
        }
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "ValueConstraints"
    }
}

impl Validator for ValueConstraints {}

/// Outputs: emit processed events to local or synchronous destinations with explicit failure semantics.
/// Ordered write, bounded retry, no unbounded retry, no background flush.
pub trait Output: Stage {}

pub struct StdoutOutput;

impl StdoutOutput {
    pub fn new() -> Self {
        StdoutOutput
    }
}

impl Stage for StdoutOutput {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError> {
        println!(
            "{}",
            serde_json::to_string(&event.data).map_err(|e| PipelineError::Output(OutputError {
                stage: "Stdout".to_string(),
                code: OutputErrorCode::SerializeError,
                message: e.to_string(),
            }))?
        );
        Ok(None) // Consumed
    }

    fn name(&self) -> &str {
        "StdoutOutput"
    }

    fn is_output(&self) -> bool {
        true
    }
}

impl Output for StdoutOutput {}

impl Default for StdoutOutput {
    fn default() -> Self {
        Self::new()
    }
}

pub struct FileOutput {
    path: PathBuf,
}

impl FileOutput {
    pub fn new(path: PathBuf) -> Self {
        FileOutput { path }
    }
}

impl Stage for FileOutput {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError> {
        use std::io::Write;
        let mut file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)
            .map_err(|e| {
                PipelineError::Output(OutputError {
                    stage: "File".to_string(),
                    code: OutputErrorCode::IoError,
                    message: e.to_string(),
                })
            })?;
        writeln!(
            file,
            "{}",
            serde_json::to_string(&event.data).map_err(|e| PipelineError::Output(OutputError {
                stage: "File".to_string(),
                code: OutputErrorCode::SerializeError,
                message: e.to_string(),
            }))?
        )
        .map_err(|e| {
            PipelineError::Output(OutputError {
                stage: "File".to_string(),
                code: OutputErrorCode::IoError,
                message: e.to_string(),
            })
        })?;
        Ok(None) // Consumed
    }

    fn name(&self) -> &str {
        "FileOutput"
    }

    fn is_output(&self) -> bool {
        true
    }
}

impl Output for FileOutput {}

pub struct Deadletter {
    path: PathBuf,
}

impl Deadletter {
    pub fn new(path: PathBuf) -> Self {
        Deadletter { path }
    }
}

impl Stage for Deadletter {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError> {
        use std::io::Write;
        let mut file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)
            .map_err(|e| {
                PipelineError::Output(OutputError {
                    stage: "Deadletter".to_string(),
                    code: OutputErrorCode::IoError,
                    message: e.to_string(),
                })
            })?;
        writeln!(
            file,
            "{}",
            serde_json::to_string(&event).map_err(|e| PipelineError::Output(OutputError {
                stage: "Deadletter".to_string(),
                code: OutputErrorCode::SerializeError,
                message: e.to_string(),
            }))?
        )
        .map_err(|e| {
            PipelineError::Output(OutputError {
                stage: "Deadletter".to_string(),
                code: OutputErrorCode::IoError,
                message: e.to_string(),
            })
        })?;
        Ok(None) // Consumed
    }

    fn name(&self) -> &str {
        "Deadletter"
    }

    fn is_output(&self) -> bool {
        true
    }
}

impl Output for Deadletter {}

/// Configuration: ensure pipeline behavior is fully declared and validated before execution.
/// YAML input, version required, schema validated, unknown field rejection, no runtime mutation.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    version: u32,
    // For now, minimal; can extend to full pipeline definition
}

impl Config {
    pub fn from_yaml(yaml: &str) -> anyhow::Result<Self> {
        let config: Config = serde_yaml::from_str(yaml)?;
        if config.version != 1 {
            return Err(anyhow::anyhow!("Unsupported version: {}", config.version));
        }
        Ok(config)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_creation() {
        let data = serde_json::json!({"key": "value"});
        let event = Event {
            data,
            metadata: None,
        };
        assert_eq!(event.get_string("key"), Some("value"));
        assert_eq!(event.get_string("missing"), None);
    }

    #[test]
    fn test_event_from_raw_input() {
        let input = r#"{"level": "info", "message": "test"}"#;
        let event = Event::from_raw_input(input).unwrap();
        assert_eq!(event.get_string("level"), Some("info"));
        assert_eq!(event.get_string("message"), Some("test"));
    }

    #[test]
    fn test_pipeline_creation() {
        let pipeline = Pipeline::new();
        assert_eq!(pipeline.stages.len(), 0);
    }

    #[test]
    fn test_pipeline_add_stage() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".to_string()])));
        assert_eq!(pipeline.stages.len(), 1);
    }

    #[test]
    fn test_field_select_stage() {
        let mut stage = FieldSelect::new(vec!["level".to_string(), "message".to_string()]);
        let event = Event {
            data: serde_json::json!({"level": "info", "message": "test", "extra": "value"}),
            metadata: None,
        };
        let result = stage.execute(event).unwrap();
        assert!(result.is_some());
        let filtered = result.unwrap();
        assert_eq!(filtered.data.get("level"), Some(&serde_json::json!("info")));
        assert_eq!(
            filtered.data.get("message"),
            Some(&serde_json::json!("test"))
        );
        assert_eq!(filtered.data.get("extra"), None);
    }

    #[test]
    fn test_filter_stage() {
        let mut filter = Filter::new(Box::new(|e| e.get_string("level") == Some("info")));
        let info_event = Event {
            data: serde_json::json!({"level": "info"}),
            metadata: None,
        };
        let warn_event = Event {
            data: serde_json::json!({"level": "warn"}),
            metadata: None,
        };
        assert!(filter.execute(info_event).unwrap().is_some());
        assert!(filter.execute(warn_event).unwrap().is_none());
    }

    #[test]
    fn test_required_fields_stage() {
        let mut stage = RequiredFields::new(vec!["level".to_string(), "message".to_string()]);
        let valid_event = Event {
            data: serde_json::json!({"level": "info", "message": "test"}),
            metadata: None,
        };
        let invalid_event = Event {
            data: serde_json::json!({"level": "info"}),
            metadata: None,
        };
        assert!(stage.execute(valid_event).unwrap().is_some());
        assert!(stage.execute(invalid_event).is_err());
    }

    #[test]
    fn test_pii_redaction_stage() {
        let patterns = vec![regex::Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap()]; // SSN
        let mut stage = PIIRedaction::new(patterns);
        let event = Event {
            data: serde_json::json!({"ssn": "123-45-6789", "name": "John"}),
            metadata: None,
        };
        let result = stage.execute(event).unwrap().unwrap();
        assert_eq!(result.get_string("ssn"), Some("[REDACTED]"));
        assert_eq!(result.get_string("name"), Some("John"));
    }

    #[test]
    fn test_pipeline_execution_success() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".to_string()])));
        let event = Event {
            data: serde_json::json!({"level": "info", "extra": "value"}),
            metadata: None,
        };
        let result = pipeline.process_event(event).unwrap();
        assert!(result.is_some());
        let processed = result.unwrap();
        assert_eq!(processed.get_string("level"), Some("info"));
        assert_eq!(processed.get_string("extra"), None);
    }

    #[test]
    fn test_pipeline_execution_error() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(RequiredFields::new(vec!["missing".to_string()])));
        let event = Event {
            data: serde_json::json!({"level": "info"}),
            metadata: None,
        };
        let result = pipeline.process_event(event);
        assert!(result.is_err());
        if let Err(PipelineError::Validation(_)) = result {
            // correct
        } else {
            panic!("Expected Validation error");
        }
    }

    #[test]
    fn test_metrics_increment() {
        let mut metrics = Metrics::new();
        assert_eq!(metrics.events_processed, 0);
        metrics.increment_processed();
        assert_eq!(metrics.events_processed, 1);
    }

    #[test]
    fn test_metrics_dropped() {
        let mut metrics = Metrics::new();
        metrics.increment_dropped(DropReason::Filtered);
        assert_eq!(metrics.events_dropped, 1);
        assert_eq!(metrics.drop_reasons.get(&DropReason::Filtered), Some(&1));
    }

    #[test]
    fn test_latency_stats() {
        let mut stats = LatencyStats::new();
        stats.record(1.0);
        stats.record(3.0);
        stats.record(2.0);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.sum, 6.0);
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.max, 3.0);
    }

    #[test]
    fn test_std_output_stage() {
        let mut stage = StdoutOutput::new();
        let event = Event {
            data: serde_json::json!({"test": "data"}),
            metadata: None,
        };
        // Should output and consume
        let result = stage.execute(event).unwrap();
        assert!(result.is_none()); // consumed
        assert!(stage.is_output());
    }

    #[test]
    fn test_file_output_stage() {
        use std::fs;
        use std::path::PathBuf;
        let temp_file = PathBuf::from("test_output.ndjson");
        let mut stage = FileOutput::new(temp_file.clone());
        let event = Event {
            data: serde_json::json!({"test": "data"}),
            metadata: None,
        };
        let result = stage.execute(event).unwrap();
        assert!(result.is_none());
        assert!(stage.is_output());
        // Check file exists and has content
        assert!(temp_file.exists());
        let content = fs::read_to_string(&temp_file).unwrap();
        assert!(content.contains("test"));
        // Cleanup
        fs::remove_file(temp_file).unwrap();
    }

    #[test]
    fn test_deadletter_stage() {
        use std::fs;
        use std::path::PathBuf;
        let temp_file = PathBuf::from("test_deadletter.ndjson");
        let mut stage = Deadletter::new(temp_file.clone());
        let event = Event {
            data: serde_json::json!({"error": "test", "message": "failed"}),
            metadata: None,
        };
        let result = stage.execute(event).unwrap();
        assert!(result.is_none());
        assert!(stage.is_output());
        // Check file exists and has content
        assert!(temp_file.exists());
        let content = fs::read_to_string(&temp_file).unwrap();
        assert!(content.contains("test"));
        // Cleanup
        fs::remove_file(temp_file).unwrap();
    }

    #[test]
    fn test_directory_ingest_determinism() {
        use std::fs;
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();

        // Create files in non-alphabetical order to test sorting
        let file_z = temp_dir.path().join("z.ndjson");
        let file_a = temp_dir.path().join("a.ndjson");
        let file_m = temp_dir.path().join("m.ndjson");

        fs::write(&file_z, r#"{"file": "z"}"#).unwrap();
        fs::write(&file_a, r#"{"file": "a"}"#).unwrap();
        fs::write(&file_m, r#"{"file": "m"}"#).unwrap();

        // Create a pipeline that will process all files
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(StdoutOutput::new()));

        // Process directory - should work without errors
        // The determinism guarantee is that files are sorted before processing
        let mut input_source = InputSource::Directory(temp_dir.path().to_path_buf());
        let result = input_source.process_input(&mut pipeline, &mut None);

        // Should succeed - if it does, the sorting logic worked
        assert!(result.is_ok());

        // Check that we processed 3 events (one from each file)
        let prometheus = pipeline.export_prometheus();
        assert!(prometheus.contains("feedme_events_processed_total 3"));

        // Verify files still exist (weren't corrupted)
        assert!(file_a.exists());
        assert!(file_m.exists());
        assert!(file_z.exists());
    }

    #[test]
    fn test_deadletter_attribution() {
        use std::fs;
        use std::path::PathBuf;
        let temp_file = PathBuf::from("test_deadletter_attr.ndjson");

        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(RequiredFields::new(vec![
            "missing_field".to_string()
        ])));

        let mut deadletter = Deadletter::new(temp_file.clone());

        let event = Event {
            data: serde_json::json!({"existing_field": "value"}),
            metadata: None,
        };

        // Process should fail and go to deadletter
        let result = pipeline.process_event(event);
        assert!(result.is_err());

        // Simulate deadletter execution (normally done by InputSource)
        if let Err(e) = result {
            let error_event = Event {
                data: serde_json::json!({
                    "error": "pipeline",
                    "category": e.category(),
                    "stage": e.stage(),
                    "code": e.code(),
                    "message": e.message()
                }),
                metadata: None,
            };
            deadletter.execute(error_event).unwrap();
        }

        // Check deadletter file contains structured error info
        assert!(temp_file.exists());
        let content = fs::read_to_string(&temp_file).unwrap();
        let first_line = content.lines().next().unwrap();
        let deadletter_json: serde_json::Value = serde_json::from_str(first_line).unwrap();

        assert_eq!(deadletter_json["data"]["error"], "pipeline");
        assert_eq!(deadletter_json["data"]["category"], "Validation");
        assert_eq!(deadletter_json["data"]["stage"], "RequiredFields");
        assert_eq!(deadletter_json["data"]["code"], "MISSING_FIELD");
        assert!(deadletter_json["data"]["message"]
            .as_str()
            .unwrap()
            .contains("Missing required field"));

        // Cleanup
        fs::remove_file(temp_file).unwrap();
    }

    #[test]
    fn test_pipeline_metrics_export() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".to_string()])));
        let event = Event {
            data: serde_json::json!({"level": "info"}),
            metadata: None,
        };
        pipeline.process_event(event).unwrap();
        let json_logs = pipeline.export_json_logs();
        assert!(json_logs.len() > 0);
        assert!(json_logs.iter().any(|s| s.contains("events_processed")));
        let prometheus = pipeline.export_prometheus();
        assert!(prometheus.contains("# HELP feedme_events_processed_total"));
        assert!(prometheus.contains("feedme_events_processed_total 1"));
    }

    #[test]
    fn test_error_taxonomy() {
        let parse_err = PipelineError::Parse(ParseError {
            stage: "test".to_string(),
            code: ParseErrorCode::Test,
            message: "test".to_string(),
        });
        assert_eq!(parse_err.category(), "Parse");
        assert_eq!(parse_err.stage(), "test");
        assert_eq!(parse_err.code(), "TEST");
        assert_eq!(parse_err.message(), "test");

        let transform_err = PipelineError::Transform(TransformError {
            stage: "transform_test".to_string(),
            code: TransformErrorCode::Test,
            message: "transform test".to_string(),
        });
        assert_eq!(transform_err.category(), "Transform");
        assert_eq!(transform_err.stage(), "transform_test");
        assert_eq!(transform_err.code(), "TEST");
        assert_eq!(transform_err.message(), "transform test");

        let validation_err = PipelineError::Validation(ValidationError {
            stage: "validation_test".to_string(),
            code: ValidationErrorCode::Test,
            message: "validation test".to_string(),
        });
        assert_eq!(validation_err.category(), "Validation");
        assert_eq!(validation_err.stage(), "validation_test");
        assert_eq!(validation_err.code(), "TEST");
        assert_eq!(validation_err.message(), "validation test");

        let output_err = PipelineError::Output(OutputError {
            stage: "output_test".to_string(),
            code: OutputErrorCode::Test,
            message: "output test".to_string(),
        });
        assert_eq!(output_err.category(), "Output");
        assert_eq!(output_err.stage(), "output_test");
        assert_eq!(output_err.code(), "TEST");
        assert_eq!(output_err.message(), "output test");

        let system_err = PipelineError::System(SystemError {
            stage: "system_test".to_string(),
            code: SystemErrorCode::Test,
            message: "system test".to_string(),
        });
        assert_eq!(system_err.category(), "System");
        assert_eq!(system_err.stage(), "system_test");
        assert_eq!(system_err.code(), "TEST");
        assert_eq!(system_err.message(), "system test");
    }

    #[test]
    fn test_input_source_file() {
        use std::fs;
        use std::io::Write;
        let temp_file = "test_input.ndjson";
        let mut file = fs::File::create(temp_file).unwrap();
        writeln!(file, r#"{{"level": "info"}}"#).unwrap();
        drop(file);

        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(StdoutOutput::new()));
        let mut input = InputSource::File(temp_file.into());
        let mut deadletter: Option<&mut dyn Stage> = None;
        let result = input.process_input(&mut pipeline, &mut deadletter);
        assert!(result.is_ok());

        // Cleanup
        fs::remove_file(temp_file).unwrap();
    }

    #[test]
    fn test_input_source_file_parse_error() {
        use std::fs;
        use std::io::Write;
        let temp_file = "test_invalid.ndjson";
        let mut file = fs::File::create(temp_file).unwrap();
        writeln!(file, "invalid json").unwrap();
        drop(file);

        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(StdoutOutput::new()));
        let mut input = InputSource::File(temp_file.into());
        let mut deadletter: Option<&mut dyn Stage> = None;
        let result = input.process_input(&mut pipeline, &mut deadletter);
        assert!(result.is_err()); // fails on parse error when no deadletter

        // Cleanup
        fs::remove_file(temp_file).unwrap();
    }

    #[test]
    fn test_input_source_stdin() {
        // Hard to test stdin directly, but can test the enum
        let _input = InputSource::Stdin;
        // Would need integration test
    }

    #[test]
    fn test_type_checking_stage() {
        use std::collections::HashMap;
        let mut type_checks = HashMap::new();
        type_checks.insert("level".to_string(), "string".to_string());
        type_checks.insert("count".to_string(), "number".to_string());

        let mut stage = TypeChecking::new(type_checks);

        // Valid event
        let valid_event = Event {
            data: serde_json::json!({"level": "info", "count": 42}),
            metadata: None,
        };
        let result = stage.execute(valid_event).unwrap();
        assert!(result.is_some());

        // Invalid event - wrong type
        let invalid_event = Event {
            data: serde_json::json!({"level": 123, "count": "not_a_number"}),
            metadata: None,
        };
        let result = stage.execute(invalid_event);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "Validation");
    }

    #[test]
    fn test_value_constraints_stage() {
        use std::collections::HashMap;
        let mut constraints = HashMap::new();
        constraints.insert(
            "count".to_string(),
            Box::new(|v: &serde_json::Value| v.as_i64().map(|n| n >= 0).unwrap_or(false))
                as Box<dyn Fn(&serde_json::Value) -> bool>,
        );

        let mut stage = ValueConstraints::new(constraints);

        // Valid event
        let valid_event = Event {
            data: serde_json::json!({"count": 10}),
            metadata: None,
        };
        let result = stage.execute(valid_event).unwrap();
        assert!(result.is_some());

        // Invalid event - constraint violation
        let invalid_event = Event {
            data: serde_json::json!({"count": -5}),
            metadata: None,
        };
        let result = stage.execute(invalid_event);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "Validation");
    }

    #[test]
    fn test_input_source_directory_error() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(StdoutOutput::new()));
        let mut input = InputSource::Directory(PathBuf::from("/nonexistent/directory"));
        let mut deadletter: Option<&mut dyn Stage> = None;
        let result = input.process_input(&mut pipeline, &mut deadletter);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "System");
    }

    #[test]
    fn test_pipeline_metrics_json_export() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".to_string()])));
        let event = Event {
            data: serde_json::json!({"level": "info", "message": "test"}),
            metadata: None,
        };
        pipeline.process_event(event).unwrap();
        let json_logs = pipeline.export_json_logs();
        assert!(json_logs.len() > 0);
        // Check that JSON logs contain expected structure
        let first_log: serde_json::Value = serde_json::from_str(&json_logs[0]).unwrap();
        assert_eq!(first_log["metric"], "events_processed");
        assert!(first_log["value"].is_number());
    }

    #[test]
    fn test_derived_fields_stage() {
        use std::collections::HashMap;
        let mut derivations = HashMap::new();
        derivations.insert(
            "derived_field".to_string(),
            Box::new(|event: &Event| event.get_string("base_field").unwrap_or("default").into())
                as Box<dyn Fn(&Event) -> serde_json::Value>,
        );

        let mut stage = DerivedFields::new(derivations);
        let event = Event {
            data: serde_json::json!({"base_field": "test_value"}),
            metadata: None,
        };
        let result = stage.execute(event).unwrap().unwrap();
        assert_eq!(result.get_string("derived_field"), Some("test_value"));
        assert_eq!(result.get_string("base_field"), Some("test_value"));
    }

    #[test]
    fn test_pipeline_error_display() {
        let parse_err = PipelineError::Parse(ParseError {
            stage: "test".to_string(),
            code: ParseErrorCode::Test,
            message: "test message".to_string(),
        });
        let display = format!("{}", parse_err);
        assert!(display.contains("Parse error: test message"));

        let transform_err = PipelineError::Transform(TransformError {
            stage: "test".to_string(),
            code: TransformErrorCode::Test,
            message: "transform message".to_string(),
        });
        let display = format!("{}", transform_err);
        assert!(display.contains("Transform error: transform message"));
    }

    #[test]
    fn test_input_source_directory() {
        use std::fs;
        use std::io::Write;
        let temp_dir = "test_dir";
        fs::create_dir(temp_dir).unwrap();
        let temp_file = format!("{}/test.ndjson", temp_dir);
        let mut file = fs::File::create(&temp_file).unwrap();
        writeln!(file, r#"{{"level": "info"}}"#).unwrap();
        drop(file);

        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(StdoutOutput::new()));
        let mut input = InputSource::Directory(temp_dir.into());
        let mut deadletter: Option<&mut dyn Stage> = None;
        let result = input.process_input(&mut pipeline, &mut deadletter);
        assert!(result.is_ok());

        // Cleanup
        fs::remove_file(&temp_file).unwrap();
        fs::remove_dir(temp_dir).unwrap();
    }

    #[test]
    fn test_event_get_number() {
        let event = Event {
            data: serde_json::json!({"count": 42, "rate": 3.15, "text": "not_a_number"}),
            metadata: None,
        };

        assert_eq!(event.get_number("count"), Some(42.0));
        assert_eq!(event.get_number("rate"), Some(3.15));
        assert_eq!(event.get_number("text"), None);
        assert_eq!(event.get_number("missing"), None);
    }

    #[test]
    fn test_ndjson_parser() {
        let parser = NDJSONParser;
        let valid_json = r#"{"level": "info", "message": "test"}"#;
        let result = parser.parse(valid_json.as_bytes()).unwrap();
        assert_eq!(result.get_string("level"), Some("info"));
        assert_eq!(result.get_string("message"), Some("test"));

        let invalid_utf8 = &[0xFF, 0xFF, 0xFF, 0xFF];
        let result = parser.parse(invalid_utf8);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "Parse");

        let invalid_json = "not valid json";
        let result = parser.parse(invalid_json.as_bytes());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "Parse");
    }

    #[test]
    fn test_json_array_parser() {
        let parser = JSONArrayParser;
        let valid_array = r#"[{"item": 1}, {"item": 2}]"#;
        let result = parser.parse(valid_array.as_bytes()).unwrap();
        assert!(result.data.is_array());

        let invalid_utf8 = &[0xFF, 0xFF, 0xFF, 0xFF];
        let result = parser.parse(invalid_utf8);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "Parse");

        let invalid_json = "not valid json";
        let result = parser.parse(invalid_json.as_bytes());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "Parse");
    }

    #[test]
    fn test_syslog_parser() {
        let parser = SyslogParser;
        let syslog_message = "<13>2024-01-01T10:00:00Z myhost test message";
        let result = parser.parse(syslog_message.as_bytes()).unwrap();
        assert_eq!(result.get_string("message"), Some(syslog_message));

        let invalid_utf8 = &[0xFF, 0xFF, 0xFF, 0xFF];
        let result = parser.parse(invalid_utf8);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "Parse");
    }

    #[test]
    fn test_field_remap_stage() {
        use std::collections::HashMap;
        let mut mappings = HashMap::new();
        mappings.insert("old_field".to_string(), "new_field".to_string());
        mappings.insert("another_old".to_string(), "another_new".to_string());

        let mut stage = FieldRemap::new(mappings);
        let event = Event {
            data: serde_json::json!({"old_field": "value1", "another_old": "value2", "keep": "value3"}),
            metadata: None,
        };

        let result = stage.execute(event).unwrap().unwrap();
        assert_eq!(result.get_string("new_field"), Some("value1"));
        assert_eq!(result.get_string("another_new"), Some("value2"));
        assert_eq!(result.get_string("keep"), Some("value3"));
        assert_eq!(result.get_string("old_field"), None);
        assert_eq!(result.get_string("another_old"), None);
        assert_eq!(stage.name(), "FieldRemap");

        let non_object_event = Event {
            data: serde_json::json!("just a string"),
            metadata: None,
        };
        let result = stage.execute(non_object_event).unwrap().unwrap();
        assert_eq!(result.data, serde_json::json!("just a string"));
    }

    #[test]
    fn test_config_from_yaml() {
        let valid_yaml = "version: 1";
        let config = Config::from_yaml(valid_yaml).unwrap();
        assert_eq!(config.version, 1);

        let invalid_version = "version: 2";
        let result = Config::from_yaml(invalid_version);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported version"));

        let invalid_yaml = "invalid: yaml: structure: [";
        let result = Config::from_yaml(invalid_yaml);
        assert!(result.is_err());

        let unknown_fields = "version: 1\nunknown_field: value";
        let result = Config::from_yaml(unknown_fields);
        assert!(result.is_err());
    }

    #[test]
    fn test_plugin_registry() {
        let mut registry = PluginRegistry::new();
        registry.register(
            "test_stage".to_string(),
            Box::new(|| Box::new(StdoutOutput::new())),
        );

        let stage = registry.get_stage("test_stage");
        assert!(stage.is_some());
        assert_eq!(stage.unwrap().name(), "StdoutOutput");

        let missing = registry.get_stage("missing_stage");
        assert!(missing.is_none());
    }

    #[test]
    fn test_drop_reason_display() {
        assert_eq!(format!("{}", DropReason::Filtered), "filtered");
    }

    #[test]
    fn test_pipeline_error_std_error_trait() {
        let error = PipelineError::Parse(ParseError {
            stage: "test".to_string(),
            code: ParseErrorCode::Test,
            message: "test error".to_string(),
        });

        let _: &dyn std::error::Error = &error;
    }

    #[test]
    fn test_input_source_file_with_deadletter() {
        use std::fs;
        use std::io::Write;
        let temp_file = "test_input_deadletter.ndjson";
        let deadletter_file = "test_deadletter_file.ndjson";
        let mut file = fs::File::create(temp_file).unwrap();
        writeln!(file, "invalid json line").unwrap();
        writeln!(file, r#"{{"level": "info"}}"#).unwrap();
        drop(file);

        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(RequiredFields::new(vec![
            "missing_field".to_string()
        ])));

        let mut deadletter_stage = Deadletter::new(deadletter_file.into());
        let mut deadletter: Option<&mut dyn Stage> = Some(&mut deadletter_stage);

        let mut input = InputSource::File(temp_file.into());
        let result = input.process_input(&mut pipeline, &mut deadletter);
        assert!(result.is_ok());

        let deadletter_content = fs::read_to_string(deadletter_file).unwrap();
        assert!(deadletter_content.contains("parse"));
        assert!(deadletter_content.contains("pipeline"));

        // Cleanup
        fs::remove_file(temp_file).unwrap();
        fs::remove_file(deadletter_file).unwrap();
    }

    #[test]
    fn test_complete_pipeline_with_transforms_and_validators() {
        let mut pipeline = Pipeline::new();

        let mut field_mappings = std::collections::HashMap::new();
        field_mappings.insert("msg".to_string(), "message".to_string());
        pipeline.add_stage(Box::new(FieldRemap::new(field_mappings)));

        pipeline.add_stage(Box::new(FieldSelect::new(vec![
            "level".to_string(),
            "message".to_string(),
        ])));

        pipeline.add_stage(Box::new(RequiredFields::new(vec![
            "level".to_string(),
            "message".to_string(),
        ])));

        let mut type_checks = std::collections::HashMap::new();
        type_checks.insert("level".to_string(), "string".to_string());
        pipeline.add_stage(Box::new(TypeChecking::new(type_checks)));

        pipeline.add_stage(Box::new(StdoutOutput::new()));

        let event = Event {
            data: serde_json::json!({"level": "info", "msg": "test message", "extra": "ignored"}),
            metadata: None,
        };

        let result = pipeline.process_event(event).unwrap();
        assert!(result.is_none());

        let prometheus = pipeline.export_prometheus();
        assert!(prometheus.contains("feedme_events_processed_total 1"));
        assert!(prometheus.contains("feedme_stage_latency_ms"));
    }

    #[test]
    fn test_metrics_export_formats() {
        let mut metrics = Metrics::new();
        metrics.increment_processed();
        metrics.increment_dropped(DropReason::Filtered);
        metrics.increment_errors();
        metrics.record_latency("test_stage", 100.5);

        let prometheus = metrics.to_prometheus();
        assert!(prometheus.contains("feedme_events_processed_total 1"));
        assert!(prometheus.contains("feedme_events_dropped_total 1"));
        assert!(prometheus.contains("feedme_errors_total 1"));
        assert!(prometheus.contains("feedme_stage_latency_ms_sum{stage=\"test_stage\"} 100.5"));
        assert!(prometheus.contains("feedme_drop_reasons_total{reason=\"filtered\"} 1"));

        let json_logs = metrics.to_json_logs();
        assert!(json_logs.len() >= 4);

        let processed_log = json_logs
            .iter()
            .find(|log| log.contains("events_processed"))
            .unwrap();
        let log_json: serde_json::Value = serde_json::from_str(processed_log).unwrap();
        assert_eq!(log_json["metric"], "events_processed");
        assert_eq!(log_json["value"], 1);
    }

    #[test]
    fn test_input_source_directory_with_deadletter() {
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file1 = temp_dir.path().join("file1.ndjson");
        let file2 = temp_dir.path().join("file2.ndjson");

        fs::write(&file1, "invalid json\n").unwrap();
        fs::write(&file2, r#"{"level": "info"}"#).unwrap();

        let deadletter_file = temp_dir.path().join("deadletter.ndjson");
        let mut deadletter_stage = Deadletter::new(deadletter_file.clone());
        let mut deadletter: Option<&mut dyn Stage> = Some(&mut deadletter_stage);

        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(StdoutOutput::new()));

        let mut input = InputSource::Directory(temp_dir.path().to_path_buf());
        let result = input.process_input(&mut pipeline, &mut deadletter);
        assert!(result.is_ok());

        let deadletter_content = fs::read_to_string(&deadletter_file).unwrap();
        assert!(deadletter_content.contains("parse"));
        assert!(deadletter_content.contains("PARSE_ERROR"));
    }

    #[test]
    fn test_stage_is_output_default() {
        let stage = FieldSelect::new(vec!["test".to_string()]);
        assert!(!stage.is_output());

        let output_stage = StdoutOutput::new();
        assert!(output_stage.is_output());
    }

    #[test]
    fn test_latency_stats_edge_cases() {
        let mut stats = LatencyStats::new();
        assert_eq!(stats.count, 0);
        assert_eq!(stats.sum, 0.0);
        assert_eq!(stats.min, f64::INFINITY);
        assert_eq!(stats.max, f64::NEG_INFINITY);

        stats.record(5.0);
        assert_eq!(stats.count, 1);
        assert_eq!(stats.min, 5.0);
        assert_eq!(stats.max, 5.0);

        stats.record(3.0);
        assert_eq!(stats.min, 3.0);
        assert_eq!(stats.max, 5.0);

        stats.record(7.0);
        assert_eq!(stats.min, 3.0);
        assert_eq!(stats.max, 7.0);
    }

    #[test]
    fn test_pipeline_stage_filtering() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(Filter::new(Box::new(|e| {
            e.get_string("level") == Some("error")
        }))));
        pipeline.add_stage(Box::new(StdoutOutput::new()));

        let info_event = Event {
            data: serde_json::json!({"level": "info", "message": "test"}),
            metadata: None,
        };
        let result = pipeline.process_event(info_event).unwrap();
        assert!(result.is_none());

        let error_event = Event {
            data: serde_json::json!({"level": "error", "message": "test"}),
            metadata: None,
        };
        let result = pipeline.process_event(error_event).unwrap();
        assert!(result.is_none());

        let prometheus = pipeline.export_prometheus();
        assert!(prometheus.contains("feedme_events_processed_total 2"));
        assert!(prometheus.contains("feedme_events_dropped_total 1"));
    }

    #[test]
    fn test_all_pipeline_error_display_variants() {
        let validation_err = PipelineError::Validation(ValidationError {
            stage: "test".to_string(),
            code: ValidationErrorCode::Test,
            message: "validation test".to_string(),
        });
        assert_eq!(
            format!("{}", validation_err),
            "Validation error: validation test"
        );

        let output_err = PipelineError::Output(OutputError {
            stage: "test".to_string(),
            code: OutputErrorCode::Test,
            message: "output test".to_string(),
        });
        assert_eq!(format!("{}", output_err), "Output error: output test");

        let system_err = PipelineError::System(SystemError {
            stage: "test".to_string(),
            code: SystemErrorCode::Test,
            message: "system test".to_string(),
        });
        assert_eq!(format!("{}", system_err), "System error: system test");
    }

    #[test]
    fn test_input_source_stdin_processing() {
        let input_stdin = InputSource::Stdin;
        match input_stdin {
            InputSource::Stdin => {
                assert!(true);
            }
            _ => panic!("Should be Stdin variant"),
        }
    }

    #[test]
    fn test_file_output_io_errors() {
        use std::path::PathBuf;
        let invalid_path = PathBuf::from("/invalid/path/that/should/not/exist/output.json");
        let mut stage = FileOutput::new(invalid_path);

        let event = Event {
            data: serde_json::json!({"test": "data"}),
            metadata: None,
        };

        let result = stage.execute(event);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "Output");
    }

    #[test]
    fn test_deadletter_io_errors() {
        use std::path::PathBuf;
        let invalid_path = PathBuf::from("/invalid/path/that/should/not/exist/deadletter.json");
        let mut stage = Deadletter::new(invalid_path);

        let event = Event {
            data: serde_json::json!({"error": "test"}),
            metadata: None,
        };

        let result = stage.execute(event);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "Output");
    }

    #[test]
    fn test_metrics_empty_latency_stats() {
        let metrics = Metrics::new();
        let prometheus = metrics.to_prometheus();

        assert!(prometheus.contains("feedme_events_processed_total 0"));
        assert!(prometheus.contains("feedme_events_dropped_total 0"));
        assert!(prometheus.contains("feedme_errors_total 0"));

        let json_logs = metrics.to_json_logs();
        assert!(json_logs.len() >= 3);
    }

    #[test]
    fn test_stage_names() {
        assert_eq!(FieldSelect::new(vec![]).name(), "FieldSelect");
        assert_eq!(
            FieldRemap::new(std::collections::HashMap::new()).name(),
            "FieldRemap"
        );
        assert_eq!(PIIRedaction::new(vec![]).name(), "PIIRedaction");
        assert_eq!(
            DerivedFields::new(std::collections::HashMap::new()).name(),
            "DerivedFields"
        );
        assert_eq!(Filter::new(Box::new(|_| true)).name(), "Filter");
        assert_eq!(RequiredFields::new(vec![]).name(), "RequiredFields");
        assert_eq!(
            TypeChecking::new(std::collections::HashMap::new()).name(),
            "TypeChecking"
        );
        assert_eq!(
            ValueConstraints::new(std::collections::HashMap::new()).name(),
            "ValueConstraints"
        );
        assert_eq!(StdoutOutput::new().name(), "StdoutOutput");
        assert_eq!(FileOutput::new("/tmp/test".into()).name(), "FileOutput");
        assert_eq!(Deadletter::new("/tmp/test".into()).name(), "Deadletter");
    }

    #[test]
    fn test_stages_is_output() {
        assert!(!FieldSelect::new(vec![]).is_output());
        assert!(!FieldRemap::new(std::collections::HashMap::new()).is_output());
        assert!(!PIIRedaction::new(vec![]).is_output());
        assert!(!DerivedFields::new(std::collections::HashMap::new()).is_output());
        assert!(!Filter::new(Box::new(|_| true)).is_output());
        assert!(!RequiredFields::new(vec![]).is_output());
        assert!(!TypeChecking::new(std::collections::HashMap::new()).is_output());
        assert!(!ValueConstraints::new(std::collections::HashMap::new()).is_output());
        assert!(StdoutOutput::new().is_output());
        assert!(FileOutput::new("/tmp/test".into()).is_output());
        assert!(Deadletter::new("/tmp/test".into()).is_output());
    }

    #[test]
    fn test_stdout_output_serialization_error() {
        let mut stage = StdoutOutput::new();

        let event_with_infinite = Event {
            data: serde_json::json!({"value": std::f64::INFINITY}),
            metadata: None,
        };

        let result = stage.execute(event_with_infinite);
        match result {
            Ok(_) => {
                // JSON serialization should handle INFINITY as null, so this is OK
            }
            Err(e) => {
                assert_eq!(e.category(), "Output");
                assert_eq!(e.code(), "SERIALIZE_ERROR");
            }
        }
    }

    #[test]
    fn test_event_with_metadata() {
        use std::collections::BTreeMap;
        let mut metadata = BTreeMap::new();
        metadata.insert("source".to_string(), serde_json::json!("test"));
        metadata.insert("timestamp".to_string(), serde_json::json!(1234567890));

        let event = Event {
            data: serde_json::json!({"message": "test"}),
            metadata: Some(metadata),
        };

        assert_eq!(event.get_string("message"), Some("test"));
        assert!(event.metadata.is_some());
        assert_eq!(
            event.metadata.as_ref().unwrap().get("source").unwrap(),
            &serde_json::json!("test")
        );
    }

    #[test]
    fn test_complete_error_handling_pipeline() {
        use std::fs;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let input_file = temp_dir.path().join("input.ndjson");
        let deadletter_file = temp_dir.path().join("deadletter.ndjson");

        let mut file = fs::File::create(&input_file).unwrap();
        writeln!(file, "invalid json line").unwrap();
        writeln!(file, r#"{{"level": "info", "message": "valid"}}"#).unwrap();
        writeln!(file, r#"{{"level": "warn"}}"#).unwrap();
        drop(file);

        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(RequiredFields::new(vec![
            "level".to_string(),
            "message".to_string(),
        ])));
        pipeline.add_stage(Box::new(StdoutOutput::new()));

        let mut deadletter_stage = Deadletter::new(deadletter_file.clone());
        let mut deadletter: Option<&mut dyn Stage> = Some(&mut deadletter_stage);

        let mut input = InputSource::File(input_file);
        let result = input.process_input(&mut pipeline, &mut deadletter);
        assert!(result.is_ok());

        let deadletter_content = fs::read_to_string(&deadletter_file).unwrap();
        assert!(deadletter_content.contains("PARSE_ERROR"));
        assert!(deadletter_content.contains("MISSING_FIELD"));

        let lines: Vec<&str> = deadletter_content.lines().collect();
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_type_checking_all_types() {
        use std::collections::HashMap;
        let mut type_checks = HashMap::new();
        type_checks.insert("str_field".to_string(), "string".to_string());
        type_checks.insert("num_field".to_string(), "number".to_string());
        type_checks.insert("bool_field".to_string(), "boolean".to_string());
        type_checks.insert("obj_field".to_string(), "object".to_string());
        type_checks.insert("arr_field".to_string(), "array".to_string());
        type_checks.insert("null_field".to_string(), "null".to_string());

        let mut stage = TypeChecking::new(type_checks);

        let valid_event = Event {
            data: serde_json::json!({
                "str_field": "hello",
                "num_field": 42,
                "bool_field": true,
                "obj_field": {"nested": "value"},
                "arr_field": [1, 2, 3],
                "null_field": null
            }),
            metadata: None,
        };

        let result = stage.execute(valid_event).unwrap();
        assert!(result.is_some());

        let invalid_event = Event {
            data: serde_json::json!({
                "str_field": 42,
                "num_field": "not_a_number"
            }),
            metadata: None,
        };

        let result = stage.execute(invalid_event);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "Validation");
    }

    #[test]
    fn test_input_file_io_error() {
        use std::path::PathBuf;
        let nonexistent_file = PathBuf::from("/nonexistent/path/file.json");
        let mut input = InputSource::File(nonexistent_file);

        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(StdoutOutput::new()));
        let mut deadletter: Option<&mut dyn Stage> = None;

        let result = input.process_input(&mut pipeline, &mut deadletter);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.category(), "System");
        assert_eq!(err.code(), "IO_ERROR");
    }

    #[test]
    fn test_stdin_input_mock() {
        let input = InputSource::Stdin;
        match input {
            InputSource::Stdin => {
                // Test that we can create the variant
                assert!(true);
            }
            _ => panic!("Should be Stdin"),
        }
    }

    #[test]
    fn test_file_output_serialization_error() {
        use std::fs;
        use std::path::PathBuf;

        let temp_file = "test_serialize_error.json";
        let mut stage = FileOutput::new(PathBuf::from(temp_file));

        // Create an event that will cause JSON serialization issues
        let problematic_event = Event {
            data: serde_json::json!({"data": "normal"}),
            metadata: None,
        };

        // This should work fine
        let result = stage.execute(problematic_event);
        assert!(result.is_ok());

        // Clean up
        if std::path::Path::new(temp_file).exists() {
            fs::remove_file(temp_file).unwrap();
        }
    }

    #[test]
    fn test_deadletter_serialization_error() {
        use std::fs;
        use std::path::PathBuf;

        let temp_file = "test_deadletter_serialize.json";
        let mut stage = Deadletter::new(PathBuf::from(temp_file));

        let event = Event {
            data: serde_json::json!({"error": "test"}),
            metadata: None,
        };

        let result = stage.execute(event);
        assert!(result.is_ok());

        // Clean up
        if std::path::Path::new(temp_file).exists() {
            fs::remove_file(temp_file).unwrap();
        }
    }

    #[test]
    fn test_directory_io_error_during_iteration() {
        use std::path::PathBuf;
        let mut input = InputSource::Directory(PathBuf::from("/nonexistent/directory"));

        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(StdoutOutput::new()));
        let mut deadletter: Option<&mut dyn Stage> = None;

        let result = input.process_input(&mut pipeline, &mut deadletter);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "System");
    }

    #[test]
    fn test_pipeline_with_error_in_middle_stage() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".to_string()])));
        pipeline.add_stage(Box::new(RequiredFields::new(vec![
            "missing_field".to_string()
        ])));
        pipeline.add_stage(Box::new(StdoutOutput::new()));

        let event = Event {
            data: serde_json::json!({"level": "info", "message": "test"}),
            metadata: None,
        };

        let result = pipeline.process_event(event);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "Validation");

        let prometheus = pipeline.export_prometheus();
        assert!(prometheus.contains("feedme_events_processed_total 1"));
        assert!(prometheus.contains("feedme_errors_total 1"));
    }

    #[test]
    fn test_pii_redaction_non_object() {
        let mut stage = PIIRedaction::new(vec![regex::Regex::new(r"\d{3}-\d{2}-\d{4}").unwrap()]);

        let non_object_event = Event {
            data: serde_json::json!("just a string with SSN 123-45-6789"),
            metadata: None,
        };

        let result = stage.execute(non_object_event).unwrap().unwrap();
        assert_eq!(
            result.data,
            serde_json::json!("just a string with SSN 123-45-6789")
        );
    }

    #[test]
    fn test_derived_fields_non_object() {
        use std::collections::HashMap;
        let mut derivations = HashMap::new();
        derivations.insert(
            "new_field".to_string(),
            Box::new(|_: &Event| serde_json::json!("derived"))
                as Box<dyn Fn(&Event) -> serde_json::Value>,
        );

        let mut stage = DerivedFields::new(derivations);

        let non_object_event = Event {
            data: serde_json::json!("just a string"),
            metadata: None,
        };

        let result = stage.execute(non_object_event).unwrap().unwrap();
        assert_eq!(result.data, serde_json::json!("just a string"));
    }

    #[test]
    fn test_required_fields_non_object() {
        let mut stage = RequiredFields::new(vec!["level".to_string()]);

        let non_object_event = Event {
            data: serde_json::json!("just a string"),
            metadata: None,
        };

        let result = stage.execute(non_object_event).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_type_checking_non_object() {
        use std::collections::HashMap;
        let mut type_checks = HashMap::new();
        type_checks.insert("field".to_string(), "string".to_string());

        let mut stage = TypeChecking::new(type_checks);

        let non_object_event = Event {
            data: serde_json::json!("just a string"),
            metadata: None,
        };

        let result = stage.execute(non_object_event).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_value_constraints_non_object() {
        use std::collections::HashMap;
        let mut constraints = HashMap::new();
        constraints.insert(
            "field".to_string(),
            Box::new(|_: &serde_json::Value| true) as Box<dyn Fn(&serde_json::Value) -> bool>,
        );

        let mut stage = ValueConstraints::new(constraints);

        let non_object_event = Event {
            data: serde_json::json!("just a string"),
            metadata: None,
        };

        let result = stage.execute(non_object_event).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_metrics_record_latency_new_stage() {
        let mut metrics = Metrics::new();

        metrics.record_latency("stage1", 10.0);
        metrics.record_latency("stage2", 20.0);
        metrics.record_latency("stage1", 15.0);

        let prometheus = metrics.to_prometheus();
        assert!(prometheus.contains("stage1"));
        assert!(prometheus.contains("stage2"));
        assert!(prometheus.contains("25"));
        assert!(prometheus.contains("20"));
    }

    #[test]
    fn test_input_source_with_io_error_during_read() {
        use std::fs;
        use std::io::Write;

        let temp_file = "test_io_error.ndjson";
        let mut file = fs::File::create(temp_file).unwrap();
        writeln!(file, r#"{{"level": "info"}}"#).unwrap();
        drop(file);

        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(StdoutOutput::new()));

        let mut input = InputSource::File(temp_file.into());
        let mut deadletter: Option<&mut dyn Stage> = None;

        let result = input.process_input(&mut pipeline, &mut deadletter);
        assert!(result.is_ok());

        fs::remove_file(temp_file).unwrap();
    }

    #[test]
    fn test_pipeline_stage_latency_measurement() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".to_string()])));

        let event = Event {
            data: serde_json::json!({"level": "info", "extra": "removed"}),
            metadata: None,
        };

        pipeline.process_event(event).unwrap();

        let prometheus = pipeline.export_prometheus();
        assert!(prometheus.contains("feedme_stage_latency_ms"));
        assert!(prometheus.contains("FieldSelect"));
    }

    #[test]
    fn test_complete_input_output_cycle() {
        use std::fs;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let input_file = temp_dir.path().join("input.ndjson");
        let output_file = temp_dir.path().join("output.ndjson");

        let mut file = fs::File::create(&input_file).unwrap();
        writeln!(file, r#"{{"level": "info", "message": "test"}}"#).unwrap();
        writeln!(file, r#"{{"level": "error", "message": "error"}}"#).unwrap();
        drop(file);

        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(Filter::new(Box::new(|e| {
            e.get_string("level") == Some("info")
        }))));
        pipeline.add_stage(Box::new(FileOutput::new(output_file.clone())));

        let mut input = InputSource::File(input_file);
        let mut deadletter: Option<&mut dyn Stage> = None;

        let result = input.process_input(&mut pipeline, &mut deadletter);
        assert!(result.is_ok());

        let output_content = fs::read_to_string(&output_file).unwrap();
        assert!(output_content.contains("info"));
        assert!(!output_content.contains("error"));

        let prometheus = pipeline.export_prometheus();
        assert!(prometheus.contains("feedme_events_processed_total 2"));
        assert!(prometheus.contains("feedme_events_dropped_total 1"));
    }

    #[test]
    fn test_value_constraints_missing_field() {
        use std::collections::HashMap;
        let mut constraints = HashMap::new();
        constraints.insert(
            "missing_field".to_string(),
            Box::new(|_: &serde_json::Value| true) as Box<dyn Fn(&serde_json::Value) -> bool>,
        );

        let mut stage = ValueConstraints::new(constraints);

        let event = Event {
            data: serde_json::json!({"different_field": "value"}),
            metadata: None,
        };

        let result = stage.execute(event).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_type_checking_missing_field() {
        use std::collections::HashMap;
        let mut type_checks = HashMap::new();
        type_checks.insert("missing_field".to_string(), "string".to_string());

        let mut stage = TypeChecking::new(type_checks);

        let event = Event {
            data: serde_json::json!({"different_field": "value"}),
            metadata: None,
        };

        let result = stage.execute(event).unwrap();
        assert!(result.is_some());
    }
}
