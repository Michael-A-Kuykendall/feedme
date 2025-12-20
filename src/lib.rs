use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, BufRead};
use std::path::PathBuf;
use regex::Regex;
use std::time::Instant;
use std::fmt;

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

    pub fn code(&self) -> &str {
        match self {
            PipelineError::Parse(e) => &e.code,
            PipelineError::Transform(e) => &e.code,
            PipelineError::Validation(e) => &e.code,
            PipelineError::Output(e) => &e.code,
            PipelineError::System(e) => &e.code,
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

#[derive(Debug, Clone)]
pub struct ParseError {
    pub stage: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct TransformError {
    pub stage: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct ValidationError {
    pub stage: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct OutputError {
    pub stage: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct SystemError {
    pub stage: String,
    pub code: String,
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

/// Metrics for observability: counters, latency histograms, drop reason codes.
/// No execution feedback loops. Bounded storage.
#[derive(Debug)]
pub struct Metrics {
    events_processed: u64,
    events_dropped: u64,
    errors: u64,
    stage_latencies: HashMap<String, LatencyStats>, // bounded stats
    drop_reasons: HashMap<DropReason, u64>, // bounded reasons
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
        self.stage_latencies.entry(stage.to_string()).or_insert(LatencyStats::new()).record(duration);
    }

    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();
        output.push_str("# HELP feedme_events_processed_total Total events processed\n");
        output.push_str(&format!("feedme_events_processed_total {}\n", self.events_processed));
        output.push_str("# HELP feedme_events_dropped_total Total events dropped\n");
        output.push_str(&format!("feedme_events_dropped_total {}\n", self.events_dropped));
        output.push_str("# HELP feedme_errors_total Total errors\n");
        output.push_str(&format!("feedme_errors_total {}\n", self.errors));
        output.push_str("# HELP feedme_stage_latency_ms Stage latency in milliseconds\n");
        output.push_str("# TYPE feedme_stage_latency_ms gauge\n");
        for (stage, stats) in &self.stage_latencies {
            if stats.count > 0 {
                output.push_str(&format!("feedme_stage_latency_ms_sum{{stage=\"{}\"}} {}\n", stage, stats.sum));
                output.push_str(&format!("feedme_stage_latency_ms_count{{stage=\"{}\"}} {}\n", stage, stats.count));
                output.push_str(&format!("feedme_stage_latency_ms_min{{stage=\"{}\"}} {}\n", stage, stats.min));
                output.push_str(&format!("feedme_stage_latency_ms_max{{stage=\"{}\"}} {}\n", stage, stats.max));
            }
        }
        output.push_str("# HELP feedme_drop_reasons_total Drop reasons\n");
        output.push_str("# TYPE feedme_drop_reasons_total counter\n");
        for (reason, count) in &self.drop_reasons {
            output.push_str(&format!("feedme_drop_reasons_total{{reason=\"{}\"}} {}\n", reason, count));
        }
        output
    }

    pub fn to_json_logs(&self) -> Vec<String> {
        let mut logs = Vec::new();
        logs.push(serde_json::json!({
            "metric": "events_processed",
            "value": self.events_processed
        }).to_string());
        logs.push(serde_json::json!({
            "metric": "events_dropped",
            "value": self.events_dropped
        }).to_string());
        logs.push(serde_json::json!({
            "metric": "errors",
            "value": self.errors
        }).to_string());
        for (stage, stats) in &self.stage_latencies {
            logs.push(serde_json::json!({
                "metric": "stage_latencies",
                "stage": stage,
                "count": stats.count,
                "sum": stats.sum,
                "min": stats.min,
                "max": stats.max
            }).to_string());
        }
        for (reason, count) in &self.drop_reasons {
            logs.push(serde_json::json!({
                "metric": "drop_reasons",
                "reason": reason,
                "count": count
            }).to_string());
        }
        logs
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
        self.metrics.increment_processed();
        let mut current = Some(event);
        for stage in &mut self.stages {
            if let Some(evt) = current {
                let start = Instant::now();
                match stage.execute(evt) {
                    Ok(opt) => {
                        let duration = start.elapsed().as_secs_f64() * 1000.0;
                        self.metrics.record_latency(stage.name(), duration);
                        current = opt;
                        if current.is_none() && !stage.is_output() {
                            self.metrics.increment_dropped(DropReason::Filtered);
                        }
                    }
                    Err(e) => {
                        self.metrics.increment_errors();
                        return Err(e);
                    }
                }
            } else {
                break;
            }
        }
        Ok(current)
    }

    pub fn export_prometheus(&self) -> String {
        self.metrics.to_prometheus()
    }

    pub fn export_json_logs(&self) -> Vec<String> {
        self.metrics.to_json_logs()
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
    pub fn process_input(&mut self, pipeline: &mut Pipeline, deadletter: &mut Option<&mut dyn Stage>) -> Result<(), PipelineError> {
        match self {
            InputSource::Stdin => {
                let stdin = io::stdin();
                let lines = stdin.lines();
                for line in lines {
                    let line = line.map_err(|e| PipelineError::System(SystemError {
                        stage: "Input_Stdin".to_string(),
                        code: "IO_ERROR".to_string(),
                        message: e.to_string(),
                    }))?;
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
                                    code: "PARSE_ERROR".to_string(),
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
                let file = fs::File::open(path).map_err(|e| PipelineError::System(SystemError {
                    stage: "Input_File".to_string(),
                    code: "IO_ERROR".to_string(),
                    message: e.to_string(),
                }))?;
                let lines = io::BufReader::new(file).lines();
                for line in lines {
                    let line = line.map_err(|e| PipelineError::System(SystemError {
                        stage: "Input_File".to_string(),
                        code: "IO_ERROR".to_string(),
                        message: e.to_string(),
                    }))?;
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
                                    code: "PARSE_ERROR".to_string(),
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
                let entries = fs::read_dir(dir).map_err(|e| PipelineError::System(SystemError {
                    stage: "Input_Directory".to_string(),
                    code: "IO_ERROR".to_string(),
                    message: e.to_string(),
                }))?;
                let mut paths: Vec<PathBuf> = Vec::new();
                for entry in entries {
                    let entry = entry.map_err(|e| PipelineError::System(SystemError {
                        stage: "Input_Directory".to_string(),
                        code: "IO_ERROR".to_string(),
                        message: e.to_string(),
                    }))?;
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
        let s = std::str::from_utf8(raw).map_err(|e| PipelineError::Parse(ParseError {
            stage: "NDJSON".to_string(),
            code: "UTF8_ERROR".to_string(),
            message: e.to_string(),
        }))?;
        Event::from_raw_input(s).map_err(|e| PipelineError::Parse(ParseError {
            stage: "NDJSON".to_string(),
            code: "JSON_ERROR".to_string(),
            message: e.to_string(),
        }))
    }
}

pub struct JSONArrayParser;

impl Parser for JSONArrayParser {
    fn parse(&self, raw: &[u8]) -> Result<Event, PipelineError> {
        let s = std::str::from_utf8(raw).map_err(|e| PipelineError::Parse(ParseError {
            stage: "JSONArray".to_string(),
            code: "UTF8_ERROR".to_string(),
            message: e.to_string(),
        }))?;
        let value: serde_json::Value = serde_json::from_str(s).map_err(|e| PipelineError::Parse(ParseError {
            stage: "JSONArray".to_string(),
            code: "JSON_ERROR".to_string(),
            message: e.to_string(),
        }))?;
        // For array, perhaps wrap in an event with the array as data
        Ok(Event { data: value, metadata: None })
    }
}

pub struct SyslogParser;

impl Parser for SyslogParser {
    fn parse(&self, raw: &[u8]) -> Result<Event, PipelineError> {
        // Best effort syslog parsing: simple regex or basic parsing
        let s = std::str::from_utf8(raw).map_err(|e| PipelineError::Parse(ParseError {
            stage: "Syslog".to_string(),
            code: "UTF8_ERROR".to_string(),
            message: e.to_string(),
        }))?;
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
    derivations: HashMap<String, Box<dyn Fn(&Event) -> serde_json::Value>>,
}

impl DerivedFields {
    pub fn new(derivations: HashMap<String, Box<dyn Fn(&Event) -> serde_json::Value>>) -> Self {
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
                        code: "MISSING_FIELD".to_string(),
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
                            code: "TYPE_MISMATCH".to_string(),
                            message: format!("Field {} expected {} but got {}", field, expected_type, actual_type),
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
    constraints: HashMap<String, Box<dyn Fn(&serde_json::Value) -> bool>>,
}

impl ValueConstraints {
    pub fn new(constraints: HashMap<String, Box<dyn Fn(&serde_json::Value) -> bool>>) -> Self {
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
                            code: "CONSTRAINT_VIOLATION".to_string(),
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
        println!("{}", serde_json::to_string(&event.data).map_err(|e| PipelineError::Output(OutputError {
            stage: "Stdout".to_string(),
            code: "SERIALIZE_ERROR".to_string(),
            message: e.to_string(),
        }))?);
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
        let mut file = fs::OpenOptions::new().append(true).create(true).open(&self.path).map_err(|e| PipelineError::Output(OutputError {
            stage: "File".to_string(),
            code: "IO_ERROR".to_string(),
            message: e.to_string(),
        }))?;
        writeln!(file, "{}", serde_json::to_string(&event.data).map_err(|e| PipelineError::Output(OutputError {
            stage: "File".to_string(),
            code: "SERIALIZE_ERROR".to_string(),
            message: e.to_string(),
        }))?).map_err(|e| PipelineError::Output(OutputError {
            stage: "File".to_string(),
            code: "IO_ERROR".to_string(),
            message: e.to_string(),
        }))?;
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
        let mut file = fs::OpenOptions::new().append(true).create(true).open(&self.path).map_err(|e| PipelineError::Output(OutputError {
            stage: "Deadletter".to_string(),
            code: "IO_ERROR".to_string(),
            message: e.to_string(),
        }))?;
        writeln!(file, "{}", serde_json::to_string(&event).map_err(|e| PipelineError::Output(OutputError {
            stage: "Deadletter".to_string(),
            code: "SERIALIZE_ERROR".to_string(),
            message: e.to_string(),
        }))?).map_err(|e| PipelineError::Output(OutputError {
            stage: "Deadletter".to_string(),
            code: "IO_ERROR".to_string(),
            message: e.to_string(),
        }))?;
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
#[derive(serde::Deserialize)]
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
    plugins: HashMap<String, Box<dyn Fn() -> Box<dyn Stage>>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        PluginRegistry { plugins: HashMap::new() }
    }

    pub fn register(&mut self, name: String, factory: Box<dyn Fn() -> Box<dyn Stage>>) {
        self.plugins.insert(name, factory);
    }

    pub fn get_stage(&self, name: &str) -> Option<Box<dyn Stage>> {
        self.plugins.get(name).map(|f| f())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_creation() {
        let data = serde_json::json!({"key": "value"});
        let event = Event { data, metadata: None };
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
        assert_eq!(filtered.data.get("message"), Some(&serde_json::json!("test")));
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
            code: "TEST".to_string(),
            message: "test".to_string(),
        });
        assert_eq!(parse_err.category(), "Parse");
        assert_eq!(parse_err.stage(), "test");
        assert_eq!(parse_err.code(), "TEST");
        assert_eq!(parse_err.message(), "test");
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
}
