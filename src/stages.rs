use crate::error::{
    OutputError, OutputErrorCode, PipelineError, ValidationError, ValidationErrorCode,
};
use crate::event::Event;
use crate::pipeline::Stage;
use crate::replay_spec;
use crate::EventDerivationFn;
use crate::ValueConstraintFn;
use regex::Regex;
use serde_json;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
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

    fn to_replay_spec(&self) -> Option<replay_spec::StageSpec> {
        Some(<Self as replay_spec::ReplayableStage>::to_spec(self))
    }
}

impl Transform for FieldSelect {}

impl replay_spec::ReplayableStage for FieldSelect {
    fn stage_id() -> replay_spec::StageId {
        "field_select".to_string()
    }

    fn stage_version() -> replay_spec::StageVersion {
        "1.0".to_string()
    }

    fn to_spec(&self) -> replay_spec::StageSpec {
        replay_spec::StageSpec {
            stage_id: Self::stage_id(),
            stage_version: Self::stage_version(),
            config: serde_json::json!({
                "fields": self.fields
            }),
        }
    }
}

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

impl replay_spec::ReplayableStage for FieldRemap {
    fn stage_id() -> replay_spec::StageId {
        "field_remap".to_string()
    }

    fn stage_version() -> replay_spec::StageVersion {
        "1.0".to_string()
    }

    fn to_spec(&self) -> replay_spec::StageSpec {
        replay_spec::StageSpec {
            stage_id: Self::stage_id(),
            stage_version: Self::stage_version(),
            config: serde_json::json!({
                "mappings": self.mappings
            }),
        }
    }
}

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

impl replay_spec::ReplayableStage for PIIRedaction {
    fn stage_id() -> replay_spec::StageId {
        "pii_redaction".to_string()
    }

    fn stage_version() -> replay_spec::StageVersion {
        "1.0".to_string()
    }

    fn to_spec(&self) -> replay_spec::StageSpec {
        let patterns: Vec<String> = self
            .patterns
            .iter()
            .map(|r| r.as_str().to_string())
            .collect();
        replay_spec::StageSpec {
            stage_id: Self::stage_id(),
            stage_version: Self::stage_version(),
            config: serde_json::json!({
                "patterns": patterns
            }),
        }
    }
}

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

impl replay_spec::ReplayableStage for DerivedFields {
    fn stage_id() -> replay_spec::StageId {
        "derived_fields".to_string()
    }

    fn stage_version() -> replay_spec::StageVersion {
        "1.0".to_string()
    }

    fn to_spec(&self) -> replay_spec::StageSpec {
        replay_spec::StageSpec {
            stage_id: Self::stage_id(),
            stage_version: Self::stage_version(),
            config: serde_json::json!({
                "derivations": "user-provided-closures"
            }),
        }
    }
}

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

impl replay_spec::ReplayableStage for Filter {
    fn stage_id() -> replay_spec::StageId {
        "filter".to_string()
    }

    fn stage_version() -> replay_spec::StageVersion {
        "1.0".to_string()
    }

    fn to_spec(&self) -> replay_spec::StageSpec {
        replay_spec::StageSpec {
            stage_id: Self::stage_id(),
            stage_version: Self::stage_version(),
            config: serde_json::json!({
                "predicate": "user-provided-closure"
            }),
        }
    }
}

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

    fn to_replay_spec(&self) -> Option<replay_spec::StageSpec> {
        Some(<Self as replay_spec::ReplayableStage>::to_spec(self))
    }
}

impl Validator for RequiredFields {}

impl replay_spec::ReplayableStage for RequiredFields {
    fn stage_id() -> replay_spec::StageId {
        "required_fields".to_string()
    }

    fn stage_version() -> replay_spec::StageVersion {
        "1.0".to_string()
    }

    fn to_spec(&self) -> replay_spec::StageSpec {
        replay_spec::StageSpec {
            stage_id: Self::stage_id(),
            stage_version: Self::stage_version(),
            config: serde_json::json!({
                "fields": self.fields
            }),
        }
    }
}

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

impl replay_spec::ReplayableStage for TypeChecking {
    fn stage_id() -> replay_spec::StageId {
        "type_checking".to_string()
    }

    fn stage_version() -> replay_spec::StageVersion {
        "1.0".to_string()
    }

    fn to_spec(&self) -> replay_spec::StageSpec {
        replay_spec::StageSpec {
            stage_id: Self::stage_id(),
            stage_version: Self::stage_version(),
            config: serde_json::json!({
                "type_checks": self.type_checks
            }),
        }
    }
}

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

impl replay_spec::ReplayableStage for ValueConstraints {
    fn stage_id() -> replay_spec::StageId {
        "value_constraints".to_string()
    }

    fn stage_version() -> replay_spec::StageVersion {
        "1.0".to_string()
    }

    fn to_spec(&self) -> replay_spec::StageSpec {
        replay_spec::StageSpec {
            stage_id: Self::stage_id(),
            stage_version: Self::stage_version(),
            config: serde_json::json!({
                "constraints": "user-provided-closures"
            }),
        }
    }
}

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

    fn to_replay_spec(&self) -> Option<replay_spec::StageSpec> {
        Some(<Self as replay_spec::ReplayableStage>::to_spec(self))
    }
}

impl Output for StdoutOutput {}

impl replay_spec::ReplayableStage for StdoutOutput {
    fn stage_id() -> replay_spec::StageId {
        "stdout_output".to_string()
    }

    fn stage_version() -> replay_spec::StageVersion {
        "1.0".to_string()
    }

    fn to_spec(&self) -> replay_spec::StageSpec {
        replay_spec::StageSpec {
            stage_id: Self::stage_id(),
            stage_version: Self::stage_version(),
            config: serde_json::json!({}),
        }
    }
}

impl Default for StdoutOutput {
    fn default() -> Self {
        Self::new()
    }
}

pub struct FileOutput {
    path: PathBuf,
    // Handle kept open for efficiency (one open per stage lifetime, not per event).
    // Use BufWriter for better throughput; flushes on drop. For strict no-buffer semantics
    // prefer custom output stage or StdoutOutput. This is a backstop improvement within
    // the "no hidden buffering in the *processing* pipeline" rule.
    file: Option<std::io::BufWriter<std::fs::File>>,
}

impl FileOutput {
    pub fn new(path: PathBuf) -> Self {
        FileOutput { path, file: None }
    }
}

impl Stage for FileOutput {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError> {
        use std::io::{BufWriter, Write};
        if self.file.is_none() {
            let f = fs::OpenOptions::new()
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
            self.file = Some(BufWriter::new(f));
        }
        let writer = self.file.as_mut().unwrap();
        writeln!(
            writer,
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
        writer.flush().map_err(|e| {
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

impl replay_spec::ReplayableStage for FileOutput {
    fn stage_id() -> replay_spec::StageId {
        "file_output".to_string()
    }

    fn stage_version() -> replay_spec::StageVersion {
        "1.0".to_string()
    }

    fn to_spec(&self) -> replay_spec::StageSpec {
        replay_spec::StageSpec {
            stage_id: Self::stage_id(),
            stage_version: Self::stage_version(),
            config: serde_json::json!({
                "path": self.path
            }),
        }
    }
}

pub struct Deadletter {
    path: PathBuf,
    file: Option<std::io::BufWriter<std::fs::File>>,
}

impl Deadletter {
    pub fn new(path: PathBuf) -> Self {
        Deadletter { path, file: None }
    }
}

impl Stage for Deadletter {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError> {
        use std::io::{BufWriter, Write};
        if self.file.is_none() {
            let f = fs::OpenOptions::new()
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
            self.file = Some(BufWriter::new(f));
        }
        let writer = self.file.as_mut().unwrap();
        writeln!(
            writer,
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
        writer.flush().map_err(|e| {
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

impl replay_spec::ReplayableStage for Deadletter {
    fn stage_id() -> replay_spec::StageId {
        "deadletter".to_string()
    }

    fn stage_version() -> replay_spec::StageVersion {
        "1.0".to_string()
    }

    fn to_spec(&self) -> replay_spec::StageSpec {
        replay_spec::StageSpec {
            stage_id: Self::stage_id(),
            stage_version: Self::stage_version(),
            config: serde_json::json!({
                "path": self.path
            }),
        }
    }
}
