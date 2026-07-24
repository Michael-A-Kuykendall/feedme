use serde::{Deserialize, Serialize};
use serde_json;
use std::fmt;
/// Error taxonomy for pipeline failures.
/// Explicit category, stage attribution, machine-readable code.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl From<serde_json::Error> for PipelineError {
    fn from(err: serde_json::Error) -> Self {
        PipelineError::System(SystemError {
            stage: "Serialization".to_string(),
            code: SystemErrorCode::SerializationError,
            message: format!("JSON serialization error: {}", err),
        })
    }
}

impl std::error::Error for PipelineError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParseErrorCode {
    ParseError,
    Utf8Error,
    JsonError,
    #[cfg(test)]
    Test,
}

impl fmt::Display for ParseErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseErrorCode::ParseError => write!(f, "PARSE_ERROR"),
            ParseErrorCode::Utf8Error => write!(f, "UTF8_ERROR"),
            ParseErrorCode::JsonError => write!(f, "JSON_ERROR"),
            #[cfg(test)]
            ParseErrorCode::Test => write!(f, "TEST"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseError {
    pub(crate) stage: String,
    pub(crate) code: ParseErrorCode,
    pub(crate) message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransformErrorCode {
    MissingField,
    TypeMismatch,
    ConstraintViolation,
    #[cfg(test)]
    Test,
}

impl fmt::Display for TransformErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransformErrorCode::MissingField => write!(f, "MISSING_FIELD"),
            TransformErrorCode::TypeMismatch => write!(f, "TYPE_MISMATCH"),
            TransformErrorCode::ConstraintViolation => write!(f, "CONSTRAINT_VIOLATION"),
            #[cfg(test)]
            TransformErrorCode::Test => write!(f, "TEST"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformError {
    pub(crate) stage: String,
    pub(crate) code: TransformErrorCode,
    pub(crate) message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationErrorCode {
    MissingField,
    TypeMismatch,
    ConstraintViolation,
    /// Used by [`crate::fault_injection`] to surface injected
    /// failures as structured pipeline errors.
    FaultInjected,
    #[cfg(test)]
    Test,
}

impl fmt::Display for ValidationErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationErrorCode::MissingField => write!(f, "MISSING_FIELD"),
            ValidationErrorCode::TypeMismatch => write!(f, "TYPE_MISMATCH"),
            ValidationErrorCode::ConstraintViolation => write!(f, "CONSTRAINT_VIOLATION"),
            ValidationErrorCode::FaultInjected => write!(f, "FAULT_INJECTED"),
            #[cfg(test)]
            ValidationErrorCode::Test => write!(f, "TEST"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    pub(crate) stage: String,
    pub(crate) code: ValidationErrorCode,
    pub(crate) message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputErrorCode {
    SerializeError,
    IoError,
    #[cfg(test)]
    Test,
}

impl fmt::Display for OutputErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputErrorCode::SerializeError => write!(f, "SERIALIZE_ERROR"),
            OutputErrorCode::IoError => write!(f, "IO_ERROR"),
            #[cfg(test)]
            OutputErrorCode::Test => write!(f, "TEST"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputError {
    pub(crate) stage: String,
    pub(crate) code: OutputErrorCode,
    pub(crate) message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemErrorCode {
    IoError,
    InvalidConfig,
    SerializationError,
    #[cfg(test)]
    Test,
}

impl fmt::Display for SystemErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SystemErrorCode::IoError => write!(f, "IO_ERROR"),
            SystemErrorCode::InvalidConfig => write!(f, "INVALID_CONFIG"),
            SystemErrorCode::SerializationError => write!(f, "SERIALIZATION_ERROR"),
            #[cfg(test)]
            SystemErrorCode::Test => write!(f, "TEST"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemError {
    pub(crate) stage: String,
    pub(crate) code: SystemErrorCode,
    pub(crate) message: String,
}
