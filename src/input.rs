use crate::error::{ParseError, ParseErrorCode, PipelineError, SystemError, SystemErrorCode};
use crate::event::Event;
use crate::pipeline::{Pipeline, Stage};
use serde_json;
use std::fs;
use std::io::{self, BufRead};
use std::path::PathBuf;
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
                                if let Err(dl_err) = dl.execute(error_event) {
                                    eprintln!("[feedme] deadletter write failed ΓÇö stdin parse error record lost: {}", dl_err);
                                }
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
                                if let Err(dl_err) = dl.execute(error_event) {
                                    eprintln!("[feedme] deadletter write failed ΓÇö stdin pipeline error record lost: {}", dl_err);
                                }
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
                                if let Err(dl_err) = dl.execute(error_event) {
                                    eprintln!("[feedme] deadletter write failed ΓÇö file parse error record lost: {}", dl_err);
                                }
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
                                if let Err(dl_err) = dl.execute(error_event) {
                                    eprintln!("[feedme] deadletter write failed ΓÇö file pipeline error record lost: {}", dl_err);
                                }
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

/// Parsers: optional extension for custom byte-to-Event conversion.
/// Main `InputSource` uses direct JSON. Use for syslog or other formats via
/// custom stages or wrappers. The provided impls (NDJSON, JSONArray, Syslog)
/// are best-effort examples.
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
        // Simple syslog: <pri>timestamp host message. Raw string kept for downstream parsing.
        Ok(Event {
            data: serde_json::json!({ "message": s }),
            metadata: None,
        })
    }
}
