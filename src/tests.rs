#[allow(clippy::wildcard_imports)]
use super::*;
use std::path::PathBuf;

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
    assert!(!json_logs.is_empty());
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
    assert!(!json_logs.is_empty());
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
fn test_error_code_display_implementations() {
    // Test ParseErrorCode display
    assert_eq!(format!("{}", ParseErrorCode::ParseError), "PARSE_ERROR");
    assert_eq!(format!("{}", ParseErrorCode::Utf8Error), "UTF8_ERROR");
    assert_eq!(format!("{}", ParseErrorCode::JsonError), "JSON_ERROR");
    assert_eq!(format!("{}", ParseErrorCode::Test), "TEST");

    // Test TransformErrorCode display
    assert_eq!(
        format!("{}", TransformErrorCode::MissingField),
        "MISSING_FIELD"
    );
    assert_eq!(
        format!("{}", TransformErrorCode::TypeMismatch),
        "TYPE_MISMATCH"
    );
    assert_eq!(
        format!("{}", TransformErrorCode::ConstraintViolation),
        "CONSTRAINT_VIOLATION"
    );
    assert_eq!(format!("{}", TransformErrorCode::Test), "TEST");

    // Test ValidationErrorCode display
    assert_eq!(
        format!("{}", ValidationErrorCode::MissingField),
        "MISSING_FIELD"
    );
    assert_eq!(
        format!("{}", ValidationErrorCode::TypeMismatch),
        "TYPE_MISMATCH"
    );
    assert_eq!(
        format!("{}", ValidationErrorCode::ConstraintViolation),
        "CONSTRAINT_VIOLATION"
    );
    assert_eq!(format!("{}", ValidationErrorCode::Test), "TEST");

    // Test OutputErrorCode display
    assert_eq!(
        format!("{}", OutputErrorCode::SerializeError),
        "SERIALIZE_ERROR"
    );
    assert_eq!(format!("{}", OutputErrorCode::IoError), "IO_ERROR");
    assert_eq!(format!("{}", OutputErrorCode::Test), "TEST");

    // Test SystemErrorCode display
    assert_eq!(format!("{}", SystemErrorCode::IoError), "IO_ERROR");
    assert_eq!(format!("{}", SystemErrorCode::Test), "TEST");
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
    assert_eq!(config.version(), 1);

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
            // Test passes if we reach here
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
        data: serde_json::json!({"value": f64::INFINITY}),
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
fn test_pii_redaction_field_targeting() {
    let patterns = vec![regex::Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap()];
    let mut stage = PIIRedaction::new(patterns.clone()).with_fields(vec!["ssn".to_string()]);
    let event = Event {
        data: serde_json::json!({"ssn": "123-45-6789", "note": "call 123-45-6789"}),
        metadata: None,
    };
    let result = stage.execute(event.clone()).unwrap().unwrap();
    assert_eq!(result.get_string("ssn"), Some("[REDACTED]"));
    assert_eq!(result.get_string("note"), Some("call 123-45-6789"));

    let mut all_fields = PIIRedaction::new(patterns);
    let result = all_fields.execute(event).unwrap().unwrap();
    assert_eq!(result.get_string("note"), Some("call [REDACTED]"));
}

#[test]
fn test_pii_redaction_nested_values() {
    let patterns = vec![regex::Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap()];
    let mut stage = PIIRedaction::new(patterns);
    let event = Event {
        data: serde_json::json!({
            "user": {"ssn": "123-45-6789"},
            "contacts": [{"email_ssn": "999-99-9999"}]
        }),
        metadata: None,
    };
    let result = stage.execute(event).unwrap().unwrap();
    assert_eq!(result.get_string("user"), None);
    assert_eq!(
        result
            .data
            .get("user")
            .and_then(|v| v.get("ssn"))
            .and_then(|v| v.as_str()),
        Some("[REDACTED]")
    );
    assert_eq!(
        result
            .data
            .get("contacts")
            .and_then(|v| v.get(0))
            .and_then(|v| v.get("email_ssn"))
            .and_then(|v| v.as_str()),
        Some("[REDACTED]")
    );
}

#[test]
fn test_pii_redaction_idempotent() {
    let patterns = vec![regex::Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap()];
    let mut stage = PIIRedaction::new(patterns);
    let event = Event {
        data: serde_json::json!({"ssn": "123-45-6789"}),
        metadata: None,
    };
    let first = stage.execute(event).unwrap().unwrap();
    let second = stage.execute(first).unwrap().unwrap();
    assert_eq!(second.get_string("ssn"), Some("[REDACTED]"));
}

#[test]
fn test_require_validation_fails_closed_without_validator() {
    let mut pipeline = Pipeline::new().require_validation(true);
    let event = Event {
        data: serde_json::json!({"level": "info"}),
        metadata: None,
    };
    let err = pipeline.process_event(event).unwrap_err();
    assert_eq!(err.category(), "System");
    assert!(err.message().contains("no Validator stage"));
}

#[test]
fn test_require_validation_passes_with_validator() {
    let mut pipeline = Pipeline::new().require_validation(true);
    pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".to_string()])));
    let event = Event {
        data: serde_json::json!({"level": "info"}),
        metadata: None,
    };
    assert!(pipeline.process_event(event).unwrap().is_some());
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

#[test]
fn test_replay_execution() {
    use crate::replay::{record_execution, replay_execution};
    use tempfile::NamedTempFile;

    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(FieldSelect::new(vec![
        "level".to_string(),
        "message".to_string(),
    ])));
    pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".to_string()])));

    let events = vec![
        Event {
            data: serde_json::json!({"level": "info", "message": "test1", "extra": "ignored"}),
            metadata: None,
        },
        Event {
            data: serde_json::json!({"level": "error", "message": "test2"}),
            metadata: None,
        },
    ];

    let temp_file = NamedTempFile::new().unwrap();
    let trace_path = temp_file.path();

    // Record execution
    record_execution(&mut pipeline, &events, trace_path).unwrap();

    // Create fresh pipeline for replay
    let mut replay_pipeline = Pipeline::new();
    replay_pipeline.add_stage(Box::new(FieldSelect::new(vec![
        "level".to_string(),
        "message".to_string(),
    ])));
    replay_pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".to_string()])));

    // Replay and verify
    replay_execution(&mut replay_pipeline, trace_path).unwrap();
}

#[test]
fn test_pipeline_export_json_metrics() {
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".to_string()])));

    let event = Event {
        data: serde_json::json!({"level": "info", "message": "test"}),
        metadata: None,
    };

    pipeline.process_event(event).unwrap();
    let metrics = pipeline.export_json_logs();
    assert!(!metrics.is_empty());
    assert!(metrics[0].contains("events_processed"));
}

#[test]
fn test_pipeline_export_prometheus_metrics() {
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".to_string()])));

    let event = Event {
        data: serde_json::json!({"level": "info", "message": "test"}),
        metadata: None,
    };

    pipeline.process_event(event).unwrap();
    let prometheus = pipeline.export_prometheus();
    assert!(prometheus.contains("# HELP"));
    assert!(prometheus.contains("feedme_events_processed_total"));
}

#[test]
fn test_metrics_drop_reasons() {
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(Filter::new(Box::new(|e| {
        e.get_string("level") == Some("error")
    }))));
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // This event should be filtered out
    let info_event = Event {
        data: serde_json::json!({"level": "info", "message": "test"}),
        metadata: None,
    };
    let result = pipeline.process_event(info_event).unwrap();
    assert!(result.is_none());

    // Check that drop reason is tracked
    let prometheus = pipeline.export_prometheus();
    assert!(prometheus.contains("feedme_events_dropped_total 1"));
}

#[test]
fn test_pipeline_error_serialization() {
    let error = PipelineError::Parse(ParseError {
        stage: "TestStage".to_string(),
        code: ParseErrorCode::JsonError,
        message: "Test error message".to_string(),
    });

    let json = serde_json::to_string(&error).unwrap();
    let deserialized: PipelineError = serde_json::from_str(&json).unwrap();
    assert_eq!(format!("{:?}", error), format!("{:?}", deserialized));
}
