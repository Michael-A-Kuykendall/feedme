#![no_main]

use libfuzzer_sys::fuzz_target;
use feedme::{Pipeline, FieldSelect, RequiredFields};
use std::io::{BufRead, Cursor};

fuzz_target!(|data: &[u8]| {
    // Convert fuzzer input to string, split by newlines for NDJSON
    if let Ok(input_str) = std::str::from_utf8(data) {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".to_string(), "message".to_string(), "email".to_string()])));
        // Use PII for bells-and-whistles fuzz coverage
        if let Ok(pii) = regex::Regex::new(r".*@.*") {
            pipeline.add_stage(Box::new(feedme::PIIRedaction::new(vec![pii])));
        }
        pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".to_string()])));

        // Create a cursor from the input data and try to parse as NDJSON
        let cursor = Cursor::new(input_str.as_bytes());
        let reader = std::io::BufReader::new(cursor);
        
        for line_result in reader.lines() {
            if let Ok(line) = line_result {
                if let Ok(event) = feedme::Event::from_raw_input(&line) {
                    let _ = pipeline.process_event(event);
                }
            }
        }
    }
});