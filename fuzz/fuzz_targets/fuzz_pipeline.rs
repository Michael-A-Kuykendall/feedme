#![no_main]

use libfuzzer_sys::fuzz_target;
use feedme::{Pipeline, FieldSelect, RequiredFields, StdoutOutput};
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    // Convert fuzzer input to string, split by newlines for NDJSON
    if let Ok(input_str) = std::str::from_utf8(data) {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".to_string(), "message".to_string()])));
        pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".to_string()])));
        pipeline.add_stage(Box::new(StdoutOutput::new()));

        // Create a cursor from the input data
        let cursor = Cursor::new(input_str.as_bytes());
        
        // Try to process - we don't care about success/failure, just that it doesn't crash
        let _ = feedme::InputSource::NdjsonStream(cursor).process_input(&mut pipeline, &mut None);
    }
});