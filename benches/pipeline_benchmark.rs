use criterion::{black_box, criterion_group, criterion_main, Criterion};
use feedme::{Pipeline, FieldSelect, RequiredFields, StdoutOutput, InputSource};
use std::io::Cursor;

fn benchmark_pipeline_processing(c: &mut Criterion) {
    // Create test data
    let test_data = r#"{"timestamp":"2023-10-01T10:00:00Z","level":"info","message":"Test message 1","user_id":"123"}
{"timestamp":"2023-10-01T10:01:00Z","level":"error","message":"Test message 2","user_id":"456"}
{"timestamp":"2023-10-01T10:02:00Z","level":"warn","message":"Test message 3","user_id":"789"}"#;

    c.bench_function("pipeline_process_3_events", |b| {
        b.iter(|| {
            let mut pipeline = Pipeline::new();
            pipeline.add_stage(Box::new(FieldSelect::new(vec![
                "timestamp".to_string(),
                "level".to_string(),
                "message".to_string()
            ])));
            pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".to_string()])));

            let cursor = Cursor::new(test_data.as_bytes());
            let mut input = InputSource::NdjsonStream(cursor);
            let mut deadletter = None;

            black_box(input.process_input(&mut pipeline, &mut deadletter)).unwrap();
        })
    });
}

criterion_group!(benches, benchmark_pipeline_processing);
criterion_main!(benches);