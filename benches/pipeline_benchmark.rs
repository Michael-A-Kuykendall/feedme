use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use feedme::{Event, Pipeline, FieldSelect, RequiredFields};

fn benchmark_pipeline_processing(c: &mut Criterion) {
    // Build in-memory test events once; avoid I/O inside the hot loop.
    let events: Vec<Event> = vec![
        Event {
            data: serde_json::json!({"timestamp":"2023-10-01T10:00:00Z","level":"info","message":"Test message 1","user_id":"123"}),
            metadata: None,
        },
        Event {
            data: serde_json::json!({"timestamp":"2023-10-01T10:01:00Z","level":"error","message":"Test message 2","user_id":"456"}),
            metadata: None,
        },
        Event {
            data: serde_json::json!({"timestamp":"2023-10-01T10:02:00Z","level":"warn","message":"Test message 3","user_id":"789"}),
            metadata: None,
        },
    ];

    c.bench_function("pipeline_process_3_events", |b| {
        b.iter(|| {
            let mut pipeline = Pipeline::new();
            pipeline.add_stage(Box::new(FieldSelect::new(vec![
                "timestamp".to_string(),
                "level".to_string(),
                "message".to_string(),
            ])));
            pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".to_string()])));

            for event in &events {
                black_box(pipeline.process_event(event.clone())).ok();
            }
        })
    });
}

criterion_group!(benches, benchmark_pipeline_processing);
criterion_main!(benches);
