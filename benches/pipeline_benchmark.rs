use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use std::hint::black_box;
use feedme::{Event, Pipeline, FieldSelect, RequiredFields, StdoutOutput};
#[cfg(feature = "fused")]
use feedme::fused::{FusedRuleEngine, Rule, FieldType};

fn make_events(n: usize) -> Vec<Event> {
    (0..n).map(|i| Event {
        data: serde_json::json!({
            "timestamp": "2023-10-01T10:00:00Z",
            "level": if i % 10 == 0 { "error" } else { "info" },
            "message": format!("Test message {}", i),
            "user_id": i
        }),
        metadata: None,
    }).collect()
}

fn benchmark_pipeline_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_scale");

    for size in [100, 1000, 10000].iter() {
        let events = make_events(*size);

        group.bench_with_input(BenchmarkId::new("select_required", size), size, |b, _| {
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

        // Output sink cost (realistic "bells" usage)
        group.bench_with_input(BenchmarkId::new("select_required_stdout", size), size, |b, _| {
            b.iter(|| {
                let mut pipeline = Pipeline::new();
                pipeline.add_stage(Box::new(FieldSelect::new(vec!["level".to_string(), "message".to_string()])));
                pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".to_string()])));
                pipeline.add_stage(Box::new(StdoutOutput::new()));
                for event in &events {
                    black_box(pipeline.process_event(event.clone())).ok();
                }
            })
        });
    }

    #[cfg(feature = "fused")]
    {
        let events = make_events(1000);
        group.bench_function("fused_validation_1000_events", |b| {
            b.iter(|| {
                let mut engine = FusedRuleEngine::builder("bench")
                    .require(Rule::exists("level"))
                    .require(Rule::type_is("level", FieldType::String))
                    .require(Rule::one_of("level", vec![serde_json::json!("info"), serde_json::json!("error")]))
                    .on_fail(feedme::fused::FailAction::DropEvent)
                    .build();
                let mut pipeline = Pipeline::new();
                pipeline.add_stage(Box::new(engine));
                for event in &events {
                    black_box(pipeline.process_event(event.clone())).ok();
                }
            })
        });
    }

    group.finish();
}

criterion_group!(benches, benchmark_pipeline_processing);
criterion_main!(benches);
