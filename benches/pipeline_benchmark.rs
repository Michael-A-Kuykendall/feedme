use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
#[cfg(feature = "fused")]
use feedme::fused::{FailAction, FieldType, FusedRuleEngine, Rule};
use feedme::{Event, FieldSelect, Filter, Pipeline, RequiredFields, StdoutOutput};
use std::hint::black_box;

fn make_events(n: usize) -> Vec<Event> {
    (0..n)
        .map(|i| Event {
            data: serde_json::json!({
                "timestamp": "2023-10-01T10:00:00Z",
                "level": if i % 10 == 0 { "error" } else { "info" },
                "message": format!("Test message {}", i),
                "user_id": i
            }),
            metadata: None,
        })
        .collect()
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
        group.bench_with_input(
            BenchmarkId::new("select_required_stdout", size),
            size,
            |b, _| {
                b.iter(|| {
                    let mut pipeline = Pipeline::new();
                    pipeline.add_stage(Box::new(FieldSelect::new(vec![
                        "level".to_string(),
                        "message".to_string(),
                    ])));
                    pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".to_string()])));
                    pipeline.add_stage(Box::new(StdoutOutput::new()));
                    for event in &events {
                        black_box(pipeline.process_event(event.clone())).ok();
                    }
                })
            },
        );
    }

    #[cfg(feature = "fused")]
    {
        let events = make_events(1000);
        group.bench_function("fused_validation_1000_events", |b| {
            b.iter(|| {
                let engine = FusedRuleEngine::builder("bench")
                    .require(Rule::exists("level"))
                    .require(Rule::type_is("level", FieldType::String))
                    .require(Rule::one_of(
                        "level",
                        vec![serde_json::json!("info"), serde_json::json!("error")],
                    ))
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

/// Tests the FSE's actual marketed claim: "runtime stays independent of rule
/// count for shared selectors" (README, "Fused Rule Engine"). Sweeps rule
/// count M (all rules sharing the single "count" selector field) and compares:
///
/// - `fused`: one `FusedRuleEngine` with M rules -> selector extracted once,
///   regardless of M (selector deduplication).
/// - `naive_stages`: M separate `Filter` stages, each independently
///   re-fetching the same field from the event -- the "flat, rule-first"
///   approach the fused engine is designed to beat.
///
/// If the claim holds, `fused/<M>` should grow much more slowly than
/// `naive_stages/<M>` as M increases (ideally near-flat per-event cost).
#[cfg(feature = "fused")]
fn benchmark_fused_rule_count_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("fused_vs_naive_rule_count");
    let events = make_events(1000);

    for rule_count in [1usize, 5, 10, 25, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("fused", rule_count),
            rule_count,
            |b, &n| {
                b.iter(|| {
                    let mut builder = FusedRuleEngine::builder("scaling-bench");
                    for i in 0..n {
                        // All rules read the same "user_id" field (shared
                        // selector); each rule only fires past its own
                        // threshold so most events pass most rules.
                        builder = builder.require(Rule::greater_than("user_id", -(i as f64) - 1.0));
                    }
                    let engine = builder.on_fail(FailAction::DropEvent).build();
                    let mut pipeline = Pipeline::new();
                    pipeline.add_stage(Box::new(engine));
                    for event in &events {
                        black_box(pipeline.process_event(event.clone())).ok();
                    }
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("naive_stages", rule_count),
            rule_count,
            |b, &n| {
                b.iter(|| {
                    let mut pipeline = Pipeline::new();
                    for i in 0..n {
                        let threshold = -(i as f64) - 1.0;
                        pipeline.add_stage(Box::new(Filter::new(Box::new(move |e: &Event| {
                            e.data
                                .get("user_id")
                                .and_then(|v| v.as_f64())
                                .map(|v| v > threshold)
                                .unwrap_or(false)
                        }))));
                    }
                    for event in &events {
                        black_box(pipeline.process_event(event.clone())).ok();
                    }
                })
            },
        );
    }

    group.finish();
}

#[cfg(feature = "fused")]
criterion_group!(
    benches,
    benchmark_pipeline_processing,
    benchmark_fused_rule_count_scaling
);
#[cfg(not(feature = "fused"))]
criterion_group!(benches, benchmark_pipeline_processing);
criterion_main!(benches);
