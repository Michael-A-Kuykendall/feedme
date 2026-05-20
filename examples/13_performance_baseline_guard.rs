/// Example 13 — Performance Baseline Guard
///
/// The thing you'd write by hand for every production pipeline but never do:
/// capture a performance baseline, run more data, and assert no regression.
///
/// The PptManager does this in four lines. Plug it into CI and your pipeline
/// will tell you when something slows it down.
use feedme::invariant_ppt::PptManager;
use feedme::*;
use serde_json::json;

fn make_pipeline() -> Pipeline {
    let mut p = Pipeline::new();
    p.add_stage(Box::new(RequiredFields::new(vec!["level".into(), "message".into()])));
    p.add_stage(Box::new(Filter::new(Box::new(|ev| {
        ev.data.get("level").and_then(|v| v.as_str()) != Some("debug")
    }))));
    p.add_stage(Box::new(StdoutOutput::new()));
    p
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let events = vec![
        json!({"level": "info",  "message": "user logged in",  "user": "alice"}),
        json!({"level": "warn",  "message": "slow query",      "duration_ms": 450}),
        json!({"level": "debug", "message": "cache miss",      "key": "session:42"}),
        json!({"level": "error", "message": "connection lost", "host": "db-01"}),
        json!({"level": "info",  "message": "job completed",   "job": "nightly-export"}),
    ];

    // ── Build pipeline ──────────────────────────────────────────────────────
    let mut pipeline = make_pipeline();

    // ── Establish baseline after an initial warm-up pass ───────────────────
    for data in &events {
        let ev = Event { data: data.clone(), metadata: None };
        let _ = pipeline.process_event(ev);
    }

    let mut ppt = PptManager::new().with_regression_threshold(0.20); // 20% allowed
    ppt.establish_baseline(&pipeline);
    println!("Baseline captured — {} events processed", pipeline.events_processed());

    // ── Run more data (same pipeline, same conditions) ──────────────────────
    for data in events.iter().cycle().take(50) {
        let ev = Event { data: data.clone(), metadata: None };
        let _ = pipeline.process_event(ev);
    }

    // ── Check for regression ────────────────────────────────────────────────
    let report = ppt.check_regression(&pipeline)?;

    if report.has_regression {
        eprintln!(
            "REGRESSION DETECTED: {:?}",
            report.regression_details.as_ref().unwrap()
        );
        std::process::exit(1);
    } else {
        println!(
            "No regression. Latency: baseline={:.3}ms, current={:.3}ms ({:.1}% threshold)",
            report.baseline_latency_ms,
            report.current_latency_ms,
            report.threshold * 100.0
        );
    }

    println!("Total processed: {}", pipeline.events_processed());
    Ok(())
}
