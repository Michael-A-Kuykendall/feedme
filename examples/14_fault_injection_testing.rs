/// Example 14 — Fault Injection Testing
///
/// The thing you want to know before going to production: what actually
/// happens when a stage breaks mid-stream?
///
/// FaultInjector lets you inject failures, timeouts, and resource exhaustion
/// into any wrapped stage — without touching the stage's code. Run your full
/// pipeline with real data and real fault scenarios before you ever deploy.
use feedme::fault_injection::FaultInjector;
use feedme::*;
use serde_json::json;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Build the pipeline ──────────────────────────────────────────────────
    let mut injector = FaultInjector::new();

    // Wrap the transform stage so we can inject faults into it
    let wrapped = injector.wrap_and_register(
        "enrich",
        Box::new(FieldSelect::new(vec![
            "level".into(),
            "message".into(),
            "user".into(),
        ])),
    );

    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".into()])));
    pipeline.add_stage(Box::new(wrapped.stage)); // ← the fault-injectable stage
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // Use temp for hardening user-surface testing
    let dl_path: PathBuf = std::env::temp_dir().join(format!("feedme_fault_deadletter_{}.ndjson", std::process::id()));
    let mut deadletter = Deadletter::new(dl_path.clone());

    let events = vec![
        json!({"level": "info",  "message": "started",        "user": "alice"}),
        json!({"level": "warn",  "message": "slow response",  "user": "bob"}),
        json!({"level": "info",  "message": "completed",      "user": "alice"}),
        json!({"level": "error", "message": "db error",       "user": "charlie"}),
    ];

    // ── Pass 1: normal operation ────────────────────────────────────────────
    println!("=== Pass 1: normal ===");
    for data in &events {
        let ev = Event { data: data.clone(), metadata: None };
        match pipeline.process_event(ev) {
            Ok(Some(_)) => {}
            Ok(None) => {}
            Err(e) => {
                eprintln!("  pipeline error: {}", e);
            }
        }
    }
    println!("Processed: {}, Errors: {}", pipeline.events_processed(), pipeline.error_count());

    // ── Inject a failure: the enrich stage errors on the next 2 events ─────
    injector.activate_failure("enrich", "upstream enrichment service down", Some(2))?;

    // ── Pass 2: with fault active ───────────────────────────────────────────
    println!("\n=== Pass 2: fault injected (2 failures, then auto-clears) ===");
    for data in events.iter().chain(events.iter()) {
        let ev = Event { data: data.clone(), metadata: None };
        match pipeline.process_event(ev) {
            Ok(Some(_)) => {}
            Ok(None) => {}
            Err(e) => {
                eprintln!("  caught fault: {}", e);
                // In production you'd send to deadletter, retry, etc.
                let dead_ev = Event { data: json!({"error": e.to_string()}), metadata: None };
                let _ = deadletter.execute(dead_ev);
            }
        }
    }
    println!(
        "Processed: {}, Errors: {} (fault injected 2 of them)",
        pipeline.events_processed(),
        pipeline.error_count()
    );

    // ── After 2 faults, the stage auto-clears and works normally again ──────
    println!("\nFault cleared — pipeline is healthy again.");

    // Harden for user-surface: assert injected faults and cleanup
    assert!(pipeline.error_count() >= 2);
    let _ = std::fs::remove_file(dl_path);

    Ok(())
}
