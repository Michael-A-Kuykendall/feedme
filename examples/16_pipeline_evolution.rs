/// Example 16 — Pipeline Evolution (A/B Config Diff)
///
/// You refactor your pipeline — add a stage, rename one, remove another.
/// How do you know what changed between the deployed version and the new one?
///
/// ReplayManager serialises pipeline specs and diffs them structurally.
/// Plug this into your deployment pipeline to catch accidental changes
/// before they reach production.
use feedme::replay::ReplayManager;
use feedme::*;
use serde_json::json;

fn build_v1() -> Pipeline {
    let mut p = Pipeline::new();
    p.add_stage(Box::new(RequiredFields::new(vec!["level".into(), "message".into()])));
    p.add_stage(Box::new(Filter::new(Box::new(|ev| {
        ev.data.get("level").and_then(|v| v.as_str()) != Some("debug")
    }))));
    p.add_stage(Box::new(StdoutOutput::new()));
    p
}

fn build_v2() -> Pipeline {
    // v2 adds PII redaction and a field selector before the filter
    let mut p = Pipeline::new();
    p.add_stage(Box::new(PIIRedaction::new(vec![
        regex::Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b").unwrap(),
    ])));
    p.add_stage(Box::new(FieldSelect::new(vec![
        "level".into(),
        "message".into(),
        "user".into(),
    ])));
    p.add_stage(Box::new(RequiredFields::new(vec!["level".into(), "message".into()])));
    p.add_stage(Box::new(Filter::new(Box::new(|ev| {
        ev.data.get("level").and_then(|v| v.as_str()) != Some("debug")
    }))));
    p.add_stage(Box::new(StdoutOutput::new()));
    p
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline_v1 = build_v1();
    let pipeline_v2 = build_v2();

    let mut manager = ReplayManager::new();

    // ── Serialise both pipeline versions ───────────────────────────────────
    let spec_v1 = manager.serialize_pipeline(&pipeline_v1, "v1");
    let spec_v2 = manager.serialize_pipeline(&pipeline_v2, "v2");

    manager.record_spec("v1", spec_v1);
    manager.record_spec("v2", spec_v2);

    // ── Compare ────────────────────────────────────────────────────────────
    let report = manager.generate_replay_report("v1", "v2")?;

    println!("=== Pipeline Diff: {} → {} ===", report.baseline_name, report.current_name);
    println!("v1 stages: {}", report.baseline_spec.stages.len());
    println!("v2 stages: {}", report.current_spec.stages.len());

    let diff = &report.comparison.diff;
    if report.comparison.added_stages == 0
        && report.comparison.removed_stages == 0
        && report.comparison.modified_stages == 0
        && !report.comparison.settings_changed
    {
        println!("No changes detected — pipelines are structurally identical.");
    } else {
        println!("\nChanges detected:");
        for name in &diff.added_stages {
            println!("  + added stage: {}", name);
        }
        for name in &diff.removed_stages {
            println!("  - removed stage: {}", name);
        }
        for name in &diff.modified_stages {
            println!("  ~ modified stage: {}", name);
        }
        if diff.settings_changed {
            println!("  ~ pipeline-level settings changed");
        }
    }

    // ── Also verify determinism: run both on the same inputs ───────────────
    let sample_events = vec![
        json!({"level": "info",  "message": "start",  "user": "alice", "email": "a@example.com"}),
        json!({"level": "debug", "message": "trace",  "user": "bob"}),
        json!({"level": "error", "message": "failed", "user": "charlie"}),
    ];

    let mut outputs_v1: Vec<String> = Vec::new();
    let mut pipe1 = build_v1();
    for data in &sample_events {
        let ev = Event { data: data.clone(), metadata: None };
        if let Ok(Some(out)) = pipe1.process_event(ev) {
            outputs_v1.push(out.data.to_string());
        }
    }

    let mut outputs_v2: Vec<String> = Vec::new();
    let mut pipe2 = build_v2();
    for data in &sample_events {
        let ev = Event { data: data.clone(), metadata: None };
        if let Ok(Some(out)) = pipe2.process_event(ev) {
            outputs_v2.push(out.data.to_string());
        }
    }

    println!("\n=== Output Comparison ===");
    println!("v1 outputs ({}):", outputs_v1.len());
    for o in &outputs_v1 { println!("  {}", o); }
    println!("v2 outputs ({}):", outputs_v2.len());
    for o in &outputs_v2 { println!("  {}", o); }

    Ok(())
}
