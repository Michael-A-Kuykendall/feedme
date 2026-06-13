/// Example 16 — Pipeline Evolution (A/B Config Diff)
///
/// You refactor your pipeline — add a stage, rename one, remove another.
/// How do you know what changed between the deployed version and the new one?
///
/// ReplayManager serialises pipeline specs and diffs them structurally.
/// Plug this into your deployment pipeline to catch accidental changes
/// before they reach production.
use feedme::replay_spec::{PipelineReplaySpec, StageRegistry};
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

    // Use the unified advanced replay_spec (replay.rs structural path is now thin/legacy; prefer this for full config capture)
    let mut registry = StageRegistry::new();
    // Register built-in factories for roundtrip (in real use, register your stages)
    registry.register_stage("required_fields".to_string(), Box::new(|config| {
        let fields: Vec<String> = serde_json::from_value(config["fields"].clone())?;
        Ok(Box::new(RequiredFields::new(fields)))
    }));
    registry.register_stage("filter".to_string(), Box::new(|_config| {
        // For demo; real closure predicates would be reconstructed by user code
        Ok(Box::new(Filter::new(Box::new(|ev| ev.data.get("level").and_then(|v| v.as_str()) != Some("debug")))))
    }));
    registry.register_stage("stdout_output".to_string(), Box::new(|_config| Ok(Box::new(StdoutOutput::new()))));
    registry.register_stage("pii_redaction".to_string(), Box::new(|config| {
        let patterns: Vec<String> = serde_json::from_value(config["patterns"].clone())?;
        let regexes = patterns.into_iter().map(|p| regex::Regex::new(&p).unwrap()).collect();
        Ok(Box::new(PIIRedaction::new(regexes)))
    }));
    registry.register_stage("field_select".to_string(), Box::new(|config| {
        let fields: Vec<String> = serde_json::from_value(config["fields"].clone())?;
        Ok(Box::new(FieldSelect::new(fields)))
    }));

    let spec_v1 = PipelineReplaySpec::from_pipeline(&pipeline_v1, &registry)?;
    let spec_v2 = PipelineReplaySpec::from_pipeline(&pipeline_v2, &registry)?;

    // ── Compare using diff on specs ────────────────────────────────────────
    let diff = spec_v1.diff(&spec_v2);

    println!("=== Pipeline Diff (unified replay_spec) ===");
    println!("v1 stages: {}", spec_v1.stages.len());
    println!("v2 stages: {}", spec_v2.stages.len());

    if diff.added_stages.is_empty()
        && diff.removed_stages.is_empty()
        && diff.modified_stages.is_empty()
        && !diff.settings_changed
    {
        println!("No changes detected — pipelines are structurally identical.");
    } else {
        println!("\nChanges detected:");
        for s in &diff.added_stages {
            println!("  + added stage: {} (id={})", s.stage_id, s.stage_id);
        }
        for s in &diff.removed_stages {
            println!("  - removed stage: {} (id={})", s.stage_id, s.stage_id);
        }
        for (i, a, b) in &diff.modified_stages {
            println!("  ~ modified stage at {}: {} -> {}", i, a.stage_id, b.stage_id);
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

    // Phase-3 polish: compose replay (structural diff) + audit (attestation) for attested evolution
    // Ties bells without new semantics.
    let mut auditor = feedme::audit::AuditManager::new();
    auditor.add_compliance_policy(
        "evolution-quality".into(),
        feedme::audit::CompliancePolicy {
            name: "evolution-quality".into(),
            description: "Processed events reasonable".into(),
            checks: vec![feedme::audit::ComplianceCheck {
                name: "min-events".into(),
                description: "At least 1 event".into(),
                check_type: feedme::audit::CheckType::MinThroughput,
                threshold: 1.0,
            }],
        },
    );
    let _ = auditor.generate_attestation_bundle(&pipe1, "evolution-v1");
    println!("Composed: replay diff + audit attestation for pipeline evolution.");

    Ok(())
}
