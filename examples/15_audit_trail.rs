/// Example 15 — Audit Trail
///
/// Generate a cryptographically-hashed attestation bundle after every
/// pipeline run: what stages ran, how many events processed, what the
/// error rate was, and whether your compliance policy was satisfied.
///
/// Useful for any pipeline that processes data you need to account for —
/// user records, financial events, security logs, etc.
use feedme::audit::{AuditManager, CheckType, ComplianceCheck, CompliancePolicy};
use feedme::*;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Build pipeline ──────────────────────────────────────────────────────
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(PIIRedaction::new(vec![regex::Regex::new(
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
    )
    .unwrap()])));
    pipeline.add_stage(Box::new(RequiredFields::new(vec![
        "event_type".into(),
        "timestamp".into(),
    ])));
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    let events = vec![
        json!({"event_type": "login",    "timestamp": "2026-05-20T10:00:00Z", "email": "alice@example.com"}),
        json!({"event_type": "purchase", "timestamp": "2026-05-20T10:01:00Z", "email": "bob@example.com", "amount": 49.99}),
        json!({"event_type": "logout",   "timestamp": "2026-05-20T10:05:00Z", "email": "alice@example.com"}),
        json!({"event_type": "purchase", "timestamp": "2026-05-20T10:06:00Z", "amount": 129.00}),
        json!({"timestamp": "2026-05-20T10:07:00Z", "email": "ghost@example.com"}), // missing event_type → deadletter
    ];

    for data in &events {
        let ev = Event {
            data: data.clone(),
            metadata: None,
        };
        let _ = pipeline.process_event(ev);
    }

    // ── Set up audit manager with a compliance policy ───────────────────────
    let mut auditor = AuditManager::new();

    // Policy: error rate must stay below 50% (here 1 event is invalid → 20%)
    auditor.add_compliance_policy(
        "data-quality".into(),
        CompliancePolicy {
            name: "data-quality".into(),
            description: "Error rate must be below 50%".into(),
            checks: vec![ComplianceCheck {
                name: "error-rate".into(),
                description: "Max error rate 50%".into(),
                check_type: CheckType::MaxErrorRate,
                threshold: 0.5,
            }],
        },
    );

    // ── Generate attestation bundle for this run ────────────────────────────
    let bundle = auditor.generate_attestation_bundle(&pipeline, "run-2026-05-20-001")?;

    println!("\n=== Attestation Bundle ===");
    println!("Execution ID : {}", bundle.execution_id);
    println!("Pipeline hash: {}", bundle.pipeline_hash);
    println!("Events in    : {}", bundle.metrics.total_events_processed);
    println!("Error rate   : {:.1}%", bundle.metrics.error_rate * 100.0);
    println!("Drop rate    : {:.1}%", bundle.metrics.drop_rate * 100.0);
    println!("Health issues: {:?}", bundle.health_issues);
    println!("Compliance   : {} check(s)", bundle.compliance_checks.len());

    for result in &bundle.compliance_checks {
        let status = if result.overall_pass { "PASS" } else { "FAIL" };
        println!("  [{status}] {}", result.policy_name);
    }

    // ── Compliance report summary ──────────────────────────────────────────
    let report = auditor.generate_compliance_report();
    println!(
        "\nCompliance report: {}/{} checks passed",
        report.passed_checks,
        report.total_policies * report.passed_checks.max(1)
    );

    // Harden user-surface: assert attestation and compliance
    assert!(bundle.metrics.total_events_processed == 5);
    assert!(!bundle.compliance_checks.is_empty());

    Ok(())
}
