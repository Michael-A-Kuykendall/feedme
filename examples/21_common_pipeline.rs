/// Example 21 — Common Pipeline Helper
///
/// The ergonomic shortcut: one call gets you a production-ready pipeline
/// with PII redaction, field selection, and required fields validation.
///
/// `common_redact_validate_pipeline()` is the "batteries included" option
/// for the 90% use case: select some fields, redact PII, require others.
use feedme::*;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The easy way: one call gets you a complete pipeline
    let email_pattern = regex::Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")?;
    let ssn_pattern = regex::Regex::new(r"\b\d{3}-\d{2}-\d{4}\b")?;
    
    let mut pipeline = common_redact_validate_pipeline(
        // Select these fields (others are dropped)
        vec![
            "timestamp".to_string(),
            "level".to_string(),
            "message".to_string(),
            "user".to_string(),
            "email".to_string(),
        ],
        // Require these fields (events missing any are deadlettered)
        vec!["level".to_string(), "message".to_string()],
        // PII patterns to redact
        vec![email_pattern, ssn_pattern],
    );

    // Add your own stages on top
    pipeline.add_stage(Box::new(Filter::new(Box::new(|ev| {
        ev.data.get("level").and_then(|l| l.as_str()) != Some("debug")
    }))));
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // Deadletter for validation failures
    let mut deadletter = Deadletter::new(PathBuf::from("samples/deadletter.ndjson"));

    // ── Process input ───────────────────────────────────────────────────────
    let events = vec![
        // Valid: all required fields, has email for redaction
        serde_json::json!({
            "timestamp": "2026-06-13T10:00:00Z",
            "level": "info",
            "message": "User logged in",
            "user": "alice",
            "email": "alice@example.com",
            "secret": "should be dropped"
        }),
        // Valid: level and message present
        serde_json::json!({
            "timestamp": "2026-06-13T10:01:00Z",
            "level": "warn",
            "message": "Slow query",
            "user": "bob",
            "email": "bob@example.com"
        }),
        // Invalid: missing "message" → goes to deadletter
        serde_json::json!({
            "timestamp": "2026-06-13T10:02:00Z",
            "level": "error",
            "user": "charlie",
            "email": "charlie@example.com"
        }),
    ];

    println!("=== Common Pipeline Helper Demo ===\n");

    let mut dl_handler = Some(&mut deadletter as &mut dyn Stage);
    for data in events {
        let event = Event { data, metadata: None };
        match pipeline.process_event(event) {
            Ok(Some(out)) => {
                println!("PASSED → {}", out.data);
            }
            Ok(None) => {
                println!("DROPPED (filtered)");
            }
            Err(e) => {
                println!("ERROR: {}", e);
                // Error events go to deadletter
                let dl_event = Event {
                    data: serde_json::json!({"error": e.to_string()}),
                    metadata: None,
                };
                let _ = deadletter.execute(dl_event);
            }
        }
    }

    println!("\nProcessed: {} events", pipeline.events_processed());
    println!("Dropped: {} events", pipeline.events_dropped());
    println!("Errors: {} events", pipeline.error_count());

    println!("\n--- What common_redact_validate_pipeline provides ---");
    println!("1. FieldSelect: keeps only the fields you specify");
    println!("2. PIIRedaction: built-in email + common PII patterns");
    println!("3. RequiredFields: fail-closed validation");
    println!("All in ONE call — no wiring needed.");

    // Show the deadletter contents
    println!("\n--- Deadletter (missing message) ---");
    let dl_content = std::fs::read_to_string("samples/deadletter.ndjson")?;
    println!("{}", dl_content);

    // Cleanup
    let _ = std::fs::remove_file("samples/deadletter.ndjson");

    Ok(())
}