/// Example 17 — Fused Rule Engine (Selector-First Single-Pass)
///
/// FeedMe's signature feature: O(M) evaluation independent of rule count.
/// 
/// Conventional pipelines scan the event once per rule stage (O(N×M)).
/// FusedRuleEngine scans ONCE and broadcasts each field to all rules that need it.
/// 
/// This is the "selector-first" architecture — extract each field once,
/// then apply ALL predicates that reference it. Runtime stays constant
/// even as you add hundreds of rules sharing selectors.
///
/// This is FSE (Fused Semantic Execution) — the core innovation that makes
/// FeedMe different from every other pipeline library.
use feedme::fused::{FusedRuleEngine, Rule, FailAction, FieldType};
use feedme::{Event, Pipeline};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Build a fused rule engine with multiple validation rules ─────────────
    // All these rules will be evaluated in a SINGLE pass over the event
    // Note: Uses flat (top-level) fields for simplicity; nested paths work too
    let engine = FusedRuleEngine::builder("user-validation")
        // Rule 1: user_id must exist
        .require(Rule::exists("user_id"))
        // Rule 2: email must exist
        .require(Rule::exists("email"))
        // Rule 3: amount must be a number
        .require(Rule::type_is("amount", FieldType::Number))
        // Rule 4: amount must be positive
        .require(Rule::greater_than("amount", 0.0))
        // Rule 5: priority must be >= 1 (valid priority range)
        .require(Rule::greater_than("priority", 0.0))
        // Rule 6: score must be <= 100 (percentage-like value)
        .require(Rule::less_than("score", 100.0))
        // When ANY rule fails: drop the event (fail-closed)
        .on_fail(FailAction::DropEvent)
        .build();

    // Wrap it as a stage in a pipeline
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(engine));
    pipeline.add_stage(Box::new(feedme::StdoutOutput::new()));

    // ── Test with different events ───────────────────────────────────────────
    let test_cases = vec![
        // Valid: all rules pass
        serde_json::json!({
            "user_id": "u123",
            "email": "alice@example.com",
            "amount": 50.00,
            "priority": 5,
            "score": 85.5
        }),
        // Invalid: missing user_id
        serde_json::json!({
            "email": "bob@example.com",
            "amount": 25.00,
            "priority": 3,
            "score": 90.0
        }),
        // Invalid: amount is negative
        serde_json::json!({
            "user_id": "u456",
            "email": "charlie@example.com",
            "amount": -10.00,
            "priority": 2,
            "score": 75.0
        }),
        // Invalid: priority is zero (must be > 0)
        serde_json::json!({
            "user_id": "u789",
            "email": "dave@example.com",
            "amount": 100.00,
            "priority": 0,
            "score": 50.0
        }),
        // Invalid: score over 100
        serde_json::json!({
            "user_id": "u000",
            "email": "eve@example.com",
            "amount": 999.99,
            "priority": 10,
            "score": 150.0
        }),
    ];

    println!("=== Fused Rule Engine Demo ===");
    println!("Engine evaluates {} rules in a SINGLE pass over each event.\n", 6);

    for (i, data) in test_cases.into_iter().enumerate() {
        let event = Event { data, metadata: None };
        let result = pipeline.process_event(event);
        
        // Print what happened for each test case
        match &result {
            Ok(Some(out)) => println!("[{}] ✓ PASSED: {}", i + 1, out.data),
            Ok(None) =>    println!("[{}] ✗ DROPPED (rule failed)", i + 1),
            Err(e) =>      println!("[{}] ERROR: {}", i + 1, e),
        }
    }

    // Show metrics — notice how many fields were actually scanned
    println!("\nMetrics:");
    for log in pipeline.export_json_logs() {
        println!("{}", log);
    }

    // The key insight: adding more rules doesn't slow down the pipeline
    // because all rules sharing selectors are evaluated in the same pass.
    // Try adding 100 more rules — the runtime stays O(M), not O(N×M).
    
    println!("\n✓ FusedRuleEngine: O(1) per rule for shared selectors");
    println!("  Conventional pipeline: O(N) per rule (N = rule count)");
    println!("  This is FSE — selector-first architecture.");

    Ok(())
}