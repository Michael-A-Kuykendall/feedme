/// Example 19 — Type Checking & Value Constraints
///
/// Validate field types and apply custom constraint logic.
/// 
/// - TypeChecking: Ensure fields have the expected JSON type
/// - ValueConstraints: Apply arbitrary predicates to field values
use feedme::*;
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut pipeline = Pipeline::new();

    // Stage 1: TypeChecking — enforce field types
    // Validates that each field matches the expected JSON type
    pipeline.add_stage(Box::new(TypeChecking::new(HashMap::from([
        ("user_id".to_string(), "string".to_string()),
        ("age".to_string(), "number".to_string()),
        ("active".to_string(), "boolean".to_string()),
        ("tags".to_string(), "array".to_string()),
        ("metadata".to_string(), "object".to_string()),
    ]))));

    // Stage 2: ValueConstraints — custom validation logic
    pipeline.add_stage(Box::new(ValueConstraints::new(HashMap::from([
        // user_id must be non-empty
        ("user_id".to_string(), Box::new(|v: &serde_json::Value| {
            v.as_str().map(|s| !s.is_empty()).unwrap_or(false)
        }) as feedme::ValueConstraintFn),
        // age must be positive and under 150
        ("age".to_string(), Box::new(|v: &serde_json::Value| {
            v.as_f64().map(|a| a > 0.0 && a < 150.0).unwrap_or(false)
        }) as feedme::ValueConstraintFn),
        // tags must have at least one element
        ("tags".to_string(), Box::new(|v: &serde_json::Value| {
            v.as_array().map(|a| !a.is_empty()).unwrap_or(false)
        }) as feedme::ValueConstraintFn),
    ]))));

    // Stage 3: Output
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // ── Test various events ─────────────────────────────────────────────────
    let test_cases = vec![
        // Valid: all types and constraints pass
        serde_json::json!({
            "user_id": "u123",
            "age": 30,
            "active": true,
            "tags": ["premium", "beta"],
            "metadata": { "source": "web" }
        }),
        // Invalid: wrong type (age is string, not number)
        serde_json::json!({
            "user_id": "u456",
            "age": "thirty",
            "active": true,
            "tags": ["basic"],
            "metadata": {}
        }),
        // Invalid: constraint violation (empty tags)
        serde_json::json!({
            "user_id": "u789",
            "age": 25,
            "active": false,
            "tags": [],
            "metadata": null
        }),
        // Invalid: age out of range
        serde_json::json!({
            "user_id": "u000",
            "age": -5,
            "active": true,
            "tags": ["test"],
            "metadata": {}
        }),
    ];

    println!("=== Type Checking & Value Constraints Demo ===\n");

    for (i, data) in test_cases.into_iter().enumerate() {
        let event = Event { data, metadata: None };
        let result = pipeline.process_event(event);
        
        match &result {
            Ok(Some(out)) => println!("[{}] ✓ PASSED → {}", i + 1, out.data),
            Ok(None) =>    println!("[{}] ✗ REJECTED (type or constraint failed)", i + 1),
            Err(e) =>      println!("[{}] ERROR: {}", i + 1, e),
        }
    }

    println!("\n--- Validation Layers ---");
    println!("1. TypeChecking: Ensures field has correct JSON type (string/number/boolean/array/object)");
    println!("2. ValueConstraints: Arbitrary predicates — any logic you can express in Rust");

    Ok(())
}