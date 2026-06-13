/// Example 18 — Derived Fields & Field Remapping
///
/// Transform your data with computed fields and field renaming.
/// 
/// - DerivedFields: Create new fields computed from existing ones
/// - FieldRemap: Rename fields without changing values
use feedme::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Build pipeline with derived and remapped fields ─────────────────────
    let mut pipeline = Pipeline::new();

    // Stage 1: FieldRemap — rename fields for consistency
    // Input: "user_id" → Output: "user.id"
    pipeline.add_stage(Box::new(FieldRemap::new(std::collections::HashMap::from([
        ("user_id".to_string(), "user.id".to_string()),
        ("timestamp".to_string(), "event.timestamp".to_string()),
    ]))));

    // Stage 2: DerivedFields — compute new fields from existing ones
    // Note: Need explicit type annotation for each closure
    let derivations: std::collections::HashMap<String, feedme::EventDerivationFn> = std::collections::HashMap::new();
    let mut derivations = derivations;
    
    // Derive "user.is_admin" from "user.role"
    derivations.insert(
        "user.is_admin".to_string(),
        Box::new(|ev: &feedme::Event| {
            let role = ev.data.get("user")
                .and_then(|u| u.get("role"))
                .and_then(|r| r.as_str())
                .unwrap_or("user");
            serde_json::json!(role == "admin")
        }) as feedme::EventDerivationFn,
    );
    
    // Derive "event.is_critical" from "level"
    derivations.insert(
        "event.is_critical".to_string(),
        Box::new(|ev: &feedme::Event| {
            let level = ev.data.get("level")
                .and_then(|l| l.as_str())
                .unwrap_or("info");
            serde_json::json!(level == "error" || level == "critical")
        }) as feedme::EventDerivationFn,
    );
    
    // Derive "amount.with_tax" from "amount"
    derivations.insert(
        "amount.with_tax".to_string(),
        Box::new(|ev: &feedme::Event| {
            let base = ev.data.get("amount")
                .and_then(|a| a.as_f64())
                .unwrap_or(0.0);
            serde_json::json!(base * 1.08) // 8% tax
        }) as feedme::EventDerivationFn,
    );
    
    pipeline.add_stage(Box::new(DerivedFields::new(derivations)));

    // Stage 3: Select the final fields we want
    pipeline.add_stage(Box::new(FieldSelect::new(vec![
        "user.id".to_string(),
        "user.is_admin".to_string(),
        "event.timestamp".to_string(),
        "event.is_critical".to_string(),
        "amount".to_string(),
        "amount.with_tax".to_string(),
    ])));

    // Stage 4: Output
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // ── Process events ──────────────────────────────────────────────────────
    let events = vec![
        // Original field names get remapped
        serde_json::json!({
            "user_id": "u123",
            "timestamp": "2026-06-13T10:00:00Z",
            "level": "error",
            "amount": 100.00
        }),
        serde_json::json!({
            "user_id": "u456",
            "timestamp": "2026-06-13T10:01:00Z",
            "level": "info",
            "amount": 50.00
        }),
        serde_json::json!({
            "user_id": "u789",
            "timestamp": "2026-06-13T10:02:00Z",
            "level": "critical",
            "amount": 250.00
        }),
    ];

    println!("=== Derived Fields & Remapping Demo ===\n");

    for data in events {
        let event = Event { data, metadata: None };
        if let Ok(Some(out)) = pipeline.process_event(event) {
            println!("{}", out.data);
        }
    }

    // Show how the transformations work
    println!("\n--- Transformations Applied ---");
    println!("1. FieldRemap: user_id → user.id, timestamp → event.timestamp");
    println!("2. DerivedFields: user.is_admin (from role), event.is_critical (from level), amount.with_tax (8% tax)");
    println!("3. FieldSelect: keep only final fields");

    Ok(())
}