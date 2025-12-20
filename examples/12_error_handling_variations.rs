use feedme::*;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Error handling variations
    // Shows different ways errors are handled: fail-fast vs continue with deadletter

    println!("=== Fail-fast mode (no deadletter) ===");
    let mut pipeline1 = Pipeline::new();
    pipeline1.add_stage(Box::new(RequiredFields::new(vec!["nonexistent".to_string()])));
    let mut input1 = InputSource::File(PathBuf::from("samples/messy.ndjson"));
    let mut none = None;
    // This will fail on first event
    match input1.process_input(&mut pipeline1, &mut none) {
        Ok(_) => println!("Unexpected success"),
        Err(e) => println!("Failed as expected: {:?}", e),
    }

    println!("\n=== Continue with deadletter ===");
    let mut pipeline2 = Pipeline::new();
    pipeline2.add_stage(Box::new(RequiredFields::new(vec!["nonexistent".to_string()])));
    let mut deadletter = Box::new(Deadletter::new(PathBuf::from("samples/error_demo_deadletter.ndjson")));
    let mut input2 = InputSource::File(PathBuf::from("samples/messy.ndjson"));
    let mut deadletter_opt = Some(&mut *deadletter as &mut dyn Stage);
    input2.process_input(&mut pipeline2, &mut deadletter_opt)?;
    println!("Processed with errors sent to deadletter.");

    Ok(())
}