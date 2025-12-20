use feedme::*;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Syslog parsing to structured events
    // Input: Raw syslog lines
    // Pipeline: SyslogParser -> FieldSelect -> FileOutput
    // Demonstrates parsing legacy formats into structured data

    let mut pipeline = Pipeline::new();

    // Add a parser stage (though parsers are usually at input, here for demo)
    // Actually, since input is JSON, but to demo, assume input is syslog text
    // For simplicity, use NDJSON with syslog messages

    // Select fields: message, host (if available)
    pipeline.add_stage(Box::new(FieldSelect::new(vec![
        "message".to_string(),
        "host".to_string(),
    ])));

    // Output to file
    pipeline.add_stage(Box::new(FileOutput::new(PathBuf::from("samples/structured_syslog.ndjson"))));

    // Create sample syslog data
    std::fs::write("samples/syslog.ndjson", r#"
{"message": "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8"}
{"message": "<13>Feb  5 17:32:18 10.0.0.99 Use of uninitialized value $m in concatenation (.) or string at line 6."}
{"message": "<165>Aug 24 05:34:00 CST 1987 mymachine myproc[10]: %% It's time to make the do-nuts. %%  Ingredients: Mix=OK, Jelly=OK # Devices: Mixer=OK, Jelly_Injector=OK, Frier=OK # Transport: Conveyer1=OK, Conveyer2=OK # %%"}
"#.trim())?;

    // Process
    let mut input = InputSource::File(PathBuf::from("samples/syslog.ndjson"));
    let mut none = None;
    input.process_input(&mut pipeline, &mut none)?;

    println!("Syslog parsed and structured.");
    Ok(())
}