use feedme::*;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parser trait is an extension point (not used by default InputSource, which
    // does direct JSON). Demo: parse a raw syslog line and feed the resulting Event.
    let parser = SyslogParser;
    let raw = b"<34>Oct 11 22:14:15 mymachine su: 'su root' failed";
    let event = parser.parse(raw).expect("parse syslog");

    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(FieldSelect::new(vec!["message".to_string()])));
    pipeline.add_stage(Box::new(StdoutOutput::new()));
    let _ = pipeline.process_event(event);

    println!("Syslog parsed via Parser extension.");
    Ok(())
}
