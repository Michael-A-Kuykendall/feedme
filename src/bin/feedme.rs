//! FeedMe CLI — process and validate NDJSON event streams.
//!
//! # Usage
//!
//! ```text
//! feedme validate --input events.ndjson
//! feedme run --input events.ndjson
//! cat events.ndjson | feedme validate
//! cat events.ndjson | feedme run --fields level,message
//! ```

use clap::{Parser, Subcommand};
use feedme::*;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "feedme",
    about = "Deterministic streaming data pipeline engine",
    long_about = "Process and validate NDJSON event streams using FeedMe's deterministic pipeline engine.\n\nPass '-' or omit --input to read from stdin.",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate NDJSON events: report parse errors and event count
    Validate {
        /// Input file ('-' or omit for stdin)
        #[arg(short, long, default_value = "-")]
        input: String,
    },

    /// Run NDJSON events through a pipeline and write to stdout
    Run {
        /// Input file ('-' or omit for stdin)
        #[arg(short, long, default_value = "-")]
        input: String,

        /// Comma-separated list of fields to keep (omit to pass all fields through)
        #[arg(long, value_delimiter = ',')]
        fields: Option<Vec<String>>,

        /// Comma-separated required fields; events missing any are dropped and reported
        #[arg(long, value_delimiter = ',')]
        require: Option<Vec<String>>,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Validate { input } => cmd_validate(&input),
        Commands::Run {
            input,
            fields,
            require,
        } => cmd_run(&input, fields, require),
    }
}

fn resolve_input(input: &str) -> InputSource {
    if input == "-" {
        InputSource::Stdin
    } else {
        InputSource::File(PathBuf::from(input))
    }
}

fn cmd_validate(input: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut source = resolve_input(input);

    // Pass-through pipeline with stdout sink so we can count events.
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    let mut none: Option<&mut dyn Stage> = None;
    source.process_input(&mut pipeline, &mut none)?;

    let processed = pipeline.events_processed();
    let errors = pipeline.error_count();
    eprintln!(
        "validated: {} events processed, {} errors",
        processed, errors
    );

    if errors > 0 {
        std::process::exit(1);
    }

    Ok(())
}

fn cmd_run(
    input: &str,
    fields: Option<Vec<String>>,
    require: Option<Vec<String>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut source = resolve_input(input);
    let mut pipeline = Pipeline::new();

    if let Some(field_list) = fields {
        pipeline.add_stage(Box::new(FieldSelect::new(field_list)));
    }

    if let Some(required) = require {
        pipeline.add_stage(Box::new(RequiredFields::new(required)));
    }

    pipeline.add_stage(Box::new(StdoutOutput::new()));

    let mut none: Option<&mut dyn Stage> = None;
    source.process_input(&mut pipeline, &mut none)?;

    let processed = pipeline.events_processed();
    let errors = pipeline.error_count();
    eprintln!("run: {} events processed, {} errors", processed, errors);

    Ok(())
}
