mod cli;

use crate::cli::{logging, Cli};
use clap::Parser;
use std::process::ExitCode;

fn main() -> ExitCode {
    logging::init();
    Cli::parse().run()
}
