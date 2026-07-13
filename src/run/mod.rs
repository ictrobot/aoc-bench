//! Benchmark run values and workflow.
//!
//! The engine chooses a mode and handles reporting; this module owns the run-domain values,
//! scheduling policy, and selected-group processing workflow.

mod model;
pub mod process;
pub mod schedule;
#[cfg(all(test, unix))]
mod test_support;

pub use model::{Run, RunSeries};
pub use process::RunProcessError;
pub use schedule::{NewGroupOrder, RunScheduleConfig};
