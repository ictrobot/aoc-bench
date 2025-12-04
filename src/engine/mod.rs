mod run;
mod stats;
pub use run::{RunEngine, RunEngineConfig, RunEngineError};
pub use stats::{
    FastestResult, ImpactEntry, ImpactSummary, StatsEngine, StatsEngineError, TimelinePoint,
    TimelineResult, TimelineSummary,
};
