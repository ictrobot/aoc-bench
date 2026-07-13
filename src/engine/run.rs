use crate::config::{Benchmark, Config, ConfigFile, KeyValue};
use crate::group::{
    IsolatedGroup, ResolveFailure, RunGroup, config_matches_filter, resolve_and_group,
};
use crate::host_config::{HostConfig, HostConfigError};
use crate::run::RunProcessError;
use crate::run::process::{GroupOutcome, ProcessContext, process_group};
use crate::run::schedule::{RunScheduleConfig, select_for_run, select_for_run_all};
use crate::storage::{FileLock, HybridDiskError, HybridDiskStorage, Storage};
use std::collections::BTreeMap;
use tracing::{error, info_span};

#[derive(Debug, Clone)]
pub struct RunEngineConfig {
    pub config_file: ConfigFile,
    pub host_kv: KeyValue,
    pub dry_run: bool,
    pub force_update_stable: bool,
}

#[derive(Debug)]
pub struct RunEngine {
    pub config_file: ConfigFile,
    pub host_config: HostConfig,
    pub host_kv: KeyValue,
    pub storage: HybridDiskStorage,
    pub dry_run: bool,
    pub force_update_stable: bool,
    _lock: FileLock,
}

impl RunEngine {
    pub fn new(config: RunEngineConfig) -> Result<Self, RunEngineError> {
        let RunEngineConfig {
            config_file,
            host_kv,
            dry_run,
            force_update_stable,
        } = config;

        let host = host_kv.value_name();

        let host_config = HostConfig::load(config_file.data_dir(), host)?;

        let storage = HybridDiskStorage::new(config_file.clone(), host)
            .map_err(RunEngineError::StorageError)?;

        let lock = storage.acquire_lock().map_err(RunEngineError::LockError)?;

        Ok(Self {
            config_file,
            host_config,
            host_kv,
            storage,
            dry_run,
            force_update_stable,
            _lock: lock,
        })
    }

    /// Run the given benchmarks.
    ///
    /// Every benchmark contributes run groups — shared benchmarks contribute inode-grouped groups,
    /// isolated benchmarks contribute one isolated group per case — which are classified, limited
    /// (per `mode`), and processed uniformly. A shared group inherits or
    /// executes; an isolated group is resolved lazily and executes. Resolution or processing
    /// failures matching the command filter are fatal for [`RunMode::All`] and skipped otherwise.
    /// Identity hash/full-value mismatches are invariant violations and panic before execution can
    /// continue with an unsafe association.
    pub fn run(
        &self,
        benchmarks: &[&Benchmark],
        config_filter: &Config,
        mode: RunMode,
    ) -> Result<RunReport, RunEngineError> {
        let (mut groups, failures) = self.build_groups(benchmarks);

        let fail_fast = matches!(mode, RunMode::All);
        let mut report = RunReport::default();

        // Resolution failures (shared groups only) that match the filter are reported per case.
        for failure in failures {
            if !config_matches_filter(&failure.config, config_filter) {
                continue;
            }
            if fail_fast {
                return Err(RunEngineError::CaseResolution {
                    benchmark: failure.benchmark.to_string(),
                    config: failure.config.to_string(),
                    source: failure.error,
                });
            }
            error!(
                bench = %failure.benchmark,
                config = %failure.config,
                error = %failure.error,
                "failed to resolve case, skipping"
            );
            report.failed += 1;
        }

        let selected = match mode {
            RunMode::Sample(schedule) => select_for_run(
                &self.storage,
                &mut groups,
                config_filter,
                schedule,
                &mut rand::rng(),
            )
            .map_err(RunEngineError::SchedulingError)?,
            RunMode::All => select_for_run_all(&groups, config_filter),
        };
        report.groups_selected = selected.len();

        let process_context = ProcessContext {
            config_file: &self.config_file,
            host_config: &self.host_config,
            host_kv: &self.host_kv,
            storage: &self.storage,
            dry_run: self.dry_run,
            force_update_stable: self.force_update_stable,
        };

        for selection in selected {
            let group = &groups[selection.index];
            let _span = info_span!(
                "group",
                bench = %group.benchmark(),
                cases = group.case_count()
            )
            .entered();

            match process_group(group, &process_context, selection.reuse) {
                Ok(GroupOutcome::Reused { .. } | GroupOutcome::WouldReuse) => report.reused += 1,
                Ok(GroupOutcome::Executed { .. } | GroupOutcome::WouldExecute { .. }) => {
                    report.executed += 1;
                }
                Err(error) if fail_fast => {
                    return Err(RunEngineError::ProcessError(error));
                }
                Err(error) => {
                    error!(error = %error, "failed to process group, skipping");
                    report.failed += 1;
                }
            }
        }

        Ok(report)
    }

    /// Build the schedulable groups for all in-scope benchmarks.
    ///
    /// Shared benchmarks are eagerly resolved and inode-grouped (grouping must precede limits so
    /// hardlinks share a slot). Isolated benchmarks contribute one isolated group per case,
    /// resolved only if selected — so a missing executable never breaks a filtered
    /// command and path-dependent programs are never content-hashed.
    fn build_groups(&self, benchmarks: &[&Benchmark]) -> (Vec<RunGroup>, Vec<ResolveFailure>) {
        let shared_variants = benchmarks
            .iter()
            .filter(|benchmark| benchmark.dedupe().is_some())
            .flat_map(|benchmark| benchmark.variants());
        let (shared, failures) = resolve_and_group(self.config_file.data_dir(), shared_variants);
        let mut shared_by_benchmark = BTreeMap::new();
        for group in shared {
            shared_by_benchmark
                .entry(group.key.benchmark.clone())
                .or_insert_with(Vec::new)
                .push(group);
        }
        let mut groups = Vec::new();

        for benchmark in benchmarks {
            if benchmark.dedupe().is_some() {
                if let Some(shared) = shared_by_benchmark.remove(benchmark.id()) {
                    groups.extend(shared.into_iter().map(RunGroup::Shared));
                }
            } else {
                for variant in benchmark.variants() {
                    for config in variant.config() {
                        groups.push(RunGroup::Isolated(IsolatedGroup {
                            benchmark: benchmark.id().clone(),
                            config,
                        }));
                    }
                }
            }
        }
        debug_assert!(shared_by_benchmark.is_empty());

        (groups, failures)
    }
}

/// How the run command selects work.
#[derive(Debug, Clone, Copy)]
pub enum RunMode {
    /// `run`: classify into new/rerun pools and cap by the given limits.
    Sample(RunScheduleConfig),
    /// `run-all`: process every eligible group unconditionally.
    All,
}

/// Counts from a run.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RunReport {
    /// Number of groups selected for processing.
    pub groups_selected: usize,
    /// Groups that inherited an existing workload (or would, on a dry run).
    pub reused: usize,
    /// Groups that ran the workload (or would, on a dry run).
    pub executed: usize,
    /// Groups or cases that failed to resolve or process.
    pub failed: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum RunEngineError {
    #[error("failed to load host config: {0}")]
    HostConfigError(#[from] HostConfigError),
    #[error("failed to open storage: {0}")]
    StorageError(#[source] HybridDiskError),
    #[error("failed to acquire exclusive benchmark lock: {0}")]
    LockError(#[source] HybridDiskError),
    #[error("failed to select benchmark groups: {0}")]
    SchedulingError(#[source] HybridDiskError),
    #[error("failed to process benchmark group: {0}")]
    ProcessError(#[from] RunProcessError),
    #[error("failed to resolve case for benchmark '{benchmark}' config '{config}': {source}")]
    CaseResolution {
        benchmark: String,
        config: String,
        #[source]
        source: std::sync::Arc<crate::group::GroupError>,
    },
}
