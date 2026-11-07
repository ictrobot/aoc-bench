use crate::config::{Benchmark, Config, ConfigFile, KeyValue};
use crate::engine::batch_metrics::BatchMetrics;
use crate::group::{
    IsolatedGroup, ResolveFailure, RunGroup, config_matches_filter, resolve_and_group,
};
use crate::host_config::{HostConfig, HostConfigError};
use crate::metrics::{self, Labels};
use crate::run::RunProcessError;
use crate::run::process::{GroupOutcome, ProcessContext, process_group};
use crate::run::schedule::{
    PoolStats, RunScheduleConfig, SelectedGroup, select_for_run_all, select_for_run_with_pool,
};
use crate::storage::{FileLock, HybridDiskError, HybridDiskStorage, Storage};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tracing::{error, info, info_span};

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
        host_config.check_config_overrides(&config_file)?;

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
    ///
    /// Once `stop` is set, no further groups are started. The in-progress group completes and is
    /// recorded, and the report is returned with `stopped` set.
    pub fn run(
        &self,
        benchmarks: &[&Benchmark],
        config_filter: &Config,
        mode: RunMode,
        stop: &Arc<AtomicBool>,
    ) -> Result<RunReport, RunEngineError> {
        let started = Instant::now();
        let (mut groups, failures) = self.build_groups(benchmarks);

        let fail_fast = matches!(mode, RunMode::All);
        let mut report = RunReport::default();

        // Resolution failures (shared groups only) that match the filter are reported per case.
        let mut unresolved = Vec::new();
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
            unresolved.push(Labels::case(&failure.benchmark, &failure.config));
        }

        let (selected, pool) = match mode {
            RunMode::Sample(schedule) => {
                let selection = select_for_run_with_pool(
                    &self.storage,
                    &mut groups,
                    config_filter,
                    schedule,
                    &mut rand::rng(),
                )
                .map_err(RunEngineError::SchedulingError)?;
                (selection.groups, selection.pool)
            }
            RunMode::All => (select_for_run_all(&groups, config_filter), None),
        };
        report.groups_selected = selected.len();

        let mut batch = self.batch_metrics(mode, &selected, pool.as_ref(), started);
        if let Some(batch) = batch.as_mut() {
            for labels in &unresolved {
                batch.group_failed_early(labels, unclassified_class(mode));
            }
        }

        let process_context = ProcessContext {
            config_file: &self.config_file,
            host_config: &self.host_config,
            host_kv: &self.host_kv,
            storage: &self.storage,
            dry_run: self.dry_run,
            force_update_stable: self.force_update_stable,
            stop,
        };

        for selection in selected {
            if stop.load(Ordering::Relaxed) {
                info!(
                    remaining =
                        report.groups_selected - report.reused - report.executed - report.failed,
                    "stop requested, skipping remaining groups"
                );
                report.stopped = true;
                break;
            }

            let group = &groups[selection.index];
            let _span = info_span!(
                "group",
                bench = %group.benchmark(),
                cases = group.case_count()
            )
            .entered();
            if let Some(batch) = batch.as_mut() {
                batch.group_started(group, group_class(mode, selection));
            }

            let outcome = match process_group(group, &process_context, selection.reuse) {
                Ok(GroupOutcome::Reused { .. } | GroupOutcome::WouldReuse) => {
                    report.reused += 1;
                    "reused"
                }
                Ok(GroupOutcome::Executed { .. } | GroupOutcome::WouldExecute { .. }) => {
                    report.executed += 1;
                    "executed"
                }
                Err(error) if fail_fast => {
                    return Err(RunEngineError::ProcessError(error));
                }
                Err(error) => {
                    error!(error = %error, "failed to process group, skipping");
                    report.failed += 1;
                    "failed"
                }
            };

            if let Some(batch) = batch.as_mut() {
                batch.group_finished(outcome);
            }
        }

        Ok(report)
    }

    /// Push the opening batch state, if metrics are enabled. A dry run records nothing.
    fn batch_metrics(
        &self,
        mode: RunMode,
        selected: &[SelectedGroup],
        pool: Option<&PoolStats>,
        started: Instant,
    ) -> Option<BatchMetrics<'static>> {
        if self.dry_run {
            return None;
        }
        let metrics = metrics::global()?;
        let classes: &[&'static str] = match mode {
            RunMode::All => &["all"],
            RunMode::Sample(_) => &["new", "rerun"],
        };
        let counts: Vec<(&'static str, usize)> = classes
            .iter()
            .map(|&class| {
                let count = selected
                    .iter()
                    .filter(|&&selection| group_class(mode, selection) == class)
                    .count();
                (class, count)
            })
            .collect();
        Some(BatchMetrics::start(
            metrics,
            &self.storage,
            started,
            pool,
            &counts,
        ))
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

/// The `class` metric label of a selected group.
fn group_class(mode: RunMode, selection: SelectedGroup) -> &'static str {
    match mode {
        RunMode::All => "all",
        RunMode::Sample(_) if selection.reuse => "new",
        RunMode::Sample(_) => "rerun",
    }
}

/// The `class` metric label of a group that failed before it could be classified.
fn unclassified_class(mode: RunMode) -> &'static str {
    match mode {
        RunMode::All => "all",
        RunMode::Sample(_) => "new",
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
    /// A shutdown signal was received, so selected groups were left unprocessed.
    pub stopped: bool,
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
