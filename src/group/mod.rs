//! Logical cases grouped into schedulable units.
//!
//! Shared cases are resolved and grouped cheaply by inode before selection. Isolated cases remain
//! singleton groups and are resolved only if selected. Durable content identity lives separately in
//! [`crate::workload`].
//!
//! Both kinds flow through the run workflow's build → classify → limit → process pipeline.

mod resolve;

pub use resolve::{
    CaseGroup, GroupError, GroupKey, HashedArtifacts, ResolveFailure, ResolvedCase,
    resolve_and_group, resolve_case,
};

use crate::config::{BenchmarkId, Config};
use crate::storage::{HybridDiskError, HybridDiskStorage, Storage, WorkloadId};
use jiff::Timestamp;
use rusqlite::Transaction;

/// One schedulable unit.
#[derive(Debug, Clone)]
pub enum RunGroup {
    /// A content-deduplicated group of hardlinked cases sharing one executable and stdin.
    Shared(CaseGroup),
    /// A single case from an isolated benchmark (one with no dedupe strategy).
    Isolated(IsolatedGroup),
}

/// A single isolated case, resolved and executed lazily (only if selected).
#[derive(Debug, Clone)]
pub struct IsolatedGroup {
    pub benchmark: BenchmarkId,
    /// The case's canonical hostless config.
    pub config: Config,
}

/// The cheap classification of a group, computed without reading executable/stdin bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupClass {
    /// Needs new processing (missing result, isolated/spec-changed/disagreeing shared association).
    New {
        /// The oldest last measurement of the workloads its cases point at, `None` if none has one
        oldest_measurement: Option<Timestamp>,
    },
    /// A rerun candidate, ordered by its workload's last-measurement time.
    Rerun {
        workload: WorkloadId,
        last_measurement: Timestamp,
    },
}

impl RunGroup {
    /// The owning benchmark.
    #[must_use]
    pub fn benchmark(&self) -> &BenchmarkId {
        match self {
            RunGroup::Shared(group) => &group.key.benchmark,
            RunGroup::Isolated(group) => &group.benchmark,
        }
    }

    /// The config the group executes: the representative of a shared group, or the single case.
    #[must_use]
    pub fn representative_config(&self) -> Config {
        match self {
            RunGroup::Shared(group) => group.representative_config(),
            RunGroup::Isolated(group) => group.config.clone(),
        }
    }

    /// The number of cases this group covers.
    #[must_use]
    pub fn case_count(&self) -> usize {
        match self {
            RunGroup::Shared(group) => group.case_count(),
            RunGroup::Isolated(_) => 1,
        }
    }

    /// Whether any covered case matches the command's logical filter.
    #[must_use]
    pub fn eligible(&self, filter: &Config) -> bool {
        match self {
            RunGroup::Shared(group) => group
                .configs()
                .any(|config| config_matches_filter(&config, filter)),
            RunGroup::Isolated(group) => config_matches_filter(&group.config, filter),
        }
    }

    /// Classify the group as new or a rerun candidate, using only recorded rows (no bytes read).
    pub fn classify(
        &self,
        storage: &HybridDiskStorage,
        tx: &Transaction<'_>,
    ) -> Result<GroupClass, HybridDiskError> {
        match self {
            RunGroup::Shared(group) => classify_shared(storage, tx, group),
            RunGroup::Isolated(group) => {
                classify_isolated(storage, tx, &group.benchmark, &group.config)
            }
        }
    }
}

/// Whether `config` satisfies every key/value in `filter` (host key ignored).
#[must_use]
pub fn config_matches_filter(config: &Config, filter: &Config) -> bool {
    filter.iter().all(|kv| config.get(kv.key()) == Some(kv))
}

/// Classify a shared group: new unless every member agrees on one shared workload whose group spec
/// matches the current group.
fn classify_shared(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
    group: &CaseGroup,
) -> Result<GroupClass, HybridDiskError> {
    let current_spec_digest = group.key.group_spec_digest;
    // Look up every member, even once the group is known to be new, to find its oldest last
    // measurement
    let mut workloads: Vec<WorkloadId> = Vec::new();
    let mut all_recorded = true;

    for config in group.configs() {
        let config_json = serde_json::to_string(&config)?;
        let workload = match storage.find_case_id(tx, group.key.benchmark.as_str(), &config_json)? {
            Some(case) => storage.get_case_workload(tx, case)?,
            None => None, // case never recorded
        };
        match workload {
            Some(workload) if !workloads.contains(&workload) => workloads.push(workload),
            Some(_) => {}
            None => all_recorded = false, // no recorded result
        }
    }

    let new = || {
        Ok(GroupClass::New {
            oldest_measurement: oldest_last_measurement(storage, tx, &workloads)?,
        })
    };
    // Members disagree, none has a result, or the group is empty (should not happen)
    let &[workload] = workloads.as_slice() else {
        return new();
    };
    if !all_recorded {
        return new();
    }
    let Some(meta) = storage.get_workload_meta(tx, workload)? else {
        return new();
    };
    if !meta.is_shared || meta.group_spec_digest != Some(current_spec_digest) {
        // Isolated association or the recorded spec no longer matches the current group.
        return new();
    }
    Ok(GroupClass::Rerun {
        workload,
        last_measurement: last_measurement_ts(storage, tx, workload)?,
    })
}

/// Classify an isolated single case: new if it has no recorded result, otherwise a rerun.
fn classify_isolated(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
    benchmark: &BenchmarkId,
    config: &Config,
) -> Result<GroupClass, HybridDiskError> {
    let config_json = serde_json::to_string(config)?;
    let new = GroupClass::New {
        oldest_measurement: None,
    };
    let Some(case) = storage.find_case_id(tx, benchmark.as_str(), &config_json)? else {
        return Ok(new);
    };
    let Some(workload) = storage.get_case_workload(tx, case)? else {
        return Ok(new);
    };
    Ok(GroupClass::Rerun {
        workload,
        last_measurement: last_measurement_ts(storage, tx, workload)?,
    })
}

/// The oldest last measurement of `workloads`, `None` if none has one.
fn oldest_last_measurement(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
    workloads: &[WorkloadId],
) -> Result<Option<Timestamp>, HybridDiskError> {
    let mut oldest: Option<Timestamp> = None;
    for &workload in workloads {
        if let Some(last) = storage.get_workload_last_measurement_ts(tx, workload)? {
            oldest = Some(oldest.map_or(last, |oldest| oldest.min(last)));
        }
    }
    Ok(oldest)
}

fn last_measurement_ts(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
    workload: WorkloadId,
) -> Result<Timestamp, HybridDiskError> {
    Ok(storage
        .get_workload_last_measurement_ts(tx, workload)?
        .unwrap_or_else(|| Timestamp::from_second(0).unwrap()))
}
