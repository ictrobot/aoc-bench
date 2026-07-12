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
    New,
    /// A rerun candidate, ordered by its workload's last-measurement time.
    Rerun(Timestamp),
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
    let mut agreed_workload: Option<WorkloadId> = None;

    for config in group.configs() {
        let config_json = serde_json::to_string(&config)?;
        let Some(case) = storage.find_case_id(tx, group.key.benchmark.as_str(), &config_json)?
        else {
            return Ok(GroupClass::New); // case never recorded
        };
        let Some(workload) = storage.get_case_workload(tx, case)? else {
            return Ok(GroupClass::New); // no recorded result
        };
        match agreed_workload {
            None => agreed_workload = Some(workload),
            Some(existing) if existing == workload => {}
            Some(_) => return Ok(GroupClass::New), // members disagree
        }
    }

    let Some(workload) = agreed_workload else {
        return Ok(GroupClass::New); // empty group (should not happen)
    };
    let Some(meta) = storage.get_workload_meta(tx, workload)? else {
        return Ok(GroupClass::New);
    };
    if !meta.is_shared || meta.group_spec_digest != Some(current_spec_digest) {
        // Isolated association or the recorded spec no longer matches the current group.
        return Ok(GroupClass::New);
    }
    Ok(GroupClass::Rerun(last_measurement_ts(
        storage, tx, workload,
    )?))
}

/// Classify an isolated single case: new if it has no recorded result, otherwise a rerun.
fn classify_isolated(
    storage: &HybridDiskStorage,
    tx: &Transaction<'_>,
    benchmark: &BenchmarkId,
    config: &Config,
) -> Result<GroupClass, HybridDiskError> {
    let config_json = serde_json::to_string(config)?;
    let Some(case) = storage.find_case_id(tx, benchmark.as_str(), &config_json)? else {
        return Ok(GroupClass::New);
    };
    let Some(workload) = storage.get_case_workload(tx, case)? else {
        return Ok(GroupClass::New);
    };
    Ok(GroupClass::Rerun(last_measurement_ts(
        storage, tx, workload,
    )?))
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
