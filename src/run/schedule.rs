//! Eligibility filtering, classification, and limit application over [`RunGroup`]s.
//!
//! Runs entirely on process-local group state plus the recorded case/workload rows — it reads no
//! executable or stdin bytes. Each group classifies itself as new or a rerun candidate (see
//! [`RunGroup::classify`]); this module samples new groups and retains the oldest reruns within the
//! requested limits.

use crate::config::{Config, Key, KeyValue};
use crate::group::{GroupClass, RunGroup, config_matches_filter};
use crate::storage::{HybridDiskError, HybridDiskStorage, StorageRead};
use jiff::Timestamp;
use rand::prelude::*;
use std::collections::BinaryHeap;

/// Ordering applied to the new-group pool before its limit is taken.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum NewGroupOrder {
    /// Uniformly sample new groups from the complete eligible pool.
    #[default]
    Random,
    /// Prefer groups containing earlier values of the configured timeline key.
    TimelineAsc,
    /// Prefer groups containing later values of the configured timeline key.
    TimelineDesc,
}

/// Limits and ordering used to select work for one sampled run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunScheduleConfig {
    /// Maximum number of new groups to select.
    pub new_limit: usize,
    /// Maximum number of reruns to select alongside new groups.
    pub rerun_limit: usize,
    /// Maximum number of reruns to select when no new groups are selected.
    pub rerun_only_limit: usize,
    /// Ordering applied to new groups before limiting them.
    pub new_order: NewGroupOrder,
}

/// A group selected for processing, with its execution intent.
#[derive(Debug, Clone, Copy)]
pub struct SelectedGroup {
    /// Index into the input `groups` slice.
    pub index: usize,
    /// `true` for groups from the new pool (reuse allowed); `false` for deliberate reruns.
    pub reuse: bool,
}

/// Classify and select groups for a sampled `run` command.
///
/// A group is eligible only if at least one covered case matches `config_filter`.
pub fn select_for_run<R: Rng>(
    storage: &HybridDiskStorage,
    groups: &mut [RunGroup],
    config_filter: &Config,
    schedule: RunScheduleConfig,
    rng: &mut R,
) -> Result<Vec<SelectedGroup>, HybridDiskError> {
    let RunScheduleConfig {
        new_limit,
        rerun_limit,
        rerun_only_limit,
        new_order,
    } = schedule;
    if new_limit == 0 && rerun_limit == 0 && rerun_only_limit == 0 {
        return Ok(Vec::new());
    }

    order_groups(
        groups,
        config_filter,
        new_order,
        storage.config_file().timeline_key(),
        rng,
    );

    let mut selected_new = Vec::with_capacity(new_limit.min(groups.len()));
    let retained_rerun_limit = rerun_limit.max(rerun_only_limit);
    // Max-heap: the newest retained rerun is at the top and is evicted when an older one appears.
    let mut oldest_reruns: BinaryHeap<(Timestamp, usize)> =
        BinaryHeap::with_capacity(retained_rerun_limit.min(groups.len()));

    storage.read_transaction(|tx| {
        for (index, group) in groups.iter().enumerate() {
            if !group.eligible(config_filter) {
                continue;
            }
            match group.classify(storage, tx)? {
                GroupClass::New if selected_new.len() < new_limit => selected_new.push(index),
                GroupClass::New => {}
                GroupClass::Rerun(ts) => {
                    retain_oldest(&mut oldest_reruns, retained_rerun_limit, ts, index);
                }
            }

            // Oldest-first rerun selection requires a complete scan. With no reruns requested, the
            // transaction can end as soon as the ordered new selection is full.
            if retained_rerun_limit == 0 && selected_new.len() == new_limit {
                break;
            }
        }
        Ok(())
    })?;

    let effective_rerun_limit = if selected_new.is_empty() {
        rerun_only_limit
    } else {
        rerun_limit
    };
    let mut reruns: Vec<_> = oldest_reruns.into_iter().collect();
    reruns.sort_unstable_by_key(|&(timestamp, _)| timestamp);
    reruns.truncate(effective_rerun_limit);
    let mut rerun_indices: Vec<usize> = reruns.into_iter().map(|(_, index)| index).collect();
    rerun_indices.shuffle(rng);

    let mut selected: Vec<SelectedGroup> = selected_new
        .into_iter()
        .map(|index| SelectedGroup { index, reuse: true })
        .collect();
    selected.extend(rerun_indices.into_iter().map(|index| SelectedGroup {
        index,
        reuse: false,
    }));
    Ok(selected)
}

fn order_groups<R: Rng>(
    groups: &mut [RunGroup],
    config_filter: &Config,
    order: NewGroupOrder,
    timeline_key: Option<&Key>,
    rng: &mut R,
) {
    // Randomise first so equal-priority groups remain a uniform sample. Ordering happens before
    // the read transaction to keep its lifetime limited to database work.
    groups.shuffle(rng);

    let Some(timeline_key) = timeline_key else {
        return;
    };

    let descending = match order {
        NewGroupOrder::Random => return,
        NewGroupOrder::TimelineAsc => false,
        NewGroupOrder::TimelineDesc => true,
    };

    groups.sort_by_cached_key(|group| {
        let priority = timeline_priority(group, config_filter, timeline_key, descending);
        (
            priority.is_none(),
            priority.map_or(0, |index| {
                if descending {
                    usize::MAX - index
                } else {
                    index
                }
            }),
        )
    });
}

fn timeline_priority(
    group: &RunGroup,
    config_filter: &Config,
    timeline_key: &Key,
    descending: bool,
) -> Option<usize> {
    let index = |config: &Config| {
        if !config_matches_filter(config, config_filter) {
            return None;
        }
        config.get(timeline_key).map(KeyValue::value_index)
    };
    let combine: fn(usize, usize) -> usize = if descending { usize::max } else { usize::min };

    match group {
        RunGroup::Shared(group) => group
            .configs()
            .filter_map(|config| index(&config))
            .reduce(combine),
        RunGroup::Isolated(group) => index(&group.config),
    }
}

fn retain_oldest(
    heap: &mut BinaryHeap<(Timestamp, usize)>,
    limit: usize,
    timestamp: Timestamp,
    index: usize,
) {
    if limit == 0 {
        return;
    }
    if heap.len() < limit {
        heap.push((timestamp, index));
    } else if heap
        .peek()
        .is_some_and(|&(newest_retained, _)| timestamp < newest_retained)
    {
        heap.pop();
        heap.push((timestamp, index));
    }
}

/// Select every eligible group for a `run-all` command, all with deliberate-run intent.
#[must_use]
pub fn select_for_run_all(groups: &[RunGroup], config_filter: &Config) -> Vec<SelectedGroup> {
    // Classification is unnecessary for run-all; eligibility still gates the group.
    groups
        .iter()
        .enumerate()
        .filter(|(_, group)| group.eligible(config_filter))
        .map(|(index, _)| SelectedGroup {
            index,
            reuse: false,
        })
        .collect()
}

#[cfg(unix)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::group::{CaseGroup, IsolatedGroup, RunGroup, resolve_and_group};
    use crate::run::process::process_shared_group;
    use crate::run::test_support::{Fixture, write_sampler};
    use crate::storage::Storage;

    fn fixture() -> Fixture {
        let json = r#"{
            "config_keys": { "commit": { "values": ["a", "b"] } },
            "benchmarks": [
                {
                    "benchmark": "bench",
                    "command": ["builds/{commit}/bin"],
                    "config": { "commit": ["a", "b"] },
                    "dedupe": "inode-content",
                    "stats": {
                        "min_samples": 2, "min_time_ns": 1, "runs_per_series": 1,
                        "min_warmup_samples": 1, "min_warmup_time_ns": 1
                    }
                }
            ]
        }"#;
        Fixture::new(json, |data| {
            // Distinct inode per commit (copies) so each commit is its own group.
            for commit in ["a", "b"] {
                write_sampler(&data.join(format!("builds/{commit}/bin")));
            }
        })
    }

    fn shared_groups(f: &Fixture) -> Vec<RunGroup> {
        let variant = &f.storage.config_file().benchmarks()[0].variants()[0];
        let data = f.storage.config_file().data_dir();
        resolve_and_group(data, [variant])
            .0
            .into_iter()
            .map(RunGroup::Shared)
            .collect()
    }

    fn config(f: &Fixture, commit: &str) -> Config {
        f.storage
            .config_file()
            .config_from_string(&format!("commit={commit}"))
            .unwrap()
    }

    fn first_shared(f: &Fixture) -> CaseGroup {
        match shared_groups(f).swap_remove(0) {
            RunGroup::Shared(g) => g,
            RunGroup::Isolated(_) => unreachable!(),
        }
    }

    fn random_schedule(new_limit: usize, rerun_limit: usize) -> RunScheduleConfig {
        RunScheduleConfig {
            new_limit,
            rerun_limit,
            rerun_only_limit: rerun_limit,
            new_order: NewGroupOrder::Random,
        }
    }

    #[test]
    fn unrecorded_groups_are_all_new() {
        let f = fixture();
        let mut groups = shared_groups(&f);
        assert_eq!(groups.len(), 2);
        let selected = select_for_run(
            &f.storage,
            &mut groups,
            &Config::new(),
            random_schedule(16, 8),
            &mut rand::rng(),
        )
        .unwrap();
        assert_eq!(selected.len(), 2);
        assert!(selected.iter().all(|s| s.reuse));
    }

    #[test]
    fn recorded_group_becomes_rerun() {
        let f = fixture();
        let _lock = f.storage.acquire_lock().unwrap();

        // Record commit a.
        process_shared_group(
            &f.storage,
            &f.host_config,
            &first_shared(&f),
            true,
            false,
            false,
        )
        .unwrap();

        let mut groups = shared_groups(&f);
        let selected = select_for_run(
            &f.storage,
            &mut groups,
            &Config::new(),
            random_schedule(16, 8),
            &mut rand::rng(),
        )
        .unwrap();
        // a is now a rerun (reuse=false), b is still new (reuse=true).
        assert_eq!(selected.iter().filter(|s| !s.reuse).count(), 1);
        assert_eq!(selected.iter().filter(|s| s.reuse).count(), 1);
    }

    #[test]
    fn rerun_only_limit_applies_when_no_new_groups_are_selected() {
        let f = fixture();
        let _lock = f.storage.acquire_lock().unwrap();

        for group in shared_groups(&f) {
            let RunGroup::Shared(group) = group else {
                unreachable!();
            };
            process_shared_group(&f.storage, &f.host_config, &group, true, false, false).unwrap();
        }

        let mut groups = shared_groups(&f);
        let selected = select_for_run(
            &f.storage,
            &mut groups,
            &Config::new(),
            RunScheduleConfig {
                new_limit: 16,
                rerun_limit: 1,
                rerun_only_limit: 2,
                new_order: NewGroupOrder::Random,
            },
            &mut rand::rng(),
        )
        .unwrap();
        assert_eq!(selected.len(), 2);
        assert!(selected.iter().all(|selection| !selection.reuse));
    }

    #[test]
    fn equivalent_group_spec_json_stays_a_rerun() {
        let f = fixture();
        let _lock = f.storage.acquire_lock().unwrap();

        let recorded = first_shared(&f);
        process_shared_group(&f.storage, &f.host_config, &recorded, true, false, false).unwrap();

        // Simulate a future serde-only addition plus a serializer formatting change. The decoded
        // GroupSpec and its canonical binary digest are unchanged, despite different JSON text.
        let representative = recorded.resolve_representative().unwrap();
        let mut stored = serde_json::to_value(&representative.group_spec).unwrap();
        stored
            .as_object_mut()
            .unwrap()
            .insert("serde_only_metadata".into(), serde_json::Value::Bool(true));
        let reformatted = serde_json::to_string_pretty(&stored).unwrap();
        f.storage
            .write_transaction(|tx| {
                tx.execute(
                    "UPDATE workloads SET group_spec = ?1 WHERE executable_sha256 IS NOT NULL",
                    [&reformatted],
                )?;
                Ok(())
            })
            .unwrap();

        let mut groups = shared_groups(&f);
        let selected = select_for_run(
            &f.storage,
            &mut groups,
            &Config::new(),
            random_schedule(16, 8),
            &mut rand::rng(),
        )
        .unwrap();
        // The recorded group remains in the rerun pool; only the unrecorded group is new.
        assert_eq!(selected.iter().filter(|s| !s.reuse).count(), 1);
        assert_eq!(selected.iter().filter(|s| s.reuse).count(), 1);
    }

    #[test]
    fn isolated_singleton_groups_classify_new_then_rerun() {
        let f = fixture();
        let bench = "bench".try_into().unwrap();
        let mut groups = vec![RunGroup::Isolated(IsolatedGroup {
            benchmark: bench,
            config: config(&f, "a"),
        })];

        // No recorded result yet => new.
        let selected = select_for_run(
            &f.storage,
            &mut groups,
            &Config::new(),
            random_schedule(16, 8),
            &mut rand::rng(),
        )
        .unwrap();
        assert_eq!(selected.len(), 1);
        assert!(selected[0].reuse);
    }

    #[test]
    fn new_limit_caps_new_groups() {
        let f = fixture();
        let mut groups = shared_groups(&f);
        let selected = select_for_run(
            &f.storage,
            &mut groups,
            &Config::new(),
            random_schedule(1, 8),
            &mut rand::rng(),
        )
        .unwrap();
        assert_eq!(selected.iter().filter(|s| s.reuse).count(), 1);
    }

    #[test]
    fn timeline_order_selects_newest_or_oldest_new_group() {
        let f = fixture();
        let timeline_key = f.storage.config_file().timeline_key().unwrap();

        for (order, expected) in [
            (NewGroupOrder::TimelineAsc, "a"),
            (NewGroupOrder::TimelineDesc, "b"),
        ] {
            let mut groups = shared_groups(&f);
            let selected = select_for_run(
                &f.storage,
                &mut groups,
                &Config::new(),
                RunScheduleConfig {
                    new_limit: 1,
                    rerun_limit: 0,
                    rerun_only_limit: 0,
                    new_order: order,
                },
                &mut rand::rng(),
            )
            .unwrap();

            assert_eq!(selected.len(), 1);
            let RunGroup::Shared(group) = &groups[selected[0].index] else {
                unreachable!();
            };
            let selected_config = group.configs().next().unwrap();
            assert_eq!(
                selected_config.get(timeline_key).unwrap().value_name(),
                expected
            );
        }
    }

    #[test]
    fn filter_gates_eligibility() {
        let f = fixture();
        let mut groups = shared_groups(&f);
        let filter = config(&f, "a");
        let selected = select_for_run(
            &f.storage,
            &mut groups,
            &filter,
            random_schedule(16, 8),
            &mut rand::rng(),
        )
        .unwrap();
        // Only the commit=a group is eligible.
        assert_eq!(selected.len(), 1);
    }

    #[test]
    fn bounded_heap_retains_only_oldest_reruns() {
        let mut heap = BinaryHeap::new();
        for (index, second) in [(0, 30), (1, 10), (2, 40), (3, 20)] {
            retain_oldest(&mut heap, 2, Timestamp::from_second(second).unwrap(), index);
        }

        let mut retained: Vec<_> = heap.into_iter().map(|(_, index)| index).collect();
        retained.sort_unstable();
        assert_eq!(retained, [1, 3]);
    }

    #[test]
    fn zero_rerun_limit_retains_nothing() {
        let mut heap = BinaryHeap::new();
        retain_oldest(&mut heap, 0, Timestamp::from_second(10).unwrap(), 0);
        assert!(heap.is_empty());
    }

    #[test]
    fn run_all_selects_every_eligible_group() {
        let f = fixture();
        let groups = shared_groups(&f);
        let selected = select_for_run_all(&groups, &Config::new());
        assert_eq!(selected.len(), 2);
        assert!(selected.iter().all(|s| !s.reuse));
    }
}
