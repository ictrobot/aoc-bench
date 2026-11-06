//! Batch state and per-group accounting pushed by the run engine, see [`crate::metrics`].
//!
//! Lives for one `run` or `run-all` invocation. Each group's increments are checkpointed when it
//! finishes, so they carry the group's end time. Dropping it, on any exit path including a
//! fail-fast error or a panic, closes the group in flight as failed, records the batch's own wall
//! time under the `other` phase, pushes the closing state with `aoc_bench_active` at 0 and waits
//! briefly for that push.

use crate::group::RunGroup;
use crate::metrics::{self, Labels, Metrics, Phase, count};
use crate::run::PoolStats;
use crate::storage::HybridDiskStorage;
use jiff::Timestamp;
use std::time::{Duration, Instant};
use tracing::warn;

/// How long the closing push may take before the process moves on without it.
const EXIT_PUSH_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) struct BatchMetrics<'a> {
    metrics: &'a Metrics,
    /// When the run began, before any scheduling, so the phases sum to its wall clock
    started: Instant,
    /// Wall time of the finished groups. The rest of the batch is `other` with no bench.
    group_wall: Duration,
    remaining: Vec<(&'static str, usize)>,
    current: Option<Group>,
}

struct Group {
    started: Instant,
    accounted: Duration,
    class: &'static str,
    labels: Labels,
}

impl<'a> BatchMetrics<'a> {
    /// Push the opening state: the selection by class, and the dataset numbers.
    ///
    /// `started` is when the run began. `pool` is what the scheduler saw, when it scanned all of
    /// it, and `selected` the groups chosen by class.
    pub fn start(
        metrics: &'a Metrics,
        storage: &HybridDiskStorage,
        started: Instant,
        pool: Option<&PoolStats>,
        selected: &[(&'static str, usize)],
    ) -> Self {
        let none = Labels::new();
        metrics.gauge_set(metrics::ACTIVE, &none, 1.0);
        metrics.gauge_timestamp(metrics::BATCH_START_TIMESTAMP, &none, Timestamp::now());
        for &(class, groups) in selected {
            let labels = none.clone().with("class", class);
            metrics.gauge_set(metrics::BATCH_GROUPS_SELECTED, &labels, count(groups));
            metrics.gauge_set(metrics::BATCH_GROUPS_REMAINING, &labels, count(groups));
        }

        if let Some(pool) = pool {
            for (class, groups, cases) in [
                ("new", pool.new_groups, pool.new_cases),
                ("rerun", pool.rerun_groups, pool.rerun_cases),
            ] {
                let labels = none.clone().with("class", class);
                metrics.gauge_set(metrics::DATASET_GROUPS, &labels, count(groups));
                metrics.gauge_set(metrics::DATASET_CASES, &labels, count(cases));
            }
            metrics.gauge_set(
                metrics::DATASET_WORKLOADS_SUSPICIOUS,
                &none,
                count(pool.suspicious_workloads),
            );
            if let Some(oldest) = pool.oldest_measurement {
                metrics.gauge_timestamp(
                    metrics::DATASET_OLDEST_MEASUREMENT_TIMESTAMP,
                    &none,
                    oldest,
                );
            }
        }
        match storage.dataset_stats() {
            Ok(stats) => {
                metrics.gauge_count(metrics::DATASET_MEASUREMENTS, &none, stats.measurements);
                metrics.gauge_count(
                    metrics::DATASET_MEASUREMENT_RUNS,
                    &none,
                    stats.measurement_runs,
                );
                metrics.gauge_count(metrics::DATASET_DB_BYTES, &none, stats.db_bytes);
            }
            Err(error) => warn!(%error, "dataset metrics not available"),
        }

        metrics.flush();
        Self {
            metrics,
            started,
            group_wall: Duration::ZERO,
            remaining: selected.to_vec(),
            current: None,
        }
    }

    /// Note the group now running.
    pub fn group_started(&mut self, group: &RunGroup, class: &'static str) {
        let bench = group.benchmark();
        self.metrics.gauge_timestamp(
            metrics::GROUP_START_TIMESTAMP,
            &Labels::new().with("bench", bench),
            Timestamp::now(),
        );
        self.metrics.gauge_set(
            metrics::GROUP_CASES,
            &Labels::new(),
            count(group.case_count()),
        );
        self.current = Some(Group {
            started: Instant::now(),
            accounted: self.metrics.accounted(),
            class,
            labels: Labels::case(bench, &group.representative_config()),
        });
    }

    /// Count the group, attribute its wall time not already under a run phase to `other`, and
    /// checkpoint its increments at this moment.
    pub fn group_finished(&mut self, outcome: &'static str) {
        let Some(group) = self.current.take() else {
            return;
        };
        let wall = group.started.elapsed();
        let in_runs = self.metrics.accounted().saturating_sub(group.accounted);
        self.metrics
            .add_time(Phase::Other, &group.labels, wall.saturating_sub(in_runs));
        self.group_wall += wall;
        self.count_group(&group.labels, group.class, outcome);
        self.metrics.gauge_clear(metrics::GROUP_START_TIMESTAMP);
        self.metrics.gauge_clear(metrics::GROUP_CASES);
        self.metrics.checkpoint(Timestamp::now());
    }

    /// Count a group that failed before it could start, such as one that did not resolve.
    pub fn group_failed_early(&mut self, labels: &Labels, class: &'static str) {
        self.count_group(labels, class, "failed");
        self.metrics.checkpoint(Timestamp::now());
    }

    fn count_group(&mut self, labels: &Labels, class: &'static str, outcome: &'static str) {
        let labels = labels.clone().with("class", class).with("outcome", outcome);
        self.metrics.increment(metrics::GROUPS, &labels, 1.0);
        if let Some((_, remaining)) = self.remaining.iter_mut().find(|(c, _)| *c == class) {
            *remaining = remaining.saturating_sub(1);
            self.metrics.gauge_set(
                metrics::BATCH_GROUPS_REMAINING,
                &Labels::new().with("class", class),
                count(*remaining),
            );
        }
    }
}

impl Drop for BatchMetrics<'_> {
    fn drop(&mut self) {
        // A group still open here ended in an error or a panic
        self.group_finished("failed");
        let outside_groups = self.started.elapsed().saturating_sub(self.group_wall);
        self.metrics
            .add_time(Phase::Other, &Labels::new(), outside_groups);
        self.metrics.checkpoint(Timestamp::now());
        self.metrics.gauge_set(metrics::ACTIVE, &Labels::new(), 0.0);
        self.metrics.flush_and_wait(EXIT_PUSH_TIMEOUT);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::group::IsolatedGroup;
    use crate::metrics::test_support::sink;
    use crate::run::test_support::Fixture;

    #[test]
    fn dropping_with_a_group_open_counts_it_as_failed() {
        let f = Fixture::new(
            r#"{
                "config_keys": {},
                "benchmarks": [{ "benchmark": "b", "command": ["true"], "config": {} }]
            }"#,
            |_| {},
        );
        let (url, rx) = sink("204 No Content");
        let metrics = Metrics::start(&url, "testhost", Duration::from_secs(3600));
        let group = RunGroup::Isolated(IsolatedGroup {
            benchmark: "b".try_into().unwrap(),
            config: Config::new(),
        });
        let labels = Labels::case(group.benchmark(), &Config::new());

        {
            let mut batch =
                BatchMetrics::start(&metrics, &f.storage, Instant::now(), None, &[("new", 1)]);
            let opening = rx.recv().unwrap();
            assert!(
                opening.contains("aoc_bench_active{host=\"testhost\"} 1 "),
                "{opening}"
            );
            assert!(
                opening.contains(
                    "aoc_bench_batch_groups_remaining{host=\"testhost\",class=\"new\"} 1 "
                )
            );
            batch.group_started(&group, "new");
            metrics.add_time(Phase::Measurement, &labels, Duration::from_millis(1));
            // Dropped without group_finished, as on a fail-fast error or a panic
        }

        let closing = rx.recv().unwrap();
        for line in [
            "aoc_bench_groups{host=\"testhost\",bench=\"b\",class=\"new\",outcome=\"failed\"} 1 ",
            "aoc_bench_time_seconds{host=\"testhost\",bench=\"b\",phase=\"measurement\"} 0.001 ",
            "aoc_bench_time_seconds{host=\"testhost\",phase=\"other\"} ",
            "aoc_bench_batch_groups_remaining{host=\"testhost\",class=\"new\"} 0 ",
            "aoc_bench_batch_groups{host=\"testhost\",class=\"new\",outcome=\"failed\"} 1 ",
            "aoc_bench_batch_time_seconds{host=\"testhost\",phase=\"measurement\"} 0.001 ",
            "aoc_bench_active{host=\"testhost\"} 0 ",
        ] {
            assert!(closing.contains(line), "{line} in {closing}");
        }
        assert!(
            !closing.contains("aoc_bench_group_start_timestamp_seconds"),
            "{closing}"
        );
    }
}
