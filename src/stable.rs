//! Stable result management: drift detection and promotion logic.
//!
//! This module owns the storage-independent drift/promotion state machine ([`compute_drift`]) and
//! its result types. Measurement recording in [`crate::run::process`] applies it to both shared and
//! isolated workloads.

use crate::storage::MeasurementStats;
use std::fmt::{Display, Formatter};

const STABLE_RESULT_CHANGE_REL_THRESHOLD: f64 = 0.03; // 3%
const STABLE_RESULT_CHANGE_REQUIRED_COUNT: u64 = 3;

/// Drift counters that move together as measurements are recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DriftCounters {
    /// Total matches since the last replacement.
    pub matched_count: u64,
    /// Consecutive suspicious series since the last match.
    pub suspicious_count: u64,
    /// Total replacements.
    pub replaced_count: u64,
}

/// The result of applying one measurement to a set of drift counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriftUpdate {
    /// The updated counters.
    pub counters: DriftCounters,
    /// Whether the stable pointer should move to the new measurement.
    pub stable_moved: bool,
}

/// Pure drift-detection state machine for updating per-workload stable state.
///
/// Given the current stable stats, the new measurement's stats, and the current counters, returns
/// the updated counters (and whether the stable pointer moves) plus the [`RecordOutcome`]. This is
/// the single source of truth for promotion after `STABLE_RESULT_CHANGE_REQUIRED_COUNT` consecutive
/// suspicious series, forced replacement, and match/reset behavior.
#[must_use]
pub fn compute_drift(
    stable_stats: MeasurementStats,
    new_stats: MeasurementStats,
    counters: DriftCounters,
    force_update_stable: bool,
) -> (DriftUpdate, RecordOutcome) {
    let is_suspicious = significant_change(stable_stats, new_stats).is_some();

    if is_suspicious || force_update_stable {
        let suspicious_count = counters.suspicious_count + 1;

        if suspicious_count >= STABLE_RESULT_CHANGE_REQUIRED_COUNT {
            (
                DriftUpdate {
                    counters: DriftCounters {
                        matched_count: 0,
                        suspicious_count: 0,
                        replaced_count: counters.replaced_count + 1,
                    },
                    stable_moved: true,
                },
                RecordOutcome::Replaced {
                    old_stable: stable_stats,
                },
            )
        } else if force_update_stable {
            (
                DriftUpdate {
                    counters: DriftCounters {
                        matched_count: 0,
                        suspicious_count: 0,
                        replaced_count: counters.replaced_count + 1,
                    },
                    stable_moved: true,
                },
                RecordOutcome::Forced {
                    old_stable: stable_stats,
                },
            )
        } else {
            // Does not reset matched_count which is the number of matches since replacement, not
            // the number of consecutive matches
            (
                DriftUpdate {
                    counters: DriftCounters {
                        suspicious_count,
                        ..counters
                    },
                    stable_moved: false,
                },
                RecordOutcome::Suspicious {
                    current_stable: stable_stats,
                    suspicious_count,
                },
            )
        }
    } else {
        (
            DriftUpdate {
                counters: DriftCounters {
                    matched_count: counters.matched_count + 1,
                    suspicious_count: 0,
                    replaced_count: counters.replaced_count,
                },
                stable_moved: false,
            },
            RecordOutcome::Matched,
        )
    }
}

/// Determine whether `new_stats` represents a significant change relative to `stable`.
///
/// A change is significant when the confidence intervals do not overlap and the relative
/// difference in means is at least 3%. Returns the direction of the change if significant.
#[must_use]
pub fn significant_change(stable: MeasurementStats, new_stats: MeasurementStats) -> Option<Change> {
    significant_change_with_threshold(stable, new_stats, STABLE_RESULT_CHANGE_REL_THRESHOLD)
}

/// Determine whether `new_stats` represents a significant change relative to `stable`,
/// using a caller-provided relative threshold.
///
/// A change is significant when the confidence intervals do not overlap and the relative
/// difference in means is at least `rel_threshold`.
#[must_use]
pub fn significant_change_with_threshold(
    stable: MeasurementStats,
    new_stats: MeasurementStats,
    rel_threshold: f64,
) -> Option<Change> {
    let (stable_low, stable_high) = stable.bounds();
    let (new_low, new_high) = new_stats.bounds();
    let overlap = !(stable_high < new_low || new_high < stable_low);

    let rel_diff = if stable.median_run_mean_ns == 0.0 {
        f64::INFINITY
    } else {
        (new_stats.median_run_mean_ns - stable.median_run_mean_ns).abs() / stable.median_run_mean_ns
    };

    if overlap || rel_diff < rel_threshold {
        None
    } else if new_stats.median_run_mean_ns > stable.median_run_mean_ns {
        Some(Change {
            direction: ChangeDirection::Regression,
            rel_change: rel_diff,
        })
    } else if new_stats.median_run_mean_ns < stable.median_run_mean_ns {
        Some(Change {
            direction: ChangeDirection::Improvement,
            rel_change: rel_diff,
        })
    } else {
        None
    }
}

/// Direction of a statistically significant change between two stable results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeDirection {
    Regression,
    Improvement,
}

impl Display for ChangeDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeDirection::Regression => write!(f, "REGRESSION"),
            ChangeDirection::Improvement => write!(f, "IMPROVEMENT"),
        }
    }
}

/// Description of a significant change, including its direction and magnitude.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Change {
    pub direction: ChangeDirection,
    /// Absolute relative change (e.g. 0.05 = 5%)
    pub rel_change: f64,
}

/// Outcome of applying one measurement to a workload's stable/drift state.
#[derive(Debug, Clone, PartialEq)]
pub enum RecordOutcome {
    Initial,
    Matched,
    Suspicious {
        current_stable: MeasurementStats,
        suspicious_count: u64,
    },
    Replaced {
        old_stable: MeasurementStats,
    },
    Forced {
        old_stable: MeasurementStats,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(mean: f64, ci: f64) -> MeasurementStats {
        MeasurementStats {
            run_count: 3,
            median_run_mean_ns: mean,
            median_run_ci95_half_ns: ci,
            median_run_outlier_count: 0,
            median_run_sample_count: 32,
        }
    }

    fn counters(matched: u64, suspicious: u64, replaced: u64) -> DriftCounters {
        DriftCounters {
            matched_count: matched,
            suspicious_count: suspicious,
            replaced_count: replaced,
        }
    }

    #[test]
    fn overlapping_small_change_matches_and_resets_suspicious() {
        // CIs overlap and means are within threshold => Matched.
        let (update, outcome) = compute_drift(
            stats(1000.0, 100.0),
            stats(1010.0, 50.0),
            counters(5, 1, 2),
            false,
        );
        assert_eq!(outcome, RecordOutcome::Matched);
        assert_eq!(update.counters, counters(6, 0, 2));
        assert!(!update.stable_moved);
    }

    #[test]
    fn significant_change_accumulates_suspicion_without_moving_stable() {
        // Non-overlapping CIs, >3% change, first suspicious.
        let (update, outcome) = compute_drift(
            stats(1000.0, 10.0),
            stats(1100.0, 10.0),
            counters(4, 0, 0),
            false,
        );
        assert_eq!(
            outcome,
            RecordOutcome::Suspicious {
                current_stable: stats(1000.0, 10.0),
                suspicious_count: 1,
            }
        );
        // matched_count is preserved (matches since replacement), stable does not move.
        assert_eq!(update.counters, counters(4, 1, 0));
        assert!(!update.stable_moved);
    }

    #[test]
    fn third_consecutive_suspicious_replaces_stable() {
        let (update, outcome) = compute_drift(
            stats(1000.0, 10.0),
            stats(1100.0, 10.0),
            counters(9, 2, 1),
            false,
        );
        assert_eq!(
            outcome,
            RecordOutcome::Replaced {
                old_stable: stats(1000.0, 10.0),
            }
        );
        assert_eq!(update.counters, counters(0, 0, 2));
        assert!(update.stable_moved);
    }

    #[test]
    fn force_update_replaces_immediately() {
        // Even with overlapping CIs, force moves the stable pointer.
        let (update, outcome) = compute_drift(
            stats(1000.0, 100.0),
            stats(1005.0, 100.0),
            counters(7, 0, 3),
            true,
        );
        assert_eq!(
            outcome,
            RecordOutcome::Forced {
                old_stable: stats(1000.0, 100.0),
            }
        );
        assert_eq!(update.counters, counters(0, 0, 4));
        assert!(update.stable_moved);
    }

    #[test]
    fn significant_change_direction() {
        assert_eq!(
            significant_change(stats(1000.0, 10.0), stats(1100.0, 10.0))
                .unwrap()
                .direction,
            ChangeDirection::Regression
        );
        assert_eq!(
            significant_change(stats(1000.0, 10.0), stats(900.0, 10.0))
                .unwrap()
                .direction,
            ChangeDirection::Improvement
        );
        // Overlapping CIs => not significant.
        assert!(significant_change(stats(1000.0, 100.0), stats(1010.0, 100.0)).is_none());
    }
}
