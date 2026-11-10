use aoc_bench::run::RunSeries;
use aoc_bench::stable::{Change, ChangeDirection};

// 4 before dot, 1 for dot, 2 after dot, 1 for space, 2 for unit
pub const DURATION_WIDTH: usize = 10;

// 1 for +-, 3 before dot, 1 for dot, 2 after dot, 1 for %
pub const DELTA_WIDTH: usize = 8;

pub fn format_duration_ns(ns: f64) -> String {
    if ns >= 1_000_000_000.0 {
        format!("{:.2} s", ns / 1_000_000_000.0)
    } else if ns >= 1_000_000.0 {
        format!("{:.2} ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:.2} µs", ns / 1_000.0)
    } else {
        format!("{ns:.0} ns")
    }
}

/// A measurement without a run interval (a single run, or a legacy row) shows `--`, matching
/// the delta column's marker for the initial point.
pub fn format_ci(ns: Option<f64>) -> String {
    match ns {
        Some(ns) => format!("±{}", format_duration_ns(ns)),
        None => "--".to_string(),
    }
}

pub fn format_delta(change: Change) -> String {
    let pct = change.rel_change * 100.0;
    match change.direction {
        ChangeDirection::Regression => format!("+{pct:.2}%"),
        ChangeDirection::Improvement => format!("-{pct:.2}%"),
    }
}

/// The unit the headline mean lands in, as a divisor and label, so every value of a series can be
/// printed in the same unit and line up.
fn unit_for_ns(ns: f64) -> (f64, &'static str) {
    if ns >= 1_000_000_000.0 {
        (1_000_000_000.0, "s")
    } else if ns >= 1_000_000.0 {
        (1_000_000.0, "ms")
    } else if ns >= 1_000.0 {
        (1_000.0, "µs")
    } else {
        (1.0, "ns")
    }
}

fn scaled(ns: f64, unit: (f64, &str)) -> String {
    if unit.1 == "ns" {
        format!("{ns:.0}")
    } else {
        format!("{:.2}", ns / unit.0)
    }
}

fn percent_of(part: f64, whole: f64) -> String {
    format!("{:.2}%", part / whole * 100.0)
}

/// The human-readable form of a run series: the summary line, the sorted run values, and with
/// `with_runs` one line per run in the order they ran. Every number comes from the series itself.
///
/// ```text
/// 17.37 ms/iter ±0.16% (9 runs)
/// runs: 17.32 17.35 17.35 17.36 17.36 17.37 17.40 17.41 17.44 ms
/// ```
#[must_use]
pub fn format_series_summary(series: &RunSeries, with_runs: bool) -> String {
    use std::fmt::Write as _;

    let summary = series.summary();
    let unit = unit_for_ns(summary.mean_ns);
    let mut out = String::new();

    let _ = write!(out, "{} {}/iter ", scaled(summary.mean_ns, unit), unit.1);
    match summary.ci95_half_ns {
        Some(half) => {
            let _ = write!(
                out,
                "±{} ({} runs)",
                percent_of(half, summary.mean_ns),
                summary.run_count
            );
        }
        None => out.push_str("(1 run)"),
    }
    out.push('\n');

    let mut means: Vec<f64> = series
        .runs
        .iter()
        .map(|r| r.stats.mean_ns_per_iter)
        .collect();
    means.sort_by(f64::total_cmp);
    let values: Vec<String> = means.iter().map(|&m| scaled(m, unit)).collect();
    let _ = writeln!(out, "runs: {} {}", values.join(" "), unit.1);

    if with_runs {
        let mut runs: Vec<_> = series.runs.iter().collect();
        runs.sort_by_key(|r| r.timestamp);
        for (i, run) in runs.iter().enumerate() {
            let mean = run.stats.mean_ns_per_iter;
            let _ = write!(out, "run {}: {} {}/iter", i + 1, scaled(mean, unit), unit.1);
            if let Some(se) = run.stats.se_ns {
                let _ = write!(out, " se ±{}", percent_of(se, mean));
            }
            let _ = writeln!(out, " ({} samples)", run.stats.samples.len());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use aoc_bench::config::Config;
    use aoc_bench::run::Run;
    use aoc_bench::stats::{EstimationMode, Sample, StatsResult};
    use jiff::Timestamp;

    fn run(second: i64, mean_ns: f64, se_ns: Option<f64>, samples: usize) -> Run {
        Run {
            timestamp: Timestamp::from_second(second).unwrap(),
            stats: StatsResult {
                mean_ns_per_iter: mean_ns,
                mode: EstimationMode::PerIter,
                intercept_ns: None,
                se_ns,
                // Only the sample count is shown, not the samples themselves
                samples: vec![
                    Sample {
                        iters: 1,
                        total_ns: 1
                    };
                    samples
                ],
            },
            warmup: None,
            power: None,
            wall_ns: None,
        }
    }

    fn series(runs: Vec<Run>) -> RunSeries {
        RunSeries {
            schema: 1,
            bench: "2015-04".try_into().unwrap(),
            config: Config::new(),
            timestamp: Timestamp::from_second(0).unwrap(),
            wall: std::time::Duration::ZERO,
            runs,
            checksum: None,
        }
    }

    #[test]
    fn summary_lists_sorted_runs_in_the_headline_unit() {
        // Means in ms, given out of order; sample sd 40 µs over 9 runs
        let means = [
            17.36, 17.41, 17.32, 17.40, 17.35, 17.35, 17.44, 17.37, 17.36,
        ];
        let runs = means
            .iter()
            .enumerate()
            .map(|(i, m)| run(i64::try_from(i).unwrap() * 15, m * 1e6, Some(3_500.0), 112))
            .collect();
        let text = format_series_summary(&series(runs), false);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("17.37 ms/iter ±0."), "{}", lines[0]);
        assert!(lines[0].ends_with("% (9 runs)"), "{}", lines[0]);
        assert_eq!(
            lines[1],
            "runs: 17.32 17.35 17.35 17.36 17.36 17.37 17.40 17.41 17.44 ms"
        );
    }

    #[test]
    fn summary_with_runs_lists_them_in_time_order() {
        // The runs line is sorted but the per-run lines are not
        let runs = vec![
            run(30, 95_500_000.0, Some(28_000.0), 24),
            run(0, 82_000_000.0, Some(25_000.0), 24),
            run(15, 95_400_000.0, None, 24),
            run(45, 82_100_000.0, Some(25_000.0), 24),
        ];
        let text = format_series_summary(&series(runs), true);
        let lines: Vec<&str> = text.lines().collect();
        assert!(lines[0].ends_with("(4 runs)"), "{}", lines[0]);
        assert_eq!(lines[1], "runs: 82.00 82.10 95.40 95.50 ms");
        assert_eq!(lines[2], "run 1: 82.00 ms/iter se ±0.03% (24 samples)");
        assert_eq!(lines[3], "run 2: 95.40 ms/iter (24 samples)");
        assert_eq!(lines[4], "run 3: 95.50 ms/iter se ±0.03% (24 samples)");
        assert_eq!(lines[5], "run 4: 82.10 ms/iter se ±0.03% (24 samples)");
    }

    #[test]
    fn single_run_has_no_interval_and_nanoseconds_print_whole() {
        let text = format_series_summary(&series(vec![run(0, 612.4, None, 200)]), false);
        assert_eq!(text, "612 ns/iter (1 run)\nruns: 612 ns\n");
    }
}
