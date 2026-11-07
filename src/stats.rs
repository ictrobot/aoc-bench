// Statistics engine: per-run estimation (mode detection, WLS, the run value) and the
// across-run summary (mean and 95% t-interval over runs).
//
// A run continues until its sampling floors are met and then stops. Nothing here adapts to the data:
// the interval reported for a measurement is computed over runs, and the samples inside a
// run only determine how precisely that run was timed.

#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use serde::{Deserialize, Serialize};
use std::num::{NonZeroU64, NonZeroUsize};
use tracing::trace;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[must_use]
pub struct StatsOptions {
    /// Minimum post-warmup samples per run
    pub min_samples: NonZeroUsize,
    /// Minimum total post-warmup sample time per run (nanoseconds)
    pub min_total_time_ns: NonZeroU64,
    /// Warmup minimum samples, counted from the first settled batch
    pub min_warmup_samples: NonZeroUsize,
    /// Warmup minimum total time (nanoseconds), counted from the first settled batch
    pub min_warmup_time_ns: NonZeroU64,
    /// Number of runs per measurement
    pub runs_per_series: NonZeroUsize,
    /// Wall-clock timeout for a single run (nanoseconds)
    pub run_timeout_ns: NonZeroU64,
}

impl StatsOptions {
    pub const DEFAULT_MIN_SAMPLES: usize = 4;
    pub const DEFAULT_MIN_TOTAL_TIME_NS: u64 = 2_000_000_000; // 2 seconds
    pub const DEFAULT_MIN_WARMUP_SAMPLES: usize = 2;
    pub const DEFAULT_MIN_WARMUP_TIME_NS: u64 = 500_000_000; // 0.5 seconds
    pub const DEFAULT_RUNS_PER_SERIES: usize = 9;
    pub const DEFAULT_RUN_TIMEOUT_NS: u64 = 600_000_000_000; // 600 s
}

impl Default for StatsOptions {
    fn default() -> Self {
        Self {
            min_samples: NonZeroUsize::new(Self::DEFAULT_MIN_SAMPLES).unwrap(),
            min_total_time_ns: NonZeroU64::new(Self::DEFAULT_MIN_TOTAL_TIME_NS).unwrap(),
            min_warmup_samples: NonZeroUsize::new(Self::DEFAULT_MIN_WARMUP_SAMPLES).unwrap(),
            min_warmup_time_ns: NonZeroU64::new(Self::DEFAULT_MIN_WARMUP_TIME_NS).unwrap(),
            runs_per_series: NonZeroUsize::new(Self::DEFAULT_RUNS_PER_SERIES).unwrap(),
            run_timeout_ns: NonZeroU64::new(Self::DEFAULT_RUN_TIMEOUT_NS).unwrap(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[must_use]
pub enum EstimationMode {
    Regression,
    PerIter,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[must_use]
pub enum StatsState {
    MoreSamplesRequired,
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub iters: u64,
    pub total_ns: u64,
}

#[derive(Debug, Clone)]
pub struct StatsAccumulator {
    options: StatsOptions,
    samples: Vec<Sample>,
    total_time_ns: u64,
}

#[derive(Debug, Clone, Copy)]
#[must_use]
pub struct RegressionResult {
    pub slope: f64,     // β: nanoseconds per iteration
    pub intercept: f64, // α: fixed overhead per batch
}

/// The value of one run plus its diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[must_use]
pub struct StatsResult {
    /// Mean time per iteration in nanoseconds
    pub mean_ns_per_iter: f64,
    /// Estimation mode used
    pub mode: EstimationMode,
    /// Fixed overhead per batch (only for regression mode, null otherwise)
    pub intercept_ns: Option<f64>,
    /// Standard error of `mean_ns_per_iter` under the estimator that produced it (the WLS slope
    /// in regression mode, the weighted mean otherwise), a diagnostic only. Absent when there are
    /// too few samples to estimate it.
    #[serde(default)]
    pub se_ns: Option<f64>,
    /// All samples collected for this run
    pub samples: Vec<Sample>,
}

impl StatsAccumulator {
    /// Memory guard only. Far above anything the time floor can collect before it is met, so it
    /// never shortens a run in practice.
    pub const MAX_SAMPLES: usize = 1 << 16;

    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_options(options: StatsOptions) -> Self {
        StatsAccumulator {
            options,
            samples: Vec::new(),
            total_time_ns: 0,
        }
    }

    /// Add a sample. Warmup samples are discarded before this by [`crate::warmup::Warmup`].
    ///
    /// Returns whether the run's sampling floors have been met.
    pub fn add_sample(&mut self, iters: u64, total_ns: u64) -> StatsState {
        trace!(iters, total_ns, "new sample");
        self.samples.push(Sample { iters, total_ns });
        self.total_time_ns = self.total_time_ns.saturating_add(total_ns);
        self.state()
    }

    /// Returns the number of samples collected
    #[must_use]
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Returns a reference to all collected samples
    #[must_use]
    pub fn samples(&self) -> &[Sample] {
        &self.samples
    }

    /// `Done` once both the sample and time floors are met (or the memory guard is hit).
    pub fn state(&self) -> StatsState {
        if self.samples.len() >= Self::MAX_SAMPLES {
            return StatsState::Done;
        }
        if self.samples.len() < self.options.min_samples.get()
            || self.total_time_ns < self.options.min_total_time_ns.get()
        {
            return StatsState::MoreSamplesRequired;
        }
        StatsState::Done
    }

    /// Finalizes sampling and computes the run value.
    pub fn finish(self) -> StatsResult {
        let mode = self.detect_mode();
        let (mean_ns_per_iter, regression) = match mode {
            EstimationMode::Regression => {
                let regression = self.compute_wls();
                (regression.slope, Some(regression))
            }
            EstimationMode::PerIter => (self.compute_weighted_mean(), None),
        };
        let se_ns = self.compute_se(mean_ns_per_iter, regression);

        StatsResult {
            mode,
            mean_ns_per_iter,
            intercept_ns: regression.map(|r| r.intercept),
            se_ns,
            samples: self.samples,
        }
    }

    /// Determines which estimation mode to use based on iteration count variation
    pub fn detect_mode(&self) -> EstimationMode {
        if self.samples.is_empty() {
            return EstimationMode::PerIter;
        }

        let iters = self.samples.iter().map(|s| s.iters);

        // Count distinct iteration counts
        let mut sorted_iters: Vec<u64> = iters.clone().collect();
        sorted_iters.sort_unstable();
        sorted_iters.dedup();
        let distinct_n = sorted_iters.len();

        // Compute range: max/min
        let min_n = iters.clone().min().unwrap() as f64;
        let max_n = iters.clone().max().unwrap() as f64;
        let range_n = if min_n > 0.0 { max_n / min_n } else { 1.0 };

        // Compute coefficient of variation: stdev/mean
        let sum_n = iters.clone().sum::<u64>();
        let mean_n = sum_n as f64 / self.samples.len() as f64;
        let variance = iters
            .map(|n| {
                let diff = n as f64 - mean_n;
                diff * diff
            })
            .sum::<f64>()
            / self.samples.len() as f64;
        let stdev_n = variance.sqrt();
        let cv_n = if mean_n > 0.0 { stdev_n / mean_n } else { 0.0 };

        // Mode detection heuristic from design doc:
        if distinct_n >= 3 && (range_n >= 2.0 || cv_n >= 0.15) {
            EstimationMode::Regression
        } else {
            EstimationMode::PerIter
        }
    }

    /// Compute weighted least squares regression: T = α + β·N with weights w = 1/N
    pub fn compute_wls(&self) -> RegressionResult {
        Self::compute_wls_from_samples(&self.samples)
    }

    /// Helper: compute WLS from a given set of samples
    fn compute_wls_from_samples(samples: &[Sample]) -> RegressionResult {
        if samples.is_empty() {
            return RegressionResult {
                slope: 0.0,
                intercept: 0.0,
            };
        }

        // Pass 1: means
        let mut w_sum = 0.0;
        let mut w_n_sum = 0.0;
        let mut w_t_sum = 0.0;
        for s in samples {
            if s.iters == 0 {
                continue;
            }

            let n = s.iters as f64;
            let t = s.total_ns as f64;
            let w = 1.0 / n;

            w_sum += w;
            w_n_sum += w * n;
            w_t_sum += w * t;
        }

        if w_sum == 0.0 {
            return RegressionResult {
                slope: 0.0,
                intercept: 0.0,
            };
        }

        let n_mean = w_n_sum / w_sum;
        let t_mean = w_t_sum / w_sum;

        // Pass 2: covariance and variance
        let mut var_n = 0.0;
        let mut cov_nt = 0.0;

        for s in samples {
            if s.iters == 0 {
                continue;
            }

            let n = s.iters as f64;
            let t = s.total_ns as f64;
            let w = 1.0 / n;

            let dn = n - n_mean;
            let dt = t - t_mean;

            var_n += w * dn * dn;
            cov_nt += w * dn * dt;
        }

        if var_n == 0.0 {
            return RegressionResult {
                slope: 0.0,
                intercept: t_mean,
            };
        }

        let slope = cov_nt / var_n;
        let intercept = t_mean - slope * n_mean;

        RegressionResult { slope, intercept }
    }

    /// Compute weighted mean: μ = `Σ(T_i)` / `Σ(N_i)`
    #[must_use]
    pub fn compute_weighted_mean(&self) -> f64 {
        Self::compute_weighted_mean_from_samples(&self.samples)
    }

    /// Helper: compute weighted mean from a given set of samples
    fn compute_weighted_mean_from_samples(samples: &[Sample]) -> f64 {
        if samples.is_empty() {
            return 0.0;
        }

        let total_iters: u64 = samples.iter().map(|s| s.iters).sum();
        let total_ns: u64 = samples.iter().map(|s| s.total_ns).sum();

        if total_iters > 0 {
            total_ns as f64 / total_iters as f64
        } else {
            0.0
        }
    }

    /// Standard error of the run value for the estimator that produced it.
    ///
    /// Regression mode: the weighted-least-squares standard error of the slope, with residual
    /// variance on `n - 2` degrees of freedom and the same `1/N` weights as the fit.
    ///
    /// Per-iter mode: the ratio estimator's error, `sqrt(n / (n - 1) · Σ(T_i - μ·N_i)²) / ΣN`,
    /// which is `sd(T_i / N_i) / sqrt(n)` when every sample has the same iteration count.
    ///
    /// `None` when there are too few usable samples (two for per-iter, three for regression).
    fn compute_se(&self, estimate: f64, regression: Option<RegressionResult>) -> Option<f64> {
        let samples: Vec<&Sample> = self.samples.iter().filter(|s| s.iters > 0).collect();
        if let Some(regression) = regression {
            Self::slope_se(&samples, regression)
        } else {
            Self::weighted_mean_se(&samples, estimate)
        }
    }

    fn slope_se(samples: &[&Sample], regression: RegressionResult) -> Option<f64> {
        let n = samples.len();
        if n < 3 {
            return None;
        }
        let mut w_sum = 0.0;
        let mut w_n_sum = 0.0;
        for s in samples {
            let w = 1.0 / s.iters as f64;
            w_sum += w;
            w_n_sum += w * s.iters as f64;
        }
        let n_mean = w_n_sum / w_sum;
        let mut weighted_rss = 0.0;
        let mut sxx = 0.0;
        for s in samples {
            let iters = s.iters as f64;
            let w = 1.0 / iters;
            let residual = s.total_ns as f64 - regression.intercept - regression.slope * iters;
            weighted_rss += w * residual * residual;
            sxx += w * (iters - n_mean) * (iters - n_mean);
        }
        if sxx == 0.0 {
            return None;
        }
        let sigma_sq = weighted_rss / (n as f64 - 2.0);
        Some((sigma_sq / sxx).sqrt())
    }

    fn weighted_mean_se(samples: &[&Sample], estimate: f64) -> Option<f64> {
        let n = samples.len();
        if n < 2 {
            return None;
        }
        let total_iters: f64 = samples.iter().map(|s| s.iters as f64).sum();
        let rss: f64 = samples
            .iter()
            .map(|s| {
                let residual = s.total_ns as f64 - estimate * s.iters as f64;
                residual * residual
            })
            .sum();
        Some((rss * n as f64 / (n as f64 - 1.0)).sqrt() / total_iters)
    }
}

impl Default for StatsAccumulator {
    fn default() -> Self {
        Self::with_options(StatsOptions::default())
    }
}

/// The headline numbers for one measurement, derived only from its run values.
///
/// Each run of a series is a separate process launch, and the run-to-run spread is the noise
/// that matters, so the interval is a plain Student t-interval over the run values. Sampling
/// inside a run only sets how precisely that run was timed, and its error is already part of
/// the spread between runs, so nothing from inside a run is combined with it. No step here
/// adapts to the data: the run count is fixed by configuration.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RunSummary {
    /// Number of runs the summary is over
    pub run_count: usize,
    /// Mean of the run values, nanoseconds per iteration
    pub mean_ns: f64,
    /// Half-width of the 95% t-interval over runs. `None` with a single run.
    pub ci95_half_ns: Option<f64>,
}

/// Summarise the run values of one measurement. The values need not be sorted.
#[must_use]
pub fn run_summary(means: &[f64]) -> RunSummary {
    let n = means.len();
    let mean_ns = if n == 0 {
        0.0
    } else {
        means.iter().sum::<f64>() / n as f64
    };
    let ci95_half_ns = (n >= 2).then(|| {
        let var = means
            .iter()
            .map(|m| (m - mean_ns) * (m - mean_ns))
            .sum::<f64>()
            / (n as f64 - 1.0);
        t_quantile_975(n - 1) * var.sqrt() / (n as f64).sqrt()
    });
    RunSummary {
        run_count: n,
        mean_ns,
        ci95_half_ns,
    }
}

/// 97.5th percentile of Student's t for `df` degrees of freedom, i.e. the two-sided 95%
/// multiplier. Tabulated to four decimals, rounded up, for 1 to 30 df. Beyond that the value at
/// the start of each range stands in: 31 df up to 60, 61 df up to 100, and 101 df above. So the
/// multiplier never understates the exact one and overstates it by at most 2%.
#[must_use]
pub fn t_quantile_975(df: usize) -> f64 {
    const TABLE: [f64; 30] = [
        12.7063, 4.3027, 3.1825, 2.7765, 2.5706, 2.4470, 2.3647, 2.3061, 2.2622, 2.2282, 2.2010,
        2.1789, 2.1604, 2.1448, 2.1315, 2.1200, 2.1099, 2.1010, 2.0931, 2.0860, 2.0797, 2.0739,
        2.0687, 2.0639, 2.0596, 2.0556, 2.0519, 2.0485, 2.0453, 2.0423,
    ];
    match df {
        0 => f64::INFINITY,
        1..=30 => TABLE[df - 1],
        31..=60 => 2.0396,
        61..=100 => 1.9997,
        _ => 1.9838,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::{NonZeroU64, NonZeroUsize};

    #[test]
    fn test_mode_detection_per_iter() {
        let mut acc = StatsAccumulator::new();
        for _ in 0..10 {
            let _ = acc.add_sample(1000, 50000);
        }
        assert_eq!(acc.detect_mode(), EstimationMode::PerIter);
    }

    #[test]
    fn test_mode_detection_regression() {
        let mut acc = StatsAccumulator::new();
        // Varying iteration counts with range >= 2.0
        let _ = acc.add_sample(100, 5000);
        let _ = acc.add_sample(200, 10000);
        let _ = acc.add_sample(300, 15000);
        let _ = acc.add_sample(400, 20000);

        assert_eq!(acc.detect_mode(), EstimationMode::Regression);
    }

    #[test]
    fn test_weighted_mean() {
        let mut acc = StatsAccumulator::new();
        // 1000 iters @ 50ns/iter = 50_000ns total
        let _ = acc.add_sample(1000, 50_000);
        // 2000 iters @ 50ns/iter = 100_000ns total
        let _ = acc.add_sample(2000, 100_000);

        let mean = acc.compute_weighted_mean();
        // Total: 3000 iters, 150_000ns => 50ns/iter
        assert!((mean - 50.0).abs() < 0.001);
    }

    #[test]
    fn test_wls_perfect_fit() {
        let mut acc = StatsAccumulator::new();
        // Perfect linear relationship: T = 10 + 50*N (10ns overhead, 50ns/iter)
        let _ = acc.add_sample(100, 5010);
        let _ = acc.add_sample(200, 10010);
        let _ = acc.add_sample(300, 15010);
        let _ = acc.add_sample(400, 20010);

        let result = acc.compute_wls();
        // Should recover slope ≈ 50, intercept ≈ 10
        assert!((result.slope - 50.0).abs() < 0.001);
        assert!((result.intercept - 10.0).abs() < 0.001);
    }

    #[test]
    fn test_empty_accumulator() {
        let acc = StatsAccumulator::new();
        assert_eq!(acc.sample_count(), 0);
        assert_eq!(acc.detect_mode(), EstimationMode::PerIter);
        assert!((acc.compute_weighted_mean() - 0.0).abs() < 0.001);
        assert!((acc.compute_wls().slope - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_single_sample() {
        let mut acc = StatsAccumulator::new();
        let _ = acc.add_sample(1000, 50_000);

        assert_eq!(acc.detect_mode(), EstimationMode::PerIter);
        assert!((acc.compute_weighted_mean() - 50.0).abs() < 0.001);
    }

    #[test]
    fn test_mode_detection_boundary_two_distinct() {
        let mut acc = StatsAccumulator::new();
        // Only 2 distinct values - should be per_iter
        let _ = acc.add_sample(100, 5000);
        let _ = acc.add_sample(200, 10000);
        let _ = acc.add_sample(100, 5000);
        let _ = acc.add_sample(200, 10000);

        assert_eq!(acc.detect_mode(), EstimationMode::PerIter);
    }

    #[test]
    fn test_mode_detection_cv_threshold() {
        let mut acc = StatsAccumulator::new();
        let _ = acc.add_sample(100, 5000);
        let _ = acc.add_sample(120, 6000);
        let _ = acc.add_sample(150, 7500);

        assert_eq!(acc.detect_mode(), EstimationMode::Regression);
    }

    #[test]
    fn test_weighted_mean_zero_iters() {
        let mut acc = StatsAccumulator::new();
        let _ = acc.add_sample(0, 0);

        // Should return 0 and not panic
        assert!((acc.compute_weighted_mean() - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_wls_with_zero_iters() {
        let mut acc = StatsAccumulator::new();

        // Add sample with zero iterations (should be handled gracefully)
        let _ = acc.add_sample(0, 0);
        let _ = acc.add_sample(100, 5000);
        let _ = acc.add_sample(200, 10000);

        // Should not panic
        let result = acc.compute_wls();
        assert!(result.slope.is_finite());
    }

    #[test]
    fn test_wls_with_zero_variance() {
        let mut acc = StatsAccumulator::new();
        // All samples with same iters count
        let _ = acc.add_sample(100, 5000);
        let _ = acc.add_sample(100, 5010);
        let _ = acc.add_sample(100, 4990);

        // Should not panic, slope should be 0 due to zero variance in N
        let result = acc.compute_wls();
        assert!((result.slope - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_mode_detection_exactly_three_distinct() {
        let mut acc = StatsAccumulator::new();
        // Exactly 3 distinct with range >= 2.0
        let _ = acc.add_sample(100, 5000);
        let _ = acc.add_sample(150, 7500);
        let _ = acc.add_sample(250, 12500);

        assert_eq!(acc.detect_mode(), EstimationMode::Regression);
    }

    #[test]
    fn state_done_once_floors_met() {
        let mut acc = StatsAccumulator::new();

        // 4 samples of 100 ms meet the sample floor but not the 2 s time floor
        for i in 1..=4 {
            assert_eq!(
                acc.add_sample(100, 100_000_000),
                StatsState::MoreSamplesRequired
            );
            assert_eq!(acc.sample_count(), i);
        }
        // 2 s of sample time is reached on the 20th sample
        for _ in 5..20 {
            assert_eq!(
                acc.add_sample(100, 100_000_000),
                StatsState::MoreSamplesRequired
            );
        }
        assert_eq!(acc.add_sample(100, 100_000_000), StatsState::Done);
    }

    #[test]
    fn state_waits_for_sample_floor_after_time_floor() {
        let mut acc = StatsAccumulator::new();

        // One 3 s sample meets the time floor but not the 4-sample floor
        assert_eq!(
            acc.add_sample(1, 3_000_000_000),
            StatsState::MoreSamplesRequired
        );
        assert_eq!(
            acc.add_sample(1, 3_000_000_000),
            StatsState::MoreSamplesRequired
        );
        assert_eq!(
            acc.add_sample(1, 3_000_000_000),
            StatsState::MoreSamplesRequired
        );
        assert_eq!(acc.add_sample(1, 3_000_000_000), StatsState::Done);
    }

    #[test]
    fn state_honours_custom_floors() {
        let opts = StatsOptions {
            min_samples: NonZeroUsize::new(2).unwrap(),
            min_total_time_ns: NonZeroU64::new(1).unwrap(),
            ..StatsOptions::default()
        };

        let mut acc = StatsAccumulator::with_options(opts);
        assert_eq!(
            acc.add_sample(100, 10_000_000),
            StatsState::MoreSamplesRequired
        );
        assert_eq!(acc.add_sample(100, 10_000_000), StatsState::Done);
    }

    #[test]
    fn se_ns_from_samples() {
        let mut acc = StatsAccumulator::new();
        // Per-iteration times 48, 50, 52: sd = 2, se = 2 / sqrt(3)
        let _ = acc.add_sample(100, 4800);
        let _ = acc.add_sample(100, 5000);
        let _ = acc.add_sample(100, 5200);

        let result = acc.finish();
        let se = result.se_ns.unwrap();
        assert!((se - 2.0 / 3f64.sqrt()).abs() < 1e-9, "{se}");
        assert!((result.mean_ns_per_iter - 50.0).abs() < 1e-9);
    }

    #[test]
    fn se_ns_is_zero_for_a_perfect_regression_fit() {
        // T = 10000 + 50N: the intercept is fixed overhead, not uncertainty in the slope
        let mut acc = StatsAccumulator::new();
        for n in [100, 200, 300, 400] {
            let _ = acc.add_sample(n, 10_000 + 50 * n);
        }
        let result = acc.finish();
        assert_eq!(result.mode, EstimationMode::Regression);
        assert!((result.mean_ns_per_iter - 50.0).abs() < 1e-9);
        assert!(result.se_ns.unwrap().abs() < 1e-9, "{:?}", result.se_ns);
    }

    #[test]
    fn se_ns_follows_the_weighted_mean_with_unequal_iteration_counts() {
        // Per-iter mode (two distinct counts). μ = (1000 + 2100) / (10 + 20) = 103.33; the
        // residuals T - μN are -33.3 and +33.3, so se = sqrt(2 · 2 · 33.3²) / 30
        let mut acc = StatsAccumulator::new();
        let _ = acc.add_sample(10, 1000);
        let _ = acc.add_sample(20, 2100);
        let result = acc.finish();
        assert_eq!(result.mode, EstimationMode::PerIter);
        let mu: f64 = 3100.0 / 30.0;
        let residual = 1000.0 - mu * 10.0;
        let expected = (2.0 * 2.0 * residual * residual).sqrt() / 30.0;
        assert!((result.se_ns.unwrap() - expected).abs() < 1e-9);
    }

    #[test]
    fn se_ns_none_below_two_samples() {
        let mut acc = StatsAccumulator::new();
        let _ = acc.add_sample(100, 5000);
        assert_eq!(acc.finish().se_ns, None);

        let mut acc = StatsAccumulator::new();
        let _ = acc.add_sample(0, 0);
        let _ = acc.add_sample(100, 5000);
        assert_eq!(acc.finish().se_ns, None);
    }

    #[test]
    fn test_estimation_mode_json_serialization() {
        // Test that EstimationMode serializes to snake_case strings
        let per_iter_json = serde_json::to_string(&EstimationMode::PerIter).unwrap();
        assert_eq!(per_iter_json, "\"per_iter\"");

        let regression_json = serde_json::to_string(&EstimationMode::Regression).unwrap();
        assert_eq!(regression_json, "\"regression\"");

        // Test deserialization
        let parsed_per_iter: EstimationMode = serde_json::from_str("\"per_iter\"").unwrap();
        assert_eq!(parsed_per_iter, EstimationMode::PerIter);

        let parsed_regression: EstimationMode = serde_json::from_str("\"regression\"").unwrap();
        assert_eq!(parsed_regression, EstimationMode::Regression);
    }

    #[test]
    fn single_run_has_no_interval() {
        let s = run_summary(&[100.0]);
        assert_eq!(s.run_count, 1);
        assert!((s.mean_ns - 100.0).abs() < 1e-9);
        assert_eq!(s.ci95_half_ns, None);
    }

    #[test]
    fn nine_runs_use_t_with_eight_df() {
        // Values with mean 100 and sample sd 1 (ddof 1): seven at 100, one at 98, one at 102
        let s = run_summary(&[100.0, 102.0, 100.0, 98.0, 100.0, 100.0, 100.0, 100.0, 100.0]);
        assert_eq!(s.run_count, 9);
        assert!((s.mean_ns - 100.0).abs() < 1e-9);
        let expected = 2.3061 * 1.0 / 3.0;
        assert!((s.ci95_half_ns.unwrap() - expected).abs() < 1e-9);
    }

    #[test]
    fn t_table_values() {
        assert!((t_quantile_975(1) - 12.7063).abs() < 1e-9);
        assert!((t_quantile_975(2) - 4.3027).abs() < 1e-9);
        assert!((t_quantile_975(8) - 2.3061).abs() < 1e-9);
        assert!((t_quantile_975(30) - 2.0423).abs() < 1e-9);
        // Fallbacks never understate: each is the value at the start of its range
        assert!((t_quantile_975(31) - 2.0396).abs() < 1e-9);
        assert!((t_quantile_975(60) - 2.0396).abs() < 1e-9);
        assert!((t_quantile_975(61) - 1.9997).abs() < 1e-9);
        assert!((t_quantile_975(100) - 1.9997).abs() < 1e-9);
        assert!((t_quantile_975(101) - 1.9838).abs() < 1e-9);
        assert!((t_quantile_975(100_000) - 1.9838).abs() < 1e-9);
        assert!(t_quantile_975(0).is_infinite());
    }
}
