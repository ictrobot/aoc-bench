// Statistics engine: mode detection, WLS, bootstrap CI, outlier detection

#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use rand::prelude::*;
use rand_xoshiro::Xoshiro256PlusPlus;
use serde::{Deserialize, Serialize};
use tracing::debug;

const WARMUP_SAMPLES: usize = 16;
const MIN_SAMPLES: usize = 32;
const CHECK_EVERY: usize = 32;
const QUICK_BOOTSTRAP_SAMPLES: usize = 1000;
const FINAL_BOOTSTRAP_SAMPLES: usize = 10000;
const TARGET_REL_CI: f64 = 0.01; // 1%
const MAX_SAMPLES: usize = 2048;
const OUTLIER_IQR_FACTOR: f64 = 3.0;
const OUTLIER_MAX_FRACTION: f64 = 0.05; // 5%

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[must_use]
pub enum EstimationMode {
    Regression,
    PerIter,
}

#[derive(Debug, Clone, PartialEq)]
#[must_use]
pub enum StatsState {
    MoreSamplesNeeded,
    Abort(StatsError),
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub iters: u64,
    pub total_ns: u64,
}

#[derive(Debug, Clone)]
pub struct StatsAccumulator {
    samples: Vec<Sample>,
    warmup_remaining: usize,
}

#[derive(Debug, Clone, Copy)]
#[must_use]
pub struct BootstrapResult {
    pub ci_lower: f64,
    pub ci_upper: f64,
    pub half_width: f64,
}

#[derive(Debug, Clone, Copy)]
#[must_use]
pub struct RegressionResult {
    pub slope: f64,     // β: nanoseconds per iteration
    pub intercept: f64, // α: fixed overhead per batch
}

#[derive(Debug, Clone)]
#[must_use]
pub struct StatsResult {
    pub mode: EstimationMode,
    pub mean_ns_per_iter: f64,
    pub ci95_half_width_ns: f64,
    pub intercept_ns: Option<f64>,
    pub outlier_count: usize,
    pub samples: Vec<Sample>,
}

impl StatsAccumulator {
    pub fn new(with_warmup: bool) -> Self {
        StatsAccumulator {
            samples: Vec::new(),
            warmup_remaining: if with_warmup { WARMUP_SAMPLES } else { 0 },
        }
    }

    /// Add a sample, handling warmup by skipping the first `WARMUP_SAMPLES`
    ///
    /// Returns the current state, see [`StatsAccumulator::state`].
    pub fn add_sample(&mut self, iters: u64, total_ns: u64) -> StatsState {
        if self.warmup_remaining > 0 {
            self.warmup_remaining -= 1;
        } else {
            self.samples.push(Sample { iters, total_ns });
        }
        self.state()
    }

    /// Returns the number of samples collected (excluding warmup samples)
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Returns a reference to all collected samples (excluding warmup samples)
    pub fn samples(&self) -> &[Sample] {
        &self.samples
    }

    /// Evaluates the current sampling state and determines if more samples are needed
    ///
    /// Returns:
    /// - `MoreSamplesNeeded` if below minimum samples or convergence criteria not met
    /// - `Done` if confidence interval has converged within target relative width
    /// - `Abort` if too many outliers detected or failed to converge after maximum samples
    pub fn state(&self) -> StatsState {
        if self.samples.len() < MIN_SAMPLES {
            return StatsState::MoreSamplesNeeded;
        }

        // Check every CHECK_EVERY samples after MIN_SAMPLES
        if !(self.samples.len() - MIN_SAMPLES).is_multiple_of(CHECK_EVERY) {
            return StatsState::MoreSamplesNeeded;
        }

        let mode = self.detect_mode();
        let (mean, regression) = match mode {
            EstimationMode::Regression => {
                let regression = self.compute_wls();
                (regression.slope, Some(regression))
            }
            EstimationMode::PerIter => (self.compute_weighted_mean(), None),
        };

        // Check for too many outliers
        let outlier_count = self.detect_outliers(regression);
        let outlier_fraction = outlier_count as f64 / self.samples.len() as f64;
        if outlier_fraction > OUTLIER_MAX_FRACTION {
            return StatsState::Abort(StatsError::TooManyOutliers {
                samples: self.samples.len(),
                outlier_count,
            });
        }

        // Check for CI convergence
        let ci = self.bootstrap_ci(mode, false);
        let relative_ci_half_width = ci.half_width / mean;
        if relative_ci_half_width <= TARGET_REL_CI {
            StatsState::Done
        } else if self.samples.len() < MAX_SAMPLES {
            debug!(
                samples = self.samples.len(),
                relative_ci_half_width, "ci too wide, waiting for more samples"
            );
            StatsState::MoreSamplesNeeded
        } else {
            StatsState::Abort(StatsError::FailedToConverge {
                samples: self.samples.len(),
                relative_ci_half_width,
            })
        }
    }

    /// Finalizes sampling and computes the final statistics result
    pub fn finish(self) -> StatsResult {
        let mode = self.detect_mode();
        let (mean_ns_per_iter, regression) = match mode {
            EstimationMode::Regression => {
                let regression = self.compute_wls();
                (regression.slope, Some(regression))
            }
            EstimationMode::PerIter => (self.compute_weighted_mean(), None),
        };

        let outlier_count = self.detect_outliers(regression);
        let ci = self.bootstrap_ci(mode, true);

        StatsResult {
            mode,
            mean_ns_per_iter,
            ci95_half_width_ns: ci.half_width,
            intercept_ns: regression.map(|r| r.intercept),
            outlier_count,
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

    /// Computes bootstrap confidence interval for the mean estimate
    ///
    /// Uses resampling with replacement to estimate the 95% confidence interval.
    /// - Quick bootstrap (1,000 iterations) during convergence checks
    /// - Final bootstrap (10,000 iterations) for final result
    ///
    /// Returns the lower and upper CI bounds and half-width of the interval.
    pub fn bootstrap_ci(&self, mode: EstimationMode, is_final: bool) -> BootstrapResult {
        let n_bootstrap = if is_final {
            FINAL_BOOTSTRAP_SAMPLES
        } else {
            QUICK_BOOTSTRAP_SAMPLES
        };

        if self.samples.is_empty() {
            return BootstrapResult {
                ci_lower: 0.0,
                ci_upper: 0.0,
                half_width: f64::INFINITY,
            };
        }

        let mut rng = Xoshiro256PlusPlus::from_os_rng();
        let mut bootstrap_estimates = Vec::with_capacity(n_bootstrap);

        // Perform bootstrap resampling
        for _ in 0..n_bootstrap {
            // Resample with replacement
            let resampled: Vec<Sample> = (0..self.samples.len())
                .map(|_| self.samples.choose(&mut rng).unwrap().clone())
                .collect();

            // Compute estimate for this resample
            let estimate = match mode {
                EstimationMode::Regression => Self::compute_wls_from_samples(&resampled).slope,
                EstimationMode::PerIter => Self::compute_weighted_mean_from_samples(&resampled),
            };

            bootstrap_estimates.push(estimate);
        }

        // Sort and compute percentiles
        bootstrap_estimates.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // 2.5th and 97.5th percentiles for 95% CI
        let ci_lower =
            bootstrap_estimates[Self::percentile_index(bootstrap_estimates.len(), 0.025)];
        let ci_upper =
            bootstrap_estimates[Self::percentile_index(bootstrap_estimates.len(), 0.975)];
        let half_width = (ci_upper - ci_lower) / 2.0;

        BootstrapResult {
            ci_lower,
            ci_upper,
            half_width,
        }
    }

    /// Detects outliers using the IQR (interquartile range) method on residuals
    ///
    /// Computes residuals based on the estimation mode:
    /// - For regression: residual = actual time - predicted time from linear model
    /// - For per-iter: residual = time per iteration
    ///
    /// Outliers are defined as values beyond Q1 - 3×IQR or Q3 + 3×IQR.
    /// Returns the count of detected outliers.
    pub fn detect_outliers(&self, regression: Option<RegressionResult>) -> usize {
        if self.samples.is_empty() {
            return 0;
        }

        // Compute residuals based on mode
        let mut residuals: Vec<f64> = match regression {
            Some(regression) => self
                .samples
                .iter()
                .map(|s| {
                    let n = s.iters as f64;
                    let t = s.total_ns as f64;
                    let predicted = regression.intercept + regression.slope * n;
                    t - predicted
                })
                .collect(),
            None => {
                // Per-iteration residuals
                self.samples
                    .iter()
                    .map(|s| {
                        let n = s.iters as f64;
                        let t = s.total_ns as f64;
                        t / n
                    })
                    .collect()
            }
        };

        // Sort residuals for percentile calculation
        residuals.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // Compute Q1, Q3, IQR
        let q1 = residuals[Self::percentile_index(residuals.len(), 0.25)];
        let q3 = residuals[Self::percentile_index(residuals.len(), 0.75)];
        let iqr = q3 - q1;

        // Compute outlier bounds
        let lower_bound = q1 - OUTLIER_IQR_FACTOR * iqr;
        let upper_bound = q3 + OUTLIER_IQR_FACTOR * iqr;

        // Count outliers
        residuals
            .iter()
            .filter(|&&r| r < lower_bound || r > upper_bound)
            .count()
    }

    fn percentile_index(len: usize, p: f64) -> usize {
        // Pick the smallest element such that at least p% of the data is ≤ that element
        ((len as f64 * p).ceil() - 1.0).clamp(0.0, len as f64 - 1.0) as usize
    }
}

impl Default for StatsAccumulator {
    fn default() -> Self {
        Self::new(true)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum StatsError {
    TooManyOutliers {
        samples: usize,
        outlier_count: usize,
    },
    FailedToConverge {
        samples: usize,
        relative_ci_half_width: f64,
    },
}

impl std::fmt::Display for StatsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            StatsError::TooManyOutliers {
                samples,
                outlier_count,
            } => write!(
                f,
                "Too many outliers: {}/{} ({:.1}% > {:.1}%)",
                outlier_count,
                samples,
                (outlier_count as f64 / samples as f64) * 100.0,
                OUTLIER_MAX_FRACTION * 100.0
            ),
            StatsError::FailedToConverge {
                samples,
                relative_ci_half_width,
            } => write!(
                f,
                "Failed to converge after {} samples: relative CI half-width {:.1}% > target {:.1}%",
                samples,
                relative_ci_half_width * 100.0,
                TARGET_REL_CI * 100.0
            ),
        }
    }
}

impl std::error::Error for StatsError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mode_detection_per_iter() {
        let mut acc = StatsAccumulator::new(false);
        for _ in 0..10 {
            let _ = acc.add_sample(1000, 50000);
        }
        assert_eq!(acc.detect_mode(), EstimationMode::PerIter);
    }

    #[test]
    fn test_mode_detection_regression() {
        let mut acc = StatsAccumulator::new(false);
        // Varying iteration counts with range >= 2.0
        let _ = acc.add_sample(100, 5000);
        let _ = acc.add_sample(200, 10000);
        let _ = acc.add_sample(300, 15000);
        let _ = acc.add_sample(400, 20000);

        assert_eq!(acc.detect_mode(), EstimationMode::Regression);
    }

    #[test]
    fn test_weighted_mean() {
        let mut acc = StatsAccumulator::new(false);
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
        let mut acc = StatsAccumulator::new(false);
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
        let acc = StatsAccumulator::new(false);
        assert_eq!(acc.sample_count(), 0);
        assert_eq!(acc.detect_mode(), EstimationMode::PerIter);
        assert!((acc.compute_weighted_mean() - 0.0).abs() < 0.001);
        assert!((acc.compute_wls().slope - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_single_sample() {
        let mut acc = StatsAccumulator::new(false);
        let _ = acc.add_sample(1000, 50_000);

        assert_eq!(acc.detect_mode(), EstimationMode::PerIter);
        assert!((acc.compute_weighted_mean() - 50.0).abs() < 0.001);
    }

    #[test]
    fn test_mode_detection_boundary_two_distinct() {
        let mut acc = StatsAccumulator::new(false);
        // Only 2 distinct values - should be per_iter
        let _ = acc.add_sample(100, 5000);
        let _ = acc.add_sample(200, 10000);
        let _ = acc.add_sample(100, 5000);
        let _ = acc.add_sample(200, 10000);

        assert_eq!(acc.detect_mode(), EstimationMode::PerIter);
    }

    #[test]
    fn test_mode_detection_cv_threshold() {
        let mut acc = StatsAccumulator::new(false);
        let _ = acc.add_sample(100, 5000);
        let _ = acc.add_sample(120, 6000);
        let _ = acc.add_sample(150, 7500);

        assert_eq!(acc.detect_mode(), EstimationMode::Regression);
    }

    #[test]
    fn test_weighted_mean_zero_iters() {
        let mut acc = StatsAccumulator::new(false);
        let _ = acc.add_sample(0, 0);

        // Should return 0 and not panic
        assert!((acc.compute_weighted_mean() - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_wls_with_zero_iters() {
        let mut acc = StatsAccumulator::new(false);

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
        let mut acc = StatsAccumulator::new(false);
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
        let mut acc = StatsAccumulator::new(false);
        // Exactly 3 distinct with range >= 2.0
        let _ = acc.add_sample(100, 5000);
        let _ = acc.add_sample(150, 7500);
        let _ = acc.add_sample(250, 12500);

        assert_eq!(acc.detect_mode(), EstimationMode::Regression);
    }

    // Phase 2 tests: Warmup and sampling logic
    #[test]
    fn test_warmup_samples_excluded() {
        let mut acc = StatsAccumulator::new(true);

        // Add WARMUP_SAMPLES (16) with high noise - these are skipped
        for _ in 0..WARMUP_SAMPLES {
            let _ = acc.add_sample(100, 100_000); // 1000 ns/iter (wrong - discarded)
        }

        // Add good samples after warmup
        for _ in 0..20 {
            let _ = acc.add_sample(100, 5000); // 50 ns/iter (correct - used)
        }

        // Should only use post-warmup samples
        let mean = acc.compute_weighted_mean();
        assert!(
            (mean - 50.0).abs() < 0.001,
            "Mean should be ~50, got {mean}"
        );
    }

    #[test]
    fn test_state_transitions() {
        let mut acc = StatsAccumulator::new(true);

        // First 16 are warmup (automatically skipped)
        for _ in 0..WARMUP_SAMPLES {
            let state = acc.add_sample(100, 5000);
            assert!(matches!(state, StatsState::MoreSamplesNeeded));
        }

        // Add samples until MIN_SAMPLES
        for _ in 0..MIN_SAMPLES - 1 {
            let state = acc.add_sample(100, 5000);
            assert!(matches!(state, StatsState::MoreSamplesNeeded));
        }

        // At exactly MIN_SAMPLES, state checks stopping
        let state = acc.add_sample(100, 5000);
        // With consistent samples, should eventually reach Done
        assert!(matches!(state, StatsState::Done));
    }

    #[test]
    fn test_max_samples_abort() {
        let mut acc = StatsAccumulator::new(false);

        for i in 0..MAX_SAMPLES - 1 {
            assert_eq!(
                acc.add_sample(100, if i.is_multiple_of(2) { 0 } else { i as u64 }),
                StatsState::MoreSamplesNeeded
            );
        }

        assert!(matches!(
            acc.add_sample(100, 0),
            StatsState::Abort(StatsError::FailedToConverge {
                samples: MAX_SAMPLES,
                relative_ci_half_width: _,
            })
        ));
    }

    // Phase 2 tests: Bootstrap CI
    #[test]
    fn test_bootstrap_ci_per_iter_mode() {
        let mut acc = StatsAccumulator::new(true);

        // Add consistent samples (low variance)
        for _ in 0..100 + WARMUP_SAMPLES {
            let _ = acc.add_sample(100, 5000); // Exactly 50 ns/iter
        }

        let result = acc.bootstrap_ci(EstimationMode::PerIter, false);

        // With no variance, CI should be very narrow
        assert!(result.ci_lower > 0.0);
        assert!(result.ci_upper > 0.0);
        assert!(result.ci_lower <= 50.0);
        assert!(result.ci_upper >= 50.0);
        let mean = acc.compute_weighted_mean();
        let relative_half_width = result.half_width / mean;
        assert!(relative_half_width < 0.1); // Should be < 10%
    }

    #[test]
    fn test_bootstrap_ci_regression_mode() {
        let mut acc = StatsAccumulator::new(false);

        let _ = acc.add_sample(100, 5010); // 50 ns/iter + 10 ns overhead
        let _ = acc.add_sample(200, 10010);
        let _ = acc.add_sample(300, 15010);
        let _ = acc.add_sample(400, 20010);

        let result = acc.bootstrap_ci(EstimationMode::Regression, false);

        // CI should contain true value of 50
        assert!(result.ci_lower <= 50.0);
        assert!(result.ci_upper >= 50.0);
        let regression = acc.compute_wls();
        let relative_half_width = result.half_width / regression.slope;
        assert!(relative_half_width < 0.1); // Should be < 10%
    }

    // Phase 2 tests: Outlier detection
    #[test]
    fn test_outlier_detection_no_outliers() {
        let mut acc = StatsAccumulator::new(false);

        // Add consistent samples (no outliers)
        for _ in 0..100 {
            let _ = acc.add_sample(100, 5000); // 50 ns/iter
        }

        let outlier_count = acc.detect_outliers(None);

        assert_eq!(outlier_count, 0);
    }

    #[test]
    fn test_outlier_detection_with_outliers() {
        let mut acc = StatsAccumulator::new(false);

        // Add mostly good samples
        for _ in 0..90 {
            let _ = acc.add_sample(100, 5000); // 50 ns/iter
        }

        // Add some outliers (10% outliers)
        for _ in 0..10 {
            let _ = acc.add_sample(100, 50000); // 500 ns/iter (10x slower!)
        }

        let outlier_count = acc.detect_outliers(None);

        assert!(outlier_count > 0);
        let outlier_fraction = outlier_count as f64 / 100.0;
        assert!(outlier_fraction > 0.05); // More than 5%
    }

    #[test]
    fn test_outlier_detection_regression_mode() {
        let mut acc = StatsAccumulator::new(false);

        // Add samples following linear relationship: T = 10 + 50*N
        for _ in 0..40 {
            let _ = acc.add_sample(100, 5010);
            let _ = acc.add_sample(200, 10010);
            let _ = acc.add_sample(300, 15010);
        }

        // Add outliers
        for _ in 0..10 {
            let _ = acc.add_sample(100, 50000); // Way off the line
        }

        let regression = acc.compute_wls();
        let outlier_count = acc.detect_outliers(Some(regression));

        assert!(outlier_count > 0);
        let outlier_fraction = outlier_count as f64 / acc.sample_count() as f64;
        assert!(outlier_fraction > 0.05);
    }

    #[test]
    fn test_outlier_threshold_boundary() {
        let mut acc = StatsAccumulator::new(false);

        // Add 100 good samples
        for _ in 0..100 {
            let _ = acc.add_sample(100, 5000);
        }

        // Add exactly 5 outliers (5% threshold)
        for _ in 0..5 {
            let _ = acc.add_sample(100, 50000);
        }

        let outlier_count = acc.detect_outliers(None);
        assert_eq!(outlier_count, 5);

        let outlier_fraction = outlier_count as f64 / acc.sample_count() as f64;
        assert!((outlier_fraction - (5.0 / 105.0)) < 0.0001);
    }

    #[test]
    fn test_bootstrap_with_empty_samples() {
        let acc = StatsAccumulator::new(true);

        let result = acc.bootstrap_ci(EstimationMode::PerIter, false);

        assert!((result.ci_lower - 0.0).abs() < 0.001);
        assert!((result.ci_upper - 0.0).abs() < 0.001);
        assert!(result.half_width.is_infinite());
    }

    #[test]
    fn test_outliers_with_insufficient_samples() {
        let mut acc = StatsAccumulator::new(true);

        // Only add warmup samples (which are skipped)
        for _ in 0..16 {
            let _ = acc.add_sample(100, 5000);
        }

        let outlier_count = acc.detect_outliers(None);

        assert_eq!(outlier_count, 0);
        assert_eq!(acc.sample_count(), 0);
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
}
