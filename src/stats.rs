// Statistics engine: mode detection, WLS, bootstrap CI, outlier detection

#![allow(clippy::cast_precision_loss)]

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EstimationMode {
    Regression,
    PerIter,
}

#[derive(Debug, Clone)]
pub struct Sample {
    pub iters: u64,
    pub total_ns: u64,
}

#[derive(Debug, Clone)]
pub struct StatsAccumulator {
    samples: Vec<Sample>,
}

#[derive(Debug, Clone)]
pub struct RegressionResult {
    pub slope: f64,     // β: nanoseconds per iteration
    pub intercept: f64, // α: fixed overhead per batch
}

impl StatsAccumulator {
    pub fn new() -> Self {
        StatsAccumulator {
            samples: Vec::new(),
        }
    }

    pub fn add_sample(&mut self, iters: u64, total_ns: u64) {
        self.samples.push(Sample { iters, total_ns });
    }

    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    pub fn samples(&self) -> &[Sample] {
        &self.samples
    }

    /// Determine which estimation mode to use based on iteration count variation
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
        // if distinct_N >= 3 and (range_N >= 2.0 or cv_N >= 0.15):
        //     mode = "regression"
        // else:
        //     mode = "per_iter"
        if distinct_n >= 3 && (range_n >= 2.0 || cv_n >= 0.15) {
            EstimationMode::Regression
        } else {
            EstimationMode::PerIter
        }
    }

    /// Compute weighted least squares regression: T = α + β·N with weights w = 1/N
    pub fn compute_wls(&self) -> RegressionResult {
        if self.samples.is_empty() {
            return RegressionResult {
                slope: 0.0,
                intercept: 0.0,
            };
        }

        // Pass 1: means
        let mut w_sum = 0.0;
        let mut w_n_sum = 0.0;
        let mut w_t_sum = 0.0;
        for s in &self.samples {
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

        for s in &self.samples {
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
        if self.samples.is_empty() {
            return 0.0;
        }

        let total_iters: u64 = self.samples.iter().map(|s| s.iters).sum();
        let total_ns: u64 = self.samples.iter().map(|s| s.total_ns).sum();

        if total_iters > 0 {
            total_ns as f64 / total_iters as f64
        } else {
            0.0
        }
    }

    /// Get the point estimate based on the mode
    pub fn point_estimate(&self, mode: EstimationMode) -> f64 {
        match mode {
            EstimationMode::Regression => self.compute_wls().slope,
            EstimationMode::PerIter => self.compute_weighted_mean(),
        }
    }
}

impl Default for StatsAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mode_detection_per_iter() {
        let mut acc = StatsAccumulator::new();
        // All samples with same iteration count
        for _ in 0..10 {
            acc.add_sample(1000, 50000);
        }
        assert_eq!(acc.detect_mode(), EstimationMode::PerIter);
    }

    #[test]
    fn test_mode_detection_regression() {
        let mut acc = StatsAccumulator::new();
        // Varying iteration counts with range >= 2.0
        acc.add_sample(100, 5000);
        acc.add_sample(200, 10000);
        acc.add_sample(300, 15000);
        acc.add_sample(400, 20000);

        assert_eq!(acc.detect_mode(), EstimationMode::Regression);
    }

    #[test]
    fn test_weighted_mean() {
        let mut acc = StatsAccumulator::new();
        // 1000 iters @ 50ns/iter = 50_000ns total
        acc.add_sample(1000, 50_000);
        // 2000 iters @ 50ns/iter = 100_000ns total
        acc.add_sample(2000, 100_000);

        let mean = acc.compute_weighted_mean();
        // Total: 3000 iters, 150_000ns => 50ns/iter
        assert!((mean - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_wls_perfect_fit() {
        let mut acc = StatsAccumulator::new();
        // Perfect linear relationship: T = 10 + 50*N (10ns overhead, 50ns/iter)
        acc.add_sample(100, 5010);
        acc.add_sample(200, 10010);
        acc.add_sample(300, 15010);
        acc.add_sample(400, 20010);

        let result = acc.compute_wls();
        // Should recover slope ≈ 50, intercept ≈ 10
        assert!((result.slope - 50.0).abs() < 1.0);
        assert!((result.intercept - 10.0).abs() < 100.0);
    }

    #[test]
    fn test_point_estimate_regression() {
        let mut acc = StatsAccumulator::new();
        acc.add_sample(100, 5010);
        acc.add_sample(200, 10010);
        acc.add_sample(300, 15010);

        let estimate = acc.point_estimate(EstimationMode::Regression);
        assert!((estimate - 50.0).abs() < 1.0);
    }

    #[test]
    fn test_point_estimate_per_iter() {
        let mut acc = StatsAccumulator::new();
        acc.add_sample(1000, 50_000);
        acc.add_sample(2000, 100_000);

        let estimate = acc.point_estimate(EstimationMode::PerIter);
        assert!((estimate - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_empty_accumulator() {
        let acc = StatsAccumulator::new();
        assert_eq!(acc.sample_count(), 0);
        assert_eq!(acc.detect_mode(), EstimationMode::PerIter);
        assert_eq!(acc.compute_weighted_mean(), 0.0);
        assert_eq!(acc.compute_wls().slope, 0.0);
    }

    #[test]
    fn test_single_sample() {
        let mut acc = StatsAccumulator::new();
        acc.add_sample(1000, 50_000);

        assert_eq!(acc.detect_mode(), EstimationMode::PerIter);
        assert!((acc.compute_weighted_mean() - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_mode_detection_boundary_two_distinct() {
        let mut acc = StatsAccumulator::new();
        // Only 2 distinct values - should be per_iter
        acc.add_sample(100, 5000);
        acc.add_sample(200, 10000);
        acc.add_sample(100, 5000);
        acc.add_sample(200, 10000);

        assert_eq!(acc.detect_mode(), EstimationMode::PerIter);
    }

    #[test]
    fn test_mode_detection_cv_threshold() {
        let mut acc = StatsAccumulator::new();
        // 3 distinct with cv >= 0.15 should trigger regression
        // Using values: 100, 120, 150 gives cv ~0.17
        acc.add_sample(100, 5000);
        acc.add_sample(120, 6000);
        acc.add_sample(150, 7500);

        assert_eq!(acc.detect_mode(), EstimationMode::Regression);
    }

    #[test]
    fn test_weighted_mean_zero_iters() {
        let mut acc = StatsAccumulator::new();
        acc.add_sample(0, 0);

        // Should return 0 and not panic
        assert_eq!(acc.compute_weighted_mean(), 0.0);
    }

    #[test]
    fn test_wls_with_zero_variance() {
        let mut acc = StatsAccumulator::new();
        // All samples with same iters count
        acc.add_sample(100, 5000);
        acc.add_sample(100, 5010);
        acc.add_sample(100, 4990);

        // Should not panic, slope should be 0 due to zero variance in N
        let result = acc.compute_wls();
        assert_eq!(result.slope, 0.0);
    }

    #[test]
    fn test_mode_detection_exactly_three_distinct() {
        let mut acc = StatsAccumulator::new();
        // Exactly 3 distinct with range >= 2.0
        acc.add_sample(100, 5000);
        acc.add_sample(150, 7500);
        acc.add_sample(250, 12500);

        assert_eq!(acc.detect_mode(), EstimationMode::Regression);
    }
}
