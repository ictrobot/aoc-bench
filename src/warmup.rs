//! Warmup gate: discards samples until the process is at steady state.
//!
//! Samples are batched in sample time, and package power is read once per batch so the gate
//! works from bands, never watts. The first batch is always discarded so process startup never
//! affects a decision, and its reading becomes the start reading for the next. The batch whose
//! reading is settled is the start of warmup, including the 1 s and 15 s clocks, and its samples
//! are the first warmup samples. Completion is only checked at batch ends.

#![allow(clippy::cast_precision_loss)]

use crate::rapl::RaplError;
use crate::stats::{Sample, StatsOptions};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;
use tracing::{debug, trace};

/// Package power relative to the host's sustained limit, margins already applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PowerBand {
    Unavailable,
    /// The limiter has engaged
    Settled,
    /// Tolerated during warmup: a phase excursion at steady state
    Elevated,
    Above,
}

pub trait PowerSource {
    /// Band for the batch that just closed, given the sample time it covered.
    fn band(&mut self, batch_ns: u64) -> Result<PowerBand, WarmupError>;
}

/// What became of a closed batch, so the runner can attribute its power reading. The batch that
/// opens warmup counts as warmup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Batch {
    Discarded,
    Warmup,
}

#[derive(Debug, thiserror::Error)]
pub enum WarmupError {
    #[error("package power stayed above the limit for {:.0}s", *.waited_ns as f64 / 1e9)]
    PowerWaitTimeout { waited_ns: u64 },
    #[error("package power rose above the limit during warmup")]
    PowerAboveLimit,
    #[error("failed to read package power: {0}")]
    Power(#[from] RaplError),
    #[error(
        "{:?} of samples arrived in {:?}, is the benchmark flushing each sample?",
        Duration::from_nanos(*.sample_ns),
        Duration::from_nanos(*.wall_ns)
    )]
    SamplesBehindClock { sample_ns: u64, wall_ns: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmupReason {
    Stable,
    MaxTime,
}

/// Recorded with each run.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WarmupSummary {
    /// Every discarded sample: initial batch, power wait and warmup
    pub samples: usize,
    pub total_ns: u64,
    /// The subset discarded while package power was above the limit
    #[serde(default)]
    pub waited_samples: usize,
    #[serde(default)]
    pub waited_ns: u64,
    pub reason: WarmupReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Initial,
    Waiting,
    Warmup,
    Done,
}

#[derive(Debug, Clone)]
pub struct Warmup {
    phase: Phase,
    reason: Option<WarmupReason>,
    batch_samples: usize,
    batch_ns: u64,
    /// The last [`Warmup::STABILITY_WINDOW`] samples of a waiting batch, enough to replay into the
    /// window if it opens warmup. Bounded so a flood of tiny samples cannot grow memory.
    recent: VecDeque<Sample>,
    discarded_samples: usize,
    discarded_ns: u64,
    waited_samples: usize,
    waited_ns: u64,
    stable_samples: usize,
    stable_ns: u64,
    min_samples: NonZeroUsize,
    min_time_ns: NonZeroU64,
    last_per_iter: [f64; Warmup::STABILITY_WINDOW],
}

impl Warmup {
    pub const BATCH_NS: u64 = 200_000_000; // 200 ms
    pub const WAIT_LIMIT_NS: u64 = 120_000_000_000; // 120 s
    pub const MAX_TIME_NS: u64 = 15_000_000_000; // 15 s
    pub const STABILITY_WINDOW: usize = 8;
    pub const STABILITY_TOLERANCE: f64 = 0.05; // ±5%

    #[must_use]
    pub fn new(options: &StatsOptions) -> Self {
        Self {
            phase: Phase::Initial,
            reason: None,
            batch_samples: 0,
            batch_ns: 0,
            recent: VecDeque::with_capacity(Self::STABILITY_WINDOW),
            discarded_samples: 0,
            discarded_ns: 0,
            waited_samples: 0,
            waited_ns: 0,
            stable_samples: 0,
            stable_ns: 0,
            min_samples: options.min_warmup_samples,
            min_time_ns: options.min_warmup_time_ns,
            last_per_iter: [0.0; _],
        }
    }

    #[must_use]
    pub fn done(&self) -> bool {
        self.phase == Phase::Done
    }

    #[must_use]
    pub fn summary(&self) -> Option<WarmupSummary> {
        Some(WarmupSummary {
            samples: self.discarded_samples + self.stable_samples,
            total_ns: self.discarded_ns.saturating_add(self.stable_ns),
            waited_samples: self.waited_samples,
            waited_ns: self.waited_ns,
            reason: self.reason?,
        })
    }

    /// Feed one sample, which is discarded. Returns what became of the batch if it closed one.
    pub fn push(
        &mut self,
        sample: Sample,
        power: &mut dyn PowerSource,
    ) -> Result<Option<Batch>, WarmupError> {
        debug_assert!(!self.done(), "sample pushed to a completed warmup gate");
        if sample.iters == 0 || self.done() {
            return Ok(None);
        }
        self.batch_samples += 1;
        self.batch_ns = self.batch_ns.saturating_add(sample.total_ns);
        match self.phase {
            Phase::Initial => {}
            Phase::Waiting => {
                if self.recent.len() == Self::STABILITY_WINDOW {
                    self.recent.pop_front();
                }
                self.recent.push_back(sample);
            }
            Phase::Warmup => self.push_warmup(sample),
            Phase::Done => unreachable!(),
        }
        if self.batch_ns < Self::BATCH_NS {
            return Ok(None);
        }

        let band = power.band(self.batch_ns)?;
        self.close_batch(band).map(Some)
    }

    fn close_batch(&mut self, band: PowerBand) -> Result<Batch, WarmupError> {
        let ns = std::mem::take(&mut self.batch_ns);
        let samples = std::mem::take(&mut self.batch_samples);
        match self.phase {
            Phase::Initial => {
                self.discard(samples, ns);
                self.phase = if band == PowerBand::Unavailable {
                    Phase::Warmup
                } else {
                    Phase::Waiting
                };
                trace!(samples, ns, ?band, "discarded initial batch");
                Ok(Batch::Discarded)
            }
            Phase::Waiting => match band {
                PowerBand::Elevated | PowerBand::Above => {
                    self.discard(samples, ns);
                    self.waited_samples += samples;
                    self.waited_ns = self.waited_ns.saturating_add(ns);
                    trace!(
                        samples,
                        ns,
                        ?band,
                        "package power above limit, batch discarded"
                    );
                    if self.waited_ns >= Self::WAIT_LIMIT_NS {
                        return Err(WarmupError::PowerWaitTimeout {
                            waited_ns: self.waited_ns,
                        });
                    }
                    Ok(Batch::Discarded)
                }
                PowerBand::Settled | PowerBand::Unavailable => {
                    debug!(
                        waited_samples = self.waited_samples,
                        waited_seconds = self.waited_ns as f64 / 1_000_000_000.0,
                        "package power settled, starting warmup"
                    );
                    self.phase = Phase::Warmup;
                    // Count the whole batch, replay only its tail into the window
                    let tail_ns: u64 = self.recent.iter().map(|s| s.total_ns).sum();
                    self.stable_samples += samples - self.recent.len();
                    self.stable_ns = self.stable_ns.saturating_add(ns - tail_ns);
                    for sample in std::mem::take(&mut self.recent) {
                        self.push_warmup(sample);
                    }
                    self.check_complete();
                    Ok(Batch::Warmup)
                }
            },
            Phase::Warmup => {
                if band == PowerBand::Above {
                    return Err(WarmupError::PowerAboveLimit);
                }
                self.check_complete();
                Ok(Batch::Warmup)
            }
            Phase::Done => unreachable!("batch closed after warmup completed"),
        }
    }

    fn discard(&mut self, samples: usize, ns: u64) {
        self.recent.clear();
        self.discarded_samples += samples;
        self.discarded_ns = self.discarded_ns.saturating_add(ns);
    }

    fn push_warmup(&mut self, sample: Sample) {
        let per_iter = sample.total_ns as f64 / sample.iters as f64;
        self.last_per_iter[self.stable_samples % Self::STABILITY_WINDOW] = per_iter;
        self.stable_samples += 1;
        self.stable_ns = self.stable_ns.saturating_add(sample.total_ns);
    }

    fn check_complete(&mut self) {
        if self.stable_samples < self.min_samples.get() || self.stable_ns < self.min_time_ns.get() {
            return;
        }

        if self.stable_ns >= Self::MAX_TIME_NS {
            self.finish(WarmupReason::MaxTime);
            return;
        }

        if self.stable_samples < Self::STABILITY_WINDOW {
            return;
        }

        let min = self
            .last_per_iter
            .iter()
            .fold(f64::INFINITY, |a, &b| a.min(b));
        let max = self
            .last_per_iter
            .iter()
            .fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let mean: f64 = self.last_per_iter.iter().sum::<f64>() / Self::STABILITY_WINDOW as f64;

        if mean == 0.0 {
            return;
        }

        let rel_range = (max - min) / mean;
        if rel_range <= Self::STABILITY_TOLERANCE {
            self.finish(WarmupReason::Stable);
        }
    }

    fn finish(&mut self, reason: WarmupReason) {
        debug!(
            ?reason,
            samples = self.stable_samples,
            seconds = self.stable_ns as f64 / 1_000_000_000.0,
            discarded_samples = self.discarded_samples + self.stable_samples,
            "warmup complete"
        );
        self.phase = Phase::Done;
        self.reason = Some(reason);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// Answers in order, then unavailable.
    #[derive(Default)]
    struct Script(VecDeque<PowerBand>);

    impl Script {
        fn of(bands: &[PowerBand]) -> Self {
            Self(bands.iter().copied().collect())
        }
    }

    impl PowerSource for Script {
        fn band(&mut self, _batch_ns: u64) -> Result<PowerBand, WarmupError> {
            Ok(self.0.pop_front().unwrap_or(PowerBand::Unavailable))
        }
    }

    fn gate() -> Warmup {
        Warmup::new(&StatsOptions::default())
    }

    fn sample(total_ns: u64) -> Sample {
        Sample {
            iters: 100,
            total_ns,
        }
    }

    fn push_n(
        gate: &mut Warmup,
        power: &mut dyn PowerSource,
        n: usize,
        total_ns: u64,
    ) -> Vec<Batch> {
        let mut batches = Vec::new();
        for _ in 0..n {
            batches.extend(gate.push(sample(total_ns), power).unwrap());
        }
        batches
    }

    #[test]
    fn first_batch_always_discarded() {
        let mut gate = gate();
        let batches = push_n(&mut gate, &mut Script::default(), 2, 150_000_000);
        assert_eq!(batches, [Batch::Discarded]);
        assert_eq!(gate.phase, Phase::Warmup);
        assert_eq!(gate.discarded_samples, 2);
        assert_eq!(gate.discarded_ns, 300_000_000);
        assert!(gate.summary().is_none());

        let mut gate = self::gate();
        push_n(
            &mut gate,
            &mut Script::of(&[PowerBand::Settled]),
            2,
            150_000_000,
        );
        assert_eq!(gate.phase, Phase::Waiting, "discarded even when settled");
    }

    #[test]
    fn stops_on_stability_after_minimums() {
        let mut gate = gate();
        let mut power = Script::default();

        // Initial batch (2 x 150ms), then 8 stable samples => 1.2s and a full window.
        // Completion is checked at batch ends, so warmup ends on the 10th sample.
        for i in 0..10 {
            let _ = gate.push(sample(150_000_000), &mut power).unwrap();
            assert!(!gate.done() || i == 9, "finished early at sample {i}");
        }
        assert!(gate.done());
        let summary = gate.summary().unwrap();
        assert_eq!(summary.reason, WarmupReason::Stable);
        assert_eq!(summary.samples, 10);
        assert_eq!(summary.total_ns, 1_500_000_000);
        assert_eq!(summary.waited_samples, 0);
    }

    #[test]
    fn respects_min_time_before_stability() {
        let mut gate = gate();
        let mut power = Script::default();

        // Initial batch: 2 x 125ms. Then 7 samples => 875ms, below the 1s minimum, and the
        // batch after that (2 samples, 1.125s) completes warmup.
        for _ in 0..9 {
            let _ = gate.push(sample(125_000_000), &mut power).unwrap();
            assert!(!gate.done());
        }
        let _ = gate.push(sample(125_000_000), &mut power).unwrap();
        assert!(gate.done());
    }

    #[test]
    fn caps_at_max_time() {
        let mut gate = gate();
        let mut power = Script::default();

        // Initial batch is one 1s sample. Then alternate fast/slow to avoid stability; the
        // warmup total reaches 15s at the 15th warmup sample.
        let _ = gate.push(sample(1_000_000_000), &mut power).unwrap();
        for i in 0..15 {
            let total_ns = if i % 2 == 0 {
                1_000_000_000
            } else {
                1_100_000_000
            };
            assert!(!gate.done());
            let _ = gate.push(sample(total_ns), &mut power).unwrap();
        }
        assert!(gate.done());
        assert_eq!(gate.summary().unwrap().reason, WarmupReason::MaxTime);
    }

    #[test]
    fn min_time_can_exceed_max_cap() {
        let options = StatsOptions {
            min_warmup_samples: NonZeroUsize::new(1).unwrap(),
            min_warmup_time_ns: NonZeroU64::new(60_000_000_000).unwrap(), // 60s
            ..StatsOptions::default()
        };
        let mut gate = Warmup::new(&options);
        let mut power = Script::default();

        // Initial batch, then 12 x 5s = 60s of warmup
        let _ = gate.push(sample(5_000_000_000), &mut power).unwrap();
        for _ in 1..=12 {
            assert!(!gate.done());
            let _ = gate.push(sample(5_000_000_000), &mut power).unwrap();
        }
        assert!(gate.done());
        assert_eq!(gate.summary().unwrap().reason, WarmupReason::MaxTime);
    }

    #[test]
    fn waits_for_power_then_opens_on_first_settled_batch() {
        let mut gate = gate();
        let mut power = Script::of(&[
            PowerBand::Above,    // initial batch, discarded regardless
            PowerBand::Above,    // waiting
            PowerBand::Elevated, // still waiting: elevated is not settled
            PowerBand::Above,
            PowerBand::Settled, // opens warmup
            PowerBand::Settled,
            PowerBand::Elevated, // tolerated during warmup
            PowerBand::Settled,
        ]);

        let batches = push_n(&mut gate, &mut power, 1, 250_000_000);
        assert_eq!(batches, [Batch::Discarded]);
        assert_eq!(gate.phase, Phase::Waiting);

        let batches = push_n(&mut gate, &mut power, 6, 150_000_000);
        assert_eq!(batches, [Batch::Discarded; 3]);
        assert_eq!(gate.phase, Phase::Waiting);
        assert_eq!(gate.waited_samples, 6);
        assert_eq!(gate.waited_ns, 900_000_000);
        assert_eq!(gate.discarded_samples, 7);

        // The settled batch opens warmup with its own samples
        let batches = push_n(&mut gate, &mut power, 2, 150_000_000);
        assert_eq!(batches, [Batch::Warmup]);
        assert_eq!(gate.phase, Phase::Warmup);
        assert_eq!(gate.stable_samples, 2);
        assert_eq!(gate.waited_samples, 6);

        // 6 more stable samples fill the window (8 total) and exceed 1s
        let batches = push_n(&mut gate, &mut power, 6, 150_000_000);
        assert_eq!(batches, [Batch::Warmup; 3]);
        assert!(gate.done());
        let summary = gate.summary().unwrap();
        assert_eq!(summary.reason, WarmupReason::Stable);
        assert_eq!(summary.samples, 15);
        assert_eq!(summary.waited_samples, 6);
        assert!(power.0.is_empty(), "every scripted reading was consumed");
    }

    #[test]
    fn power_wait_times_out() {
        let mut gate = gate();
        let mut power = Script::of(&[PowerBand::Above; 200]);
        let _ = gate.push(sample(250_000_000), &mut power).unwrap();
        for _ in 0..119 {
            let batch = gate.push(sample(1_000_000_000), &mut power).unwrap();
            assert_eq!(batch, Some(Batch::Discarded));
        }
        assert!(matches!(
            gate.push(sample(1_000_000_000), &mut power),
            Err(WarmupError::PowerWaitTimeout {
                waited_ns: 120_000_000_000
            })
        ));
    }

    #[test]
    fn aborts_when_power_rises_during_warmup() {
        let mut gate = gate();
        let mut power = Script::of(&[PowerBand::Above, PowerBand::Settled, PowerBand::Above]);
        let _ = gate.push(sample(250_000_000), &mut power).unwrap();
        let _ = gate.push(sample(250_000_000), &mut power).unwrap();
        assert_eq!(gate.phase, Phase::Warmup);
        assert!(matches!(
            gate.push(sample(250_000_000), &mut power),
            Err(WarmupError::PowerAboveLimit)
        ));
    }

    #[test]
    fn zero_iteration_samples_are_ignored() {
        let mut gate = gate();
        let mut power = Script::default();
        let zero = Sample {
            iters: 0,
            total_ns: 250_000_000,
        };
        assert_eq!(gate.push(zero, &mut power).unwrap(), None);
        assert_eq!(gate.batch_samples, 0);
        assert_eq!(gate.batch_ns, 0);
    }

    #[test]
    fn waiting_buffer_is_bounded_and_opening_batch_is_fully_counted() {
        let mut gate = gate();
        let mut power = Script::of(&[PowerBand::Above, PowerBand::Settled]);
        let _ = gate.push(sample(250_000_000), &mut power).unwrap();
        assert_eq!(gate.phase, Phase::Waiting);

        // A waiting batch of 100,000 tiny samples keeps only the window's worth
        for _ in 0..99_999 {
            assert_eq!(gate.push(sample(2_000), &mut power).unwrap(), None);
        }
        assert_eq!(gate.recent.len(), Warmup::STABILITY_WINDOW);
        assert_eq!(gate.batch_samples, 99_999);

        // Closing it settled opens warmup with every sample counted
        let batch = gate.push(sample(2_000), &mut power).unwrap();
        assert_eq!(batch, Some(Batch::Warmup));
        assert_eq!(gate.stable_samples, 100_000);
        assert_eq!(gate.stable_ns, 200_000_000);
        assert_eq!(gate.recent.len(), 0);
        assert_eq!(gate.summary().map(|s| s.samples), None);
    }

    #[test]
    fn power_read_errors_propagate() {
        struct Broken;
        impl PowerSource for Broken {
            fn band(&mut self, _batch_ns: u64) -> Result<PowerBand, WarmupError> {
                Err(RaplError::NoPackageZone.into())
            }
        }
        let mut gate = gate();
        assert!(matches!(
            gate.push(sample(250_000_000), &mut Broken),
            Err(WarmupError::Power(RaplError::NoPackageZone))
        ));
    }
}
