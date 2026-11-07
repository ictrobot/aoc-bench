use std::fmt::Display;
use std::io::Write;
use std::io::{stdin, stdout, Read};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

const MAX_ITERS_PER_SAMPLE: u32 = 1024 * 1024;

// ~1.4ms to 32ms and back
const TARGET_SAMPLE_DURATIONS: [Duration; 18] = [
    Duration::from_nanos(1_414_214),
    Duration::from_nanos(2_000_000),
    Duration::from_nanos(2_828_427),
    Duration::from_nanos(4_000_000),
    Duration::from_nanos(5_656_854),
    Duration::from_nanos(8_000_000),
    Duration::from_nanos(11_313_708),
    Duration::from_nanos(16_000_000),
    Duration::from_nanos(22_627_417),
    Duration::from_nanos(32_000_000),
    Duration::from_nanos(22_627_417),
    Duration::from_nanos(16_000_000),
    Duration::from_nanos(11_313_708),
    Duration::from_nanos(8_000_000),
    Duration::from_nanos(5_656_854),
    Duration::from_nanos(4_000_000),
    Duration::from_nanos(2_828_427),
    Duration::from_nanos(2_000_000),
];

const EWMA_ALPHA: f64 = 0.2;

pub fn main<I, E>(
    prepare: fn(&str) -> Result<(u64, I), E>,
    multiversion_used: fn() -> bool,
    get_supported_versions: fn() -> Vec<String>,
    get_all_versions: fn() -> Vec<String>,
    set_override: fn(&str),
    set_thread_count: fn(NonZeroUsize),
    multithreading: bool,
) where
    I: Fn(&str) -> bool,
    E: Display,
{
    let mut input = String::new();
    stdin()
        .read_to_string(&mut input)
        .expect("reading input from stdin failed");
    input = input.replace("\r\n", "\n");
    input = input.strip_suffix('\n').unwrap_or(&input).to_string();

    // Benchmark runner doesn't need to recover from errors and should exit immediately
    let prepare = |input: &str| match prepare(input) {
        Ok(prepared) => prepared,
        // Print InputError display implementation
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    };

    let args = std::env::args().collect::<Vec<_>>();
    let args = args.iter().skip(1).map(|s| s.as_str()).collect::<Vec<_>>();
    match args.as_slice() {
        ["check"] => {
            // The reference solve has to come before the multiversion probe below, which detects
            // whether solving used the dispatch
            let (checksum, _) = prepare(&input);
            println!("Checksum: {checksum:016x}");

            if multiversion_used() {
                println!(
                    "Multiversion: default,{}",
                    get_supported_versions().join(",")
                );
                println!("MultiversionAll: default,{}", get_all_versions().join(","));
            } else {
                println!("Multiversion: default");
                println!("MultiversionAll: default");
            }

            // config.py checks this against whether the check run actually created threads
            println!("Multithreading: {multithreading}");
        }
        ["bench", threads, multiversion] => {
            if *threads != "n" {
                let threads: usize = threads.parse().unwrap();
                let threads = NonZeroUsize::new(threads).unwrap();
                set_thread_count(threads);
            }
            if *multiversion != "default" {
                set_override(multiversion);
            }

            println!("META\tversion=1");

            // Only solve once the thread count and variant are set: the first solve initialises
            // the dispatch, after which set_override panics
            let start = Instant::now();
            let (checksum, iterate) = prepare(&input);
            let reference_duration = start.elapsed();

            // aoc-bench always discards the first sample, so reporting the reference solve as the
            // first sample saves a solve per run, which adds up on slow days. Kept out of the EWMA
            // so the sample sizes that follow are unchanged
            if writeln!(
                stdout(),
                "SAMPLE\t1\t{}\tchecksum={checksum:016x}",
                reference_duration.as_nanos()
            )
            .is_err()
            {
                return;
            }

            let mut iters_per_sample = 1;
            let mut ewma_iter_seconds = 0.0;
            for i in 0.. {
                let duration = sample(&iterate, &input, iters_per_sample);

                if writeln!(
                    stdout(),
                    "SAMPLE\t{iters_per_sample}\t{}\tchecksum={checksum:016x}",
                    duration.as_nanos()
                )
                .is_err()
                {
                    break;
                }

                let iter_seconds = duration.as_secs_f64() / iters_per_sample as f64;
                ewma_iter_seconds = if ewma_iter_seconds == 0.0 {
                    iter_seconds
                } else {
                    ewma_iter_seconds * (1.0 - EWMA_ALPHA) + iter_seconds * EWMA_ALPHA
                };
                let target =
                    TARGET_SAMPLE_DURATIONS[i % TARGET_SAMPLE_DURATIONS.len()].as_secs_f64();
                let target_iters = (target / ewma_iter_seconds)
                    .round()
                    .clamp(1.0, MAX_ITERS_PER_SAMPLE as f64)
                    as u32;
                iters_per_sample = target_iters
                    .clamp(iters_per_sample / 2, iters_per_sample * 2)
                    .max(1);
            }
        }
        _ => {
            panic!("invalid args");
        }
    }
}

/// Times `n` iterations, each one solve compared against the reference answers and dropped.
///
/// The harness never allocates in here: the allocation history would otherwise depend on the
/// timing-derived sample sizes, changing the heap layout from launch to launch and with it the
/// measured speed of layout-sensitive solutions. Whatever the puzzle code itself allocates is
/// released again before the next iteration, so the harness adds nothing that varies.
#[inline(never)]
fn sample(iterate: &impl Fn(&str) -> bool, input: &str, n: u32) -> Duration {
    let start = Instant::now();
    let mut mismatch = false;
    for _ in 0..n {
        mismatch |= iterate(input);
    }
    let duration = start.elapsed();

    assert!(!mismatch, "results differ between iterations");

    duration
}

/// Checksum of the two answers, matching the output of `--stdin` mode: FNV-1a over
/// `"{part1}\n{part2}\n"`
pub fn checksum(part1: &impl Display, part2: &impl Display) -> u64 {
    use std::fmt::Write;

    let mut hasher = Fnv1a64(0xcbf2_9ce4_8422_2325);
    write!(hasher, "{part1}\n{part2}\n").expect("hashing answers failed");
    hasher.0
}

struct Fnv1a64(u64);

impl std::fmt::Write for Fnv1a64 {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        for &b in s.as_bytes() {
            self.0 ^= b as u64;
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Ok(())
    }
}

// The optional trailing identifiers select the real multiversion or multithreading glue instead of
// the stub implementations. build.sh builds every binary with the stub implementation first and
// then rebuilds any binaries where the built binary shows the day's own code pulls in the
// multiversion/multithreading logic.
#[macro_export]
macro_rules! main {
    ($year:ident $day:ident) => {
        $crate::main!(@multiversion_stub);
        $crate::main!(@multithreading_stub);
        $crate::main!(@impl $year $day);
    };
    ($year:ident $day:ident multiversion) => {
        $crate::main!(@multiversion_glue);
        $crate::main!(@multithreading_stub);
        $crate::main!(@impl $year $day);
    };
    ($year:ident $day:ident multithreading) => {
        $crate::main!(@multiversion_stub);
        $crate::main!(@multithreading_glue);
        $crate::main!(@impl $year $day);
    };
    ($year:ident $day:ident multiversion multithreading) => {
        $crate::main!(@multiversion_glue);
        $crate::main!(@multithreading_glue);
        $crate::main!(@impl $year $day);
    };
    (@multiversion_stub) => {
        fn get_supported_versions() -> Vec<String> {
            panic!("built with multiversion stub, cannot list supported versions");
        }

        fn get_all_versions() -> Vec<String> {
            panic!("built with multiversion stub, cannot list compiled versions");
        }

        fn multiversion_used() -> bool {
            false
        }

        fn set_override(version: &str) {
            panic!("built with multiversion stub, cannot set override to {version}");
        }
    };
    (@multiversion_glue) => {
        #[cfg(feature = "glue-v5")]
        use ::aoc::utils::multiversion::{Version, VERSIONS};
        #[cfg(not(feature = "glue-v5"))]
        use ::utils::multiversion::{Version, VERSIONS};

        fn get_supported_versions() -> Vec<String> {
            VERSIONS.iter().map(|v| format!("{v:?}")).collect()
        }

        #[cfg(feature = "glue-v6")]
        fn get_all_versions() -> Vec<String> {
            Version::all().iter().map(|v| format!("{v:?}")).collect()
        }

        #[cfg(not(feature = "glue-v6"))]
        fn get_all_versions() -> Vec<String> {
            // All names used before Version::all was added. Parsing checks whether the version
            // was compiled into this revision, without requiring CPU support or executing it.
            [
                "Scalar", "Array128", "Array256", "Array4096", "AVX2", "AVX2x2", "AVX2x4", "AVX2x8",
                "AVX512", "AVX512x2", "AVX512x4", "AVX512x8", "Neon", "Neonx2", "Neonx4", "Neonx8",
            ]
            .into_iter()
            .filter_map(|name| name.parse::<Version>().ok())
            .map(|v| format!("{v:?}"))
            .collect()
        }

        // This can only be used once, and returns if the override was set or get prior.
        fn multiversion_used() -> bool {
            let default_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            let result = std::panic::catch_unwind(|| Version::set_override(Version::Scalar));
            std::panic::set_hook(default_hook);
            result.is_err()
        }

        fn set_override(version: &str) {
            Version::set_override(version.parse().unwrap());
        }
    };
    (@multithreading_stub) => {
        const MULTITHREADING: bool = false;

        fn set_thread_count(threads: std::num::NonZeroUsize) {
            // config.py sets one thread by default
            assert_eq!(threads.get(), 1, "built with multithreading stub, cannot set thread count");
        }
    };
    (@multithreading_glue) => {
        const MULTITHREADING: bool = true;

        #[cfg(feature = "glue-v5")]
        use ::aoc::utils::multithreading::set_thread_count;
        #[cfg(not(feature = "glue-v5"))]
        use ::utils::multithreading::set_thread_count;
    };
    (@impl $year:ident $day:ident) => {
        fn main() {
            $crate::main(
                prepare,
                multiversion_used,
                get_supported_versions,
                get_all_versions,
                set_override,
                set_thread_count,
                MULTITHREADING,
            );
        }

        /// Solves the puzzle once as the reference and returns the checksum of its answers along
        /// with the iteration the harness times: one fresh solve, compared against the reference.
        ///
        /// This lives in the macro so the answers keep their concrete types: some days return
        /// `&str` borrowed from the solution, so `to_owned` copies those into a `String` and the
        /// solution itself is dropped before any timing. `#[inline(never)]` so the reference solve
        /// exists once in the binary rather than once per caller.
        #[inline(never)]
        fn prepare(
            input: &str,
        ) -> Result<(u64, impl Fn(&str) -> bool), $crate::puzzle_glue::InputError> {
            let reference = $crate::puzzle_glue::$year::$day::new(
                std::hint::black_box(input),
                $crate::puzzle_glue::InputType::Real,
            )?;
            let part1 = std::hint::black_box(reference.part1()).to_owned();
            let part2 = std::hint::black_box(reference.part2()).to_owned();
            // Guards against a borrowed answer type slipping through: it would fail to build here
            drop(reference);
            let checksum = $crate::checksum(&part1, &part2);

            Ok((checksum, move |input: &str| {
                let solution = match $crate::puzzle_glue::$year::$day::new(
                    std::hint::black_box(input),
                    $crate::puzzle_glue::InputType::Real,
                ) {
                    Ok(solution) => solution,
                    Err(err) => {
                        eprintln!("{}", err);
                        std::process::exit(1);
                    }
                };
                // The barriers on the answers keep them from being folded into the comparison:
                // f(x) == f(y) simplifies to x == y for any injective f the optimiser can see
                (std::hint::black_box(solution.part1()) != part1)
                    | (std::hint::black_box(solution.part2()) != part2)
            }))
        }
    };
}

// Puzzle glue
#[cfg(any(
    feature = "glue-v0",
    feature = "glue-v1",
    feature = "glue-v2",
    feature = "glue-v3",
    feature = "glue-v4"
))]
pub mod puzzle_glue {
    pub use ::year2015;
    #[cfg(feature = "glue-v4")]
    pub use ::year2016;

    #[cfg(not(feature = "glue-v0"))]
    pub use ::utils::input::{InputError, InputType};
    #[cfg(feature = "glue-v0")]
    pub use ::utils::input::{InputType, InvalidInputError as InputError};
}

#[cfg(feature = "glue-v5")]
pub mod puzzle_glue {
    pub use ::aoc::*;

    pub use ::aoc::utils::input::{InputError, InputType};
}
