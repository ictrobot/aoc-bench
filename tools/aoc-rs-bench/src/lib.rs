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

pub fn main<P1, P2, E>(puzzle_fn: fn(&str) -> Result<(P1, P2), E>)
where
    P1: ToString,
    P2: ToString,
    E: Display,
{
    let mut input = String::new();
    stdin()
        .read_to_string(&mut input)
        .expect("reading input from stdin failed");
    input = input.replace("\r\n", "\n");
    input = input.strip_suffix('\n').unwrap_or(&input).to_string();

    // Benchmark runner doesn't need to recover from errors and should exit immediately
    let bench_fn = || match puzzle_fn(std::hint::black_box(&input)) {
        Ok((p1, p2)) => (
            std::hint::black_box(p1).to_string(),
            std::hint::black_box(p2).to_string(),
        ),
        // Print InputError display implementation
        Err(err) => {
            std::eprintln!("{}", err);
            std::process::exit(1);
        }
    };

    let args = std::env::args().collect::<Vec<_>>();
    let args = args.iter().skip(1).map(|s| s.as_str()).collect::<Vec<_>>();
    match args.as_slice() {
        ["check"] => {
            let (part1, part2) = bench_fn();
            println!("Checksum: {}", checksum(&part1, &part2));

            if multiversion_glue::multiversion_used() {
                println!(
                    "Multiversion: default,{}",
                    multiversion_glue::get_supported_versions().join(",")
                )
            } else {
                println!("Multiversion: default")
            }
        }
        ["bench", threads, multiversion] => {
            if *threads != "n" {
                let threads: usize = threads.parse().unwrap();
                let threads = NonZeroUsize::new(threads).unwrap();
                multithreading_glue::set_thread_count(threads);
            }
            if *multiversion != "default" {
                multiversion_glue::set_override(multiversion);
            }

            println!("META\tversion=1");

            let mut iters_per_sample = 1;
            let mut ewma_iter_seconds = 0.0;
            for i in 0.. {
                let (duration, (part1, part2)) = sample(&bench_fn, iters_per_sample);
                let checksum = checksum(&part1, &part2);

                if writeln!(
                    stdout(),
                    "SAMPLE\t{iters_per_sample}\t{}\tchecksum={checksum}",
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

fn sample(f: impl Fn() -> (String, String), n: u32) -> (Duration, (String, String)) {
    // These need to be empty strings to avoid the hot loop needing to drop allocations
    let mut results = vec![(String::new(), String::new()); n as usize];

    let start = Instant::now();
    for r in results.iter_mut() {
        *r = std::hint::black_box(f());
    }
    let duration = start.elapsed();

    let (part1, part2) = results.pop().unwrap();
    for (p1, p2) in results {
        assert_eq!(p1, part1, "part 1 results differ");
        assert_eq!(p2, part2, "part 2 results differ");
    }

    (duration, (part1, part2))
}

fn checksum(part1: &str, part2: &str) -> String {
    // Match the output of `--stdin` mode
    let mut bytes = Vec::new();
    bytes.extend_from_slice(part1.as_bytes());
    bytes.push(b'\n');
    bytes.extend_from_slice(part2.as_bytes());
    bytes.push(b'\n');

    format!("{:016x}", fnv1a64(&bytes))
}

fn fnv1a64(data: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[macro_export]
macro_rules! main {
    ($year:ident $day:ident) => {
        fn main() {
            $crate::main(puzzle_fn);
        }

        #[inline(never)]
        fn puzzle_fn(input: &str) -> Result<(String, String), $crate::puzzle_glue::InputError> {
            let solution =
                $crate::puzzle_glue::$year::$day::new(input, $crate::puzzle_glue::InputType::Real)?;
            let part1 = solution.part1();
            let part2 = solution.part2();
            Ok((part1.to_string(), part2.to_string()))
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

#[cfg(not(any(
    feature = "glue-v2",
    feature = "glue-v3",
    feature = "glue-v4",
    feature = "glue-v5"
)))]
mod multiversion_glue {
    pub fn get_supported_versions() -> Vec<String> {
        Vec::new()
    }

    pub fn multiversion_used() -> bool {
        false
    }

    pub fn set_override(_: &str) {}
}

#[cfg(any(
    feature = "glue-v2",
    feature = "glue-v3",
    feature = "glue-v4",
    feature = "glue-v5"
))]
mod multiversion_glue {
    #[cfg(feature = "glue-v5")]
    use ::aoc::utils::multiversion::{Version, VERSIONS};
    #[cfg(any(feature = "glue-v2", feature = "glue-v3", feature = "glue-v4"))]
    use ::utils::multiversion::{Version, VERSIONS};
    use std::panic;

    pub fn get_supported_versions() -> Vec<String> {
        VERSIONS
            .iter()
            .map(|v| format!("{v:?}"))
            .collect::<Vec<_>>()
    }

    // This can only be used once, and returns if the override was set or get prior to this point.
    // The override cannot be set again after this point.
    // This is a hack, but works from the first commit multiversion overrides were added.
    pub fn multiversion_used() -> bool {
        // Prevent the default hook from logging the below expected panic
        let default_hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));

        let result = panic::catch_unwind(|| Version::set_override(Version::Scalar));

        panic::set_hook(default_hook);

        result.is_err()
    }

    pub fn set_override(version: &str) {
        Version::set_override(version.parse().unwrap());
    }
}

mod multithreading_glue {
    #[cfg(feature = "glue-v5")]
    pub use ::aoc::utils::multithreading::set_thread_count;
    #[cfg(any(feature = "glue-v3", feature = "glue-v4"))]
    pub use ::utils::multithreading::set_thread_count;
    #[cfg(not(any(feature = "glue-v3", feature = "glue-v4", feature = "glue-v5")))]
    pub fn set_thread_count(_: std::num::NonZeroUsize) {}
}
