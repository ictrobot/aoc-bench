use crate::commands::DEFAULT_DATA_DIR;
use aoc_bench::config::{BenchmarkId, Config, ConfigFile};
use aoc_bench::runner::Runner;
use aoc_bench::stable::{record_run_series, RecordOptions, RecordOutcome};
use aoc_bench::storage::{HybridDiskStorage, Storage};
use clap::Args;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::{io, slice};
use tracing::{error, info, info_span, trace_span, warn};

#[derive(Args, Debug)]
pub struct RunArgs {
    /// Benchmark name to run. Runs all if omitted.
    #[arg(value_name = "BENCH")]
    pub benchmark: Option<String>,

    /// Config filter (key=value,key=value format, host key not allowed)
    #[arg(long)]
    pub config: Option<String>,

    /// Path to the data directory (defaults to ./data)
    #[arg(long, value_parser = clap::value_parser!(PathBuf))]
    pub data_dir: Option<PathBuf>,

    /// Force the new run series to become the stable result
    #[arg(long)]
    pub force_update_stable: bool,
}

#[allow(clippy::too_many_lines)]
pub fn execute(args: RunArgs) -> ExitCode {
    let host = match current_host() {
        Ok(host) => host,
        Err(error) => {
            error!(%error, "failed to determine current host name");
            return ExitCode::FAILURE;
        }
    };

    let data_dir = args
        .data_dir
        .as_deref()
        .unwrap_or_else(|| Path::new(DEFAULT_DATA_DIR));

    let config_file = match ConfigFile::new(data_dir, Some(&host)) {
        Ok(cfg) => cfg,
        Err(error) => {
            error!(%error, "failed to load data/config.json");
            return ExitCode::FAILURE;
        }
    };

    let host_key = config_file.host_key().clone();
    let host_kv = host_key.value_from_name(&host).unwrap();

    let filter = match args
        .config
        .map(|s| {
            config_file
                .config_without_host_from_string(&s)
                .map_err(|e| e.to_string())
        })
        .transpose()
    {
        Ok(Some(f)) => f,
        Ok(None) => Config::new(),
        Err(error) => {
            error!(%error, "invalid config filter");
            return ExitCode::FAILURE;
        }
    };

    let benches = match args.benchmark {
        Some(id) => {
            let Some(benchmark) = BenchmarkId::try_from(id)
                .ok()
                .and_then(|id| config_file.benchmark_by_id(&id))
            else {
                error!("unknown benchmark name");
                return ExitCode::FAILURE;
            };
            slice::from_ref(benchmark)
        }
        None => config_file.benchmarks(),
    };

    let storage = match HybridDiskStorage::new(config_file.clone(), &host) {
        Ok(storage) => storage,
        Err(error) => {
            error!(%error, "failed to open storage");
            return ExitCode::FAILURE;
        }
    };

    // Acquire global lock to prevent concurrent benchmark runs
    let _lock = match storage.acquire_lock() {
        Ok(lock) => lock,
        Err(error) => {
            error!(%error, "failed to acquire storage lock");
            return ExitCode::FAILURE;
        }
    };

    let mut count = 0usize;
    for bench in benches {
        let span = info_span!("bench", bench = %bench.id());
        let _enter = span.enter();

        for (i, variant) in bench.variants().iter().enumerate() {
            let Some(product) = variant.config().filter(&filter) else {
                continue;
            };

            let span = trace_span!("variant", variant = i);
            let _enter = span.enter();

            for config in product.iter().map(|c| c.with(host_kv.clone())) {
                let span = info_span!("config", %config);
                let _enter = span.enter();

                let runner = match Runner::new(config_file.data_dir(), variant, config.clone()) {
                    Ok(runner) => runner,
                    Err(error) => {
                        error!(%error, bench = bench.id().as_str(), %config, "failed to build runner");
                        return ExitCode::FAILURE;
                    }
                };

                info!("running series");
                let series = match runner.run_series() {
                    Ok(series) => series,
                    Err(error) => {
                        error!(%error, bench = bench.id().as_str(), %config, "benchmark run failed");
                        return ExitCode::FAILURE;
                    }
                };

                match record_run_series(
                    &storage,
                    &series,
                    RecordOptions {
                        force_update_stable: args.force_update_stable,
                    },
                ) {
                    Ok((outcome, json_path)) => {
                        count += 1;
                        info!(
                            path = %json_path.display(),
                            "stored run series"
                        );
                        match outcome {
                            RecordOutcome::Initial => {
                                info!("new run series");
                            }
                            RecordOutcome::Matched => {
                                info!("matched existing stable result");
                            }
                            RecordOutcome::Suspicious {
                                current_stable,
                                suspicious_count,
                            } => {
                                warn!(
                                    suspicious_count,
                                    stable_ns = current_stable.mean_ns_per_iter,
                                    "didn't match stable result, suspicious"
                                );
                            }
                            RecordOutcome::Replaced { old_stable } => {
                                warn!(
                                    old_stable_ns = old_stable.mean_ns_per_iter,
                                    "didn't match stable result, replaced"
                                );
                            }
                            RecordOutcome::Forced { old_stable } => {
                                warn!(
                                    old_stable_ns = old_stable.mean_ns_per_iter,
                                    "forced replacement of stable result"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        error!(%error, bench = bench.id().as_str(), %config, "failed to persist results");
                        return ExitCode::FAILURE;
                    }
                }
            }
        }
    }

    if count == 0 {
        warn!("no benchmark/config combinations matched the provided filters");
        ExitCode::FAILURE
    } else {
        info!(count, "complete");
        ExitCode::SUCCESS
    }
}

fn current_host() -> io::Result<String> {
    if let Ok(host) = std::env::var("BENCH_HOST") {
        return Ok(host);
    }
    hostname::get().map(|os| os.to_string_lossy().to_string())
}
