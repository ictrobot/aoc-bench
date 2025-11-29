use ahash::{HashMap, HashMapExt as _, HashSet, HashSetExt as _};
use serde::Deserialize;
use std::borrow::Cow;
use std::fs;
use std::path::{Path, PathBuf};

use super::{
    Benchmark, BenchmarkId, BenchmarkVariant, ConfigError, ConfigFile, ConfigProduct, Key,
    KeyValuesSubset,
};
use crate::stats::StatsOptions;
use std::num::{NonZeroU64, NonZeroUsize};

pub(super) struct ParsedConfigFile {
    pub config_keys: Vec<Key>,
    pub benchmarks: Vec<Benchmark>,
    pub host_key: Key,
}

pub(super) fn parse_config_file(
    data_dir: &Path,
    current_host: Option<&str>,
    json_str: &str,
) -> Result<ParsedConfigFile, ConfigError> {
    let json: ConfigFileJson<'_> = serde_json::from_str(json_str)?;
    let ParsedKeys {
        mut config_keys,
        key_lookup,
    } = parse_config_keys(json.config_keys)?;

    let benchmarks = parse_benchmarks(data_dir, json.benchmarks, &key_lookup)?;

    let host_key = Key::new_host_key(&data_dir.join(ConfigFile::RESULTS_DIR), current_host)?;
    config_keys.push(host_key.clone());
    config_keys.sort_unstable();

    Ok(ParsedConfigFile {
        config_keys,
        benchmarks,
        host_key,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFileJson<'a> {
    #[serde(borrow)]
    config_keys: HashMap<&'a str, ConfigKeyDef<'a>>,
    #[serde(borrow)]
    benchmarks: Vec<BenchmarkDef<'a>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigKeyDef<'a> {
    #[serde(borrow)]
    values: Vec<&'a str>,
    #[serde(borrow, default)]
    presets: HashMap<&'a str, Vec<&'a str>>,
}

#[derive(Deserialize)]
struct BenchmarkDef<'a> {
    benchmark: &'a str,
    // Plain &'a str doesn't support parsing strings with escaped characters (e.g. \t), so use Cow
    // to avoid allocating where possible but still allow escaped strings to be parsed
    #[serde(borrow, default)]
    command: Option<Vec<Cow<'a, str>>>,
    #[serde(borrow, default)]
    input: Option<&'a str>,
    #[serde(borrow, default)]
    checksum: Option<&'a str>,
    #[serde(default)]
    stats: Option<StatsOverrideDef>,
    #[serde(borrow, default)]
    config: Option<HashMap<&'a str, ConfigSpec<'a>>>,
    #[serde(borrow, default)]
    variants: Option<Vec<BenchmarkVariantDef<'a>>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BenchmarkVariantDef<'a> {
    #[serde(borrow, default)]
    command: Option<Vec<Cow<'a, str>>>,
    #[serde(borrow, default)]
    input: Option<&'a str>,
    #[serde(borrow, default)]
    checksum: Option<&'a str>,
    #[serde(default)]
    stats: Option<StatsOverrideDef>,
    #[serde(borrow)]
    config: HashMap<&'a str, ConfigSpec<'a>>,
}

#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatsOverrideDef {
    min_samples: Option<NonZeroUsize>,
    min_time_ns: Option<NonZeroU64>,
    target_rel_ci: Option<f64>,
    min_warmup_samples: Option<NonZeroUsize>,
    min_warmup_time_ns: Option<NonZeroU64>,
    runs_per_series: Option<NonZeroUsize>,
}

#[derive(Deserialize)]
#[serde(untagged, expecting = "expected a string or an array of strings")]
enum ConfigSpec<'a> {
    Preset(&'a str),
    #[serde(borrow)]
    Literal(Vec<&'a str>),
}

struct ParsedKeys<'a> {
    config_keys: Vec<Key>,
    key_lookup: HashMap<&'a str, KeyLookup<'a>>,
}

struct KeyLookup<'a> {
    key: Key,
    presets: HashMap<&'a str, KeyValuesSubset>,
}

fn parse_config_keys<'a>(
    raw_keys: HashMap<&'a str, ConfigKeyDef<'a>>,
) -> Result<ParsedKeys<'a>, ConfigError> {
    let mut config_keys = Vec::with_capacity(raw_keys.len());
    let mut key_lookup = HashMap::with_capacity(raw_keys.len());

    for (key_name, key_def) in raw_keys {
        let key = Key::new(key_name, key_def.values)?;
        let presets = parse_presets(&key, key_def.presets)?;
        config_keys.push(key.clone());
        key_lookup.insert(key_name, KeyLookup { key, presets });
    }

    Ok(ParsedKeys {
        config_keys,
        key_lookup,
    })
}

fn parse_presets<'a>(
    key: &Key,
    presets: HashMap<&'a str, Vec<&str>>,
) -> Result<HashMap<&'a str, KeyValuesSubset>, ConfigError> {
    let mut parsed = HashMap::with_capacity(presets.len());
    for (preset_name, preset_values) in presets {
        parsed.insert(
            preset_name,
            key.subset_from_names(preset_values.into_iter())?,
        );
    }
    Ok(parsed)
}

fn parse_benchmarks<'a>(
    data_dir: &Path,
    benchmark_defs: Vec<BenchmarkDef<'a>>,
    key_lookup: &HashMap<&'a str, KeyLookup<'a>>,
) -> Result<Vec<Benchmark>, ConfigError> {
    let mut benchmarks = Vec::with_capacity(benchmark_defs.len());
    let mut seen_benchmarks = HashSet::with_capacity(benchmark_defs.len());

    for bench_def in benchmark_defs {
        let benchmark_id: BenchmarkId = bench_def.benchmark.try_into()?;
        if !seen_benchmarks.insert(benchmark_id.clone()) {
            return Err(ConfigError::DuplicateBenchmark(benchmark_id.to_string()));
        }

        let bench_stats = apply_stats_overrides(StatsOptions::default(), bench_def.stats)?;

        let base_input = bench_def
            .input
            .map(|name| resolve_input_path(data_dir, name))
            .transpose()?;

        let benchmark = match (bench_def.config, bench_def.command, bench_def.variants) {
            // Single benchmark
            (Some(config), Some(command), None) => Benchmark::new(
                benchmark_id,
                build_config_product(key_lookup, config)?,
                command.into_iter().map(Cow::into_owned).collect::<Vec<_>>(),
                base_input,
                bench_def.checksum.map(str::to_string),
                bench_stats,
            )?,
            // Multi benchmark
            (None, base_command, Some(variants)) => Benchmark::new_with_variants(
                benchmark_id.clone(),
                variants
                    .into_iter()
                    .map(|variant| {
                        let command = variant
                            .command
                            .or_else(|| base_command.clone())
                            .map(|cmd| cmd.into_iter().map(Cow::into_owned).collect::<Vec<_>>())
                            .unwrap_or_default();
                        let input = match variant.input {
                            Some(name) => Some(resolve_input_path(data_dir, name)?),
                            None => base_input.clone(),
                        };
                        let stats = apply_stats_overrides(bench_stats, variant.stats)?;

                        BenchmarkVariant::new(
                            benchmark_id.clone(),
                            build_config_product(key_lookup, variant.config)?,
                            command,
                            input,
                            variant.checksum.or(bench_def.checksum).map(str::to_string),
                            stats,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )?,
            _ => {
                return Err(ConfigError::InvalidBenchmarkOptions(
                    benchmark_id.to_string(),
                ));
            }
        };

        benchmarks.push(benchmark);
    }

    Ok(benchmarks)
}

fn resolve_input_path(data_dir: &Path, name: &str) -> Result<PathBuf, ConfigError> {
    let path = data_dir.join(ConfigFile::INPUTS_DIR).join(name);
    fs::canonicalize(&path).map_err(|error| ConfigError::MissingInput {
        name: name.to_string(),
        path,
        error,
    })
}

fn build_config_product<'a>(
    key_lookup: &HashMap<&'a str, KeyLookup<'a>>,
    config_map: HashMap<&'a str, ConfigSpec<'a>>,
) -> Result<ConfigProduct, ConfigError> {
    let mut subsets = Vec::with_capacity(config_map.len());
    for (key_name, spec) in config_map {
        let entry = key_lookup
            .get(key_name)
            .ok_or_else(|| ConfigError::UnknownKey(key_name.to_string()))?;

        let subset = match spec {
            ConfigSpec::Preset(preset_name) => {
                entry.presets.get(preset_name).cloned().ok_or_else(|| {
                    ConfigError::UnknownPreset {
                        key: key_name.to_string(),
                        preset: preset_name.to_string(),
                    }
                })?
            }
            ConfigSpec::Literal(values) => entry.key.subset_from_names(values.into_iter())?,
        };
        subsets.push(subset);
    }

    Ok(ConfigProduct::new(subsets))
}

fn apply_stats_overrides(
    base: StatsOptions,
    overrides: Option<StatsOverrideDef>,
) -> Result<StatsOptions, ConfigError> {
    let mut options = base;
    let Some(overrides) = overrides else {
        return Ok(options);
    };

    if let Some(min_samples) = overrides.min_samples {
        options.min_samples = min_samples;
    }
    if let Some(min_time_ns) = overrides.min_time_ns {
        options.min_total_time_ns = min_time_ns;
    }
    if let Some(target_rel_ci) = overrides.target_rel_ci {
        options.target_rel_ci = target_rel_ci;
    }
    if let Some(min_warmup_samples) = overrides.min_warmup_samples {
        options.min_warmup_samples = min_warmup_samples;
    }
    if let Some(min_warmup_time_ns) = overrides.min_warmup_time_ns {
        options.min_warmup_time_ns = min_warmup_time_ns;
    }
    if let Some(runs_per_series) = overrides.runs_per_series {
        options.runs_per_series = runs_per_series;
    }

    if let Err((field, reason)) = options.validate() {
        return Err(ConfigError::InvalidStatsOverride { field, reason });
    }

    Ok(options)
}
