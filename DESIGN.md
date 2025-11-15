# 1. Scope and Requirements

You want a system that:

* Benchmarks arbitrary commands with flexible **configuration dimensions**.
* Uses **in-process benchmarking** (no process spawn overhead) with a simple text protocol.
* Can run for **months/years**, but:
    * You're happy to **replace results** when the environment clearly changed.
    * Old runs are immutable and never removed or modified.
* Keeps storage **self-contained**, **file/JSON based**, with SQLite DB for faster queries.

Key ideas:

* A **runner protocol** (`SAMPLE <iters> <total_ns> ...`) that any benchmark implementation can emit.
* A **stats engine** that turns those samples into "mean time per iteration ± CI".
* A **storage model** that keeps one "stable result" per `(benchmark, config)` pair and updates it when
  environment changes are detected.

## CLI Commands

The `aoc-bench` tool provides the following subcommands:

* **`run`** - Execute benchmarks by spawning commands from the config file
    * Reads benchmark definitions and command templates from config file
    * Substitutes `{key}` placeholders with config values
    * Spawns child processes and collects SAMPLE output
    * Performs run series (default: 7 runs) and stores results

* **`sample`** - Periodically re-run benchmarks for drift detection
    * Automatically selects (bench, config) pairs to re-run
    * Prioritizes configs without results or with oldest timestamps
    * Useful for nightly/periodic runs to detect environment changes

* **`export`** - Query and export benchmark results
    * Outputs TSV suitable for plotting
    * Supports partial config matching for filtering
    * Extracts config keys into separate columns (with `cfg_` prefix)

* **`timeline`** - Show performance history across one config dimension
    * Displays ordered results with performance change highlights
    * Automatically detects comparison key (unspecified dimension)
    * Shows regressions and improvements with delta percentages

* **`impact`** - Show which benchmarks changed for a specific config value
    * Compares results before/after a config key change
    * Groups results into regressions, improvements, and unchanged
    * Useful for commit impact analysis or profile comparisons

* **`debug`** - Debug the runner independently with a raw command
    * Accepts `--input` option to provide stdin input to the command
    * Accepts `--checksum` option to validate output checksums
    * Takes exact command and arguments after `--` (no template expansion)
    * Executes a full run series and outputs JSON result or error
    * Useful for testing benchmark implementations and troubleshooting runner issues

## Data Directory Structure

All benchmark data and configuration is stored in a single `data/` directory with the following structure:

* **`data/config.json`** (required): Benchmark configuration file defining available config keys, benchmarks, and
  commands
* **`data/inputs/`** (user-managed): Input files for benchmarks (e.g., `2015-04.txt`)
    - Benchmark definitions reference these files via the `input` field
    - Can be `null` if no input file is needed
* **`data/builds/`** or other user directories (user-managed): Compiled binaries or other resources referenced by
  commands
    - Organization is flexible and determined by command templates in `config.json`
    - Example: `data/builds/native/abc1234` could be an executable referenced by a command template
* **`data/results/`** (auto-created by tool): Benchmark results organized by host
    - **`results/{host}/metadata.db`**: SQLite database with indexed results for fast queries
    - **`results/{host}/runs/`**: Immutable JSON files containing full run series data

# 2. Concepts and Terminology

* **Benchmark (bench)**
  A named task/workload to measure, e.g. `"2015-04"` or `"compile-aoc-rs"`.
  The benchmark identifies **what** to run. It is the primary organizing dimension for storage and visualization.

* **Configuration (config)**
  A JSON object containing key-value pairs that specify **how** to run a benchmark, e.g.:
  ```json
  {"commit":"abc1234","host":"pi5","profile":"release","threads":"1"}
  ```

  The config contains all variable dimensions (commit, host, build settings, etc.) but **not** the benchmark name.
  Together, `(benchmark, config)` uniquely identifies a benchmark execution variant.

  **Character constraints**: To ensure filesystem safety and portability:
    - Keys: Must match regex `[a-z][a-z0-9_]*` (lowercase alphanumeric + underscores, must start with letter)
    - Values: Must match regex `[a-zA-Z0-9_-]+` (alphanumeric + underscores + hyphens, no spaces or special chars)
    - These constraints ensure config strings can be used directly in file paths on all platforms

  **Well-known keys** (by convention, not required):
    - `commit`: Git commit hash (typically 7 characters, e.g., `"abc1234"`)
    - `host`: Machine identifier (e.g., `"pi5"`, `"pi3"`)
    - `profile`: Build profile (e.g., `"release"`, `"debug"`)
    - `threads`: Thread count (e.g., `"1"`, `"32"`)
    - `simd`: SIMD instruction set (e.g., `"avx2"`, `"avx512"`)

  The `benchmark` name is not part of the config, and `benchmark` is disallowed as a config key.

  Additional keys can be added freely for custom dimensions. All values are strings in JSON.

  **Partial config matching**: When querying or comparing results, you can filter by a subset of keys (e.g.,
  `host=pi5`),
  and the system will match all configs that contain those key-value pairs. This allows flexible querying and comparison
  across configurations that differ only in certain parameters.

  **Canonical sorting requirement**: Config JSON keys must always be stored in canonically sorted form (sorted
  alphabetically by key). This ensures that configs with the same keys but different ordering are recognized as
  identical. All tools that generate or manipulate configs must maintain this invariant.

* **Run**
  One full benchmark execution producing a mean + CI using sample-based estimation.

* **Run Series**
  A set of repeated runs for the same `(bench, config)` executed back-to-back (normally **7 runs**).
  Each run series is uniquely identified by `(bench, config, timestamp)` and produces a representative
  result via median-of-means. You can re-run the same (bench, config) combination multiple times
  (e.g., to check variance or after environment changes).

* **Stable Result**
  For each unique `(bench, config)` pair, the stable result is the current representative measurement shown to users.
  This is derived from a run series's representative **median run**.
  When a (bench, config) is re-run and results are within noise, the stable result doesn't change (preventing
  microscopic drift).
  When new run series are significantly different (3 consecutive times), the stable result is replaced.
  This happens independently per (bench, config) pair.

* **Sample**
  One `SAMPLE` line: `(total_ns, iters)` from one in-process batch.

# 3. System Components

The system is composed of:

1. **Benchmark Target** (your AoC binary or arbitrary command)

    * Implements a `--bench-sample` mode that:

        * Runs the hot path in a loop.
        * Emits `SAMPLE` lines continuously.
        * Can handle variable `iters_per_sample` controlled internally or externally.

2. **Collector / Runner Controller** (aoc-bench, real implementation in Rust)

    * Reads `SAMPLE` lines from the child process.
    * Applies statistics:
        * warmup,
        * mode detection (regression vs per-iter mean),
        * bootstrap CI,
        * stop when CI is small enough.
    * Performs **7 runs back-to-back** to form a run series.
    * Produces **one run series JSON** containing all runs and the median result.

3. **Store / Stable Result Manager**

    * Per `(benchmark, config)` pair:

        * Maintains a stable result timestamp.
        * Stores all run summaries for full history.
        * Detects when new runs are "too different" → updates stable result.

4. **Viewer / Exporter** (later)

    * Reads summaries from disk.
    * Produces JSON/TSV/plots: "time vs commit" using each commit's stable result.

---

# 4. Configuration File Format

The system uses a JSON configuration file located at `data/config.json` that defines:

1. Available configuration keys and their values
2. Benchmark definitions with their configs to run

**File location**: The config file must be at `data/config.json` in the benchmark data directory.
All commands and paths specified in the config are relative to the `data/` directory.

## 4.1 Structure

```json
{
  "config_keys": {
    "commit": {
      "values": [
        "abc1234",
        "def5678",
        "ghi9012",
        ...
      ],
      "presets": {
        "from_abc1234": [
          "abc1234",
          "def5678",
          "ghi9012",
          ...
        ],
        "recent": [
          "ghi9012",
          ...
        ]
      }
    },
    "build": {
      "values": [
        "generic",
        "native"
      ],
      "presets": {
        "all": [
          "generic",
          "native"
        ]
      }
    },
    "threads": {
      "values": [
        "1",
        "2",
        "4",
        "8",
        "16",
        "32"
      ],
      "presets": {
        "power_of_2": [
          "1",
          "2",
          "4",
          "8",
          "16",
          "32"
        ],
        "single": [
          "1"
        ]
      }
    }
  },
  "benchmarks": [
    {
      "benchmark": "2015-04",
      "command": "builds/{build}/{commit} bench 2015 04 {threads}",
      "input": "2015-04.txt",
      "checksum": "expected_output_hash",
      "config": {
        "commit": "from_abc1234",
        "build": "all",
        "threads": [
          "1",
          "32"
        ]
      }
    }
  ]
}
```

## 4.2 Config Keys Section

**`config_keys`**: Defines the schema for configuration dimensions.

For each key (e.g., `commit`, `build`, `threads`):

- **`values`**: Array of all valid values for this key (in canonical order)
- **`presets`**: Named groups of values for convenient reuse
    - Preset names can be any string (e.g., `"all"`, `"from_abc1234"`, `"power_of_2"`)
    - Preset values must be subsets of `values`

**Canonical ordering**: The `values` array defines the canonical order for this key. When configs are serialized to
strings or stored, values are sorted according to this order.

**Key names**: Must match regex `[a-z][a-z0-9_]*` (lowercase, start with letter).

**All keys are optional**: A benchmark config can omit any key. Only specified keys are included in the final config.

## 4.3 Benchmarks Section

**`benchmarks`**: Array of benchmark definitions.

Each benchmark entry:

- **`benchmark`**: Benchmark name/identifier (string)
- **`command`**: Command template with `{key}` placeholders for config values. Path is relative to the `data/`
  directory.
- **`input`**: Input filename in `data/inputs/` directory (e.g., `"2015-04.txt"` refers to `data/inputs/2015-04.txt`),
  or `null` if no input is required
- **`checksum`**: Expected output checksum (for correctness verification). Optional. If present the command must output
  the expected checksum.
- **`config`**: Configuration specification for this benchmark

**Config specification**:

- Keys must exist in `config_keys`
- Values can be:
    - **Preset name** (string): References a preset from `config_keys[key].presets`
    - **Literal array** (array of strings): Explicit list of values
- Values must be subsets of the canonical `values` for that key

**Expansion**: The config expands via **Cartesian product** of all key values.

Example:

```json
"config": {
"commit": "recent", // Preset → ["ghi9012", "jkl3456"]
"build": ["generic"], // Literal
"threads": ["1", "32"]   // Literal
}
```

Expands to 4 configs:

- `commit=ghi9012,build=generic,threads=1`
- `commit=ghi9012,build=generic,threads=32`
- `commit=jkl3456,build=generic,threads=1`
- `commit=jkl3456,build=generic,threads=32`

**Multiple entries with same benchmark**: The benchmarks array can have multiple entries with the same `benchmark` name
but different configs. This allows specifying disjoint config sets (e.g., different commit ranges) for the same
benchmark.

## 4.4 Command Templating

The `command` field is a template string with `{key}` placeholders that are filled in during execution.

**Command path resolution:**

- Commands are executed relative to the `data/` directory
- The command path itself can contain placeholders

**Example:**

```json
"command": "builds/{build}/{commit} bench 2015 04 {threads}"
```

For config `commit=abc1234,build=native,threads=1`, the command becomes:

```
builds/native/abc1234 bench 2015 04 1
```

This is relative to the `data/` directory, so the full path is `data/builds/native/abc1234`.

All config keys used in the command template must be present in the config.

---

# 5. Runner Protocol Design

## 5.1 Line format

The benchmark process writes lines to stdout:

```text
META	version=1
SAMPLE	<iters>	<total_ns>	checksum=<hex>
SAMPLE	<iters>	<total_ns>	checksum=<hex>
...
```

Where:

* `iters`: number of iterations in the sample.
* `total_ns`: integer wall-clock nanoseconds for that sample.

Fields on each line are **tab-separated**.

Each line may end in one or more comma-separated percent-encoded key=value pairs. These may contain arbitrary data.
`[a-zA-Z0-9_.~-]` are safe characters and do not need to be encoded. All other characters (including spaces, tabs,
commas, equals signs, and non-ASCII characters) must be percent-encoded (e.g. space as %20) inside KV pairs.

The following keys are defined by the spec:

- For `META` lines:
    - `version`: currently always 1
- For `SAMPLE` lines:
    - `checksum`: optional deterministic checksum of output. Must be present if configured in the benchmark's config.

`META` lines are optional

**Example with no key-value pairs:**

```text
SAMPLE	1	1000
```

**Example with multiple key-value pairs:**

```text
SAMPLE	1000	50000	checksum=abc123,foo=bar,msg=hello%20world
```

## 5.2 In-process timing

Inside the benchmark:

* You perform something like:

```rust
loop {
let iters = iters_per_sample;
let start = now();
for _ in 0..iters {
run_benchmark_iteration();
}
let end = now();
let total_ns = end - start;
println ! ("SAMPLE\t{iters}\t{total_ns}\tchecksum={checksum}");
}
```

## 5.3 Iteration scheduling

The specific iteration scheduling algorithm is left to the benchmark runner implementation.
The runner should vary `iters_per_sample` to provide diverse sample durations, which helps with statistical robustness.

General guidelines:

* Target sample durations in the range of 1-50ms work well for most benchmarks
* Varying iteration counts helps detect timing artifacts
* The collector works with any iteration schedule, including constant iteration counts
* For slow benchmarks (50+ms) using a constant `iters_per_sample=1` is recommended

---

# 6. Process Spawning Considerations

When spawning the child benchmark command process, several practical considerations help to ensure reproducible and
reliable measurements:

## 6.1 Process Lock File

To prevent multiple instances of `aoc-bench` from running benchmarks simultaneously, the system uses a lock file
at `data/.lock`. This is implemented using `std::fs::File::lock` to acquire an exclusive lock.

## 6.2 Working Directory

The child process should be spawned with its working directory set to a **temporary directory**.
The temporary directory should be created fresh for each run and cleaned up afterward.

## 6.3 CPU Affinity (Linux)

The system provides a top-level configuration option for CPU affinity to pin the benchmark process to specific CPU
cores:

**In `data/config.json` (top-level):**

```json
{
  "cpu_affinity": [
    0,
    1
  ],
  "config_keys": {
    ...
  },
  "benchmarks": [
    ...
  ]
}
```

The `cpu_affinity` field is optional and specifies a list of CPU core IDs to pin the process to. If present, the child
process is pinned using a `pre_exec` hook that calls `sched_setaffinity`:

**Rationale**:

* Reduces measurement variance from process migration across cores (particularly with multiple CPUs)
* Particularly important for multithreaded benchmarks where thread placement affects performance
* Can optionally be used to run the benchmark on dedicatecd cores (e.g. `isolcpus` kernel boot parameter)

## 6.4 ASLR (Linux)

On Linux, Address Space Layout Randomization (ASLR) should be disabled for the child process to reduce run-to-run
variance. This is accomplished using a `pre_exec` hook that calls `personality(ADDR_NO_RANDOMIZE)`:

**Rationale**: ASLR randomizes the location of code and data in memory, which can affect cache behavior and branch
prediction. While the median-of-means approach in run series helps mitigate this, disabling ASLR provides more
consistent measurements within individual runs.

---

# 7. Statistics Engine

This is the core logic that turns a stream of samples into "mean time per iteration + CI".

## 7.1 Data representation

For each sample batch `i`:

* `N_i` = iteration count for batch `i`
* `T_i` = total nanoseconds for batch `i`

We accumulate:

```text
N = [N_1, N_2, ..., N_n]
T = [T_1, T_2, ..., T_n]
```

## 7.2 Warmup and sampling

Constants:

```rust
const WARMUP_SAMPLES: usize = 16;
const MIN_SAMPLES: usize = 32;
const CHECK_EVERY: usize = 32;
const RUN_SERIES_COUNT: usize = 7;  // Number of runs in a series
```

Process:

* **Warmup**: ignore the first `WARMUP_SAMPLES` samples

* After warmup, for each new sample:
    * Append `(N_i, T_i)` to accumulated data
    * When we have at least `MIN_SAMPLES`, check CI every `CHECK_EVERY` samples to determine if we can stop

* **Run series**: Repeat the entire process `RUN_SERIES_COUNT` times (default 7) to collect multiple independent runs

  For each run a new instance of the benchmark command is run, and the process is repeated (including warmup).

## 7.3 Mode decision: regression vs per-iter mean

We choose between two estimation approaches based on iteration count variation:

**Mode detection heuristic:**

```text
distinct_N = number of distinct N_i values
range_N    = max(N_i) / min(N_i)
cv_N       = stdev(N_i) / mean(N_i)

if distinct_N >= 3 and (range_N >= 2.0 or cv_N >= 0.15):
    mode = "regression"
else:
    mode = "per_iter"
```

### Regression mode

When iteration counts vary significantly, use **weighted least squares** on `T = α + β·N` with weights `w_i = 1/N_i`:

**Rationale**: Per-iteration timing noise is approximately constant, so total noise for a batch of N iterations is
proportional to N. Using weights `w = 1/N` accounts for this heteroscedasticity.

1. Compute weighted least squares fit:
   ```text
   w_i = 1/N_i
   W = Σw_i
   N̄ = (Σw_i·N_i) / W
   T̄ = (Σw_i·T_i) / W
   β = Σw_i·(N_i - N̄)·(T_i - T̄) / Σw_i·(N_i - N̄)²
   α = T̄ - β·N̄
   ```

2. The slope `β` is the estimated nanoseconds per iteration
3. The intercept `α` represents fixed overhead per batch

### Per-iter mean mode

When iteration counts are similar, use **weighted mean** with weights proportional to `N_i`:

**Rationale**: Since timer noise per batch is approximately constant (independent of N), batches with more iterations
provide more information about the per-iteration time. The global weighted mean `Σ(T_i) / Σ(N_i)` is optimal for this
noise model.

1. Compute the weighted mean:
   ```text
   μ = Σ(T_i) / Σ(N_i)
   ```

   This is equivalent to a weighted average of per-iteration times `t_i = T_i/N_i` with weights `w_i = N_i`.

This is simple, robust, and gives more weight to samples with larger iteration counts that have proportionally less
timer noise.

## 7.4 Point estimation and bootstrap confidence intervals

We compute the point estimate and confidence interval **separately** to avoid bias from bootstrap variance.

Constants:

```rust
const QUICK_BOOTSTRAP_SAMPLES: usize = 1000;   // during sampling for quick CI check
const FINAL_BOOTSTRAP_SAMPLES: usize = 10000;  // final estimate after stopping
```

### Point estimate (no bootstrap)

**Regression mode**: Use WLS slope `β` from the full dataset
**Per-iter mode**: Use weighted mean `μ = Σ(T_i) / Σ(N_i)` from the full dataset

### Bootstrap confidence intervals

Algorithm (depends on mode):

#### For regression mode:

1. During sampling (every `CHECK_EVERY` samples):
    * Perform `QUICK_BOOTSTRAP_SAMPLES` resamples of sample indices with replacement
    * For each resample: recompute WLS slope `β` using the resampled data
    * Sort the bootstrap estimates and take 2.5th & 97.5th percentiles → `[β_lo, β_hi]`
    * Compute `relative_half_width = (β_hi - β_lo) / (2 * β_point_estimate)`

2. After stopping:
    * Recompute CI with `FINAL_BOOTSTRAP_SAMPLES` for higher precision

#### For per-iter mean mode:

1. During sampling (every `CHECK_EVERY` samples):
    * Perform `QUICK_BOOTSTRAP_SAMPLES` resamples of sample indices with replacement
    * For each resample: compute weighted mean `Σ(T_i) / Σ(N_i)` using the resampled data
    * Sort the bootstrap estimates and take 2.5th & 97.5th percentiles → `[μ_lo, μ_hi]`
    * Compute `relative_half_width = (μ_hi - μ_lo) / (2 * μ_point_estimate)`

2. After stopping:
    * Recompute CI with `FINAL_BOOTSTRAP_SAMPLES` for higher precision

### Stop condition (both modes):

   ```text
   stop if relative_half_width <= TARGET_REL_CI (default: 0.01 = 1%)
   or n_samples >= MAX_SAMPLES (default: 2048)
   or run_time >= TIMEOUT (default: 120s)
   ```

## 7.5 Outlier detection and noisy system warnings

To detect unstable measurement environments (thermal throttling, background load, etc.), we use **IQR-based outlier
detection** on the residuals.

Constants:

```rust
const OUTLIER_IQR_FACTOR: f64 = 3.0;         // IQR multiplier for outlier bounds
const OUTLIER_MAX_FRACTION: f64 = 0.05;      // abort if >5% of samples are outliers
```

Algorithm:

1. Compute residuals based on the mode:
    * **Regression mode**: `residual_i = T_i - (α + β·N_i)` (deviation from fitted line)
    * **Per-iter mode**: `residual_i = T_i / N_i` (per-iteration time)

2. Compute IQR bounds:
   ```text
   Q1 = 25th percentile of residuals
   Q3 = 75th percentile of residuals
   IQR = Q3 - Q1
   lower_bound = Q1 - k·IQR  (where k = OUTLIER_IQR_FACTOR)
   upper_bound = Q3 + k·IQR
   ```

3. Count outliers: samples where `residual_i < lower_bound` or `residual_i > upper_bound`

4. Compute outlier fraction: `outlier_count / total_samples`

5. If `outlier_fraction > OUTLIER_MAX_FRACTION` during stopping checks, abort the benchmark run and do not store results
   (system is too noisy for reliable measurements)

**Rationale**: A high outlier fraction indicates systematic measurement instability (not just statistical noise).
The benchmark should be re-run in a quieter environment rather than producing unreliable results with wide confidence
intervals.

The outlier detection runs during the stopping check (every `CHECK_EVERY` samples after `MIN_SAMPLES`), alongside the CI
width check, allowing early termination (as noisy systems take far longer to converge).

## 7.6 Output fields (per individual run)

Per run within a series, stats engine produces:

* `timestamp`: when this individual run was performed
* `mode`: `"regression"` or `"per_iter"`
* `mean_ns_per_iter`: estimated time per iteration (β for regression, μ for per-iter mean)
* `ci95_half_width_ns`: half-width of 95% CI (display as mean ± half_width)
* `intercept_ns`: fixed overhead per batch (only for regression mode, null otherwise)
* `outlier_count`: number of samples flagged as outliers
* `samples`: array of `{"iters": N_i, "total_ns": T_i}` for each sample

## 7.7 Output fields (per run series)

Per run series (collection of runs), the system produces:

* `bench`: benchmark name
* `config`: configuration JSON
* `timestamp`: when the run series was performed (start time)
* `runs`: array of individual run results (sorted by mean)
* `median_mean_ns_per_iter`: mean from the median run (representative value)
* `median_ci95_half_width_ns`: CI from the median run
* `checksum`: output validation

The median run's estimates become the **representative values** for:

* stable result determination
* commit-to-commit comparison
* UI display

Raw run and samples are stored to allow later re-analysis if required.

**Note**: The outlier statistics are computed after collection completes and stored for diagnostic purposes. During
collection, if the outlier fraction exceeds `OUTLIER_MAX_FRACTION`, the benchmark is aborted and no results are stored.

---

# 8. Run Series Summary and Storage Format

Each benchmark run series produces:

1. **One immutable JSON file** (source of truth)
2. **Database updates** (for fast queries)

## 8.1 Run Series JSON format

Each timestamped run series file (e.g., `2025-11-12T18-53-21.json`) contains:

```json
{
  "schema": 1,
  "bench": "2015-04",
  "config": {
    "commit": "abc1234",
    "host": "pi5",
    "profile": "release",
    "threads": "1"
  },
  "timestamp": 1731437601,
  "runs": [
    {
      "timestamp": 1731437602,
      "mean_ns_per_iter": 30920000,
      "ci95_half_width_ns": 31000,
      "mode": "per_iter",
      "intercept_ns": null,
      "outlier_count": 0,
      "samples": [
        {
          "iters": 10000000,
          "total_ns": 30920000000
        },
        ...
      ]
    },
    ...
    // 7 runs total, sorted by mean_ns_per_iter
  ],
  "median_mean_ns_per_iter": 30930000,
  "median_ci95_half_width_ns": 30000,
  "checksum": "8f024a8e..."
}
```

**Fields:**

* `schema`: format version (currently 1)
* `bench`: benchmark name/identifier (what to run)
* `config`: JSON object with configuration dimensions (commit, host, profile, threads, etc.) - how to run it
* `timestamp`: when this run series was performed (unix timestamp, seconds since epoch, start time)
* `runs`: array of individual run results, **sorted by mean_ns_per_iter**
    * Each run contains: `timestamp`, `mean_ns_per_iter`, `ci95_half_width_ns`, `mode`, `intercept_ns`
* `median_mean_ns_per_iter`: mean from the median run (representative value, integer nanoseconds)
* `median_ci95_half_width_ns`: CI from the median run (integer nanoseconds)
* `checksum`: output correctness validation

**Display format:**

The result is displayed as:

```
106.3 µs/iter (±0.4%, 95% CI, median of 7 run means)
```

or more compact:

```
106.3 µs/iter ±0.4%  (median of 7 run means)
```

**These JSON files are immutable** - once written, they are never modified. They are the authoritative source of truth.

---

# 9. Storage Architecture

## 9.1 Dual Storage Model

The system uses a **dual storage approach**:

1. **Immutable JSON files** (source of truth): All raw run series data
2. **SQLite database** (fast queries): Metadata and stable results

```text
data/
  config.json                       # Benchmark configuration (required at this location)
  inputs/                           # Input files for benchmarks
    2015-04.txt                     # Referenced by benchmark "input" field
    2015-05.txt
    ...
  builds/                           # Example: compiled benchmark binaries (user-managed)
    native/
      abc1234                       # Executables referenced by commands
      def5678
    generic/
      abc1234
  results/                          # Benchmark results (auto-created by tool)
    pi5/                            # One directory per host
      metadata.db                   # SQLite database for this host
      runs/                         # Immutable JSON files
        2015-04/                    # Benchmark name (top-level organization)
          commit=abc1234,profile=release,threads=1/
            2025-11-12T18-53-21.json  # Timestamped run series files
            2025-11-12T19-05-33.json
          commit=abc1234,profile=release,threads=32/
            2025-11-13T09-22-10.json
        2015-05/
          commit=def5678,profile=debug,threads=1/
            2025-11-13T10-15-00.json
```

**Path encoding rules:**

* Benchmark directory: The `bench` value becomes a top-level directory under `runs/`
    - Example: `{"bench":"2015-04", ...}` → `runs/2015-04/`
    - Organizes samples by benchmark first, making it easy to find all configs for a given benchmark
    - Character constraints (documented in section 2) ensure filesystem safety
* Config subdirectory: Canonical config string in `key=value,key=value` format (without the `bench` key and `host` key)
    - Keys are sorted alphabetically (same as canonical JSON ordering)
    - Format: `key1=value1,key2=value2,...` where keys and values follow the character constraints
    - The `bench` key is excluded from this string since it's already represented by the parent directory
    - The `host` key is excluded from this string since it's represented by the top-level host directory
    - Example: `{"bench":"2015-04","commit":"abc1234","host":"silicon","profile":"release","threads":"1"}` →
      `runs/2015-04/commit=abc1234,profile=release,threads=1/`
    - Human-readable: can grep/glob for specific configs with shell patterns
    - Character constraints (documented in section 2) ensure filesystem safety across all platforms

* Timestamps in filenames: ISO 8601 compact format with hyphens (e.g., `2025-11-12T18-53-21`) for human readability
* Timestamps in database/JSON: Unix timestamps (64-bit integer seconds since epoch) for efficiency

**Host-level organization:**

* Each host has its own directory under `data/results/` (e.g., `data/results/silicon/`)
* The `host` key is still part of the config JSON for consistency
* Physical separation by host enables independent environment tracking per machine

## 9.2 SQLite Schema

Each host has its own `metadata.db` with two core tables:

**`run_series`**: One row per run series (collection of runs)

- Primary key: `(bench, config, timestamp)` where bench is the benchmark name and config is the JSON (excluding bench)
- Stores: bench, config (JSON), timestamp, mean_ns_per_iter, ci95_half_width_ns
    - All statistical values are from the **median run** of the series
- Immutable: rows are never updated after insertion
- Links to: JSON file at `runs/{bench}/{config_string}/{timestamp}.json` where config_string is the canonical
  key=value format (excluding `bench` and `host`)
- `WITHOUT ROWID` table for efficiency

**`results`**: One row per (bench, config) pair

- Primary key: `(bench, config)`
- Stores: which run series is the stable result, which run series is the most recent
- Fields: bench, config, stable_series_timestamp, last_series_timestamp, suspicious_series_count
- Mutable: updated as new run series arrive and drift is detected
- Generated virtual columns: `commit`, `host` (extracted from config JSON for fast filtering)
- Partial indexes on generated columns for fast filtering

**Commit ordering**: Commit relationships and ordering are defined in the configuration file (section 4), not in the
database. The config file's `config_keys.commit.values` array defines the canonical commit order, which can be used for
timeline views and commit-based queries

See `schema.sql` for complete schema definition with indexes and constraints.

**Critical rule: Always write JSON before updating the database.**

**Why this ordering:**

- JSON files are the source of truth
- If DB write fails, JSON exists and can be re-indexed
- If process crashes after JSON write but before DB commit, recovery can scan for unindexed JSONs
- Never have DB entries pointing to non-existent JSON files

**SQLite transaction semantics:**

- Use `BEGIN IMMEDIATE` to acquire write lock upfront
- Set `journal_mode=WAL` for better concurrency (multiple readers during write)
- Set `synchronous=NORMAL` for durability with good performance
- Each run storage is one transaction (atomic all-or-nothing)

## 9.3 Stable result update detection

When a new run series arrives for a `config`:

1. Query the `results` table for this config (exact match on canonical JSON).

2. If no row exists (first run series for this config):
    * This run series becomes both the stable and last run series
    * Insert into `run_series` table with median run values
    * Insert into `results` table with `config`, `stable_series_timestamp = last_series_timestamp = timestamp`
    * Done

3. Otherwise, load the stable result from the `results` table and compare using **median run values**:

   ```text
   μ_stable = stable_run.mean_ns_per_iter  (median mean from stable series)
   h_stable = stable_run.ci95_half_width_ns  (CI from median run of stable series)
   μ_new = new_series.median_mean_ns_per_iter  (median mean from new series)
   h_new = new_series.median_ci95_half_width_ns  (CI from median run of new series)

   CI_stable = [μ_stable - h_stable, μ_stable + h_stable]
   CI_new = [μ_new - h_new, μ_new + h_new]

   overlap = not (CI_stable[1] < CI_new[0] or CI_new[1] < CI_stable[0])
   rel_diff = |μ_new - μ_stable| / μ_stable
   ```

4. Check if this run series is suspicious:

   ```text
   const STABLE_RESULT_CHANGE_REL_THRESHOLD = 0.03  // 3% difference
   const STABLE_RESULT_CHANGE_REQUIRED_COUNT = 3    // consecutive suspicious run series

   is_suspicious = (not overlap) and (rel_diff >= STABLE_RESULT_CHANGE_REL_THRESHOLD)
   ```

5. Update suspicious series counter:

    * If `is_suspicious`:
        * `suspicious_series_count += 1`
    * Else:
        * `suspicious_series_count = 0`

6. If `suspicious_series_count >= STABLE_RESULT_CHANGE_REQUIRED_COUNT`:

   **Replace the stable result** (environment has changed):

    * Insert new run series into `run_series` table (using median run values)
    * Update `results` table: set `stable_series_timestamp = timestamp`, `last_series_timestamp = timestamp`, reset
      `suspicious_series_count = 0`

7. Otherwise (stable result unchanged):

    * Insert new run series into `run_series` table (using median run values)
    * Update `results` table: set `last_series_timestamp = timestamp`, update `suspicious_series_count`
    * Stable result remains unchanged

**Key insights:**

- Each unique (benchmark, config) tracks its own stable result independently
- All run series are kept for historical analysis (full run series JSON files on disk)
- The "current" result shown to users is **always** the stable run series (specifically, the median run from it)
- The **median run** from each series is used as the representative value for drift detection
- Re-running within noise doesn't change the displayed result (no microscopic drift)
- Users can override with `--force-update-stable` if needed
- One *series* counts as one "run" in the suspicious result counting logic

## 9.4 Database Recovery

The database can always be rebuilt from run series JSON files and the config file if corrupted.

On startup, if the database is corrupted or missing, the system will attempt to rebuild it from the JSON files.

**Algorithm:**

1. Delete or backup corrupted `metadata.db`
2. Create new database with schema from `schema.sql`
3. Scan `runs/` directory recursively for all JSON files
4. For each JSON file:
    * Parse and validate (check for `schema` version 1)
    * Extract the median run values
    * Insert into `run_series` table
5. For each unique config (exact canonical JSON match):
    * Find all run series sorted by timestamp
    * Use latest run series as both stable and last run series
    * Insert into `results` table with `suspicious_series_count = 0`

**What is lost in recovery:**

- Suspicious series counts (partial state toward next stable update)
- Original stable result designations (assumes latest = stable)

Note: Full history is preserved in the timestamped JSON files themselves.

**What is preserved:**

- All raw run series data (from JSON files, including all individual runs)
- All statistics and measurements (median values and individual run values)
- Complete temporal history

---

# 10. Error Handling

The system must handle errors gracefully and fail safely. Here are the specified behaviors:

## 10.1 Protocol errors

* **Malformed SAMPLE line**: Log warning, skip sample, continue.
* **Invalid numeric values**: Log warning, skip sample, continue.
* **Checksum mismatch with config**: If the benchmark config specifies a checksum and a `SAMPLE` line does not contain
  a checksum or contains a different checksum, fail immediately and do not store results.
* **Missing required META fields**: Warn but continue (use CLI arguments).

## 10.2 Process errors

* **Child process crashes**: Fail the run, do not store partial results.
* **Child process hangs**: After `TIMEOUT` (default 120s), kill process and fail, do not store the partial results.
* **Premature EOF**: Fail the run, do not store partial results.

## 10.3 Storage errors

* **Disk full / write error**: Fail loudly, do not corrupt existing data.
* **Missing directories**: Create automatically (with proper error handling).
* **Corrupted database**: Fail loudly, do not corrupt existing data. Database can be rebuilt manually.
* **Corrupted run JSON**: Fail loudly, skip that file during rebuild.
* **Database locked**: SQLite will automatically wait up to `busy_timeout` (default 30s), then fail with SQLITE_BUSY.

# 11. Schedulers and Workflows

## 11.1 Per-commit benchmarking

Typical workflow (for AoC):

1. You commit new code.

2. You build your AoC benchmark runner and regenerate the benchmark config.

3. You run:

   ```bash
   aoc-bench run --benchmark 2015-04 --config commit=abc123,threads=1
   ```

   The CLI accepts either:
    - `--config key=value,key2=value2` (comma-separated key=value pairs, converted to JSON)
    - `--config-json '{...}'` (explicit JSON string)

The `aoc-bench run` command:

- Reads the benchmark definition from the configuration file
- Finds the `command` template for the specified benchmark
- For each matching configuration (the `--config` options allow for partial matches):
    - Substitutes `{key}` placeholders in the command with values from the config (config values are validated to match
      `[a-zA-Z0-9_-]+` ensuring safe substitution without escaping)
    - Spawns the resulting command as a child process
    - Reads SAMPLE lines from the child process's stdout
    - Applies the statistics engine to produce run statistics
    - Repeats multiple times to produce a run series, and stores results
    - If the command/run errors, it is automatically retried (up to 5 retries across the run series)

## 11.2 Periodic runs (to detect env drift per config)

Each host can periodically re-run **a subset of configs** (e.g. nightly) using the `aoc-bench sample` command, which:

* Chooses some (bench, config) pairs to re-run. These are selected from pairs without existing results, and the results
  table entries with the oldest `last_series_timestamp`.
* Runs them similarly to the `run` command (see above).
* Stable result logic will automatically update if the environment has changed enough for that config.
* The median-of-means approach makes these drift checks more robust to transient noise.

Over time, you can detect environment changes by seeing when stable results update across multiple configs.

---

# 12. Multi-host and Multi-config

## 12.1 Hosts

Each host runs the same system independently:

* `--host` argument (or environment) identifies the host.
* Data is stored under `data/results/<host>/metadata.db` and `data/results/<host>/runs/...`.
* Each host has completely independent database - no cross-host locking or coordination needed.

Viewer can aggregate per host or just show the host you care about.

## 12.2 Configurations

Benchmark configurations use a key-value system:

* Configs are specified as comma-separated key=value pairs, e.g., `profile=release,threads=1`,
  `profile=dev,threads=32,simd=avx2`
* Keys and values are alphanumeric with underscores only
* Configs must be canonically sorted by key (alphabetically) for consistent storage and comparison
* Different configurations have their own independent stable results for each commit
* All stored in the same database, distinguished by the `config` column
* Partial matching is supported: you can query with a subset of keys (e.g., `profile=release`) to compare all configs
  that match those key-value pairs

---

# 13. Viewing and Analysis

## 13.1 Basic queries

**CLI tool for common queries:**

```bash
# Export all stable results for a host
aoc-bench export --host silicon --format tsv > results.tsv

# Query specific result (using partial config matching)
aoc-bench export --host silicon --config bench=2015-04,commit=abc1234,profile=release,threads=1
```

**Config filtering:**

- All `--config` arguments use the `key=value,key2=value2` format
- Filtering is done via partial matching: any config whose JSON contains all specified key-value pairs will match
- For exact matching, specify all keys present in the config
- The `--host` flag is a shorthand for `--config host=<value>`

## 13.2 Export for visualization

The `export` command outputs TSV suitable for plotting:

```text
host    bench   cfg_commit  cfg_profile cfg_threads     stable_timestamp  mean_ns_per_iter  ci95_half_width_ns
abc1234 2015-04 abc1234     release     1               1731437601        30930000          30000
...
```

**Column extraction:**

The config JSON is always extracted into a column per key. Keys other than `host` are prefixed with `cfg_`.

**Note:** All exported values are median run values from run series. For full run-level details (all 7 runs),
access the JSON files directly in the `runs/` directory.

Use with gnuplot, matplotlib, R, or any other plotting tool.

## 13.3 Performance change views

Two key views for tracking performance changes:

### 13.3.1 Timeline view (per benchmark)

Shows history for a specific benchmark with performance changes highlighted:

```bash
aoc-bench timeline --config host=silicon,profile=release,threads=1 2015-04
```

```text
Bench: 2015-04
Config: {"bench":"2015-04","host":"silicon","profile":"release","threads":"1"}

commit    date         mean_ns      CI        delta
abc1234   2025-11-13   30.93ms    ±0.03ms     --         (baseline)
ghi7890   2025-11-11   32.10ms    ±0.04ms   +3.78%      REGRESSION
mno3456   2025-11-09   28.50ms    ±0.03ms  -11.23%      IMPROVEMENT

(2 entries with insignificant changes omitted)
```

**Process:**

- Finds all benchmark configs matching the specified benchmark name and `--config` argument
- Identifies the comparison key: the key that varies across matched configs (usually `commit`)
- Checks that exactly one key varies. If multiple keys vary or no keys vary, error with helpful message
- Checks that all matched configs have the same set of keys (no optional keys that appear in some configs but not
  others)
- Uses the `results` table to find matching stable results
- Sorts the stable results by the comparison key's value order from the config file
- Compares the ordered stable results to determine if there was a regression, improvement, or no significant change
- Outputs the initial result and any significant changes

### 13.3.2 Impact view

Shows which benchmarks changed meaningfully under a specified config key change:

```bash
aoc-bench impact --config profile=release commit=abc1234
```

**Output format:**

```text
Commit: abc1234
Filter: profile=release

REGRESSIONS:
  2015-04 [threads=1]     30.93ms → 32.10ms  +3.7%

IMPROVEMENTS:
  2015-04 [threads=32]    5.20ms → 4.15ms    -20%
  2015-06 [threads=1]     28.50ms → 25.10ms  -11.93%

17 configs unchanged
```

**Process:**

- Finds all results with `commit=abc1234` (and optionally filtered by a partial config)
- Finds the previous config value for the specified key (e.g. `commit=abc1234` -> `commit=def4567`)
- Finds all results with `commit=def4567` (and optionally filtered by a partial config)
- For each pair of results with the same config (ignoring the comparison key), determines if there was a regression,
  improvement, or no significant change
- Outputs a summary of the regressions and improvements

## 13.4 Web UI (future)

A static HTML page can query the database (read-only) or load exported JSON:

- Time series plots per benchmark
- Commit-to-commit comparisons
- Stable result change timeline
- Cross-config comparisons

---

# 14. Extensibility / Future Ideas

These are *not* required now, but the design leaves room for them:

* Add **inner and outer CIs** (per-run noise vs across-runs variability). The run series JSON already contains all
  individual runs, making this analysis possible.
* **Variance analysis across runs**: Use the stored individual runs in each series to compute cross-run variance
  metrics.
* Use **daily roll-ups** for heavy usage (materialized views in database).
* Add a small **web UI** that reads the database and draws time-series.
* **Multi-host aggregation**: Collect databases from multiple hosts for cross-machine comparison.
* **Automatic re-run scheduling**: Cron job that re-runs old commits to detect drift.

---

# 15. Summary

This design gives you:

* A **generic, language-agnostic protocol** (`SAMPLE iters total_ns checksum=...`) for in-process benchmarking.
* A **stats engine** that:
    * auto-chooses regression vs per-iter mean,
    * uses bootstrap CIs,
    * stops when the measurement is stable.
* A **run series approach** that:
    * Performs 7 runs back-to-back for each benchmark execution
    * Uses median-of-means to eliminate run-to-run variance (ASLR, cache state, etc.)
    * Stores all runs for later analysis and variance detection
* A **dual storage architecture**:
    * Immutable JSON files (source of truth, all raw data including all runs in each series)
    * SQLite database per host (fast queries, stable result tracking using median of means)
    * JSON-first writes ensure recoverability
* A **per-config stable result system** to handle **environment changes** without environment hashing:
    * Each unique config tracks its own stable result independently.
    * If new run series are "too different" (3 consecutive, non-overlapping CIs + % threshold), update the stable
      result.
    * All run series are kept for historical analysis.
    * Database corruption is recoverable by rebuilding from JSON files.
* **Simple querying**: SQL for ad-hoc queries, CLI tools for common operations, easy export for visualization.
