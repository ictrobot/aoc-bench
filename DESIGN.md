# 1. Scope and Requirements

You want a system that:

* Benchmarks arbitrary commands with flexible **configuration dimensions**.
* Uses **in-process benchmarking** with a simple text protocol emitted by benchmark processes.
* Can run for **months/years**, but:
    * You're happy to **replace results** when the environment clearly changed.
    * Old runs are immutable and never removed or modified.
* Keeps storage **self-contained**, **file/JSON based**, with SQLite DB for faster queries.

Key ideas:

* A **runner protocol** (`SAMPLE <iters> <total_ns> ...`) that any benchmark implementation can emit.
* A **stats engine** that turns those samples into "mean time per iteration ± CI".
* A **storage model** that separates logical cases, reusable workloads, and immutable measurements.
  Stable/drift state belongs to the workload that was actually measured.

## CLI Commands

The `aoc-bench` tool provides the following subcommands:

* **`run-all`** - Execute every matching benchmark group
    * Reads benchmark definitions and command templates from config file
    * Substitutes `{key}` placeholders with config values
    * Groups shared cases that currently resolve to the same executable and input
    * Runs each selected workload and collects SAMPLE output
    * Performs run series and stores results

* **`run`** - Periodically process new groups and re-run measured workloads
    * Applies separate limits to new groups and oldest rerun candidates
    * New groups may inherit an exact measured workload without re-running the workload
    * Useful for nightly/periodic runs to detect environment changes

* **`export`** - Query and export benchmark results
    * Outputs TSV suitable for plotting
    * Supports partial config matching for filtering
    * Extracts config keys into separate columns (with `cfg_` prefix)

* **`export-web`** - Export benchmark data as JSON for the web interface
    * Writes snapshot-based JSON data and updates an index file atomically
    * Intended for static web UI consumption

* **`timeline`** - Show performance history across one config dimension
    * Displays ordered results with performance change highlights
    * Automatically detects comparison key (unspecified dimension)
    * Shows regressions and improvements with delta percentages

* **`fastest`** - List fastest config per benchmark
    * Respects benchmark/config/host filters
    * Prints benchmark name, fastest config (minus shared filters), and mean time
    * Includes a total row summing mean times across the listed benchmarks

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

Two storage versions may coexist. **V1** is the original case-oriented format: one run-series JSON
per `(benchmark, config, timestamp)` plus `run_series` and `results` metadata tables. **V2** is the
current workload-oriented format: UUID-addressed measurement JSON plus the normalized schema in
section 9. These labels describe stored measurements, not the runner protocol version or SQL
migration number. Appendix A summarizes V1 and its migration.

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
    - **`results/{host}/runs/by-measurement/`**: Sharded immutable V2 measurement JSON
    - Existing V1 run-series files remain in their reconstructible paths after migration
* **`data/hosts/{host}.json`** (optional): Host-specific runner settings
    - `cpu_affinity`: cpuset string (e.g., `"0-3,6"`) or omitted/`null` for all CPUs
    - `disable_aslr`: bool, defaults to `true` on Linux to reduce noise

# 2. Concepts and Terminology

* **Benchmark (bench)**
  A named task/workload to measure, e.g. `"2015-04"` or `"compile-aoc-rs"`.
  The benchmark identifies **what** to run. It is the primary organizing dimension for storage and visualization.

* **Configuration (config)**
  A JSON object containing key-value pairs that specify **how** to run a benchmark, e.g.:
  ```json
  {"commit":"abc1234","host":"pi5","profile":"release","threads":"1"}
  ```

  The config contains variable dimensions (commit, build settings, etc.) but **not** the benchmark
  name. `host` is synthesized for queries; durable case identity uses the hostless config because
  each database already belongs to one host. Together, `(benchmark, hostless config)` identifies a
  logical case within a host database.

  **Character constraints**: To ensure filesystem safety and portability:
    - Keys: Must match regex `[a-z][a-z0-9_]+` (lowercase alphanumeric + underscores, must start with letter)
    - Values and benchmark names: Must match regex `[a-zA-Z0-9_-]+` (alphanumeric + underscores + hyphens)
    - Length limits: keys ≤ 64 bytes; values/benchmark names ≤ 128 bytes
    - These constraints keep generated labels safe in command and path contexts

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
  The repeated runs collected during one measurement (normally **3 runs**). Their median run is the
  representative value used for stable-result and comparison logic.

* **Case**
  A logical `(benchmark, canonical hostless config)` pair. A case stores the workload whose stable
  result it currently displays. Several cases may point to one workload.

* **Group**
  A process-local scheduling unit. A shared group contains cases with the same benchmark, invocation
  specification, and current executable/input inodes. An isolated benchmark contributes one group
  per case. Inode observations are never persisted.

* **Workload**
  The durable identity of measured work. A shared workload hashes the benchmark, executable content,
  optional stdin content, expanded arguments, checksum, statistics options, and runner semantics. An
  isolated workload is case-specific and has no content hashes. Stable/drift state belongs here.

* **Measurement**
  One recorded workload run series, identified by UUIDv7, containing its runs and raw samples. It
  owns immutable V2 JSON and may be visible in the histories of several cases.

* **Stable Result**
  The workload measurement currently shown to every associated case. Measurements within noise leave
  it unchanged; three consecutive significant changes replace it. Different workload identities have
  independent stable/drift state.

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
    * Performs **multiple runs back-to-back** to form a run series.
    * Produces one measurement containing the complete run series.

3. **Store / Stable Result Manager**

    * Interns cases and workload identities.
    * Stores immutable measurements and case-visible history.
    * Maintains stable/last measurement pointers and drift counters per workload.

4. **Viewer / Exporter**

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
      "annotations": {
        "def5678": "Rust 1.85.0"
      },
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
  "timeline_key": "commit",
  "benchmarks": [
    {
      "benchmark": "2015-04",
      "dedupe": "inode-content",
      "command": [
        "builds/{build}/{commit}",
        "bench",
        "2015",
        "04",
        "{threads}"
      ],
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
- **`annotations`** *(optional)*: Map of value labels used to show markers/tooltips for specific
  values (e.g. compiler-version transitions on commit values)
- **`presets`** *(optional)*: Named groups of values for convenient reuse
    - Preset names can be any string (e.g., `"all"`, `"from_abc1234"`, `"power_of_2"`)
    - Preset values must be subsets of `values`

**Canonical ordering**: The `values` array defines the canonical order for this key. When configs are serialized to
strings or stored, values are sorted according to this order.

**Key names**: Must match regex `[a-z][a-z0-9_]+` (lowercase, start with letter).

**All keys are optional**: A benchmark config can omit any key. Only specified keys are included in the final config.

**Disallowed keys**: The following keys are reserved to avoid confusion: `bench`, `benchmark`, `host`, `timestamp`.
The system synthesizes the `host` key automatically, so user configs must never define it manually.

## 4.2.1 Top-level optional fields

- **`timeline_key`** *(optional)*: Selects the default dimension used for timeline ordering in exports/UI.
  Must reference a key defined in `config_keys`.
  If omitted, the system defaults to `commit` when present.

## 4.3 Benchmarks Section

**`benchmarks`**: Array of benchmark definitions. Each entry describes either a single benchmark variant or a
collection of mutually-exclusive variants that share an identifier.

Common fields:

- **`benchmark`**: Benchmark name/identifier (string)
- **`dedupe`** *(optional)*: Selects the benchmark's dedupe mode. Currently the only explicit mode
  is `"inode-content"`; omit the field for an isolated benchmark. See **Dedupe modes** below.
- **`command`**: Required command template expressed *as an argv array*, e.g.
  `"command": ["builds/{build}/{commit}", "bench", "2024", "04", "{threads}"]`. The first element is the
  executable relative to `data/`, the rest are arguments. Every `{key}` placeholder must match a key defined in the
  variant’s config.
- **`input`** *(optional)*: Input filename in `data/inputs/`. If provided the runner streams the file to stdin.
- **`checksum`** *(optional)*: Expected checksum emitted by the benchmark.
- **`stats`** *(optional)*: Overrides for stats defaults. Supported fields: `min_samples`, `min_time_ns`,
  `target_rel_ci`, `min_warmup_samples`, `min_warmup_time_ns`, `runs_per_series`, and `run_timeout_ns`.
  When variants are present, each variant may have its own `stats` block; for each field the lookup order is variant →
  benchmark → system default.

### Dedupe modes

#### Isolated (`dedupe` omitted)

Each case is scheduled, executed, and stored independently. Executable and stdin contents are not
hashed for workload sharing, so this mode also supports programs whose behaviour depends on paths or
other state outside the declared invocation.

#### `inode-content`

This mode has two layers of identity:

* Before applying scheduling limits, current cases are grouped cheaply by benchmark, invocation
  spec, and executable/stdin file identity (device and inode).
* After a group is selected, its artifacts are content-hashed. The durable workload identity uses
  those hashes together with the benchmark and complete invocation spec, allowing measurements to
  be inherited when the same workload identity reappears.

Cases with identical executable and input contents must refer to the same underlying files, normally
by linking paths into a content-addressed artifact store. If byte-identical copies use different
inodes, the scheduler treats them as separate groups: they may consume multiple slots, run the same
workload more than once in one command, and update its drift state more than once. Content reuse
still works when old workload bytes reappear later on a new inode; this requirement applies only to
cases configured at the same time.

### Single-variant entries

Structure:

```json
{
  "benchmark": "2024-04",
  "dedupe": "inode-content",
  "command": [
    "builds/{build}/{commit}",
    "bench",
    "2024",
    "04",
    "{threads}"
  ],
  "input": "2015-04.txt",
  "checksum": "deadbeef",
  "config": {
    "build": [
      "generic",
      "native"
    ],
    "commit": [
      "abc1234",
      "def5678"
    ]
  }
}
```

Rules:

- `command` and `config` are required; `variants` must be omitted.
- The config specification (see below) expands to a Cartesian product of concrete configs.

### Multi-variant entries

Structure:

```json
{
  "benchmark": "solver",
  "command": [
    "builds/{build}/{commit}",
    "bench"
  ],
  "input": "shared-input.bin",
  "checksum": "abcd1234",
  "variants": [
    {
      "command": [
        "builds/{build}/{commit}",
        "bench",
        "{threads}"
      ],
      "config": {
        "build": [
          "native"
        ],
        "commit": [
          "abc1234",
          "def5678"
        ],
        "threads": [
          "1",
          "8"
        ]
      }
    },
    {
      "command": [
        "builds/{build}/{commit}",
        "bench",
        "{threads}"
      ],
      "config": {
        "build": [
          "generic"
        ],
        "commit": [
          "abc1234",
          "def5678"
        ],
        "threads": [
          "1",
          "8"
        ]
      }
    }
  ]
}
```

Rules:

- The top-level `config` must be omitted and each variant must supply its own `config` map.
- Variant entries may override `command`, `input`, and `checksum`. When omitted, the system uses the top-level fallback.
- Every variant must declare the same config keys; value subsets may differ.
- At least one variant must be present, and their config products must be disjoint (the system rejects overlaps).

### Config specification

- Keys must exist in `config_keys`.
- Values can be:
    - **Preset name** (string): References `config_keys[key].presets[preset]`.
    - **Literal array** (array of strings): Explicit list of values.
- Values must be subsets of the canonical `values` for that key.
- The Cartesian product of all key subsets defines the concrete configs used for scheduling.

Example:

```json
"config": {
"commit": "recent",
"build": ["generic"],
"threads": ["1", "32"]
}
```

Expands to 4 configs:

- `commit=ghi9012,build=generic,threads=1`
- `commit=ghi9012,build=generic,threads=32`
- `commit=jkl3456,build=generic,threads=1`
- `commit=jkl3456,build=generic,threads=32`

## 4.4 Command Templating

Every command argument is a template string with `{key}` placeholders that are filled in during execution. The first
argument becomes the executable path relative to `data/`, and the rest are passed verbatim after placeholder
substitution.

**Command path resolution:**

- Commands are executed relative to the `data/` directory
- The command path itself can contain placeholders

**Example:**

```json
"command": ["builds/{build}/{commit}", "bench", "2015", "04", "{threads}", "{multiversion}"]
```

For config `commit=abc1234,build=native,threads=1`, the command becomes:

```
builds/native/abc1234 bench 2015 04 1 default
```

This is relative to the `data/` directory, so the full path is `data/builds/native/abc1234`.

**Placeholder validation:** Every key referenced in a variant’s config must appear at least once in its command
template.

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

## 6.3 Host-Specific Runtime Options

Runtime noise controls are configured per-host (not in the shared `config.json`) via optional files:
`data/hosts/{host}.json`.

Supported keys:

```json
{
  "cpu_affinity": "0-3,6",
  "disable_aslr": true
}
```

All keys are optional.

## 6.4 CPU Affinity (Linux)

The `cpu_affinity` host config option is optional and specifies a list of CPU numbers to pin the process to.
If present, the child process is pinned using a `pre_exec` hook that calls `sched_setaffinity`:

**Rationale**:

* Reduces measurement variance from process migration across cores (particularly with multiple CPUs)
* Particularly important for multithreaded benchmarks where thread placement affects performance
* Can optionally be used to run the benchmark on dedicatecd cores (e.g. `isolcpus` kernel boot parameter)

## 6.5 ASLR (Linux)

The `disable_aslr` host config option is optional and specifies whether to disable ASLR for the child process to reduce
run-to-run variance.
It defaults to `true` on Linux where it is supported, and is implemented using a `pre_exec` hook that calls
`personality(ADDR_NO_RANDOMIZE)`.

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

Defaults (overrideable per-benchmark/variant via the optional `stats` block):

```rust
const MIN_WARMUP_SAMPLES: usize = 4;
const MIN_WARMUP_TIME_NS: u64 = 1_000_000_000; // 1 second
const MAX_WARMUP_TIME_NS: u64 = 15_000_000_000; // 15 s
const STABILITY_WINDOW: usize = 8;
const STABILITY_TOLERANCE: f64 = 0.05; // ±5%

const MIN_SAMPLES: usize = 32;
const MIN_TOTAL_TIME_NS: u64 = 2_000_000_000; // 2 seconds of measured (post-warmup) samples
const CHECK_EVERY_NS: u64 = 200_000_000; // Re-evaluate convergence every ~200ms of post-warmup sample time
const MAX_SAMPLES: usize = 1024;
const RUN_SERIES_COUNT: usize = 3;  // Default runs_per_series; must stay odd for median selection
const RUN_TIMEOUT_NS: u64 = 600_000_000_000; // Default per-run timeout; override via stats.run_timeout_ns
```

Process:

* **Warmup**: collect samples until both minima are met (≥4 samples and ≥1s total),
  then stop when either (a) the last 8 warmup samples are all within ±5% of their mean,
  or (b) cumulative warmup time reaches 15s. All warmup samples are ignored for stats.

* After warmup, for each new sample:
    * Append `(N_i, T_i)` to accumulated data
    * Once both `MIN_SAMPLES` and `MIN_TOTAL_TIME_NS` are satisfied, recompute convergence every
      `CHECK_EVERY_NS` (200ms) of additional measured sample time, or sooner if we hit `MAX_SAMPLES`
    * Stop when CI target is met or `MAX_SAMPLES` is hit

* **Run series**: Repeat the entire process `RUN_SERIES_COUNT` times (default 3, configurable via
  `stats.runs_per_series`)
  to collect multiple independent runs. The value must be odd so a median run can be selected.

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

1. During sampling (every `CHECK_EVERY_NS` of accumulated sample time):
    * Perform `QUICK_BOOTSTRAP_SAMPLES` resamples of sample indices with replacement
    * For each resample: recompute WLS slope `β` using the resampled data
    * Sort the bootstrap estimates and take 2.5th & 97.5th percentiles → `[β_lo, β_hi]`
    * Compute `relative_half_width = (β_hi - β_lo) / (2 * β_point_estimate)`

2. After stopping:
    * Recompute CI with `FINAL_BOOTSTRAP_SAMPLES` for higher precision

#### For per-iter mean mode:

1. During sampling (every `CHECK_EVERY_NS` of accumulated sample time):
    * Perform `QUICK_BOOTSTRAP_SAMPLES` resamples of sample indices with replacement
    * For each resample: compute weighted mean `Σ(T_i) / Σ(N_i)` using the resampled data
    * Sort the bootstrap estimates and take 2.5th & 97.5th percentiles → `[μ_lo, μ_hi]`
    * Compute `relative_half_width = (μ_hi - μ_lo) / (2 * μ_point_estimate)`

2. After stopping:
    * Recompute CI with `FINAL_BOOTSTRAP_SAMPLES` for higher precision

### Stop condition (both modes):

   ```text
   stop early if relative_half_width <= TARGET_REL_CI (default: 0.01 = 1%)
 otherwise keep sampling until n_samples >= MAX_SAMPLES (default: 1024)
  or run_time >= TIMEOUT (default: 600s, configurable via stats.run_timeout_ns)
   ```

## 7.5 Outlier detection

**MAD-based outlier detection** (Median Absolute Deviation) is used on the residuals.

Constants:

```rust
const OUTLIER_MAD_NORMALIZATION: f64 = 1.482602218505602;  // Consistency constant for normal distribution
const OUTLIER_MAD_THRESHOLD: f64 = 3.5;                     // Modified Z-score threshold
```

Algorithm:

1. Compute residuals based on the mode:
    * **Regression mode**: `residual_i = T_i - (α + β·N_i)` (deviation from fitted line)
    * **Per-iter mode**: `residual_i = T_i / N_i` (per-iteration time)

2. Compute MAD-based modified Z-scores:
   ```text
   median = median(residuals)
   MAD = median(|residual_i - median|)
   mad_scaled = MAD × 1.4826  (normalization for normal distribution)
   modified_z_i = |residual_i - median| / mad_scaled
   ```

3. Count outliers: samples where `modified_z_i > OUTLIER_MAD_THRESHOLD`

4. Compute outlier fraction: `outlier_count / total_samples`

Outlier counts are recorded and displayed for diagnostics.

## 7.6 Temporal correlation detection (drift analysis)

To detect systematic performance drift over the course of sampling (warmup effects, thermal throttling, resource
exhaustion, caching), we compute the **temporal correlation** between run order and residuals.

Constants:

```rust
const TREND_CORRELATION_THRESHOLD: f64 = 0.5;            // Abort if |correlation| > 0.5
const TREND_CORRELATION_MIN_ITERATIONS: usize = 256;     // Wait for this many samples before aborting
```

Algorithm:

1. Compute residuals (same as outlier detection)

2. Compute Spearman rank correlation between sample index and residuals:
   ```text
   indices = [0, 1, 2, ..., n-1]  (sample order)
   residuals = [r_0, r_1, r_2, ..., r_{n-1}]
   temporal_correlation = spearman_correlation(indices, residuals)
   ```

3. Check abort condition during stopping checks:
    * If `|temporal_correlation| > TREND_CORRELATION_THRESHOLD`:
        * If `samples < TREND_CORRELATION_MIN_ITERATIONS`:
          Wait for more samples (trend may stabilize)
        * Otherwise: abort the benchmark run (systematic drift detected)

4. Store the correlation value in results for diagnostic purposes (even if below abort threshold)

**Interpretation:**

* **Positive correlation (> 0.5)**: Residuals increasing over time → performance degrading (thermal throttling, memory
  leaks, cache pollution)
* **Negative correlation (< -0.5)**: Residuals decreasing over time → performance improving (warmup, JIT optimization,
  cache warming)
* **Near zero (-0.2 to 0.2)**: No systematic trend → stable measurements (ideal)

**Rationale:**

Spearman rank correlation is used as it's robust to outliers and detects monotonic (not just linear) trends.

A strong temporal correlation indicates the benchmark environment is non-stationary, making results unreliable.

The temporal correlation check runs during the stopping check (every `CHECK_EVERY_NS` after `MIN_SAMPLES`/
`MIN_TOTAL_TIME_NS`), alongside CI checks, allowing early termination when drift is detected.

## 7.7 Output fields (per individual run)

Per run within a series, stats engine produces:

* `timestamp`: when this individual run was performed
* `mode`: `"regression"` or `"per_iter"`
* `mean_ns_per_iter`: estimated time per iteration (β for regression, μ for per-iter mean)
* `ci95_half_width_ns`: half-width of 95% CI (display as mean ± half_width)
* `intercept_ns`: fixed overhead per batch (only for regression mode, null otherwise)
* `outlier_count`: number of samples flagged as outliers
* `temporal_correlation`: Spearman correlation between sample order and residuals (range [-1, 1])
* `samples`: array of `{"iters": N_i, "total_ns": T_i}` for each sample

## 7.8 Output fields (per run series)

Per run series (collection of runs), the system produces:

* `bench`: benchmark name
* `config`: configuration JSON
* `timestamp`: when the run series was performed (start time)
* `runs`: array of individual run results (sorted by mean)
* `checksum`: output validation

The median run's estimates become the **representative values** for:

* stable result determination
* commit-to-commit comparison
* UI display

Raw run and samples are stored to allow later re-analysis if required.

**Note**: The outlier and temporal correlation statistics are computed after collection completes and stored for
diagnostic purposes. During collection, we abort only on strong temporal correlation (`|temporal_correlation| >
TREND_CORRELATION_THRESHOLD`). Outlier counts are recorded but no longer trigger an abort.

---

# 8. Measurement Format

Each successful workload run series produces one immutable V2 measurement JSON followed by one
SQLite transaction. The JSON contains identity and execution provenance in addition to the complete
series described in section 7. V1 files are retained only for migrated measurements (Appendix A).

## 8.1 V2 measurement JSON

The following is abridged; each `runs` entry uses the complete structure from section 7.

```json
{
  "schema": 2,
  "measurement_id": "0195f4c1-3a4b-7abc-8123-456789abcdef",
  "bench": "2015-04",
  "workload_sha256": "...",
  "group_spec": {
    "argv": ["bench", "2015", "04", "1"],
    "checksum": "8f024a8e...",
    "stats": { "...": "..." },
    "semantics_version": 1
  },
  "executable_sha256": "...",
  "stdin_sha256": "...",
  "executed_case": { "commit": "abc1234", "threads": "1" },
  "covered_cases": [
    { "commit": "abc1234", "threads": "1" },
    { "commit": "def5678", "threads": "1" }
  ],
  "timestamp": 1731437601,
  "checksum": "8f024a8e...",
  "runs": [{ "...": "..." }]
}
```

`measurement_id` is a time-sortable UUIDv7. Shared measurements include the full workload identity;
isolated measurements omit `group_spec` and content hashes. `executed_case` and `covered_cases` are
immutable execution provenance. Cases that inherit later are linked in SQLite and are not appended
to the file.

V2 files live at
`results/{host}/runs/by-measurement/{shard-1}/{shard-2}/{measurement-id}.json`, with shards derived
from random UUID bytes. V1 run-series JSON remains in place for migrated data but is not written for
new measurements. Normal result and history queries use SQLite summaries rather than decoding raw
JSON.

The median run remains the representative display and drift value, for example:

```text
106.3 µs/iter ±0.4% (median of 3 run means)
```

---

# 9. Storage Architecture

## 9.1 Dual Storage Model

Each host uses two complementary stores:

1. **Immutable JSON files** contain raw runs, samples, workload identity, and execution provenance.
2. **SQLite** indexes cases, workloads, measurements, current stable state, and case-visible history.

```text
data/
  config.json
  inputs/
  builds/
  results/
    pi5/
      metadata.db
      runs/
        by-measurement/<h1>/<h2>/<measurement-id>.json
        <benchmark>/<key=value>/.../<timestamp>.json  # migrated V1 files
```

V2 paths are reconstructed from `measurement_id`; V1 paths are reconstructed from benchmark,
hostless config, and timestamp. Paths are never stored in SQLite, so databases remain relocatable and
cross-host reads resolve files under the reader's live data root.

JSON is written before its database metadata. A database failure may leave an unindexed JSON file,
but a committed database row never points to an unwritten file. There is currently no automatic
JSON-to-database rebuild, and JSON alone cannot recover inherited case links or stable/drift state;
normal verified database backups remain required.

SQLite uses WAL mode, `synchronous=NORMAL`, foreign keys, a 30-second busy timeout, and one persistent
connection per `HybridDiskStorage`. The benchmark lock prevents concurrent writers, and each
measurement update uses one immediate transaction.

## 9.2 SQLite Schema

Each host's `metadata.db` has four normalized tables:

| Table | Role |
| --- | --- |
| `cases` | One row per `(benchmark, canonical hostless config)` and its current `workload_id` |
| `workloads` | Durable shared/isolated identity plus stable/last measurement pointers and drift counters |
| `measurements` | One row per workload run series, with timestamp, V1/V2 schema version, checksum, and median-run summary |
| `measurement_cases` | Append-only case-visible measurement history |

Important invariants:

* `workload_sha256` is unique. A hash hit is accepted only after the stored benchmark, executable
  digest, stdin digest, and complete group spec match.
* Shared workloads have an executable digest; isolated workloads have neither executable nor stdin
  digests. Stdin can be present only for a shared workload.
* Stable and last pointers are either both null or both point to measurements belonging to that
  workload.
* A case changes workload only inside the transaction that records or inherits a valid measured
  workload. Previous `measurement_cases` history is never deleted.
* Commit ordering comes from `config_keys.commit.values`, not the database.

Migration 03 creates this schema and copies V1 `results`/`run_series` rows into isolated workloads
inside the same transaction. Migration 04 drops those V1 metadata tables. V1 JSON stays in its
reconstructible path for future recovery or reanalysis tooling.

## 9.3 Stable result update detection

Stable/drift state is updated once per workload measurement:

1. The first measurement becomes both stable and last.
2. Every later measurement becomes last and is compared with the stable measurement using median-run
   confidence intervals and relative mean difference.
3. Overlapping intervals or a difference below 3% count as a match: increment `matched_count` and
   reset `suspicious_count`.
4. A non-overlapping difference of at least 3% increments `suspicious_count`. The third consecutive
   suspicious measurement becomes stable, resets matched/suspicious counts, and increments
   `replaced_count`.
5. `--force-update-stable` promotes the new measurement immediately.

Because this state belongs to a workload, a binary or invocation change creates independent drift
state. Measurements of W2 are never compared with W1 merely because the same case moved between
them.

## 9.4 Workload sharing and inheritance

Suppose cases A and B have different configs but currently resolve to the same hardlinked executable
and the same invocation spec:

```text
selected group [A, B] --hash--> workload W1 --run--> measurement M1

cases:              A -> W1, B -> W1
workload state:     W1 stable=M1, last=M1
visible history:    M1 -> [A, B]
JSON provenance:    M1 covered_cases=[A, B]
```

Now the config adds C, which resolves to another hardlink to the same executable and has the same
invocation spec. The next command forms the inode group `[A, B, C]`; it classifies as new because C
has no recorded workload, then hashes to W1 and inherits it without re-running the workload:

```text
cases:              A -> W1, B -> W1, C -> W1
visible history:    M1 -> [A, B, C]
JSON provenance:    M1 covered_cases=[A, B]  # immutable; C inherited later
```

Inheritance adds W1's current stable and latest measurements to C's visible history (one row when
they are the same) but does not backfill older measurements. Historical measurement rows retain
their workload and binary identity; JSON `covered_cases` distinguishes direct execution coverage
from later inheritance.

If `[A, B, C]` is deliberately rerun, the new M2 is linked to every case in the group:

```text
visible history:    M1 -> [A, B, C], M2 -> [A, B, C]
JSON provenance:    M2 covered_cases=[A, B, C]
```

If rebuilding replaces A/B's paths with hardlinks to a new executable while C retains the old
artifact, that change is noticed only when the A/B group is selected and hashed. The execution
records a distinct W2 and moves A/B to it; C remains on W1. W2 starts independent stable/drift state
unless that exact workload was already measured.

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
* **Child process hangs**: After `TIMEOUT` (default 600s), kill process and fail, do not store the partial results.
* **Premature EOF**: Fail the run, do not store partial results.

## 10.3 Storage errors

* **Disk full / write error**: Fail loudly, do not corrupt existing data.
* **Missing directories**: Create automatically (with proper error handling).
* **Corrupted database**: Fail loudly and restore a verified backup; no automatic rebuild is implemented.
* **Corrupted measurement JSON**: Fail loudly when the measurement is read.
* **Database locked**: SQLite will automatically wait up to `busy_timeout` (default 30s), then fail with SQLITE_BUSY.

# 11. Schedulers and Workflows

Both run commands first build `RunGroup`s. Shared benchmarks resolve all in-scope cases and group
hardlinks with the same invocation spec before applying limits. Isolated benchmarks contribute one
lazy group per case. A config filter makes a group eligible when any member matches; processing a
selected shared group still covers every member.

For `inode-content`, both schedulers rely on the layout contract above: they do not hash every
current artifact or coalesce byte-identical files on different inodes before applying limits.

## 11.1 Periodic `run`

`run` classifies groups without reading executable or stdin bytes:

* A shared group is **new** if a case has no recorded workload, members disagree on their workload,
  the association is isolated, or the current group spec differs from the recorded workload.
* Otherwise it is a **rerun**, ordered by the workload's last measurement time.
* An isolated case is new without a recorded workload and otherwise a rerun.

Groups are shuffled before the read transaction, and the first `--new-limit` new classifications are
retained. A bounded max-heap retains only the oldest `--rerun-limit` reruns; preserving global
oldest-first semantics requires scanning every eligible group when reruns are requested. Only
selected shared groups are content-hashed. A reuse completes its selected new-group slot; the
scheduler does not select a replacement.

New selections allow reuse: an exact measured workload is inherited without re-running the workload.
Deliberate rerun selections always execute and update that workload's drift state. Because
classification is content-free, changing a binary on disk does not itself move a previously recorded
group into the new pool; the change is discovered when the group is selected as a rerun and hashed.

## 11.2 `run-all`

`run-all` selects every eligible group with deliberate-run intent. It never uses an existing
measurement to suppress execution. Hardlinked shared cases still form one group and therefore one
workload execution; isolated cases execute separately. Process failures are retried up to five times,
except timeouts, and `run-all` fails fast when a group cannot be resolved or processed.

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
* Keys are lowercase alphanumeric with underscores; values may also contain uppercase letters and hyphens
* Configs must be canonically sorted by key (alphabetically) for consistent storage and comparison
* Configurations remain distinct cases even when several cases share one workload and stable result
* Cases are stored in the same host database with canonical hostless config JSON
* Partial matching is supported: you can query with a subset of keys (e.g., `profile=release`) to compare all configs
  that match those key-value pairs

---

# 13. Viewing and Analysis

## 13.1 Basic queries

**CLI tool for common queries:**

```bash
# Export all stable results for a host
aoc-bench export --host silicon > results.tsv

# Query specific result (using partial config matching)
aoc-bench export --host silicon --config commit=abc1234,profile=release,threads=1 2015-04
```

**Config filtering:**

- All `--config` arguments use the `key=value,key2=value2` format
- Filtering is done via partial matching: any config whose JSON contains all specified key-value pairs will match
- For exact matching, specify all keys present in the config
- The `--host` flag is a shorthand for `--config host=<value>`

## 13.2 Export for visualization

The `export` command outputs TSV suitable for plotting:

```text
host    bench   cfg_commit  cfg_profile cfg_threads  stable_timestamp  stable_measurement_id                 median_run_mean_ns  median_run_ci95_half_ns
silicon 2015-04 abc1234     release     1            1731437601        0195f4c1-3a4b-7abc-8123-456789abcdef  30930000            30000
...
```

**Column extraction:**

The config JSON is always extracted into a column per key. Keys other than `host` are prefixed with `cfg_`.

**Note:** All exported values are median run values from run series. For full run-level details (all runs in the
series),
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
Bench:      2015-04
Config:     {"bench":"2015-04","host":"silicon","profile":"release","threads":"1"}
Comparison: commit

commit          mean            CI            delta       
abc1234         30.00 ms        ±0.03 ms      --        INITIAL
def5678         34.80 ms        ±0.04 ms      +16.00%   REGRESSION
ghi9012         24.00 ms        ±0.03 ms      -31.03%   IMPROVEMENT

(1 entries with insignificant changes omitted)
```

**Process:**

- Finds all benchmark configs matching the specified benchmark name and `--config` argument. If no benchmark argument
  is provided and the config file defines exactly one benchmark, that single benchmark is used; otherwise an explicit
  benchmark is required.
- Identifies the comparison key: the single config key whose values differ across the matched configs. Errors if none
  or more than one key varies.
- Verifies that all matched configs share the same set of keys.
- Builds the timeline from **stable results only**, ordered by the comparison key's value order from the config file
  (not by timestamp).
- A change is highlighted only when the 95% confidence intervals do not overlap **and** the relative difference in
  means meets the `--threshold` percentage (default 10%). Points below the threshold are omitted but counted and the
  omission count is shown at the bottom.
- Outputs the initial point followed by each significant regression/improvement with its delta sign (+/-) and
  direction label.

### 13.3.2 Impact view

Shows which benchmarks changed meaningfully under a specified config key change:

```bash
aoc-bench impact --config profile=release commit=abc1234
```

**Output format:**

```text
Comparison from commit=aaa1111 to commit=abc1234, filtered to profile=release

IMPROVEMENTS:
  2015-04 [threads=32]    5.20ms → 4.15ms    -20%
  2015-06 [threads=1]     28.50ms → 25.10ms  -11.93%

REGRESSIONS:
  2015-04 [threads=1]     30.93ms → 32.10ms  +3.7%

17 configs unchanged
```

**Process:**

- Finds all results with `commit=abc1234` (and optionally filtered by a partial config)
- Finds the previous config value for the specified key (e.g. `commit=abc1234` -> `commit=def4567`)
- Finds all results with `commit=def4567` (and optionally filtered by a partial config)
- For each pair of results with the same config (ignoring the comparison key), determines if there was a regression,
  improvement, or no significant change
- Uses a relative change threshold (`--threshold`, default 10%) and requires non-overlapping CIs (similar to
  `timeline`).
- Outputs a summary with improvements and regressions.

## 13.4 Web UI

The web UI is a static SPA that loads exported snapshot JSON from `aoc-bench export-web`:

- **Dashboard (`/`)**
    - Host overview and benchmark table
    - Latest result filtering across config dimensions
    - Fastest/slowest benchmark leaderboard
- **Benchmark detail (`/benchmark`)**
    - Per-config cards for one benchmark
    - Links to timeline views for deeper analysis
- **Timeline (`/timeline`)**
    - Per-benchmark timeline and comparison-key analysis
    - Config filtering and history drill-down
- **Impact (`/impact`)**
    - Compare two values of a selected config key
    - Group regressions, improvements, and unchanged results with threshold control

Web export schema 2 records the config keys used by each benchmark and encodes configs in a
benchmark-local index space. Within one host snapshot, equal positive measurement tokens mean two
rows display the exact same shared measurement; zero identifies isolated measurements. Timeline
merges only contiguous rows with the same positive token and matching metrics/fixed config.

---

# 14. Extensibility / Future Ideas

These are *not* required now, but the design leaves room for them:

* Add **inner and outer CIs** (per-run noise vs across-runs variability). Measurement JSON contains all
  individual runs, making this analysis possible.
* **Variance analysis across runs**: Use the stored individual runs in each series to compute cross-run variance
  metrics.
* Use **daily roll-ups** for heavy usage (materialized views in database).
* Add richer workload/artifact diagnostics to the web UI if equality-only measurement tokens prove
  insufficient.

---

# 15. Summary

This design gives you:

* A **generic, language-agnostic protocol** (`SAMPLE iters total_ns checksum=...`) for in-process benchmarking.
* A **stats engine** that:
    * auto-chooses regression vs per-iter mean,
    * uses bootstrap CIs,
    * stops when the measurement is stable.
* A **run series approach** that:
    * Performs N runs back-to-back for each benchmark execution (default 3)
    * Uses median-of-means to eliminate run-to-run variance (ASLR, cache state, etc.)
    * Stores all runs for later analysis and variance detection
* A **dual storage architecture**:
    * Immutable versioned JSON with raw runs, samples, identity, and execution provenance
    * Normalized SQLite tables for cases, workloads, measurements, history, and stable state
    * JSON-first writes ensure committed rows never point to unwritten files
* A **workload identity system** that:
    * Groups safe hardlinked cases before limits and reuses byte-identical measured workloads
    * Keeps isolated benchmarks case-specific
    * Preserves the workload and measurement behind each case-visible historical result
* A **per-workload stable result system** for environment drift:
    * Measurements within noise retain the stable pointer
    * Three consecutive significant changes replace it
    * Binary or invocation identity changes start independent drift state
* **Simple querying**: SQL for ad-hoc queries, CLI tools for common operations, easy export for visualization.

---

# Appendix A: V1 Storage and Migration

V1 stored each logical case independently. Its immutable JSON contained `schema: 1`, benchmark,
config, timestamp, checksum, and runs. Files used the human-readable path
`runs/{benchmark}/{key=value}/.../{timestamp}.json`. SQLite duplicated each file's median-run summary
in `run_series`, while `results` held that case's stable/latest timestamps and drift counters.

| Concern | V1 | V2 |
| --- | --- | --- |
| Recorded unit | One case run series | One workload measurement covering one or more cases |
| Durable identity | Benchmark + config + timestamp | Measurement UUID and complete workload identity |
| JSON path | Benchmark/config/timestamp | UUID random-tail shards |
| Mutable state | Per case in `results` | Per workload in `workloads` |
| Case history | Implied by V1 run-series ownership | Explicit `measurement_cases` links |
| Sharing | None | Shared workloads with executable/stdin content hashes |

Migration 03 converts V1 metadata transactionally:

1. Each V1 `(benchmark, config)` becomes a `case` and a case-specific isolated workload.
2. Each `run_series` row becomes a `measurements` row with `schema_version = 1` and a deterministic
   UUIDv7 using the recorded timestamp and a hash of the benchmark and canonical `key=value` config.
3. The measurement is linked to its original case, and stable/latest pointers and counters are copied
   from `results` onto the isolated workload.
4. After that transaction commits, migration 04 drops `run_series` and `results`. V1 JSON remains in
   its reconstructible path; current result and history queries use the migrated SQLite summaries.

Migration cannot infer historical executable or stdin content, so V1 workloads remain isolated. If
a dedupe-enabled group is later selected, current content hashing may move its cases to a measured
shared workload or record a new one; the original V1 measurements remain in case-visible history.
