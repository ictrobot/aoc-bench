# Implementation Tasks

## Phase 1: Minimal Working Runner

### 1.1 Project Setup
- [x] Initialize Cargo project with dependencies (serde, serde_json, rusqlite, clap, rand, chrono)
- [x] Create module structure (protocol, stats, storage, config, runner, cli)
- [x] Set up basic CLI skeleton with clap

### 1.2 Protocol Parser
- [x] Implement `META` line parsing with key-value pairs
- [x] Implement `SAMPLE` line parsing (iters, total_ns, checksum)
- [x] Add line parsing tests with valid/invalid inputs
- [x] Add checksum validation logic

### 1.3 Basic Statistics Engine
- [x] Implement sample accumulation (Vec of (iters, total_ns))
- [x] Implement mode detection heuristic (distinct_N, range_N, cv_N)
- [x] Implement weighted least squares for regression mode
- [x] Implement weighted mean for per-iter mode
- [x] Add unit tests with known datasets

### 1.4 Simple Runner (Single Run)
- [x] Implement command spawning with stdout/stderr capture
- [x] Implement SAMPLE line collection from child process
- [x] Add timeout handling (kill process after TIMEOUT)
- [x] Add working directory (temp dir) setup
- [x] Integrate protocol parser and stats engine
- [x] Test with a mock benchmark command

## Phase 2: Statistics and Robustness ✅

### 2.1 Bootstrap Confidence Intervals
- [x] Implement bootstrap resampling (with replacement)
- [x] Implement percentile CI calculation (2.5th, 97.5th)
- [x] Add quick bootstrap (1000 samples) for stopping checks
- [x] Add final bootstrap (10000 samples) after stopping
- [x] Implement relative half-width calculation
- [x] Add stopping condition (target CI width or max samples/timeout)

### 2.2 Outlier Detection
- [x] Implement residual calculation (regression mode: T - (α + β·N), per-iter: T/N)
- [x] Implement MAD-based outlier detection (switched from IQR)
- [x] Add outlier counting and fraction calculation
- [x] Implement run abort on excessive outliers (>10%, with delayed abort logic)
- [x] Add tests for outlier detection with synthetic data

### 2.3 Warmup and Sampling Logic
- [x] Implement warmup (skip first 16 samples)
- [x] Implement MIN_SAMPLES check (32)
- [x] Implement CHECK_EVERY interval (32)
- [x] Integrate CI check and outlier check at intervals
- [x] Add tests for stopping conditions

## Phase 3: Run Series ✅

### 3.1 Run Series Execution
- [x] Implement 7-run loop (RUN_SERIES_COUNT)
- [x] Store individual run results (timestamp, mean, CI, mode, intercept, outlier_count, samples)
- [x] Sort runs by mean_ns_per_iter
- [x] Calculate median run statistics
- [x] Add retry logic (up to 5 retries on failure)

### 3.2 Run Series Output Format
- [x] Define RunSeries struct with schema version
- [x] Implement JSON serialization for run series
- [x] Add timestamp handling (unix timestamp in JSON, ISO 8601 in filenames)
- [x] Add display formatting (µs/iter ±%, median of 7)

### 3.3 `debug` Command ✅
- [x] Add Debug command to CLI with --input, --checksum, and trailing command args
- [x] Implement stdin input support in Runner (with_stdin_input method)
- [x] Implement debug command handler in main.rs
- [x] Output JSON on success, error message on failure
- [x] Add documentation to DESIGN.md

## Phase 4: Configuration System

### 4.1 Config File Parser
- [ ] Define config.json schema structs
- [ ] Implement config file loading and validation
- [ ] Add regex validation for keys ([a-z][a-z0-9_]*) and values ([a-zA-Z0-9_-]+)
- [ ] Implement preset expansion
- [ ] Implement Cartesian product expansion
- [ ] Add canonical config sorting (BTreeMap)

### 4.2 Command Templating
- [ ] Implement {key} placeholder substitution
- [ ] Add validation that all placeholders have config values
- [ ] Add path resolution relative to data/ directory
- [ ] Add tests for template expansion

### 4.3 Config String Encoding
- [ ] Implement config to canonical string (key=value,key=value)
- [ ] Implement string to config parsing
- [ ] Add tests for round-trip encoding
- [ ] Implement partial config matching logic

## Phase 5: Storage Layer

### 5.1 Directory Structure
- [ ] Implement data/ directory layout creation
- [ ] Implement results/{host}/ directory creation
- [ ] Implement runs/{bench}/{config_string}/ path generation
- [ ] Add file lock implementation (std::fs::File with try_lock_exclusive)
- [ ] Add tests for path generation and encoding

### 5.2 JSON Storage
- [ ] Implement run series JSON file writing
- [ ] Implement timestamped filename generation (ISO 8601)
- [ ] Add atomic write (write to temp, then rename)
- [ ] Add JSON file reading and parsing

### 5.3 SQLite Schema
- [ ] Create schema.sql with tables (run_series, results)
- [ ] Implement database initialization from schema
- [ ] Add generated columns for config extraction (commit, host)
- [ ] Add indexes and WITHOUT ROWID optimization
- [ ] Set up WAL mode and pragmas

### 5.4 Database Operations
- [ ] Implement run_series table insert
- [ ] Implement results table upsert
- [ ] Implement stable result query by (bench, config)
- [ ] Add transaction handling (BEGIN IMMEDIATE)
- [ ] Ensure JSON-first write ordering

## Phase 6: Stable Result Management

### 6.1 Drift Detection
- [ ] Implement CI overlap check
- [ ] Implement relative difference calculation (3% threshold)
- [ ] Implement suspicious series counter logic
- [ ] Implement stable result update (after 3 consecutive suspicious)
- [ ] Add tests for drift detection scenarios

### 6.2 Storage Integration
- [ ] Integrate drift detection with database operations
- [ ] Implement first-run logic (no existing stable result)
- [ ] Implement stable result preservation (within noise)
- [ ] Implement stable result replacement (environment changed)
- [ ] Add --force-update-stable flag

## Phase 7: CLI Commands

### 7.1 `run` Command
- [ ] Implement benchmark selection (--benchmark)
- [ ] Implement config filtering (--config key=value,key=value)
- [ ] Implement config JSON input (--config-json)
- [ ] Integrate with config expansion
- [ ] Add command execution and result storage
- [ ] Add progress output

### 7.2 `sample` Command
- [ ] Implement selection of (bench, config) pairs to re-run
- [ ] Prioritize configs without results
- [ ] Prioritize configs with oldest last_series_timestamp
- [ ] Add limit flag for number of samples to run
- [ ] Integrate with run logic

### 7.3 `export` Command
- [ ] Implement SQLite query for stable results
- [ ] Implement config filtering (partial matching)
- [ ] Implement TSV output format
- [ ] Extract config keys into cfg_ columns
- [ ] Add tests for TSV generation

### 7.4 `timeline` Command
- [ ] Implement benchmark + partial config query
- [ ] Identify comparison key (single varying dimension)
- [ ] Sort results by comparison key order (from config.json)
- [ ] Compute deltas and detect regressions/improvements
- [ ] Format output with highlights

### 7.5 `impact` Command
- [ ] Parse commit comparison (commit=X)
- [ ] Find previous commit in config.json ordering
- [ ] Query results for both commits
- [ ] Match configs (ignoring comparison key)
- [ ] Group into regressions/improvements/unchanged
- [ ] Format output summary

## Phase 8: Linux-Specific Optimizations

### 8.1 CPU Affinity
- [ ] Parse cpu_affinity from config.json
- [ ] Implement sched_setaffinity via libc
- [ ] Add pre_exec hook for process spawning
- [ ] Add tests (skip on non-Linux)

### 8.2 ASLR Disable
- [ ] Implement personality(ADDR_NO_RANDOMIZE) via libc
- [ ] Add pre_exec hook for ASLR disable
- [ ] Combine with CPU affinity hook
- [ ] Add tests (skip on non-Linux)

## Phase 9: Recovery and Robustness

### 9.1 Database Recovery
- [ ] Implement JSON file scanning
- [ ] Implement database rebuild from JSON files
- [ ] Add corruption detection (SQLite integrity check)
- [ ] Add automatic recovery on startup
- [ ] Add manual recovery command

### 9.2 Error Handling
- [ ] Add process crash detection
- [ ] Add disk full/write error handling
- [ ] Add corrupted JSON detection and skipping
- [ ] Add database lock timeout handling
- [ ] Add helpful error messages

## Phase 10: Testing and Polish

### 10.1 Integration Tests
- [ ] Create mock benchmark binary
- [ ] Test full run → storage → query cycle
- [ ] Test run series and median selection
- [ ] Test stable result updates
- [ ] Test all CLI commands end-to-end

### 10.2 Documentation
- [ ] Write README.md with usage examples
- [ ] Document config.json format
- [ ] Document SAMPLE protocol
- [ ] Add examples/ directory with sample configs

### 10.3 Performance Testing
- [ ] Benchmark statistics engine performance
- [ ] Benchmark database query performance
- [ ] Optimize hot paths if needed
- [ ] Add benchmarks for large datasets

## Phase 11: Nice-to-Have Features

### 11.1 Additional Features
- [ ] Add --dry-run flag for run command
- [ ] Add result comparison tool
- [ ] Add config validation command
- [ ] Add database statistics command (disk usage, result counts)

### 11.2 Future Extensibility
- [ ] Document extension points in code
- [ ] Add plugin hooks for custom analysis
- [ ] Design API for external tools
