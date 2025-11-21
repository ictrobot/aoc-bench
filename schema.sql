-- Schema for bench_data/$host/metadata.db
-- Each host has its own independent SQLite database

-- Core table: one row per run series (collection of 7 runs)
CREATE TABLE run_series (
    bench TEXT NOT NULL,  -- Benchmark name (e.g., "2015-04", "compile-aoc-rs")
    config TEXT NOT NULL,  -- {"commit":"abc123","host":"silicon","profile":"release","threads":"1"}
    timestamp INTEGER NOT NULL,  -- Unix timestamp when run series was performed

    -- Statistics results from median run
    mean_ns_per_iter REAL NOT NULL,  -- median mean from the run series
    ci95_half_width_ns REAL NOT NULL,  -- CI from the median run

    -- Metadata
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),  -- When this row was inserted

    PRIMARY KEY (bench, config, timestamp)
) STRICT, WITHOUT ROWID;


-- Results: one row per (bench, config) pair
-- This is the stable result that users see
CREATE TABLE results (
    bench TEXT NOT NULL,  -- Benchmark name
    config TEXT NOT NULL,  -- Config JSON (excluding bench)

    -- Generated columns for fast filtering (NULL if key not present)
    config_commit TEXT GENERATED ALWAYS AS (json_extract(config, '$.commit')) VIRTUAL,

    -- Which run series is the stable result
    stable_series_timestamp INTEGER NOT NULL,

    -- Most recent run series (for detecting drift)
    last_series_timestamp INTEGER NOT NULL,

    -- State for drift detection
    suspicious_series_count INTEGER NOT NULL DEFAULT 0,

    -- Audit trail
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),

    PRIMARY KEY (bench, config),
    FOREIGN KEY (bench, config, stable_series_timestamp) REFERENCES run_series(bench, config, timestamp) ON DELETE RESTRICT,
    FOREIGN KEY (bench, config, last_series_timestamp) REFERENCES run_series(bench, config, timestamp) ON DELETE RESTRICT
) STRICT;

-- Indexes for common query patterns
CREATE INDEX idx_results_bench ON results(bench);
CREATE INDEX idx_results_commit ON results(config_commit) WHERE config_commit IS NOT NULL;
CREATE INDEX idx_results_bench_commit ON results(bench, config_commit);
CREATE INDEX idx_results_last_series ON results(last_series_timestamp);
