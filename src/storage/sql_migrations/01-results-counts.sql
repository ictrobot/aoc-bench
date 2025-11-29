-- Rebuild table to move columns and add new matched_count and replaced_count columns

PRAGMA foreign_keys = OFF;

CREATE TABLE IF NOT EXISTS new_results
(
    bench                   TEXT    NOT NULL,           -- Benchmark name
    config                  TEXT    NOT NULL,           -- Config JSON (excluding bench)

    -- Which run series is the stable result
    stable_series_timestamp INTEGER NOT NULL,

    -- Most recent run series (for detecting drift)
    last_series_timestamp   INTEGER NOT NULL,

    -- Run counters
    matched_count           INTEGER NOT NULL DEFAULT 0, -- Total number of runs that matched since replacement
    suspicious_count        INTEGER NOT NULL DEFAULT 0, -- Number of consecutive runs that haven't matched
    replaced_count          INTEGER NOT NULL DEFAULT 0, -- Total number of replacements

    -- Metadata
    updated_at              INTEGER NOT NULL DEFAULT (unixepoch()),

    -- Generated columns for fast filtering (NULL if key not present)
    -- Put these last so more can be added easily
    config_commit           TEXT GENERATED ALWAYS AS (json_extract(config, '$.commit')) VIRTUAL,

    PRIMARY KEY (bench, config),
    FOREIGN KEY (bench, config, stable_series_timestamp) REFERENCES run_series (bench, config, timestamp) ON DELETE RESTRICT,
    FOREIGN KEY (bench, config, last_series_timestamp) REFERENCES run_series (bench, config, timestamp) ON DELETE RESTRICT
) STRICT;

INSERT INTO new_results (bench, config, stable_series_timestamp, last_series_timestamp, updated_at, suspicious_count)
SELECT bench, config, stable_series_timestamp, last_series_timestamp, updated_at, suspicious_series_count
FROM results;

DROP TABLE results;

ALTER TABLE new_results
    RENAME TO results;

CREATE INDEX IF NOT EXISTS idx_results_bench ON results (bench);
CREATE INDEX IF NOT EXISTS idx_results_commit ON results (config_commit) WHERE config_commit IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_results_bench_commit ON results (bench, config_commit);
CREATE INDEX IF NOT EXISTS idx_results_last_series ON results (last_series_timestamp);

PRAGMA foreign_keys = ON;
PRAGMA foreign_key_check;
