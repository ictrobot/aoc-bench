-- Rebuild run_series to rename median columns, add counts, and drop created_at

PRAGMA foreign_keys = OFF;

CREATE TABLE IF NOT EXISTS new_run_series
(
    bench                    TEXT    NOT NULL,
    config                   TEXT    NOT NULL,
    timestamp                INTEGER NOT NULL,

    -- Run metadata
    run_count                INTEGER NOT NULL,

    -- Statistics from the median run
    median_run_mean_ns       REAL    NOT NULL,
    median_run_ci95_half_ns  REAL    NOT NULL,
    median_run_outlier_count INTEGER NOT NULL,
    median_run_sample_count  INTEGER NOT NULL,

    PRIMARY KEY (bench, config, timestamp)
) STRICT, WITHOUT ROWID;

INSERT INTO new_run_series (bench,
                            config,
                            timestamp,
                            run_count,
                            median_run_mean_ns,
                            median_run_ci95_half_ns,
                            median_run_outlier_count,
                            median_run_sample_count)
SELECT bench,
       config,
       timestamp,
       0,
       mean_ns_per_iter,
       ci95_half_width_ns,
       0,
       0
FROM run_series;

DROP TABLE run_series;

ALTER TABLE new_run_series
    RENAME TO run_series;

PRAGMA foreign_keys = ON;
PRAGMA foreign_key_check;
