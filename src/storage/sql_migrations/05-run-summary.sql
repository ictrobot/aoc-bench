-- Rebuild measurements with the run summary in place of the median-run columns

CREATE TABLE new_measurements
(
    measurement_id BLOB    NOT NULL PRIMARY KEY, -- 16-byte UUIDv7
    workload_id    INTEGER NOT NULL,
    timestamp      INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,             -- 1 = v1 run-series JSON, 2 = measurement JSON, 3 = with run summary

    -- Run summary. Rows from before schema 3 keep their median run's mean and have NULL for the
    -- rest: their within-run bootstrap half-width was not a run interval (the raw JSON keeps it)
    run_count      INTEGER NOT NULL,
    mean_ns        REAL    NOT NULL,             -- mean over the series' runs
    ci95_half_ns   REAL,                         -- 95% t-interval half-width over runs, NULL for a single run
    multimodal     INTEGER,                      -- 0/1, the runs fell into more than one level

    checksum       TEXT,

    FOREIGN KEY (workload_id) REFERENCES workloads (workload_id) ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

INSERT INTO new_measurements (measurement_id, workload_id, timestamp, schema_version, run_count, mean_ns, ci95_half_ns,
                              multimodal, checksum)
SELECT measurement_id,
       workload_id,
       timestamp,
       schema_version,
       run_count,
       median_run_mean_ns,
       NULL,
       NULL,
       checksum
FROM measurements;

DROP TABLE measurements;

ALTER TABLE new_measurements
    RENAME TO measurements;

CREATE INDEX idx_measurements_workload ON measurements (workload_id, measurement_id);
