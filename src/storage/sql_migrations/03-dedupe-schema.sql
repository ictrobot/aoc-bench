-- Normalized identity schema for binary/workload deduplication.

CREATE TABLE IF NOT EXISTS workloads
(
    workload_id           INTEGER NOT NULL PRIMARY KEY, -- integer PK
    workload_sha256       TEXT    NOT NULL UNIQUE,      -- domain-separated full-identity hash
    benchmark             TEXT    NOT NULL,
    executable_sha256     TEXT,                         -- nullable; executable content digest
    stdin_sha256          TEXT,                         -- nullable; stdin content digest
    group_spec            TEXT    NOT NULL,             -- canonical JSON of invocation fields

    -- Mutable stable/drift state. Both pointers are NULL until the workload has a current result.
    stable_measurement_id BLOB,
    last_measurement_id   BLOB,
    matched_count         INTEGER NOT NULL DEFAULT 0,
    suspicious_count      INTEGER NOT NULL DEFAULT 0,
    replaced_count        INTEGER NOT NULL DEFAULT 0,
    updated_at            INTEGER NOT NULL DEFAULT (unixepoch()),

    -- stdin content is only meaningful for a content-backed (shared) workload
    CHECK (stdin_sha256 IS NULL OR executable_sha256 IS NOT NULL),
    CHECK ((stable_measurement_id IS NULL) = (last_measurement_id IS NULL)),

    FOREIGN KEY (stable_measurement_id) REFERENCES measurements (measurement_id) ON DELETE RESTRICT,
    FOREIGN KEY (last_measurement_id) REFERENCES measurements (measurement_id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_workloads_benchmark ON workloads (benchmark);

-- Logical cases: one row per (benchmark, canonical hostless config).
CREATE TABLE IF NOT EXISTS cases
(
    case_id       INTEGER NOT NULL PRIMARY KEY,
    benchmark     TEXT    NOT NULL,
    config        TEXT    NOT NULL, -- raw canonical hostless config JSON
    workload_id   INTEGER,          -- currently displayed workload; NULL until successfully recorded

    -- Generated column for fast filtering (NULL if key not present)
    config_commit TEXT GENERATED ALWAYS AS (json_extract(config, '$.commit')) VIRTUAL,

    UNIQUE (benchmark, config),
    FOREIGN KEY (workload_id) REFERENCES workloads (workload_id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_cases_commit ON cases (config_commit) WHERE config_commit IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cases_bench_commit ON cases (benchmark, config_commit);
CREATE INDEX IF NOT EXISTS idx_cases_workload ON cases (workload_id) WHERE workload_id IS NOT NULL;

-- One measurement per recorded workload run series (or migrated v1 series)
CREATE TABLE IF NOT EXISTS measurements
(
    measurement_id           BLOB    NOT NULL PRIMARY KEY, -- 16-byte UUIDv7
    workload_id              INTEGER NOT NULL,
    timestamp                INTEGER NOT NULL,
    schema_version           INTEGER NOT NULL,             -- 1 = v1 run-series JSON, 2 = measurement JSON

    -- Summary columns (mirrors the median-run stats stored for run_series)
    run_count                INTEGER NOT NULL,
    median_run_mean_ns       REAL    NOT NULL,
    median_run_ci95_half_ns  REAL    NOT NULL,
    median_run_outlier_count INTEGER NOT NULL,
    median_run_sample_count  INTEGER NOT NULL,
    checksum                 TEXT,

    FOREIGN KEY (workload_id) REFERENCES workloads (workload_id) ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_measurements_workload ON measurements (workload_id, measurement_id);

-- Case-visible measurement history. Execution links every covered case; inheritance links the
-- workload's current stable/last measurements; future measurements link every case currently
-- pointing at that workload.
CREATE TABLE IF NOT EXISTS measurement_cases
(
    measurement_id BLOB    NOT NULL,
    case_id        INTEGER NOT NULL,

    PRIMARY KEY (measurement_id, case_id),
    FOREIGN KEY (measurement_id) REFERENCES measurements (measurement_id) ON DELETE CASCADE,
    FOREIGN KEY (case_id) REFERENCES cases (case_id) ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_measurement_cases_case ON measurement_cases (case_id, measurement_id);
