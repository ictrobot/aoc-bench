-- Record each run series' wall time. Rows written from now on use schema_version 5, older rows stay NULL

ALTER TABLE measurements
    ADD COLUMN series_wall_ns INTEGER;
