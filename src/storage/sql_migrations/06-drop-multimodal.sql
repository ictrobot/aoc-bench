-- Drop the multimodal flag from the run summary. Rows written from now on use schema_version 4

ALTER TABLE measurements
    DROP COLUMN multimodal;
