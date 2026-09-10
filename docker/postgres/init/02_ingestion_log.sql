-- One row per ingestion run: what was fetched, how big it was, and whether it was new.
CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,  -- tlc_yellow, tlc_zones, weather, holidays
    period TEXT, -- '2025-01' (monthly), '2025' (yearly), NULL (reference data)
    file_path TEXT NOT NULL,  -- relative to the data lake, e.g. bronze/weather/...
    row_count BIGINT,
    file_bytes BIGINT,
    status TEXT NOT NULL CHECK (status IN ('downloaded', 'skipped')),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);