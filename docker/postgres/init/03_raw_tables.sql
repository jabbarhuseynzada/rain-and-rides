-- Warehouse tables for the silver data. Spark loads into these; it never creates them itself,
-- so the column types here are the contract. year/month (or year) = the batch each row came from.

CREATE TABLE IF NOT EXISTS raw.yellow_trips (
    year                  INT              NOT NULL,
    month                 INT              NOT NULL,
    vendor_id             INT,
    pickup_datetime       TIMESTAMP,       -- New York local time, no timezone (like the source)
    dropoff_datetime      TIMESTAMP,
    passenger_count       INT,
    trip_distance         DOUBLE PRECISION,
    ratecode_id           INT,
    store_and_fwd_flag    TEXT,
    pu_location_id        INT,
    do_location_id        INT,
    payment_type          INT,
    fare_amount           NUMERIC(12, 2),
    extra                 NUMERIC(12, 2),
    mta_tax               NUMERIC(12, 2),
    tip_amount            NUMERIC(12, 2),
    tolls_amount          NUMERIC(12, 2),
    improvement_surcharge NUMERIC(12, 2),
    total_amount          NUMERIC(12, 2),
    congestion_surcharge  NUMERIC(12, 2),
    airport_fee           NUMERIC(12, 2),
    cbd_congestion_fee    NUMERIC(12, 2),
    duration_min          DOUBLE PRECISION,
    pickup_date           DATE,
    pickup_hour           TIMESTAMP,
    tip_pct               DOUBLE PRECISION,
    is_airport_trip       BOOLEAN,
    loaded_at             TIMESTAMPTZ      NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS yellow_trips_year_month_idx ON raw.yellow_trips (year, month);

CREATE TABLE IF NOT EXISTS raw.weather_hourly (
    year              INT              NOT NULL,
    month             INT              NOT NULL,
    weather_hour      TIMESTAMP        PRIMARY KEY,  -- one row per hour: a duplicate would double-count trips
    temperature_c     DOUBLE PRECISION,
    precipitation_mm  DOUBLE PRECISION,
    rain_mm           DOUBLE PRECISION,
    snowfall_cm       DOUBLE PRECISION,
    wind_speed_kmh    DOUBLE PRECISION,
    weather_code      INT,
    weather_date      DATE,
    loaded_at         TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.holidays (
    year          INT          NOT NULL,
    holiday_date  DATE         NOT NULL,
    name          TEXT         NOT NULL,
    local_name    TEXT,
    is_global     BOOLEAN,     -- true = nationwide
    counties      TEXT,        -- e.g. 'US-CT,US-IL,US-NY' for state-level holidays
    types         TEXT,
    loaded_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);