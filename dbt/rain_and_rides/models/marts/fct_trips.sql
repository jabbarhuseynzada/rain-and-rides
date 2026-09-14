-- The fact table: one row per trip, with the columns a dashboard needs.
-- Kept narrower than int_trips_enriched on purpose: zone and date details live in
-- dim_zone and dim_date, and are joined on when needed (a star schema).
--
-- INCREMENTAL, one month at a time:
--   first run (or --full-refresh)  builds every month from scratch
--   later runs                     rebuild only the months you name
--
-- delete+insert on source_month means re-running a month REPLACES it. That matches how the
-- pipeline works: Spark reloads whole months, so a corrected month must fully replace the old
-- one. A plain insert would leave both copies behind and double that month's trips.
--
--   make dbt CMD="build --select fct_trips+ --vars '{months: 2025-04}'"

{#
    delete+insert with unique_key = source_month means an incremental run deletes the whole
    month first, then inserts the new version of it.
    on_schema_change = append_new_columns means a new upstream column is added to the table
    instead of being silently dropped.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='source_month',
        on_schema_change='append_new_columns'
    )
}}

with enriched as (

    select
        *,
        to_char(pickup_at, 'YYYY-MM') as source_month
    from {{ ref('int_trips_enriched') }}

    {% if is_incremental() %}
    -- Only read the months being loaded. Without this filter an incremental run would still
    -- scan every row upstream and gain nothing.
    where to_char(pickup_at, 'YYYY-MM') in (
        {%- if var('months', none) -%}
            {#- months given on the command line, e.g. --vars '{months: 2025-04}' or '{months: [2025-04, 2025-05]}' -#}
            {%- set month_list = var('months') if var('months') is not string else [var('months')] -%}
            '{{ month_list | join("', '") }}'
        {%- else -%}
            {#- no months given: pick up anything loaded into the warehouse since the last run -#}
            select distinct to_char(pickup_at, 'YYYY-MM')
            from {{ ref('int_trips_enriched') }}
            where loaded_at > (select coalesce(max(loaded_at), timestamptz '1900-01-01') from {{ this }})
        {%- endif -%}
    )
    {% endif %}

)

select
    trip_id,

    -- keys into the dimensions
    pickup_date                                  as date_key,
    pickup_location_id                           as pickup_zone_id,
    dropoff_location_id                          as dropoff_zone_id,

    -- when
    pickup_at,
    dropoff_at,
    pickup_hour,
    pickup_hour_of_day,
    is_weekend,
    is_holiday,

    -- the trip
    duration_min,
    trip_distance_miles,
    passenger_count,
    is_airport_trip,
    payment_type_name,
    ratecode_name,

    -- money
    fare_amount,
    tip_amount,
    tip_pct,
    total_amount,

    -- weather in the pickup hour
    temperature_c,
    precipitation_mm,
    snowfall_cm,
    wind_speed_kmh,
    weather_category,

    -- lineage: which month batch this row belongs to, and when it reached the warehouse
    source_month,
    loaded_at

from enriched