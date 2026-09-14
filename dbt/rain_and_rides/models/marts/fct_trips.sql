-- The fact table: one row per trip, with the columns a dashboard needs.
-- Kept narrower than int_trips_enriched on purpose: zone and date details live in
-- dim_zone and dim_date, and are joined on when needed (a star schema).

with enriched as (

    select * from {{ ref('int_trips_enriched') }}

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
    weather_category

from enriched