-- One row per trip, with everything we know about its context:
-- the weather in its pickup hour, its pickup and dropoff zones, the day type, and code names.
-- Every join is a LEFT JOIN on a key that is unique on the right-hand side,
-- so the row count here must equal stg_trips exactly (no trip lost, none duplicated).

with trips as (

    select * from {{ ref('stg_trips') }}

),

weather as (

    select * from {{ ref('stg_weather_hourly') }}

),

weather_codes as (

    select * from {{ ref('weather_codes') }}

),

zones as (

    select * from {{ ref('taxi_zone_lookup') }}

),

payment_types as (

    select * from {{ ref('payment_types') }}

),

rate_codes as (

    select * from {{ ref('rate_codes') }}

),

holidays as (

    select * from {{ ref('int_holidays_ny') }}

)

select
    trips.*,

    -- day and time context
    extract(hour from trips.pickup_at)::int              as pickup_hour_of_day,
    extract(isodow from trips.pickup_at)::int            as pickup_day_of_week,   -- 1 = Monday, 7 = Sunday
    extract(isodow from trips.pickup_at) in (6, 7)       as is_weekend,
    holidays.holiday_date is not null                    as is_holiday,
    holidays.holiday_name,

    -- weather in the pickup hour
    weather.temperature_c,
    weather.precipitation_mm,
    weather.rain_mm,
    weather.snowfall_cm,
    weather.wind_speed_kmh,
    weather.weather_code,
    weather_codes.weather_description,
    weather_codes.weather_category,

    -- zones
    pickup_zone.borough                                  as pickup_borough,
    pickup_zone.zone                                     as pickup_zone,
    dropoff_zone.borough                                 as dropoff_borough,
    dropoff_zone.zone                                    as dropoff_zone,

    -- code names
    payment_types.payment_type_name,
    rate_codes.ratecode_name

from trips
left join weather
    on weather.weather_hour = trips.pickup_hour
left join weather_codes
    on weather_codes.weather_code = weather.weather_code
left join zones as pickup_zone
    on pickup_zone.location_id = trips.pickup_location_id
left join zones as dropoff_zone
    on dropoff_zone.location_id = trips.dropoff_location_id
left join payment_types
    on payment_types.payment_type = trips.payment_type
left join rate_codes
    on rate_codes.ratecode_id = trips.ratecode_id
left join holidays
    on holidays.holiday_date = trips.pickup_date