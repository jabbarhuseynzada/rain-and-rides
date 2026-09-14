-- One row per hour: how many trips happened, and what the weather was.
-- This is the table that answers the project question, and it is small enough
-- (a few thousand rows) for a dashboard to chart instantly.

with trips as (

    select * from {{ ref('fct_trips') }}

),

by_hour as (

    select
        pickup_hour,
        count(*)                                             as trips,
        sum(passenger_count)                                 as passengers,
        round(avg(duration_min)::numeric, 2)                 as avg_duration_min,
        round(avg(trip_distance_miles)::numeric, 2)          as avg_distance_miles,
        round(avg(total_amount), 2)                          as avg_total_amount,
        round(avg(tip_pct)::numeric, 2)                      as avg_tip_pct,   -- credit card trips only
        count(*) filter (where is_airport_trip)              as airport_trips
    from trips
    group by pickup_hour

),

weather as (

    select * from {{ ref('stg_weather_hourly') }}

),

weather_codes as (

    select * from {{ ref('weather_codes') }}

),

dates as (

    select * from {{ ref('dim_date') }}

)

-- Start from the weather, not the trips: an hour with zero trips still gets a row,
-- which is exactly the kind of hour a weather analysis must not lose.
select
    weather.weather_hour                                     as hour_key,
    weather.weather_date                                     as date_key,
    extract(hour from weather.weather_hour)::int             as hour_of_day,
    dates.day_of_week,
    dates.is_weekend,
    dates.is_holiday,

    coalesce(by_hour.trips, 0)                               as trips,
    by_hour.passengers,
    by_hour.avg_duration_min,
    by_hour.avg_distance_miles,
    by_hour.avg_total_amount,
    by_hour.avg_tip_pct,
    coalesce(by_hour.airport_trips, 0)                       as airport_trips,

    weather.temperature_c,
    weather.precipitation_mm,
    weather.rain_mm,
    weather.snowfall_cm,
    weather.wind_speed_kmh,
    weather_codes.weather_category,
    {{ rain_label('weather.precipitation_mm', 'weather.snowfall_cm') }} as weather_label

from weather
left join by_hour
    on by_hour.pickup_hour = weather.weather_hour
left join weather_codes
    on weather_codes.weather_code = weather.weather_code
left join dates
    on dates.date_key = weather.weather_date