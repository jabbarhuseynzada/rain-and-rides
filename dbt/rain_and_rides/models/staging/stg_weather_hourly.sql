-- One row per New York local hour. Already converted from UTC and de-duplicated in Spark.

with source as (

    select * from {{ source('raw', 'weather_hourly') }}

),

renamed as (

    select
        weather_hour,
        weather_date,
        temperature_c,
        precipitation_mm,
        rain_mm,
        snowfall_cm,
        wind_speed_kmh,
        weather_code,
        loaded_at

    from source

)

select * from renamed