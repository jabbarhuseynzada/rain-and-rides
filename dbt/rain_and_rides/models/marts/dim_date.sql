-- One row per calendar date covering the trips we hold, with day and weather context.
-- A date dimension lets a dashboard group by weekday, month or season without
-- repeating date logic in every question.

{% set first_date = "(select min(pickup_date) from " ~ ref('int_trips_enriched') ~ ")" %}
{% set last_date  = "(select max(pickup_date) from " ~ ref('int_trips_enriched') ~ ")" %}

with dates as (

    -- date_spine generates every date in a range, so days with no trips still appear
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=first_date,
        end_date="(" ~ last_date ~ " + interval '1 day')"
    ) }}

),

holidays as (

    select * from {{ ref('int_holidays_ny') }}

),

daily_weather as (

    select
        weather_date,
        round(avg(temperature_c)::numeric, 1)    as avg_temperature_c,
        round(sum(precipitation_mm)::numeric, 1) as total_precipitation_mm,
        round(sum(snowfall_cm)::numeric, 1)      as total_snowfall_cm
    from {{ ref('stg_weather_hourly') }}
    group by weather_date

)

select
    dates.date_day::date                                     as date_key,

    -- parts of the date
    extract(year   from dates.date_day)::int                 as year,
    extract(month  from dates.date_day)::int                 as month,
    extract(day    from dates.date_day)::int                 as day_of_month,
    extract(isodow from dates.date_day)::int                 as day_of_week,   -- 1 = Monday
    to_char(dates.date_day, 'Day')                           as day_name,
    to_char(dates.date_day, 'Month')                         as month_name,
    extract(quarter from dates.date_day)::int                as quarter,
    extract(week    from dates.date_day)::int                as iso_week,

    -- day type
    extract(isodow from dates.date_day) in (6, 7)            as is_weekend,
    holidays.holiday_date is not null                        as is_holiday,
    holidays.holiday_name,

    -- daily weather summary
    daily_weather.avg_temperature_c,
    daily_weather.total_precipitation_mm,
    daily_weather.total_snowfall_cm

from dates
left join holidays
    on holidays.holiday_date = dates.date_day::date
left join daily_weather
    on daily_weather.weather_date = dates.date_day::date