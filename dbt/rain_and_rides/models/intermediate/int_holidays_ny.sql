-- One row per date that is a holiday in New York City.
--
-- Decision: a holiday counts if it is nationwide OR observed in New York State (US-NY).
-- Holidays that only apply to other states are left out. When two holidays fall on the
-- same date, their names are combined, so every date appears exactly once. That matters:
-- a duplicated date would double-count every trip on that day when joined.

with holidays as (

    select * from {{ ref('stg_holidays') }}

),

new_york as (

    select *
    from holidays
    where is_nationwide
       or observed_in like '%US-NY%'

)

select
    holiday_date,
    string_agg(holiday_name, ' / ' order by holiday_name) as holiday_name,
    bool_or(is_nationwide)                                as is_nationwide
from new_york
group by holiday_date