-- One row per taxi zone, from the TLC lookup seed.

with zones as (

    select * from {{ ref('taxi_zone_lookup') }}

)

select
    location_id                                  as zone_id,
    zone                                         as zone_name,
    borough,
    service_zone,
    location_id in (1, 132, 138)                 as is_airport,   -- Newark, JFK, LaGuardia
    borough in ('Unknown', 'N/A')                as is_unknown    -- zones 264 and 265
from zones