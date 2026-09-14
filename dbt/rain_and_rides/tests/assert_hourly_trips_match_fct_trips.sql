-- A singular test: the hourly totals must add back up to the number of trips.
-- If they don't, the aggregate lost trips (for example, an hour with no weather row)
-- and every chart built on it would understate demand.

with fct as (

    select count(*) as trips from {{ ref('fct_trips') }}

),

agg as (

    select sum(trips) as trips from {{ ref('agg_hourly_demand') }}

)

select
    fct.trips   as fct_trips,
    agg.trips   as agg_trips
from fct
cross join agg
where fct.trips != agg.trips